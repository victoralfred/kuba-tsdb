//! Gorilla TSDB HTTP Server
//!
//! This binary provides a complete HTTP server for the Gorilla time-series database.
//! It exposes REST endpoints for data ingestion, querying, and administration.

// Allow manual modulo checks since is_multiple_of is unstable on stable Rust (Docker builds)
#![allow(clippy::manual_is_multiple_of)]
//!
//! # Endpoints
//!
//! ## Write
//! - `POST /api/v1/write` - Write data points
//!
//! ## Query
//! - `GET /api/v1/query` - Query data points
//!
//! ## Series Management
//! - `POST /api/v1/series` - Register a new series
//! - `GET /api/v1/series/find` - Find series by metric and tags
//!
//! ## Admin
//! - `GET /health` - Health check
//! - `GET /metrics` - Prometheus metrics
//! - `GET /api/v1/stats` - Database statistics
//!
//! # Configuration
//!
//! The server reads configuration from:
//! 1. `TSDB_CONFIG` environment variable (path to TOML file)
//! 2. `./tsdb.toml` in current directory
//! 3. Default configuration
//!
//! # Example Usage
//!
//! ```bash
//! # Start server with default config
//! ./server
//!
//! # Start with custom config
//! TSDB_CONFIG=/etc/tsdb.toml ./server
//!
//! # Write data using metric name (recommended - series ID auto-generated)
//! curl -X POST http://localhost:8080/api/v1/write \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "metric": "cpu.usage",
//!     "tags": {"host": "server1", "region": "us-east"},
//!     "points": [{"timestamp": 1700000000000, "value": 42.5}]
//!   }'
//!
//! # Write data using explicit series_id (advanced)
//! curl -X POST http://localhost:8080/api/v1/write \
//!   -H "Content-Type: application/json" \
//!   -d '{"series_id": 12345, "points": [{"timestamp": 1700000000000, "value": 42.5}]}'
//!
//! # Query data by metric name
//! curl "http://localhost:8080/api/v1/query?metric=cpu.usage&tags={\"host\":\"server1\"}&start=0&end=2000000000000"
//!
//! # Query data by series_id
//! curl "http://localhost:8080/api/v1/query?series_id=12345&start=0&end=2000000000000"
//!
//! # Register a series explicitly
//! curl -X POST http://localhost:8080/api/v1/series \
//!   -H "Content-Type: application/json" \
//!   -d '{"metric_name": "memory.bytes", "tags": {"host": "server1"}}'
//!
//! # Find series by metric name
//! curl "http://localhost:8080/api/v1/series/find?metric_name=cpu.usage"
//! ```

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use gorilla_tsdb::{
    compression::gorilla::GorillaCompressor,
    config::ApplicationConfig,
    engine::{DatabaseConfig, DatabaseStats, InMemoryTimeIndex, TimeSeriesDB, TimeSeriesDBBuilder},
    query::{
        parse_promql, parse_sql,
        subscription::{SubscriptionConfig, SubscriptionManager},
        AggregationFunction as QueryAggFunction, Query as ParsedQuery,
    },
    storage::LocalDiskEngine,
    types::{DataPoint, SeriesId, TagFilter, TimeRange},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::signal;
use tracing::{error, info, warn};

// =============================================================================
// Server Configuration
// =============================================================================

/// Server runtime configuration derived from ApplicationConfig
///
/// This is a simplified view of the configuration used by the HTTP server.
/// It's extracted from the full ApplicationConfig to provide a convenient
/// interface for server-specific settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// HTTP server address
    pub listen_addr: String,

    /// Data directory path
    pub data_dir: PathBuf,

    /// Maximum chunk size in bytes
    pub max_chunk_size: usize,

    /// Data retention in days (None = forever)
    pub retention_days: Option<u32>,

    /// Enable Prometheus metrics endpoint
    pub enable_metrics: bool,

    /// Maximum points per write request
    pub max_write_points: usize,

    /// Subscription cleanup interval in seconds (0 = disabled)
    pub subscription_cleanup_interval_secs: u64,

    /// Maximum age of stale subscriptions in seconds before cleanup
    pub subscription_max_age_secs: u64,

    /// Metrics histogram reset interval in seconds (0 = disabled)
    pub metrics_reset_interval_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::from(ApplicationConfig::default())
    }
}

impl From<ApplicationConfig> for ServerConfig {
    /// Convert from ApplicationConfig to ServerConfig
    ///
    /// Extracts relevant settings from the centralized configuration.
    fn from(app_config: ApplicationConfig) -> Self {
        Self {
            listen_addr: app_config.server.listen_addr,
            data_dir: app_config.storage.data_dir,
            max_chunk_size: app_config.storage.max_chunk_size_bytes,
            retention_days: None, // ApplicationConfig doesn't have retention yet
            enable_metrics: app_config.monitoring.prometheus_enabled,
            max_write_points: app_config.ingestion.max_batch_size,
            subscription_cleanup_interval_secs: 300, // 5 minutes default
            subscription_max_age_secs: 3600,         // 1 hour default
            metrics_reset_interval_secs: 0,          // Disabled by default
        }
    }
}

// =============================================================================
// Application State
// =============================================================================

/// Shared application state
struct AppState {
    db: TimeSeriesDB,
    /// Storage engine reference for series metadata persistence
    storage: Arc<LocalDiskEngine>,
    config: ServerConfig,
    /// Subscription manager for live query subscriptions
    subscriptions: Arc<SubscriptionManager>,
}

// =============================================================================
// API Request/Response Types
// =============================================================================

/// Write request body
///
/// Users can identify the series in two ways:
/// 1. By explicit `series_id` (for advanced use cases)
/// 2. By `metric` name and `tags` (recommended - series ID is auto-generated)
///
/// If both are provided, `series_id` takes precedence.
#[derive(Debug, Deserialize)]
struct WriteRequest {
    /// Explicit series ID (optional - use metric+tags instead for auto-generation)
    #[serde(default)]
    series_id: Option<SeriesId>,
    /// Metric name (e.g., "cpu.usage", "memory.bytes")
    #[serde(default)]
    metric: Option<String>,
    /// Tags for the series (e.g., {"host": "server1", "region": "us-east"})
    #[serde(default)]
    tags: HashMap<String, String>,
    /// Data points to write
    points: Vec<WritePoint>,
}

/// Single point in write request
#[derive(Debug, Deserialize)]
struct WritePoint {
    timestamp: i64,
    value: f64,
}

/// Write response
#[derive(Debug, Serialize)]
struct WriteResponse {
    success: bool,
    /// The series ID that was written to (useful when auto-generated)
    #[serde(skip_serializing_if = "Option::is_none")]
    series_id: Option<SeriesId>,
    points_written: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Query parameters
///
/// Supports querying by:
/// 1. Explicit `series_id` (as string, since u128 isn't supported in query strings)
/// 2. `metric` name and optional `tags` (looks up or generates series ID)
///
/// Optionally apply an aggregation function to the results:
/// - `aggregation=count` - Count of points
/// - `aggregation=sum` - Sum of all values
/// - `aggregation=min` - Minimum value
/// - `aggregation=max` - Maximum value
/// - `aggregation=avg` - Average value
/// - `aggregation=first` - First value in time range
/// - `aggregation=last` - Last value in time range
/// - `aggregation=stddev` - Standard deviation
/// - `aggregation=variance` - Variance
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Explicit series ID to query (as string)
    #[serde(default)]
    series_id: Option<String>,
    /// Metric name (alternative to series_id)
    #[serde(default)]
    metric: Option<String>,
    /// Tags to filter by (used with metric name)
    #[serde(default)]
    tags: Option<String>, // JSON-encoded tags, e.g., {"host":"server1"}
    /// Start timestamp (inclusive)
    start: i64,
    /// End timestamp (inclusive)
    end: i64,
    /// Maximum points to return (default: 10000)
    #[serde(default = "default_limit")]
    limit: usize,
    /// Optional aggregation function to apply (count, sum, min, max, avg, first, last, stddev, variance)
    #[serde(default)]
    aggregation: Option<String>,
}

fn default_limit() -> usize {
    10_000
}

/// Query response
#[derive(Debug, Serialize)]
struct QueryResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    series_id: Option<SeriesId>,
    /// Raw data points (empty if aggregation is applied)
    points: Vec<QueryPoint>,
    /// Aggregation result (only present when aggregation function is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregation: Option<AggregationResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Result of an aggregation query
#[derive(Debug, Serialize)]
struct AggregationResult {
    /// Name of the aggregation function applied
    function: String,
    /// Computed aggregation value
    value: f64,
    /// Number of points used in the aggregation
    point_count: usize,
}

/// Single point in query response
#[derive(Debug, Serialize)]
struct QueryPoint {
    timestamp: i64,
    value: f64,
}

/// Register series request
///
/// If `series_id` is not provided, it will be auto-generated from metric_name + tags.
#[derive(Debug, Deserialize)]
struct RegisterSeriesRequest {
    /// Explicit series ID (optional - auto-generated if not provided)
    #[serde(default)]
    series_id: Option<SeriesId>,
    metric_name: String,
    #[serde(default)]
    tags: HashMap<String, String>,
}

/// Find series request
#[derive(Debug, Deserialize)]
struct FindSeriesParams {
    metric_name: String,
    #[serde(default)]
    tags: Option<HashMap<String, String>>,
}

/// Find series response
#[derive(Debug, Serialize)]
struct FindSeriesResponse {
    success: bool,
    series_ids: Vec<SeriesId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Stats response
#[derive(Debug, Serialize)]
struct StatsResponse {
    total_chunks: u64,
    total_bytes: u64,
    total_series: u64,
    write_ops: u64,
    read_ops: u64,
    compression_ratio: f64,
    /// Index cache statistics for monitoring cache effectiveness
    index_cache_hits: u64,
    index_cache_misses: u64,
    index_queries_served: u64,
    /// Cache hit rate as a percentage (0.0 to 100.0)
    index_cache_hit_rate: f64,
}

impl From<DatabaseStats> for StatsResponse {
    fn from(stats: DatabaseStats) -> Self {
        // Calculate cache hit rate
        let total_cache_ops = stats.index_cache_hits + stats.index_cache_misses;
        let cache_hit_rate = if total_cache_ops > 0 {
            (stats.index_cache_hits as f64 / total_cache_ops as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_chunks: stats.total_chunks,
            total_bytes: stats.total_bytes,
            total_series: stats.total_series,
            write_ops: stats.write_ops,
            read_ops: stats.read_ops,
            compression_ratio: stats.compression_ratio,
            index_cache_hits: stats.index_cache_hits,
            index_cache_misses: stats.index_cache_misses,
            index_queries_served: stats.index_queries_served,
            index_cache_hit_rate: cache_hit_rate,
        }
    }
}

/// Health response
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

// =============================================================================
// SQL/PromQL Query Types
// =============================================================================

/// Request for SQL or PromQL query execution
///
/// The query language is auto-detected:
/// - Queries starting with SELECT or EXPLAIN are parsed as SQL
/// - All other queries are parsed as PromQL
///
/// # Examples
///
/// SQL query:
/// ```json
/// {"query": "SELECT * FROM cpu WHERE time > now() - 1h"}
/// ```
///
/// PromQL query:
/// ```json
/// {"query": "avg(cpu_usage{host=\"server1\"})"}
/// ```
#[derive(Debug, Deserialize)]
struct SqlPromqlRequest {
    /// The query string (SQL or PromQL syntax)
    #[serde(alias = "q")]
    query: String,

    /// Output format: "json" (default), "csv", or "table"
    #[serde(default = "default_format")]
    format: String,

    /// Force a specific query language: "sql", "promql", or "auto" (default)
    #[serde(default = "default_language")]
    language: String,
}

fn default_format() -> String {
    "json".to_string()
}

fn default_language() -> String {
    "auto".to_string()
}

/// Response from SQL/PromQL query execution
#[derive(Debug, Serialize)]
struct SqlPromqlResponse {
    success: bool,
    /// Detected query language
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    /// Query type (select, aggregate, downsample, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    query_type: Option<String>,
    /// Number of rows returned
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<usize>,
    /// Result data (format depends on output format)
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    /// CSV output (only if format=csv)
    #[serde(skip_serializing_if = "Option::is_none")]
    csv: Option<String>,
    /// Aggregation result (for aggregate queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregation: Option<SqlAggregationResult>,
    /// Warnings from query execution
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Aggregation result for SQL/PromQL queries
#[derive(Debug, Serialize)]
struct SqlAggregationResult {
    function: String,
    value: f64,
    point_count: usize,
}

// =============================================================================
// Query Router (Auto-Detection and Execution)
// =============================================================================

/// Query router that auto-detects query language and executes
///
/// This module bridges the gap between the HTTP API and the query engine.
/// It parses SQL/PromQL queries, extracts parameters, and uses TimeSeriesDB
/// methods to retrieve and aggregate data.
mod query_router {
    use super::*;
    use gorilla_tsdb::query::ast::{AggregateQuery, DownsampleQuery, LatestQuery, SelectQuery};

    /// Detected query language
    #[derive(Debug, Clone, Copy)]
    pub enum QueryLanguage {
        Sql,
        PromQL,
    }

    impl std::fmt::Display for QueryLanguage {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                QueryLanguage::Sql => write!(f, "sql"),
                QueryLanguage::PromQL => write!(f, "promql"),
            }
        }
    }

    /// Execute a query string against the database
    ///
    /// Auto-detects the query language and routes to appropriate parser.
    pub async fn execute_query(
        db: &TimeSeriesDB,
        query_str: &str,
        language: &str,
    ) -> Result<(QueryLanguage, String, Vec<DataPoint>, Option<(String, f64)>), String> {
        // Detect and parse query
        let (parsed, lang) = match language.to_lowercase().as_str() {
            "sql" => {
                let q = parse_sql(query_str).map_err(|e| format!("SQL parse error: {}", e))?;
                (q, QueryLanguage::Sql)
            }
            "promql" => {
                let q =
                    parse_promql(query_str).map_err(|e| format!("PromQL parse error: {}", e))?;
                (q, QueryLanguage::PromQL)
            }
            _ => {
                // Auto-detect based on query content
                let trimmed = query_str.trim().to_uppercase();
                if trimmed.starts_with("SELECT") || trimmed.starts_with("EXPLAIN") {
                    let q = parse_sql(query_str).map_err(|e| format!("SQL parse error: {}", e))?;
                    (q, QueryLanguage::Sql)
                } else {
                    let q = parse_promql(query_str)
                        .map_err(|e| format!("PromQL parse error: {}", e))?;
                    (q, QueryLanguage::PromQL)
                }
            }
        };

        // Execute based on query type
        match parsed {
            ParsedQuery::Select(q) => execute_select(db, q, lang).await,
            ParsedQuery::Aggregate(q) => execute_aggregate(db, q, lang).await,
            ParsedQuery::Downsample(q) => execute_downsample(db, q, lang).await,
            ParsedQuery::Latest(q) => execute_latest(db, q, lang).await,
            ParsedQuery::Explain(_inner) => {
                // For EXPLAIN, return the query plan description
                Ok((lang, "explain".to_string(), vec![], None))
            }
            ParsedQuery::Stream(_) => {
                // Stream queries are not supported via HTTP (requires WebSocket)
                Err(
                    "Stream queries are not supported via HTTP. Use WebSocket for streaming."
                        .to_string(),
                )
            }
        }
    }

    /// Execute a SELECT query
    async fn execute_select(
        db: &TimeSeriesDB,
        q: SelectQuery,
        lang: QueryLanguage,
    ) -> Result<(QueryLanguage, String, Vec<DataPoint>, Option<(String, f64)>), String> {
        // Get series IDs from selector
        let series_ids: Vec<SeriesId> = match q.selector.series_id {
            Some(id) => vec![id],
            None => {
                // Look up series by measurement name using the index
                if let Some(ref measurement) = q.selector.measurement {
                    db.find_series(measurement, &TagFilter::All)
                        .await
                        .map_err(|e| format!("Series lookup error: {}", e))?
                } else {
                    return Err("No series_id or measurement specified in query".to_string());
                }
            }
        };

        // Return empty if no series found
        if series_ids.is_empty() {
            return Ok((lang, "select".to_string(), vec![], None));
        }

        // Query all matching series and merge results
        // This collects data from ALL series with the same metric name
        let mut all_points: Vec<DataPoint> = Vec::new();
        for series_id in &series_ids {
            match db.query(*series_id, q.time_range).await {
                Ok(points) => all_points.extend(points),
                Err(e) => {
                    // Log but continue with other series
                    tracing::warn!("Error querying series {}: {}", series_id, e);
                }
            }
        }

        // Sort all points by timestamp for consistent ordering
        all_points.sort_by_key(|p| p.timestamp);

        // Apply limit and offset
        let mut result_points = all_points;
        if let Some(offset) = q.offset {
            result_points = result_points.into_iter().skip(offset).collect();
        }
        if let Some(limit) = q.limit {
            result_points = result_points.into_iter().take(limit).collect();
        }

        Ok((lang, "select".to_string(), result_points, None))
    }

    /// Execute an AGGREGATE query
    async fn execute_aggregate(
        db: &TimeSeriesDB,
        q: AggregateQuery,
        lang: QueryLanguage,
    ) -> Result<(QueryLanguage, String, Vec<DataPoint>, Option<(String, f64)>), String> {
        // Get series IDs
        let series_ids: Vec<SeriesId> = match q.selector.series_id {
            Some(id) => vec![id],
            None => {
                if let Some(ref measurement) = q.selector.measurement {
                    db.find_series(measurement, &TagFilter::All)
                        .await
                        .map_err(|e| format!("Series lookup error: {}", e))?
                } else {
                    return Err("No series_id or measurement specified".to_string());
                }
            }
        };

        if series_ids.is_empty() {
            return Ok((
                lang,
                "aggregate".to_string(),
                vec![],
                Some((
                    format!("{:?}", q.aggregation.function).to_lowercase(),
                    f64::NAN,
                )),
            ));
        }

        // Query all matching series and merge results for aggregation
        let mut all_points: Vec<DataPoint> = Vec::new();
        for series_id in &series_ids {
            match db.query(*series_id, q.time_range).await {
                Ok(points) => all_points.extend(points),
                Err(e) => {
                    tracing::warn!("Error querying series {}: {}", series_id, e);
                }
            }
        }

        if all_points.is_empty() {
            return Ok((
                lang,
                "aggregate".to_string(),
                vec![],
                Some((
                    format!("{:?}", q.aggregation.function).to_lowercase(),
                    f64::NAN,
                )),
            ));
        }

        // Sort for consistent aggregation order
        all_points.sort_by_key(|p| p.timestamp);

        // Apply aggregation function across ALL series data
        let (func_name, value) = compute_aggregation_from_ast(&q.aggregation.function, &all_points);

        Ok((
            lang,
            "aggregate".to_string(),
            vec![],
            Some((func_name, value)),
        ))
    }

    /// Execute a DOWNSAMPLE query
    async fn execute_downsample(
        db: &TimeSeriesDB,
        q: DownsampleQuery,
        lang: QueryLanguage,
    ) -> Result<(QueryLanguage, String, Vec<DataPoint>, Option<(String, f64)>), String> {
        // Get series IDs
        let series_ids: Vec<SeriesId> = match q.selector.series_id {
            Some(id) => vec![id],
            None => {
                if let Some(ref measurement) = q.selector.measurement {
                    db.find_series(measurement, &TagFilter::All)
                        .await
                        .map_err(|e| format!("Series lookup error: {}", e))?
                } else {
                    return Err("No series_id or measurement specified".to_string());
                }
            }
        };

        if series_ids.is_empty() {
            return Ok((lang, "downsample".to_string(), vec![], None));
        }

        // Query all matching series and merge results
        let mut all_points: Vec<DataPoint> = Vec::new();
        for series_id in &series_ids {
            match db.query(*series_id, q.time_range).await {
                Ok(points) => all_points.extend(points),
                Err(e) => {
                    tracing::warn!("Error querying series {}: {}", series_id, e);
                }
            }
        }

        // Sort by timestamp before downsampling
        all_points.sort_by_key(|p| p.timestamp);

        // Apply downsampling (simplified - just take every Nth point)
        let step = (all_points.len() / q.target_points).max(1);
        let downsampled: Vec<DataPoint> = all_points
            .into_iter()
            .step_by(step)
            .take(q.target_points)
            .collect();

        Ok((lang, "downsample".to_string(), downsampled, None))
    }

    /// Execute a LATEST query
    async fn execute_latest(
        db: &TimeSeriesDB,
        q: LatestQuery,
        lang: QueryLanguage,
    ) -> Result<(QueryLanguage, String, Vec<DataPoint>, Option<(String, f64)>), String> {
        // Get series IDs
        let series_ids: Vec<SeriesId> = match q.selector.series_id {
            Some(id) => vec![id],
            None => {
                if let Some(ref measurement) = q.selector.measurement {
                    db.find_series(measurement, &TagFilter::All)
                        .await
                        .map_err(|e| format!("Series lookup error: {}", e))?
                } else {
                    return Err("No series_id or measurement specified".to_string());
                }
            }
        };

        if series_ids.is_empty() {
            return Ok((lang, "latest".to_string(), vec![], None));
        }

        // Query all matching series and merge results
        let now = chrono::Utc::now().timestamp_millis();
        let time_range = TimeRange::new_unchecked(0, now);

        let mut all_points: Vec<DataPoint> = Vec::new();
        for series_id in &series_ids {
            match db.query(*series_id, time_range).await {
                Ok(points) => all_points.extend(points),
                Err(e) => {
                    tracing::warn!("Error querying series {}: {}", series_id, e);
                }
            }
        }

        // Sort by timestamp to get correct ordering across all series
        all_points.sort_by_key(|p| p.timestamp);

        // Take last N points
        let latest: Vec<DataPoint> = all_points
            .into_iter()
            .rev()
            .take(q.count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        Ok((lang, "latest".to_string(), latest, None))
    }

    /// Compute aggregation from AST function enum
    fn compute_aggregation_from_ast(
        func: &QueryAggFunction,
        points: &[DataPoint],
    ) -> (String, f64) {
        let func_name = format!("{:?}", func).to_lowercase();

        if points.is_empty() {
            return (func_name, f64::NAN);
        }

        let value = match func {
            QueryAggFunction::Count => points.len() as f64,
            QueryAggFunction::Sum => {
                // Kahan summation
                let mut sum = 0.0f64;
                let mut c = 0.0f64;
                for p in points {
                    let y = p.value - c;
                    let t = sum + y;
                    c = (t - sum) - y;
                    sum = t;
                }
                sum
            }
            QueryAggFunction::Min => points.iter().map(|p| p.value).fold(f64::INFINITY, f64::min),
            QueryAggFunction::Max => points
                .iter()
                .map(|p| p.value)
                .fold(f64::NEG_INFINITY, f64::max),
            QueryAggFunction::Avg => {
                // Welford's algorithm
                let mut mean = 0.0f64;
                let mut count = 0u64;
                for p in points {
                    count += 1;
                    let delta = p.value - mean;
                    mean += delta / count as f64;
                }
                mean
            }
            QueryAggFunction::First => points.first().map(|p| p.value).unwrap_or(f64::NAN),
            QueryAggFunction::Last => points.last().map(|p| p.value).unwrap_or(f64::NAN),
            QueryAggFunction::StdDev => {
                if points.len() < 2 {
                    return (func_name, 0.0);
                }
                let mut mean = 0.0f64;
                let mut m2 = 0.0f64;
                let mut count = 0u64;
                for p in points {
                    count += 1;
                    let delta = p.value - mean;
                    mean += delta / count as f64;
                    let delta2 = p.value - mean;
                    m2 += delta * delta2;
                }
                (m2 / (count - 1) as f64).sqrt()
            }
            QueryAggFunction::Variance => {
                if points.len() < 2 {
                    return (func_name, 0.0);
                }
                let mut mean = 0.0f64;
                let mut m2 = 0.0f64;
                let mut count = 0u64;
                for p in points {
                    count += 1;
                    let delta = p.value - mean;
                    mean += delta / count as f64;
                    let delta2 = p.value - mean;
                    m2 += delta * delta2;
                }
                m2 / (count - 1) as f64
            }
            QueryAggFunction::Median => {
                let mut values: Vec<f64> = points.iter().map(|p| p.value).collect();
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = values.len() / 2;
                if values.len() % 2 == 0 {
                    (values[mid - 1] + values[mid]) / 2.0
                } else {
                    values[mid]
                }
            }
            // For rate/increase/delta, we need time-based calculations
            QueryAggFunction::Rate => {
                if points.len() < 2 {
                    return (func_name, 0.0);
                }
                let first = points.first().unwrap();
                let last = points.last().unwrap();
                let value_diff = last.value - first.value;
                let time_diff = (last.timestamp - first.timestamp) as f64 / 1000.0; // seconds
                if time_diff > 0.0 {
                    value_diff / time_diff
                } else {
                    0.0
                }
            }
            QueryAggFunction::Increase => {
                if points.len() < 2 {
                    return (func_name, 0.0);
                }
                let first = points.first().unwrap();
                let last = points.last().unwrap();
                (last.value - first.value).max(0.0) // monotonic increase
            }
            QueryAggFunction::Delta => {
                if points.len() < 2 {
                    return (func_name, 0.0);
                }
                let first = points.first().unwrap();
                let last = points.last().unwrap();
                last.value - first.value
            }
            _ => {
                // Fallback for unsupported functions
                f64::NAN
            }
        };

        (func_name, value)
    }
}

// =============================================================================
// API Handlers
// =============================================================================

/// Health check endpoint
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Generate a deterministic series ID from metric name and tags.
/// This creates a stable hash so the same metric+tags always maps to the same ID.
fn generate_series_id(metric: &str, tags: &HashMap<String, String>) -> SeriesId {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};

    // Use BTreeMap for consistent ordering
    let sorted_tags: BTreeMap<_, _> = tags.iter().collect();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric.hash(&mut hasher);
    for (k, v) in sorted_tags {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }

    // Use lower 64 bits as series ID (SeriesId is u128)
    hasher.finish() as SeriesId
}

/// Write data points
///
/// Supports two modes:
/// 1. Explicit series_id: `{"series_id": 123, "points": [...]}`
/// 2. Metric + tags (auto-generates ID): `{"metric": "cpu.usage", "tags": {"host": "server1"}, "points": [...]}`
async fn write_points(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WriteRequest>,
) -> impl IntoResponse {
    // Validate request
    if req.points.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                series_id: None,
                points_written: 0,
                chunk_id: None,
                error: Some("No points provided".to_string()),
            }),
        );
    }

    if req.points.len() > state.config.max_write_points {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                series_id: None,
                points_written: 0,
                chunk_id: None,
                error: Some(format!(
                    "Too many points: {} exceeds maximum {}",
                    req.points.len(),
                    state.config.max_write_points
                )),
            }),
        );
    }

    // Determine series ID: explicit or auto-generated from metric+tags
    let series_id = match req.series_id {
        Some(id) => id,
        None => {
            // Must have metric name for auto-generation
            match &req.metric {
                Some(metric) => {
                    let id = generate_series_id(metric, &req.tags);

                    // Auto-register the series if metric name provided
                    // First, persist to storage engine for recovery on restart
                    if let Err(e) = state
                        .storage
                        .register_series_metadata(
                            id,
                            metric,
                            req.tags.clone(),
                            state.config.retention_days,
                        )
                        .await
                    {
                        // Log but don't fail - series might already exist
                        warn!(error = %e, metric = %metric, "Series metadata persistence (may already exist)");
                    }

                    // Then register with in-memory index for query lookups
                    if let Err(e) = state.db.register_series(id, metric, req.tags.clone()).await {
                        // Log but don't fail - series might already exist
                        warn!(error = %e, metric = %metric, "Series index registration (may already exist)");
                    }

                    id
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(WriteResponse {
                            success: false,
                            series_id: None,
                            points_written: 0,
                            chunk_id: None,
                            error: Some(
                                "Must provide either 'series_id' or 'metric' name".to_string(),
                            ),
                        }),
                    );
                }
            }
        }
    };

    // Convert to DataPoints
    let points: Vec<DataPoint> = req
        .points
        .iter()
        .map(|p| DataPoint::new(series_id, p.timestamp, p.value))
        .collect();

    // Write to database
    match state.db.write(series_id, points.clone()).await {
        Ok(chunk_id) => (
            StatusCode::OK,
            Json(WriteResponse {
                success: true,
                series_id: Some(series_id),
                points_written: points.len(),
                chunk_id: Some(chunk_id.to_string()),
                error: None,
            }),
        ),
        Err(e) => {
            error!(error = %e, series_id = series_id, "Write failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WriteResponse {
                    success: false,
                    series_id: Some(series_id),
                    points_written: 0,
                    chunk_id: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Compute an aggregation over a slice of values
///
/// Implements the following aggregation functions using numerically stable algorithms:
/// - count: Number of points
/// - sum: Sum of all values (using Kahan summation)
/// - min: Minimum value
/// - max: Maximum value
/// - avg: Mean value (using Welford's algorithm)
/// - first: First value in the series
/// - last: Last value in the series
/// - stddev: Sample standard deviation
/// - variance: Sample variance
fn compute_aggregation(function: &str, points: &[DataPoint]) -> Option<f64> {
    if points.is_empty() {
        return Some(f64::NAN);
    }

    match function.to_lowercase().as_str() {
        "count" => Some(points.len() as f64),
        "sum" => {
            // Kahan summation for numerical accuracy
            let mut sum = 0.0f64;
            let mut compensation = 0.0f64;
            for p in points {
                let y = p.value - compensation;
                let t = sum + y;
                compensation = (t - sum) - y;
                sum = t;
            }
            Some(sum)
        }
        "min" => points
            .iter()
            .map(|p| p.value)
            .fold(None, |min, v| Some(min.map_or(v, |m: f64| m.min(v)))),
        "max" => points
            .iter()
            .map(|p| p.value)
            .fold(None, |max, v| Some(max.map_or(v, |m: f64| m.max(v)))),
        "avg" | "mean" => {
            // Welford's algorithm for numerical stability
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
            }
            Some(mean)
        }
        "first" => Some(points[0].value),
        "last" => Some(points[points.len() - 1].value),
        "stddev" => {
            // Welford's algorithm for sample standard deviation
            if points.len() < 2 {
                return Some(0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            let variance = m2 / (count - 1) as f64;
            Some(variance.sqrt())
        }
        "variance" => {
            // Welford's algorithm for sample variance
            if points.len() < 2 {
                return Some(0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            Some(m2 / (count - 1) as f64)
        }
        _ => None, // Unknown aggregation function
    }
}

/// Query data points
///
/// Supports querying by:
/// 1. Explicit series_id: `?series_id=123&start=0&end=1000`
/// 2. Metric + tags: `?metric=cpu.usage&tags={"host":"server1"}&start=0&end=1000`
///
/// Optional aggregation: `?aggregation=sum` to apply an aggregation function
async fn query_points(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    // Validate time range
    if params.start > params.end {
        return (
            StatusCode::BAD_REQUEST,
            Json(QueryResponse {
                success: false,
                series_id: None,
                points: vec![],
                aggregation: None,
                error: Some("start must be <= end".to_string()),
            }),
        );
    }

    // Validate aggregation function if provided
    let valid_aggregations = [
        "count", "sum", "min", "max", "avg", "mean", "first", "last", "stddev", "variance",
    ];
    if let Some(ref agg) = params.aggregation {
        if !valid_aggregations.contains(&agg.to_lowercase().as_str()) {
            return (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse {
                    success: false,
                    series_id: None,
                    points: vec![],
                    aggregation: None,
                    error: Some(format!(
                        "Invalid aggregation '{}'. Valid options: {}",
                        agg,
                        valid_aggregations.join(", ")
                    )),
                }),
            );
        }
    }

    // Determine series ID
    let series_id = match &params.series_id {
        Some(id_str) => {
            // Parse series_id from string
            match id_str.parse::<SeriesId>() {
                Ok(id) => id,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(QueryResponse {
                            success: false,
                            series_id: None,
                            points: vec![],
                            aggregation: None,
                            error: Some(format!("Invalid series_id: {}", id_str)),
                        }),
                    );
                }
            }
        }
        None => {
            // Try to derive from metric + tags
            match &params.metric {
                Some(metric) => {
                    // Parse tags if provided
                    let tags: HashMap<String, String> = match &params.tags {
                        Some(tags_str) => serde_json::from_str(tags_str).unwrap_or_default(),
                        None => HashMap::new(),
                    };
                    generate_series_id(metric, &tags)
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(QueryResponse {
                            success: false,
                            series_id: None,
                            points: vec![],
                            aggregation: None,
                            error: Some(
                                "Must provide either 'series_id' or 'metric' name".to_string(),
                            ),
                        }),
                    );
                }
            }
        }
    };

    let time_range = TimeRange::new_unchecked(params.start, params.end);

    // Query database
    match state.db.query(series_id, time_range).await {
        Ok(points) => {
            // Check if aggregation is requested
            if let Some(ref agg_func) = params.aggregation {
                // Compute aggregation over all points (not limited)
                match compute_aggregation(agg_func, &points) {
                    Some(value) => (
                        StatusCode::OK,
                        Json(QueryResponse {
                            success: true,
                            series_id: Some(series_id),
                            points: vec![], // Empty for aggregation queries
                            aggregation: Some(AggregationResult {
                                function: agg_func.to_lowercase(),
                                value,
                                point_count: points.len(),
                            }),
                            error: None,
                        }),
                    ),
                    None => (
                        StatusCode::BAD_REQUEST,
                        Json(QueryResponse {
                            success: false,
                            series_id: Some(series_id),
                            points: vec![],
                            aggregation: None,
                            error: Some(format!("Unknown aggregation function: {}", agg_func)),
                        }),
                    ),
                }
            } else {
                // Return raw points (with limit)
                let response_points: Vec<QueryPoint> = points
                    .into_iter()
                    .take(params.limit)
                    .map(|p| QueryPoint {
                        timestamp: p.timestamp,
                        value: p.value,
                    })
                    .collect();

                (
                    StatusCode::OK,
                    Json(QueryResponse {
                        success: true,
                        series_id: Some(series_id),
                        points: response_points,
                        aggregation: None,
                        error: None,
                    }),
                )
            }
        }
        Err(e) => {
            error!(error = %e, series_id = series_id, "Query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryResponse {
                    success: false,
                    series_id: Some(series_id),
                    points: vec![],
                    aggregation: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Register a new series
///
/// If series_id is not provided, it will be auto-generated from metric_name + tags.
/// The series metadata is persisted to disk so it survives server restarts.
async fn register_series(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeriesRequest>,
) -> impl IntoResponse {
    // Use provided series_id or auto-generate from metric + tags
    let series_id = req
        .series_id
        .unwrap_or_else(|| generate_series_id(&req.metric_name, &req.tags));

    // First, persist series metadata to storage for recovery on restart
    if let Err(e) = state
        .storage
        .register_series_metadata(
            series_id,
            &req.metric_name,
            req.tags.clone(),
            state.config.retention_days,
        )
        .await
    {
        warn!(error = %e, metric_name = %req.metric_name, "Series metadata persistence (may already exist)");
    }

    // Then register with in-memory index for query lookups
    match state
        .db
        .register_series(series_id, &req.metric_name, req.tags)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "series_id": series_id
            })),
        ),
        Err(e) => {
            error!(error = %e, series_id = series_id, "Register series failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "success": false,
                    "series_id": series_id,
                    "error": e.to_string()
                })),
            )
        }
    }
}

/// Find series by metric name and tags
async fn find_series(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FindSeriesParams>,
) -> impl IntoResponse {
    let tag_filter = match params.tags {
        Some(tags) if !tags.is_empty() => TagFilter::Exact(tags),
        _ => TagFilter::All,
    };

    match state.db.find_series(&params.metric_name, &tag_filter).await {
        Ok(series_ids) => (
            StatusCode::OK,
            Json(FindSeriesResponse {
                success: true,
                series_ids,
                error: None,
            }),
        ),
        Err(e) => {
            error!(error = %e, metric_name = %params.metric_name, "Find series failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FindSeriesResponse {
                    success: false,
                    series_ids: vec![],
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Get database statistics
async fn get_stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    Json(state.db.stats().into())
}

/// Prometheus metrics endpoint
///
/// Exports metrics in Prometheus text format for scraping by Prometheus server.
/// Includes storage, compression, index cache, subscription, and operational metrics.
async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.db.stats();
    let sub_stats = state.subscriptions.stats();

    // Calculate cache hit rate
    let total_cache_ops = stats.index_cache_hits + stats.index_cache_misses;
    let cache_hit_rate = if total_cache_ops > 0 {
        stats.index_cache_hits as f64 / total_cache_ops as f64
    } else {
        0.0
    };

    format!(
        "# HELP tsdb_up Indicates if TSDB is up\n\
         # TYPE tsdb_up gauge\n\
         tsdb_up 1\n\
         \n\
         # HELP tsdb_chunks_total Total number of chunks stored\n\
         # TYPE tsdb_chunks_total gauge\n\
         tsdb_chunks_total {}\n\
         \n\
         # HELP tsdb_bytes_total Total bytes stored\n\
         # TYPE tsdb_bytes_total gauge\n\
         tsdb_bytes_total {}\n\
         \n\
         # HELP tsdb_series_total Total number of time series\n\
         # TYPE tsdb_series_total gauge\n\
         tsdb_series_total {}\n\
         \n\
         # HELP tsdb_write_ops_total Total write operations\n\
         # TYPE tsdb_write_ops_total counter\n\
         tsdb_write_ops_total {}\n\
         \n\
         # HELP tsdb_read_ops_total Total read operations\n\
         # TYPE tsdb_read_ops_total counter\n\
         tsdb_read_ops_total {}\n\
         \n\
         # HELP tsdb_compression_ratio Average compression ratio\n\
         # TYPE tsdb_compression_ratio gauge\n\
         tsdb_compression_ratio {:.4}\n\
         \n\
         # HELP tsdb_index_cache_hits_total Index cache hits\n\
         # TYPE tsdb_index_cache_hits_total counter\n\
         tsdb_index_cache_hits_total {}\n\
         \n\
         # HELP tsdb_index_cache_misses_total Index cache misses\n\
         # TYPE tsdb_index_cache_misses_total counter\n\
         tsdb_index_cache_misses_total {}\n\
         \n\
         # HELP tsdb_index_queries_total Total index queries served\n\
         # TYPE tsdb_index_queries_total counter\n\
         tsdb_index_queries_total {}\n\
         \n\
         # HELP tsdb_index_cache_hit_rate Index cache hit rate (0.0 to 1.0)\n\
         # TYPE tsdb_index_cache_hit_rate gauge\n\
         tsdb_index_cache_hit_rate {:.4}\n\
         \n\
         # HELP tsdb_subscriptions_created_total Total subscriptions created\n\
         # TYPE tsdb_subscriptions_created_total counter\n\
         tsdb_subscriptions_created_total {}\n\
         \n\
         # HELP tsdb_subscription_updates_sent_total Total updates sent to subscribers\n\
         # TYPE tsdb_subscription_updates_sent_total counter\n\
         tsdb_subscription_updates_sent_total {}\n\
         \n\
         # HELP tsdb_subscription_updates_dropped_total Updates dropped (no subscribers)\n\
         # TYPE tsdb_subscription_updates_dropped_total counter\n\
         tsdb_subscription_updates_dropped_total {}\n\
         \n\
         # HELP tsdb_active_subscriptions Active series with subscriptions\n\
         # TYPE tsdb_active_subscriptions gauge\n\
         tsdb_active_subscriptions {}\n",
        stats.total_chunks,
        stats.total_bytes,
        stats.total_series,
        stats.write_ops,
        stats.read_ops,
        stats.compression_ratio,
        stats.index_cache_hits,
        stats.index_cache_misses,
        stats.index_queries_served,
        cache_hit_rate,
        sub_stats.subscriptions_created,
        sub_stats.updates_sent,
        sub_stats.updates_dropped,
        sub_stats.active_series,
    )
}

// =============================================================================
// Server Initialization
// =============================================================================

/// Load configuration from file or environment, returning both ServerConfig and ApplicationConfig
///
/// This variant is used at startup before logging is initialized, so we can use
/// the log_level from the config to set up tracing.
///
/// Configuration is loaded in the following priority order:
/// 1. GORILLA_CONFIG or TSDB_CONFIG environment variable (path to TOML file)
/// 2. application.toml in current directory
/// 3. tsdb.toml in current directory (legacy compatibility)
/// 4. Built-in defaults
///
/// Environment variables (GORILLA_*) are applied as overrides after file loading.
fn load_config_with_app() -> (ServerConfig, ApplicationConfig) {
    // Check environment variables for config file path
    let config_path = std::env::var("GORILLA_CONFIG")
        .or_else(|_| std::env::var("TSDB_CONFIG"))
        .ok();

    if let Some(path) = config_path {
        match ApplicationConfig::load(&path) {
            Ok(config) => {
                // Note: Can't log here - tracing not initialized yet
                eprintln!("[config] Loaded configuration from: {}", path);
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            }
            Err(e) => {
                eprintln!(
                    "[config] Failed to load config from {}: {}. Trying defaults.",
                    path, e
                );
            }
        }
    }

    // Try application.toml (new default)
    let app_toml_path = std::path::Path::new("application.toml");
    if app_toml_path.exists() {
        match ApplicationConfig::load("application.toml") {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from application.toml");
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            }
            Err(e) => {
                eprintln!(
                    "[config] Failed to parse application.toml: {}. Trying tsdb.toml.",
                    e
                );
            }
        }
    } else {
        eprintln!(
            "[config] application.toml not found at: {:?}",
            app_toml_path
                .canonicalize()
                .unwrap_or_else(|_| app_toml_path.to_path_buf())
        );
    }

    // Try tsdb.toml (legacy compatibility)
    let tsdb_toml_path = std::path::Path::new("tsdb.toml");
    if tsdb_toml_path.exists() {
        match ApplicationConfig::load("tsdb.toml") {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from tsdb.toml (legacy)");
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            }
            Err(e) => {
                eprintln!("[config] Failed to parse tsdb.toml: {}. Using defaults.", e);
            }
        }
    }

    // Use defaults with environment variable overrides
    eprintln!("[config] Using default configuration");
    let mut app_config = ApplicationConfig::default();
    if let Err(e) = app_config.apply_env_overrides() {
        eprintln!("[config] Failed to apply environment overrides: {}", e);
    }
    let server_config = ServerConfig::from(app_config.clone());
    (server_config, app_config)
}

/// Load configuration from file or environment
///
/// Simplified version that only returns ServerConfig (for backward compatibility).
#[allow(dead_code)]
fn load_config() -> ServerConfig {
    let (server_config, _) = load_config_with_app();
    server_config
}

// =============================================================================
// SQL/PromQL Query Handler
// =============================================================================

/// Execute SQL or PromQL query
///
/// This endpoint accepts queries in either SQL or PromQL format and auto-detects
/// the language based on the query content:
/// - Queries starting with SELECT or EXPLAIN are parsed as SQL
/// - All other queries are parsed as PromQL
///
/// # Example Usage
///
/// ```bash
/// # SQL query
/// curl -X POST http://localhost:8080/api/v1/query/sql \
///   -H "Content-Type: application/json" \
///   -d '{"query": "SELECT * FROM cpu WHERE time >= 0"}'
///
/// # PromQL query
/// curl -X POST http://localhost:8080/api/v1/query/sql \
///   -H "Content-Type: application/json" \
///   -d '{"query": "avg(cpu_usage)"}'
///
/// # Force specific language
/// curl -X POST http://localhost:8080/api/v1/query/sql \
///   -H "Content-Type: application/json" \
///   -d '{"query": "cpu_usage[5m]", "language": "promql"}'
/// ```
async fn execute_sql_promql_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlPromqlRequest>,
) -> impl IntoResponse {
    info!(query = %req.query, language = %req.language, "Executing SQL/PromQL query");

    // Execute query using the query router
    match query_router::execute_query(&state.db, &req.query, &req.language).await {
        Ok((language, query_type, points, aggregation)) => {
            // Format results based on requested format
            let (data, csv_output) = match req.format.to_lowercase().as_str() {
                "csv" => {
                    // Generate CSV output
                    let mut csv = String::from("timestamp,value\n");
                    for p in &points {
                        csv.push_str(&format!("{},{}\n", p.timestamp, p.value));
                    }
                    (None, Some(csv))
                }
                _ => {
                    // JSON output (default)
                    let json_points: Vec<serde_json::Value> = points
                        .iter()
                        .map(|p| {
                            serde_json::json!({
                                "timestamp": p.timestamp,
                                "value": p.value
                            })
                        })
                        .collect();
                    (Some(serde_json::json!(json_points)), None)
                }
            };

            let agg_result = aggregation.map(|(func, value)| SqlAggregationResult {
                function: func,
                value,
                point_count: points.len(),
            });

            (
                StatusCode::OK,
                Json(SqlPromqlResponse {
                    success: true,
                    language: Some(language.to_string()),
                    query_type: Some(query_type),
                    row_count: Some(points.len()),
                    data,
                    csv: csv_output,
                    aggregation: agg_result,
                    warnings: vec![],
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, "Query execution failed");
            (
                StatusCode::BAD_REQUEST,
                Json(SqlPromqlResponse {
                    success: false,
                    language: None,
                    query_type: None,
                    row_count: None,
                    data: None,
                    csv: None,
                    aggregation: None,
                    warnings: vec![],
                    error: Some(e),
                }),
            )
        }
    }
}

/// Initialize the database
///
/// Returns a tuple of (TimeSeriesDB, `Arc<LocalDiskEngine>`) so that the server
/// can persist series metadata directly to the storage engine.
async fn init_database(
    config: &ServerConfig,
) -> Result<(TimeSeriesDB, Arc<LocalDiskEngine>), Box<dyn std::error::Error>> {
    // Create data directory
    std::fs::create_dir_all(&config.data_dir)?;

    // Create storage engine and wrap in Arc for shared ownership
    let storage = Arc::new(LocalDiskEngine::new(config.data_dir.clone())?);

    // Load the index from disk BEFORE getting series IDs
    // This populates LocalDiskEngine's internal chunk index AND loads series metadata
    storage.load_index().await?;

    // Get all series IDs from storage AFTER loading the index
    // This allows us to rebuild the TimeIndex from persistent storage
    let series_ids = storage.get_all_series_ids();
    let series_count = series_ids.len();
    if series_count > 0 {
        info!("Found {} series on disk to index", series_count);
    }

    // Get series metadata from storage (metric_name, tags, etc.)
    // This was loaded from series_metadata.json during load_index()
    let series_metadata = storage.get_all_series_metadata();
    let metadata_count = series_metadata.len();
    if metadata_count > 0 {
        info!(
            "Loaded {} series metadata entries from disk",
            metadata_count
        );
    }

    // Create in-memory index (production would use Redis)
    let index = InMemoryTimeIndex::new();

    // Create compressor
    let compressor = GorillaCompressor::new();

    // Build database config
    let db_config = DatabaseConfig {
        data_dir: config.data_dir.clone(),
        redis_url: None,
        max_chunk_size: config.max_chunk_size,
        retention_days: config.retention_days,
        custom_options: HashMap::new(),
    };

    // Build database - use with_storage_arc to share the Arc with AppState
    // Coerce Arc<LocalDiskEngine> to Arc<dyn StorageEngine + Send + Sync>
    let storage_dyn: Arc<dyn gorilla_tsdb::engine::traits::StorageEngine + Send + Sync> =
        storage.clone();
    let db = TimeSeriesDBBuilder::new()
        .with_config(db_config)
        .with_compressor(compressor)
        .with_storage_arc(storage_dyn)
        .with_index(index)
        .build()
        .await?;

    // Rebuild index from storage if there are existing series
    // This ensures data persisted on disk is queryable after server restart
    if !series_ids.is_empty() {
        let chunks_indexed = db.rebuild_index_for_series(&series_ids).await?;
        info!(
            "Rebuilt chunk index: {} chunks across {} series",
            chunks_indexed, series_count
        );
    }

    // Restore series metadata to the in-memory index
    // This is critical for metric-based queries (e.g., SELECT * FROM temperature)
    for (series_id, metadata) in series_metadata {
        if let Err(e) = db
            .register_series(series_id, &metadata.metric_name, metadata.tags.clone())
            .await
        {
            warn!(
                series_id = series_id,
                metric_name = %metadata.metric_name,
                error = %e,
                "Failed to restore series metadata to index"
            );
        }
    }
    if metadata_count > 0 {
        info!("Restored {} series to in-memory index", metadata_count);
    }

    Ok((db, storage))
}

/// Build the router with all endpoints
fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and metrics
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        // API v1 - REST query (URL params)
        .route("/api/v1/write", post(write_points))
        .route("/api/v1/query", get(query_points))
        // API v1 - SQL/PromQL query (POST with query string)
        .route("/api/v1/query/sql", post(execute_sql_promql_query))
        // API v1 - Series management
        .route("/api/v1/series", post(register_series))
        .route("/api/v1/series/find", get(find_series))
        .route("/api/v1/stats", get(get_stats))
        .with_state(state)
}

/// Graceful shutdown handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}

// =============================================================================
// Main Entry Point
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration FIRST (before tracing, so we can use log_level from config)
    let (config, app_config) = load_config_with_app();

    // Initialize tracing with log level from config
    let log_level = app_config.server.log_level.clone();
    eprintln!("[config] Log level from config: {}", log_level);

    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive(format!("gorilla_tsdb={}", log_level).parse()?)
        .add_directive(format!("server={}", log_level).parse()?);

    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("Gorilla TSDB Server starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("Log level: {}", log_level);
    info!("Data directory: {:?}", config.data_dir);
    info!("Listen address: {}", config.listen_addr);

    // Initialize database
    info!("Initializing database...");
    let (db, storage) = init_database(&config).await?;
    info!("Database initialized successfully");

    // Create subscription manager
    let subscriptions = Arc::new(SubscriptionManager::new(SubscriptionConfig::default()));

    // Create application state
    let state = Arc::new(AppState {
        db,
        storage,
        config: config.clone(),
        subscriptions: subscriptions.clone(),
    });

    // Spawn subscription cleanup task if enabled
    if config.subscription_cleanup_interval_secs > 0 {
        let cleanup_subscriptions = subscriptions.clone();
        let cleanup_interval = Duration::from_secs(config.subscription_cleanup_interval_secs);
        let max_age = Duration::from_secs(config.subscription_max_age_secs);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cleanup_interval).await;
                let removed = cleanup_subscriptions.cleanup_stale_with_age(max_age);
                if removed > 0 {
                    info!("Cleaned up {} stale subscriptions", removed);
                }
            }
        });
        info!(
            "Subscription cleanup task started (interval: {}s, max_age: {}s)",
            config.subscription_cleanup_interval_secs, config.subscription_max_age_secs
        );
    }

    // Build router
    let app = build_router(state);

    // Parse listen address
    let addr: SocketAddr = config.listen_addr.parse()?;
    info!("Starting HTTP server on {}", addr);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shutdown complete");
    Ok(())
}
