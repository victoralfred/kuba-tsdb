//! Gorilla TSDB HTTP Server
//!
//! This binary provides a complete HTTP server for the Gorilla time-series database.
//! It exposes REST endpoints for data ingestion, querying, and administration.
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
    engine::{DatabaseConfig, DatabaseStats, InMemoryTimeIndex, TimeSeriesDB, TimeSeriesDBBuilder},
    storage::LocalDiskEngine,
    types::{DataPoint, SeriesId, TagFilter, TimeRange},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::signal;
use tracing::{error, info, warn};

// =============================================================================
// Server Configuration
// =============================================================================

/// Server configuration loaded from TOML or environment
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// HTTP server address
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Maximum chunk size in bytes
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size: usize,

    /// Data retention in days (None = forever)
    #[serde(default)]
    pub retention_days: Option<u32>,

    /// Enable Prometheus metrics endpoint
    #[serde(default = "default_true")]
    pub enable_metrics: bool,

    /// Maximum points per write request
    #[serde(default = "default_max_write_points")]
    pub max_write_points: usize,
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data/tsdb")
}

fn default_max_chunk_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_true() -> bool {
    true
}

fn default_max_write_points() -> usize {
    100_000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            data_dir: default_data_dir(),
            max_chunk_size: default_max_chunk_size(),
            retention_days: None,
            enable_metrics: true,
            max_write_points: default_max_write_points(),
        }
    }
}

// =============================================================================
// Application State
// =============================================================================

/// Shared application state
struct AppState {
    db: TimeSeriesDB,
    config: ServerConfig,
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
/// 1. Explicit `series_id`
/// 2. `metric` name and optional `tags` (looks up or generates series ID)
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Explicit series ID to query
    #[serde(default)]
    series_id: Option<SeriesId>,
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
    points: Vec<QueryPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
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
}

impl From<DatabaseStats> for StatsResponse {
    fn from(stats: DatabaseStats) -> Self {
        Self {
            total_chunks: stats.total_chunks,
            total_bytes: stats.total_bytes,
            total_series: stats.total_series,
            write_ops: stats.write_ops,
            read_ops: stats.read_ops,
            compression_ratio: stats.compression_ratio,
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
// API Handlers
// =============================================================================

/// Health check endpoint
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Generate a deterministic series ID from metric name and tags
///
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
                    if let Err(e) = state
                        .db
                        .register_series(id, metric, req.tags.clone())
                        .await
                    {
                        // Log but don't fail - series might already exist
                        warn!(error = %e, metric = %metric, "Series registration (may already exist)");
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

/// Query data points
///
/// Supports querying by:
/// 1. Explicit series_id: `?series_id=123&start=0&end=1000`
/// 2. Metric + tags: `?metric=cpu.usage&tags={"host":"server1"}&start=0&end=1000`
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
                error: Some("start must be <= end".to_string()),
            }),
        );
    }

    // Determine series ID
    let series_id = match params.series_id {
        Some(id) => id,
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
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, series_id = series_id, "Query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryResponse {
                    success: false,
                    series_id: Some(series_id),
                    points: vec![],
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Register a new series
///
/// If series_id is not provided, it will be auto-generated from metric_name + tags.
async fn register_series(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeriesRequest>,
) -> impl IntoResponse {
    // Use provided series_id or auto-generate from metric + tags
    let series_id = req
        .series_id
        .unwrap_or_else(|| generate_series_id(&req.metric_name, &req.tags));

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

/// Prometheus metrics endpoint (placeholder)
async fn metrics() -> impl IntoResponse {
    // TODO: Implement proper Prometheus metrics export
    "# HELP tsdb_up Indicates if TSDB is up\n# TYPE tsdb_up gauge\ntsdb_up 1\n"
}

// =============================================================================
// Server Initialization
// =============================================================================

/// Load configuration from file or environment
fn load_config() -> ServerConfig {
    // Check environment variable first
    if let Ok(path) = std::env::var("TSDB_CONFIG") {
        match std::fs::read_to_string(&path) {
            Ok(content) => match toml::from_str(&content) {
                Ok(config) => {
                    info!(path = %path, "Loaded configuration from file");
                    return config;
                }
                Err(e) => {
                    warn!(path = %path, error = %e, "Failed to parse config file, using defaults");
                }
            },
            Err(e) => {
                warn!(path = %path, error = %e, "Failed to read config file, using defaults");
            }
        }
    }

    // Check default config file
    if let Ok(content) = std::fs::read_to_string("tsdb.toml") {
        if let Ok(config) = toml::from_str(&content) {
            info!("Loaded configuration from tsdb.toml");
            return config;
        }
    }

    info!("Using default configuration");
    ServerConfig::default()
}

/// Initialize the database
async fn init_database(config: &ServerConfig) -> Result<TimeSeriesDB, Box<dyn std::error::Error>> {
    // Create data directory
    std::fs::create_dir_all(&config.data_dir)?;

    // Create storage engine
    let storage = LocalDiskEngine::new(config.data_dir.clone())?;

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

    // Build database
    let db = TimeSeriesDBBuilder::new()
        .with_config(db_config)
        .with_compressor(compressor)
        .with_storage(storage)
        .with_index(index)
        .build()
        .await?;

    Ok(db)
}

/// Build the router with all endpoints
fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and metrics
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        // API v1
        .route("/api/v1/write", post(write_points))
        .route("/api/v1/query", get(query_points))
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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("gorilla_tsdb=info".parse()?)
                .add_directive("server=info".parse()?),
        )
        .init();

    info!("Gorilla TSDB Server starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = load_config();
    info!("Data directory: {:?}", config.data_dir);
    info!("Listen address: {}", config.listen_addr);

    // Initialize database
    info!("Initializing database...");
    let db = init_database(&config).await?;
    info!("Database initialized successfully");

    // Create application state
    let state = Arc::new(AppState {
        db,
        config: config.clone(),
    });

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
