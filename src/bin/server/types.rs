//! Request and Response Types for the Kuba TSDB HTTP Server
//!
//! This module contains all serialization/deserialization types used by the HTTP API.

use kuba_tsdb::types::SeriesId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Write API Types
// =============================================================================

/// Write request body
///
/// Users can identify the series in two ways:
/// 1. By explicit `series_id` (for advanced use cases)
/// 2. By `metric` name and `tags` (recommended - series ID is auto-generated)
///
/// If both are provided, `series_id` takes precedence.
#[derive(Debug, Deserialize)]
pub struct WriteRequest {
    /// Explicit series ID (optional - use metric+tags instead for auto-generation)
    #[serde(default)]
    pub series_id: Option<SeriesId>,
    /// Metric name (e.g., "cpu.usage", "memory.bytes")
    #[serde(default)]
    pub metric: Option<String>,
    /// Tags for the series (e.g., {"host": "server1", "region": "us-east"})
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Data points to write
    pub points: Vec<WritePoint>,
}

/// Single point in write request
///
/// Both `timestamp` and `value` are required fields.
/// The value must be a valid JSON number (not null or string).
#[derive(Debug, Deserialize)]
pub struct WritePoint {
    /// Unix timestamp in milliseconds
    pub timestamp: i64,
    /// Numeric value (required, must be a valid number)
    pub value: f64,
}

/// Write response
#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub success: bool,
    /// The series ID that was written to (useful when auto-generated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_id: Option<SeriesId>,
    pub points_written: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// =============================================================================
// Query API Types
// =============================================================================

/// Query parameters for REST API
///
/// Supports querying by:
/// 1. Explicit `series_id` (as string, since u128 isn't supported in query strings)
/// 2. `metric` name and optional `tags` (looks up or generates series ID)
///
/// Optionally apply an aggregation function to the results.
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    /// Explicit series ID to query (as string)
    #[serde(default)]
    pub series_id: Option<String>,
    /// Metric name (alternative to series_id)
    #[serde(default)]
    pub metric: Option<String>,
    /// Tags to filter by (used with metric name)
    #[serde(default)]
    pub tags: Option<String>, // JSON-encoded tags
    /// Start timestamp (inclusive)
    pub start: i64,
    /// End timestamp (inclusive)
    pub end: i64,
    /// Maximum points to return (default: 10000)
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Optional aggregation function
    #[serde(default)]
    pub aggregation: Option<String>,
}

fn default_limit() -> usize {
    10_000
}

/// Query response
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_id: Option<SeriesId>,
    /// Raw data points (empty if aggregation is applied)
    pub points: Vec<QueryPoint>,
    /// Aggregation result (only present when aggregation function is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<AggregationResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Single point in query response
#[derive(Debug, Serialize)]
pub struct QueryPoint {
    pub timestamp: i64,
    pub value: f64,
    /// Series identifier (included in multi-series SELECT queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_id: Option<SeriesId>,
    /// Tag key-value pairs for this series
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}

/// Result of an aggregation query (REST API)
#[derive(Debug, Serialize)]
pub struct AggregationResult {
    /// Name of the aggregation function applied
    pub function: String,
    /// Computed aggregation value (scalar result when no GROUP BY)
    pub value: f64,
    /// Number of points used in the aggregation
    pub point_count: usize,
    /// Grouped results (only present when GROUP BY is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<GroupedAggregationResult>>,
}

// =============================================================================
// SQL/PromQL Query Types
// =============================================================================

/// Request for SQL or PromQL query execution
#[derive(Debug, Deserialize)]
pub struct SqlPromqlRequest {
    /// The query string (SQL or PromQL syntax)
    #[serde(alias = "q")]
    pub query: String,
    /// Output format: "json" (default)
    #[serde(default = "default_format")]
    #[allow(dead_code)]
    pub format: String,
    /// Force a specific query language: "sql", "promql", or "auto" (default)
    #[serde(default = "default_language")]
    pub language: String,
}

fn default_format() -> String {
    "json".to_string()
}

fn default_language() -> String {
    "auto".to_string()
}

/// Response from SQL/PromQL query execution (industry-standard-compatible format)
#[derive(Debug, Serialize)]
pub struct SqlPromqlResponse {
    /// Response status ("ok" or "error")
    pub status: String,
    /// Series data (industry-standard-compatible format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<Vec<SeriesData>>,
    /// Start of time range (milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_date: Option<i64>,
    /// End of time range (milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_date: Option<i64>,
    /// Query type (select, aggregate, downsample, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    /// Detected query language
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Aggregation result (for aggregate queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<SqlAggregationResult>,
    /// EXPLAIN result (for EXPLAIN queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<ExplainResult>,
    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregation result for SQL/PromQL queries
#[derive(Debug, Serialize)]
pub struct SqlAggregationResult {
    pub function: String,
    /// Scalar aggregation value (when no time bucketing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    pub point_count: usize,
    /// Time-bucketed results (when time windowing is used without GROUP BY)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<TimeBucket>>,
    /// Grouped results (only present when GROUP BY is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<GroupedAggregationResult>>,
    /// Time aggregation metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_aggregation: Option<TimeAggregationInfo>,
    /// Warnings about interval selection
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Series data for chart visualization (industry-standard-compatible format)
#[derive(Debug, Serialize, Clone)]
pub struct SeriesData {
    /// Metric name (e.g., "system.cpu.user")
    pub metric: String,
    /// Human-readable scope string (e.g., "host:server1,region:us-east")
    pub scope: String,
    /// Tag set as array of "key:value" strings
    pub tag_set: Vec<String>,
    /// Data points as [timestamp, value] pairs
    pub pointlist: Vec<[f64; 2]>,
    /// Number of points in the series
    pub length: usize,
}

// =============================================================================
// Aggregation Types
// =============================================================================

/// Result for a single group in a GROUP BY aggregation
#[derive(Debug, Serialize, Clone)]
pub struct GroupedAggregationResult {
    /// Scope string (e.g., "host:server1,region:us-east")
    pub scope: String,
    /// Tag set as array of "key:value" strings
    pub tag_set: Vec<String>,
    /// Aggregated value for this group
    pub value: f64,
    /// Number of points in this group
    pub point_count: usize,
    /// Time-bucketed results (only present when time windowing is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<TimeBucket>>,
}

/// A single time bucket with aggregated value
#[derive(Debug, Serialize, Clone)]
pub struct TimeBucket {
    /// Start timestamp of this bucket (milliseconds)
    pub timestamp: i64,
    /// Aggregated value for this bucket
    pub value: f64,
}

/// Time aggregation configuration and metadata
#[derive(Debug, Serialize, Clone)]
pub struct TimeAggregationInfo {
    /// Mode: "auto" or "custom"
    pub mode: String,
    /// The interval used (e.g., "5m", "1h")
    pub interval: String,
    /// Interval in milliseconds
    pub interval_ms: i64,
    /// Number of buckets returned
    pub bucket_count: usize,
    /// Target data points (for auto mode)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_points: Option<usize>,
}

/// Extended aggregation result with time windowing support (internal use)
#[derive(Debug, Clone)]
pub struct AggregateExecutionResult {
    /// Aggregation function name (e.g., "avg", "sum")
    pub func_name: String,
    /// Scalar aggregation value (overall result)
    pub value: f64,
    /// Grouped results (when GROUP BY is used)
    pub groups: Option<Vec<GroupedAggregationResult>>,
    /// Time aggregation metadata (when time windowing is used)
    pub time_aggregation: Option<TimeAggregationInfo>,
    /// Warnings about interval selection or data issues
    pub warnings: Vec<String>,
    /// Time-bucketed results without GROUP BY
    pub buckets: Option<Vec<TimeBucket>>,
}

// =============================================================================
// Series Management Types
// =============================================================================

/// Register series request
#[derive(Debug, Deserialize)]
pub struct RegisterSeriesRequest {
    /// Explicit series ID (optional - auto-generated if not provided)
    #[serde(default)]
    pub series_id: Option<SeriesId>,
    pub metric_name: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Find series request
#[derive(Debug, Deserialize)]
pub struct FindSeriesParams {
    pub metric_name: String,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
}

/// Find series response
#[derive(Debug, Serialize)]
pub struct FindSeriesResponse {
    pub success: bool,
    pub series_ids: Vec<SeriesId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// =============================================================================
// Admin Types
// =============================================================================

/// Stats response
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    // Storage statistics
    pub total_chunks: u64,
    pub total_bytes: u64,
    pub total_series: u64,
    pub write_ops: u64,
    pub read_ops: u64,
    pub compression_ratio: f64,

    // Index cache statistics
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
    pub index_queries_served: u64,
    pub index_cache_hit_rate: f64,

    // Query result cache statistics
    pub query_cache_hits: u64,
    pub query_cache_misses: u64,
    pub query_cache_entries: u64,
    pub query_cache_size_bytes: u64,
    pub query_cache_hit_rate: f64,
    pub query_cache_evictions: u64,
    pub query_cache_invalidations: u64,
}

/// Health response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

// =============================================================================
// EXPLAIN Types
// =============================================================================

/// Comprehensive EXPLAIN result with execution details
///
/// Provides detailed information about how a query will be executed,
/// including human-readable descriptions, step-by-step execution plan,
/// and cost estimates.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainResult {
    /// Human-readable description of what the query does
    pub description: String,

    /// Step-by-step execution plan showing how the query will be processed
    pub execution_steps: Vec<ExecutionStep>,

    /// Human-readable logical plan tree representation
    pub logical_plan: String,

    /// Cost estimates based on planner analysis
    pub cost_estimate: CostEstimateInfo,

    /// Query classification (select, aggregate, downsample, latest)
    pub query_type: String,

    /// Optimization passes that are applied to the query
    pub optimizations: Vec<OptimizationInfo>,
}

/// A single step in the query execution plan
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionStep {
    /// Step number (1-indexed)
    pub step: u32,
    /// Operation name (e.g., Scan, Filter, Aggregate)
    pub operation: String,
    /// What this step does in plain language
    pub description: String,
    /// Input to this step (None for source operators)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// Output from this step
    pub output: String,
}

/// Cost estimates for query execution
#[derive(Debug, Clone, Serialize)]
pub struct CostEstimateInfo {
    /// Estimated number of rows to process
    pub estimated_rows: u64,
    /// Estimated bytes to read from storage
    pub estimated_bytes: u64,
    /// Number of storage chunks to scan
    pub chunks_to_scan: u64,
    /// Estimated execution time category (fast, medium, slow, very_slow)
    pub execution_category: String,
}

/// Information about an optimization pass
#[derive(Debug, Clone, Serialize)]
pub struct OptimizationInfo {
    /// Optimization name (e.g., predicate_pushdown)
    pub name: String,
    /// Whether this optimization was applied
    pub applied: bool,
    /// What this optimization does
    pub description: String,
}

// =============================================================================
// Integrity Scan Types
// =============================================================================

/// Request for integrity scan (optional parameters)
#[derive(Debug, Deserialize)]
pub struct IntegrityScanRequest {
    /// Enable deep verification (decompress and validate data)
    #[serde(default)]
    pub deep_verify: bool,
    /// Enable verbose logging
    #[serde(default)]
    pub verbose: bool,
}

/// Response from integrity scan
#[derive(Debug, Serialize)]
pub struct IntegrityScanResponse {
    /// Whether the storage is healthy (no corruption)
    pub healthy: bool,
    /// Number of valid chunks
    pub valid_chunks: usize,
    /// Number of corrupted chunks detected
    pub corrupted_chunks: usize,
    /// Total bytes scanned
    pub bytes_scanned: u64,
    /// Corruption rate as percentage
    pub corruption_rate: f64,
    /// Scan duration in milliseconds
    pub duration_ms: u64,
    /// Details of corrupted chunks (only present if corruptions found)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub corruptions: Vec<CorruptedChunkInfo>,
}

/// Information about a corrupted chunk
#[derive(Debug, Serialize)]
pub struct CorruptedChunkInfo {
    /// Path to the corrupted chunk file
    pub path: String,
    /// Series ID if extractable from header
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_id: Option<u64>,
    /// Chunk ID if extractable from filename
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
    /// Type of corruption detected
    pub error: String,
    /// Severity level: "recoverable", "header_damaged", or "unrecoverable"
    pub severity: String,
    /// Suggested recovery action
    pub recovery_suggestion: String,
}
