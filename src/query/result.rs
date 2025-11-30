//! Query result types and formatters
//!
//! Provides structured result types and serialization to various formats
//! including JSON, CSV, and Protobuf.

use crate::types::{DataPoint, SeriesId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// ============================================================================
// Query Result Types
// ============================================================================

/// Complete query result with data and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Query execution metadata
    pub metadata: ResultMetadata,

    /// Result data (rows or series)
    pub data: ResultData,
}

impl QueryResult {
    /// Create a new result from rows
    pub fn new(rows: Vec<ResultRow>) -> Self {
        Self::from_rows(rows)
    }

    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            metadata: ResultMetadata::default(),
            data: ResultData::Rows(Vec::new()),
        }
    }

    /// Create result from rows
    pub fn from_rows(rows: Vec<ResultRow>) -> Self {
        let row_count = rows.len();
        Self {
            metadata: ResultMetadata {
                row_count,
                ..Default::default()
            },
            data: ResultData::Rows(rows),
        }
    }

    /// Create result from data points
    pub fn from_points(points: Vec<DataPoint>) -> Self {
        let rows: Vec<ResultRow> = points.into_iter().map(ResultRow::from).collect();
        Self::from_rows(rows)
    }

    /// Create result from aggregated series
    pub fn from_series(series: Vec<SeriesResult>) -> Self {
        let row_count = series.iter().map(|s| s.values.len()).sum();
        Self {
            metadata: ResultMetadata {
                row_count,
                series_count: series.len(),
                ..Default::default()
            },
            data: ResultData::Series(series),
        }
    }

    /// Set execution time metadata
    pub fn with_execution_time(mut self, duration: Duration) -> Self {
        self.metadata.execution_time_us = duration.as_micros() as u64;
        self
    }

    /// Set bytes scanned metadata
    pub fn with_bytes_scanned(mut self, bytes: usize) -> Self {
        self.metadata.bytes_scanned = bytes;
        self
    }

    /// Set rows scanned metadata
    pub fn with_rows_scanned(mut self, rows: u64) -> Self {
        self.metadata.rows_scanned = rows as usize;
        self
    }

    /// Set cache hit status
    pub fn with_cache_hit(mut self, hit: bool) -> Self {
        self.metadata.cache_hit = hit;
        self
    }

    /// Set detailed cache information for analysis
    ///
    /// This provides visibility into cache behavior when queries are executed,
    /// allowing users to analyze cache hit rates, entry ages, and cache configuration.
    pub fn with_cache_info(mut self, cache_info: CacheInfo) -> Self {
        self.metadata.cache_hit = cache_info.hit;
        self.metadata.cache_info = Some(cache_info);
        self
    }

    /// Create an explain result with plan text
    pub fn explain(plan_text: String) -> Self {
        Self {
            metadata: ResultMetadata::default(),
            data: ResultData::Explain(plan_text),
        }
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.metadata.row_count == 0
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.metadata.row_count
    }

    /// Format result to string
    ///
    /// Note: For Protobuf format, this returns a base64-encoded string.
    /// Use `to_protobuf_bytes()` for binary output.
    pub fn format(&self, format: ResultFormat) -> String {
        match format {
            ResultFormat::Json => self.to_json(),
            ResultFormat::JsonPretty => self.to_json_pretty(),
            ResultFormat::Csv => self.to_csv(),
            ResultFormat::Table => self.to_table(),
            ResultFormat::Protobuf => self.to_protobuf_string(),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Convert to pretty-printed JSON string
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Convert to CSV string
    pub fn to_csv(&self) -> String {
        let mut output = String::new();

        match &self.data {
            ResultData::Rows(rows) => {
                // Header
                output.push_str("timestamp,series_id,value\n");

                // Data rows
                for row in rows {
                    output.push_str(&format!(
                        "{},{},{}\n",
                        row.timestamp,
                        row.series_id.unwrap_or(0),
                        row.value
                    ));
                }
            }
            ResultData::Series(series) => {
                // Header
                output.push_str("series_id,timestamp,value\n");

                // Data rows
                for s in series {
                    for (ts, val) in &s.values {
                        output.push_str(&format!("{},{},{}\n", s.series_id, ts, val));
                    }
                }
            }
            ResultData::Scalar(value) => {
                output.push_str("value\n");
                output.push_str(&format!("{}\n", value));
            }
            ResultData::Explain(plan) => {
                // For CSV, just output the plan as-is
                output.push_str(plan);
            }
        }

        output
    }

    /// Convert to ASCII table string
    pub fn to_table(&self) -> String {
        let mut output = String::new();

        match &self.data {
            ResultData::Rows(rows) => {
                if rows.is_empty() {
                    return "No results".to_string();
                }

                // Header
                output.push_str("+----------------------+------------+------------------+\n");
                output.push_str("|     timestamp        | series_id  |      value       |\n");
                output.push_str("+----------------------+------------+------------------+\n");

                // Data rows (limit to 100 for display)
                for row in rows.iter().take(100) {
                    output.push_str(&format!(
                        "| {:>20} | {:>10} | {:>16.6} |\n",
                        row.timestamp,
                        row.series_id.unwrap_or(0),
                        row.value
                    ));
                }

                output.push_str("+----------------------+------------+------------------+\n");

                if rows.len() > 100 {
                    output.push_str(&format!("... and {} more rows\n", rows.len() - 100));
                }
            }
            ResultData::Series(series) => {
                for s in series {
                    output.push_str(&format!("\nSeries: {}\n", s.series_id));
                    output.push_str("+----------------------+------------------+\n");
                    output.push_str("|     timestamp        |      value       |\n");
                    output.push_str("+----------------------+------------------+\n");

                    for (ts, val) in s.values.iter().take(100) {
                        output.push_str(&format!("| {:>20} | {:>16.6} |\n", ts, val));
                    }

                    output.push_str("+----------------------+------------------+\n");
                }
            }
            ResultData::Scalar(value) => {
                output.push_str(&format!("Result: {}\n", value));
            }
            ResultData::Explain(plan) => {
                // For table format, just output the plan
                output.push_str(plan);
                return output; // Skip metadata footer for explain
            }
        }

        // Metadata footer
        output.push_str(&format!(
            "\n{} rows in {:.3}ms\n",
            self.metadata.row_count,
            self.metadata.execution_time_us as f64 / 1000.0
        ));

        output
    }

    /// Convert to Protocol Buffers binary format
    ///
    /// Returns the binary protobuf representation of the query result.
    /// The format uses a simple wire format compatible with standard protobuf parsers.
    pub fn to_protobuf_bytes(&self) -> Vec<u8> {
        // Simple protobuf-like wire format:
        // - Field 1 (metadata): varint row_count, varint execution_time_us
        // - Field 2 (data): repeated rows with (varint timestamp, double value, varint series_id)
        let mut bytes = Vec::new();

        // Write row count (field 1, varint)
        bytes.push(0x08); // field 1, wire type 0 (varint)
        Self::write_varint(&mut bytes, self.metadata.row_count as u64);

        // Write execution time (field 2, varint)
        bytes.push(0x10); // field 2, wire type 0 (varint)
        Self::write_varint(&mut bytes, self.metadata.execution_time_us);

        // Write data rows
        if let ResultData::Rows(rows) = &self.data {
            for row in rows {
                // Each row is a nested message (field 3)
                bytes.push(0x1a); // field 3, wire type 2 (length-delimited)
                let row_bytes = Self::encode_row(row);
                Self::write_varint(&mut bytes, row_bytes.len() as u64);
                bytes.extend(row_bytes);
            }
        }

        bytes
    }

    /// Convert to Protocol Buffers as base64-encoded string
    ///
    /// Useful when binary transport is not available.
    pub fn to_protobuf_string(&self) -> String {
        use base64::{engine::general_purpose::STANDARD, Engine};
        STANDARD.encode(self.to_protobuf_bytes())
    }

    /// Helper to write a varint
    fn write_varint(bytes: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// Helper to encode a single row
    fn encode_row(row: &ResultRow) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Field 1: timestamp (varint, signed zigzag)
        bytes.push(0x08);
        let zigzag = ((row.timestamp << 1) ^ (row.timestamp >> 63)) as u64;
        Self::write_varint(&mut bytes, zigzag);

        // Field 2: value (fixed64, as f64 bits)
        bytes.push(0x11); // field 2, wire type 1 (64-bit)
        bytes.extend(row.value.to_le_bytes());

        // Field 3: series_id (varint, optional)
        if let Some(sid) = row.series_id {
            bytes.push(0x18); // field 3, wire type 0 (varint)
            Self::write_varint(&mut bytes, sid as u64);
        }

        bytes
    }
}

/// Result data variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ResultData {
    /// Flat list of rows
    Rows(Vec<ResultRow>),

    /// Grouped by series
    Series(Vec<SeriesResult>),

    /// Single scalar value (for simple aggregations)
    Scalar(f64),

    /// Explain output (query plan as text)
    Explain(String),
}

/// Single result row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultRow {
    /// Timestamp in nanoseconds
    pub timestamp: i64,

    /// Optional series ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_id: Option<SeriesId>,

    /// Value (may be NaN for null)
    pub value: f64,

    /// Additional fields/tags
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl ResultRow {
    /// Create a new result row
    pub fn new(timestamp: i64, value: f64) -> Self {
        Self {
            timestamp,
            series_id: None,
            value,
            tags: HashMap::new(),
        }
    }

    /// Set series ID
    pub fn with_series(mut self, series_id: SeriesId) -> Self {
        self.series_id = Some(series_id);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

impl From<DataPoint> for ResultRow {
    fn from(point: DataPoint) -> Self {
        Self {
            timestamp: point.timestamp,
            series_id: Some(point.series_id),
            value: point.value,
            tags: HashMap::new(),
        }
    }
}

/// Result for a single series (used in grouped results)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesResult {
    /// Series identifier
    pub series_id: SeriesId,

    /// Series tags/labels
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub tags: HashMap<String, String>,

    /// Time-value pairs
    pub values: Vec<(i64, f64)>,
}

impl SeriesResult {
    /// Create a new series result
    pub fn new(series_id: SeriesId) -> Self {
        Self {
            series_id,
            tags: HashMap::new(),
            values: Vec::new(),
        }
    }

    /// Add tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Add a value
    pub fn add_value(&mut self, timestamp: i64, value: f64) {
        self.values.push((timestamp, value));
    }

    /// Set values
    pub fn with_values(mut self, values: Vec<(i64, f64)>) -> Self {
        self.values = values;
        self
    }
}

// ============================================================================
// Result Metadata
// ============================================================================

/// Query result metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// Number of rows returned
    pub row_count: usize,

    /// Number of series in result
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub series_count: usize,

    /// Query execution time in microseconds
    pub execution_time_us: u64,

    /// Bytes scanned during query
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub bytes_scanned: usize,

    /// Number of chunks read
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub chunks_read: usize,

    /// Number of chunks pruned (skipped via zone maps)
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub chunks_pruned: usize,

    /// Number of rows scanned during query
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub rows_scanned: usize,

    /// Whether result was served from cache
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[serde(default)]
    pub cache_hit: bool,

    /// Detailed cache information for analysis
    /// Contains cache configuration and hit/miss statistics when available
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub cache_info: Option<CacheInfo>,

    /// Whether result is truncated due to limits
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[serde(default)]
    pub truncated: bool,

    /// Warning messages
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Helper for serde skip_serializing_if
fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl ResultMetadata {
    /// Add a warning message
    pub fn add_warning(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }

    /// Set cache information for analysis
    pub fn with_cache_info(mut self, cache_info: CacheInfo) -> Self {
        self.cache_info = Some(cache_info);
        self
    }
}

// ============================================================================
// Cache Information
// ============================================================================

/// Detailed cache information for query analysis
///
/// This structure provides visibility into how the cache was used during
/// query execution, allowing users to understand cache behavior and tune
/// cache configuration if needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheInfo {
    /// Whether the result was served from cache
    pub hit: bool,

    /// Cache key hash (for debugging cache behavior)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,

    /// Time-to-live of the cached entry in seconds
    /// Only present if result was cached
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,

    /// Age of the cached entry in milliseconds
    /// Only present if result came from cache (cache_hit = true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_age_ms: Option<u64>,

    /// Current cache size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_size_bytes: Option<u64>,

    /// Maximum cache size in bytes (from config)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_max_size_bytes: Option<usize>,

    /// Current number of entries in cache
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_entries: Option<usize>,

    /// Maximum entries allowed (from config)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_max_entries: Option<usize>,

    /// Total cache hits since startup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_hits: Option<u64>,

    /// Total cache misses since startup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_misses: Option<u64>,

    /// Cache hit ratio (0.0 to 1.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hit_ratio: Option<f64>,

    /// Whether caching is enabled
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[serde(default)]
    pub enabled: bool,
}

impl CacheInfo {
    /// Create cache info for a cache hit
    pub fn hit() -> Self {
        Self {
            hit: true,
            enabled: true,
            ..Default::default()
        }
    }

    /// Create cache info for a cache miss
    pub fn miss() -> Self {
        Self {
            hit: false,
            enabled: true,
            ..Default::default()
        }
    }

    /// Create cache info when cache is disabled
    pub fn disabled() -> Self {
        Self {
            hit: false,
            enabled: false,
            ..Default::default()
        }
    }

    /// Set the cache key (for debugging)
    pub fn with_cache_key(mut self, key: impl Into<String>) -> Self {
        self.cache_key = Some(key.into());
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl_secs = Some(ttl_secs);
        self
    }

    /// Set entry age in milliseconds
    pub fn with_entry_age(mut self, age_ms: u64) -> Self {
        self.entry_age_ms = Some(age_ms);
        self
    }

    /// Set cache size statistics
    pub fn with_size_stats(
        mut self,
        current_bytes: u64,
        max_bytes: usize,
        current_entries: usize,
        max_entries: usize,
    ) -> Self {
        self.cache_size_bytes = Some(current_bytes);
        self.cache_max_size_bytes = Some(max_bytes);
        self.cache_entries = Some(current_entries);
        self.cache_max_entries = Some(max_entries);
        self
    }

    /// Set cache hit/miss statistics
    pub fn with_hit_stats(mut self, hits: u64, misses: u64) -> Self {
        self.total_hits = Some(hits);
        self.total_misses = Some(misses);
        let total = hits + misses;
        self.hit_ratio = if total > 0 {
            Some(hits as f64 / total as f64)
        } else {
            Some(0.0)
        };
        self
    }
}

// ============================================================================
// Result Format
// ============================================================================

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResultFormat {
    /// Compact JSON
    #[default]
    Json,
    /// Pretty-printed JSON
    JsonPretty,
    /// CSV format
    Csv,
    /// ASCII table (for CLI)
    Table,
    /// Protocol Buffers binary format
    Protobuf,
}

impl fmt::Display for ResultFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResultFormat::Json => write!(f, "json"),
            ResultFormat::JsonPretty => write!(f, "json-pretty"),
            ResultFormat::Csv => write!(f, "csv"),
            ResultFormat::Table => write!(f, "table"),
            ResultFormat::Protobuf => write!(f, "protobuf"),
        }
    }
}

impl std::str::FromStr for ResultFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(ResultFormat::Json),
            "json-pretty" | "jsonpretty" => Ok(ResultFormat::JsonPretty),
            "csv" => Ok(ResultFormat::Csv),
            "table" => Ok(ResultFormat::Table),
            "protobuf" | "proto" | "pb" => Ok(ResultFormat::Protobuf),
            _ => Err(format!("unknown format: {}", s)),
        }
    }
}

// ============================================================================
// Streaming Result
// ============================================================================

/// Streaming result for continuous queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingResult {
    /// Stream identifier
    pub stream_id: String,

    /// Batch sequence number
    pub sequence: u64,

    /// Data in this batch
    pub rows: Vec<ResultRow>,

    /// Whether this is the final batch
    pub is_final: bool,
}

impl StreamingResult {
    /// Create a new streaming result batch
    pub fn new(stream_id: impl Into<String>, sequence: u64, rows: Vec<ResultRow>) -> Self {
        Self {
            stream_id: stream_id.into(),
            sequence,
            rows,
            is_final: false,
        }
    }

    /// Mark as final batch
    pub fn finalize(mut self) -> Self {
        self.is_final = true;
        self
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_from_points() {
        let points = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.5,
            },
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 43.5,
            },
        ];

        let result = QueryResult::from_points(points);
        assert_eq!(result.row_count(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_query_result_json() {
        let result = QueryResult::from_rows(vec![ResultRow::new(1000, 42.5).with_series(1)]);

        let json = result.to_json();
        assert!(json.contains("1000"));
        assert!(json.contains("42.5"));
    }

    #[test]
    fn test_query_result_csv() {
        let result = QueryResult::from_rows(vec![
            ResultRow::new(1000, 42.5).with_series(1),
            ResultRow::new(2000, 43.5).with_series(1),
        ]);

        let csv = result.to_csv();
        assert!(csv.contains("timestamp,series_id,value"));
        assert!(csv.contains("1000,1,42.5"));
        assert!(csv.contains("2000,1,43.5"));
    }

    #[test]
    fn test_result_format_parsing() {
        assert_eq!("json".parse::<ResultFormat>().unwrap(), ResultFormat::Json);
        assert_eq!("csv".parse::<ResultFormat>().unwrap(), ResultFormat::Csv);
        assert_eq!(
            "table".parse::<ResultFormat>().unwrap(),
            ResultFormat::Table
        );
        assert_eq!(
            "protobuf".parse::<ResultFormat>().unwrap(),
            ResultFormat::Protobuf
        );
        assert_eq!(
            "proto".parse::<ResultFormat>().unwrap(),
            ResultFormat::Protobuf
        );
        assert_eq!(
            "pb".parse::<ResultFormat>().unwrap(),
            ResultFormat::Protobuf
        );
    }

    #[test]
    fn test_protobuf_output() {
        let result = QueryResult::from_rows(vec![
            ResultRow::new(1000, 42.5).with_series(1),
            ResultRow::new(2000, 43.5).with_series(2),
        ]);

        // Test binary output is non-empty
        let bytes = result.to_protobuf_bytes();
        assert!(!bytes.is_empty());

        // Test base64 output
        let proto_str = result.to_protobuf_string();
        assert!(!proto_str.is_empty());

        // Verify it's valid base64
        use base64::{engine::general_purpose::STANDARD, Engine};
        let decoded = STANDARD.decode(&proto_str).unwrap();
        assert_eq!(decoded, bytes);

        // Test format() method
        let formatted = result.format(ResultFormat::Protobuf);
        assert_eq!(formatted, proto_str);
    }

    #[test]
    fn test_series_result() {
        let mut series = SeriesResult::new(1);
        series.add_value(1000, 42.5);
        series.add_value(2000, 43.5);

        assert_eq!(series.values.len(), 2);
        assert_eq!(series.values[0], (1000, 42.5));
    }

    #[test]
    fn test_result_metadata() {
        let mut metadata = ResultMetadata::default();
        metadata.add_warning("partial result due to timeout");

        assert_eq!(metadata.warnings.len(), 1);
    }
}
