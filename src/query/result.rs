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

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.metadata.row_count == 0
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.metadata.row_count
    }

    /// Format result to string
    pub fn format(&self, format: ResultFormat) -> String {
        match format {
            ResultFormat::Json => self.to_json(),
            ResultFormat::JsonPretty => self.to_json_pretty(),
            ResultFormat::Csv => self.to_csv(),
            ResultFormat::Table => self.to_table(),
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
        }

        // Metadata footer
        output.push_str(&format!(
            "\n{} rows in {:.3}ms\n",
            self.metadata.row_count,
            self.metadata.execution_time_us as f64 / 1000.0
        ));

        output
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

    /// Whether result was served from cache
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[serde(default)]
    pub cache_hit: bool,

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
}

// ============================================================================
// Result Format
// ============================================================================

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    /// Compact JSON
    Json,
    /// Pretty-printed JSON
    JsonPretty,
    /// CSV format
    Csv,
    /// ASCII table (for CLI)
    Table,
}

impl Default for ResultFormat {
    fn default() -> Self {
        ResultFormat::Json
    }
}

impl fmt::Display for ResultFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResultFormat::Json => write!(f, "json"),
            ResultFormat::JsonPretty => write!(f, "json-pretty"),
            ResultFormat::Csv => write!(f, "csv"),
            ResultFormat::Table => write!(f, "table"),
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
