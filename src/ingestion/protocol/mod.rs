//! Protocol parsing module
//!
//! Provides parsers for various data ingestion formats with support for:
//! - Line Protocol (InfluxDB compatible, zero-copy parsing)
//! - JSON (streaming with simd-json acceleration)
//! - Protobuf (via prost)
//! - Protocol auto-detection from first bytes
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────┐
//! │           Protocol Auto-Detector           │
//! │  ┌─────────┬────────────┬────────────┐    │
//! │  │ First   │   Magic    │  Fallback  │    │
//! │  │ Bytes   │   Number   │  Heuristic │    │
//! │  └────┬────┴─────┬──────┴─────┬──────┘    │
//! │       │          │            │            │
//! └───────┼──────────┼────────────┼────────────┘
//!         │          │            │
//!         v          v            v
//! ┌───────────┐ ┌───────────┐ ┌───────────┐
//! │   Line    │ │   JSON    │ │ Protobuf  │
//! │  Protocol │ │  Parser   │ │  Parser   │
//! │  Parser   │ │           │ │           │
//! └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
//!       │             │             │
//!       └──────────┬──┴─────────────┘
//!                  │
//!                  v
//!         ┌───────────────┐
//!         │  ParsedPoint  │
//!         │  (validated)  │
//!         └───────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::protocol::{ProtocolParser, LineProtocolParser, ParsedPoint};
//!
//! // Parse InfluxDB line protocol
//! let parser = LineProtocolParser::new();
//! let input = b"cpu,host=server01 value=42.0 1234567890000000000";
//! let points = parser.parse(input)?;
//!
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod error;
pub mod line;
// pub mod json;      // TODO: Implement JSON parser
// pub mod protobuf;  // TODO: Implement Protobuf parser
// pub mod detect;    // TODO: Implement protocol detection

pub use error::{ParseError, ParseErrorKind};
pub use line::{LineProtocolParser, ParsedLine};

// Re-export key types from this module
// (ParsedPoint, OwnedParsedPoint, FieldValue, Protocol, ProtocolParser are already public)

use std::borrow::Cow;
use std::collections::HashMap;

/// Owned version of ParsedPoint (for when data needs to outlive input buffer)
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedParsedPoint {
    /// Measurement name (required)
    pub measurement: String,
    /// Tag key-value pairs (optional)
    pub tags: HashMap<String, String>,
    /// Field key-value pairs (at least one required)
    pub fields: HashMap<String, FieldValue>,
    /// Timestamp in nanoseconds (optional - server may assign if missing)
    pub timestamp: Option<i64>,
}

impl OwnedParsedPoint {
    /// Check if the point is valid (has measurement and at least one field)
    pub fn is_valid(&self) -> bool {
        !self.measurement.is_empty() && !self.fields.is_empty()
    }

    /// Generate a series key from measurement and sorted tags
    pub fn series_key(&self) -> String {
        let mut key = self.measurement.clone();

        if !self.tags.is_empty() {
            let mut tags: Vec<_> = self.tags.iter().collect();
            tags.sort_by(|a, b| a.0.cmp(b.0));

            for (k, v) in tags {
                key.push(',');
                key.push_str(k);
                key.push('=');
                key.push_str(v);
            }
        }

        key
    }
}

/// Parsed data point from any protocol
///
/// This is the normalized representation of a data point that
/// all protocol parsers produce. It uses Cow for zero-copy
/// parsing where possible.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPoint<'a> {
    /// Measurement name (required)
    pub measurement: Cow<'a, str>,
    /// Tag key-value pairs (optional)
    pub tags: HashMap<Cow<'a, str>, Cow<'a, str>>,
    /// Field key-value pairs (at least one required)
    pub fields: HashMap<Cow<'a, str>, FieldValue>,
    /// Timestamp in nanoseconds (optional - server may assign if missing)
    pub timestamp: Option<i64>,
}

impl<'a> ParsedPoint<'a> {
    /// Create a new parsed point with required fields
    pub fn new(measurement: impl Into<Cow<'a, str>>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: HashMap::new(),
            fields: HashMap::new(),
            timestamp: None,
        }
    }

    /// Add a tag to the point
    pub fn with_tag(
        mut self,
        key: impl Into<Cow<'a, str>>,
        value: impl Into<Cow<'a, str>>,
    ) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Add a field to the point
    pub fn with_field(mut self, key: impl Into<Cow<'a, str>>, value: FieldValue) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Set the timestamp
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Check if the point is valid (has measurement and at least one field)
    pub fn is_valid(&self) -> bool {
        !self.measurement.is_empty() && !self.fields.is_empty()
    }

    /// Convert to owned version (useful when data needs to outlive input buffer)
    pub fn into_owned(self) -> OwnedParsedPoint {
        OwnedParsedPoint {
            measurement: self.measurement.into_owned(),
            tags: self
                .tags
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect(),
            fields: self
                .fields
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v))
                .collect(),
            timestamp: self.timestamp,
        }
    }

    /// Generate a series key from measurement and sorted tags
    ///
    /// The series key uniquely identifies a time series and is used
    /// for routing, deduplication, and storage.
    pub fn series_key(&self) -> String {
        let mut key = self.measurement.to_string();

        if !self.tags.is_empty() {
            // Sort tags for consistent key generation
            let mut tags: Vec<_> = self.tags.iter().collect();
            tags.sort_by(|a, b| a.0.cmp(b.0));

            for (k, v) in tags {
                key.push(',');
                key.push_str(k);
                key.push('=');
                key.push_str(v);
            }
        }

        key
    }

    /// Estimate memory size of this point
    pub fn estimated_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.measurement.len();
        for (k, v) in &self.tags {
            size += k.len() + v.len();
        }
        for (k, v) in &self.fields {
            size += k.len() + v.estimated_size();
        }
        size
    }
}

/// Field value types supported by the protocol
///
/// These match the InfluxDB field value types and provide
/// type-safe storage of different value kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// 64-bit floating point value
    Float(f64),
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit unsigned integer
    UInteger(u64),
    /// UTF-8 string value
    String(String),
    /// Boolean value
    Boolean(bool),
}

impl FieldValue {
    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Float(_) => "float",
            FieldValue::Integer(_) => "integer",
            FieldValue::UInteger(_) => "uinteger",
            FieldValue::String(_) => "string",
            FieldValue::Boolean(_) => "boolean",
        }
    }

    /// Try to get as f64 (converts integers to float)
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            FieldValue::Integer(v) => Some(*v as f64),
            FieldValue::UInteger(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(v) => Some(*v),
            FieldValue::Float(v) if v.fract() == 0.0 => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::String(v) => Some(v),
            _ => None,
        }
    }

    /// Estimate memory size
    pub fn estimated_size(&self) -> usize {
        match self {
            FieldValue::Float(_) => 8,
            FieldValue::Integer(_) => 8,
            FieldValue::UInteger(_) => 8,
            FieldValue::String(s) => s.len() + std::mem::size_of::<String>(),
            FieldValue::Boolean(_) => 1,
        }
    }
}

impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::Float(v) => write!(f, "{}", v),
            FieldValue::Integer(v) => write!(f, "{}i", v),
            FieldValue::UInteger(v) => write!(f, "{}u", v),
            FieldValue::String(v) => write!(f, "\"{}\"", v),
            FieldValue::Boolean(v) => write!(f, "{}", v),
        }
    }
}

/// Protocol parser trait
///
/// All protocol parsers implement this trait, allowing for
/// uniform handling regardless of the input format.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of the input data (for zero-copy parsing)
pub trait ProtocolParser<'a> {
    /// Parse input bytes into data points
    ///
    /// # Arguments
    ///
    /// * `input` - Raw bytes to parse
    ///
    /// # Returns
    ///
    /// Vector of parsed points, or parse error.
    /// Partial success is allowed - some points may be returned
    /// along with errors in the error variant.
    fn parse(&self, input: &'a [u8]) -> Result<Vec<ParsedPoint<'a>>, ParseError>;

    /// Parse a single line/record
    ///
    /// More efficient when input is known to contain exactly one record.
    fn parse_single(&self, input: &'a [u8]) -> Result<ParsedPoint<'a>, ParseError>;

    /// Get the protocol name
    fn protocol_name(&self) -> &'static str;

    /// Check if this parser can likely handle the input
    ///
    /// Used by auto-detection to quickly filter candidate parsers.
    fn can_parse(&self, input: &[u8]) -> bool;
}

/// Protocol type enumeration
///
/// Used for explicit protocol selection when auto-detection
/// is not desired or when configuration specifies a format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Protocol {
    /// InfluxDB line protocol
    #[default]
    LineProtocol,
    /// JSON format
    Json,
    /// Protocol Buffers
    Protobuf,
}

impl Protocol {
    /// Get the protocol name
    pub fn name(&self) -> &'static str {
        match self {
            Protocol::LineProtocol => "line-protocol",
            Protocol::Json => "json",
            Protocol::Protobuf => "protobuf",
        }
    }

    /// Parse protocol from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "line-protocol" | "lineprotocol" | "line" | "influx" => Some(Protocol::LineProtocol),
            "json" => Some(Protocol::Json),
            "protobuf" | "proto" | "pb" => Some(Protocol::Protobuf),
            _ => None,
        }
    }

    /// Get Content-Type header value for this protocol
    pub fn content_type(&self) -> &'static str {
        match self {
            Protocol::LineProtocol => "text/plain",
            Protocol::Json => "application/json",
            Protocol::Protobuf => "application/x-protobuf",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_point_new() {
        let point = ParsedPoint::new("cpu")
            .with_tag("host", "server01")
            .with_field("value", FieldValue::Float(42.0))
            .with_timestamp(1234567890);

        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host").map(|v| v.as_ref()), Some("server01"));
        assert!(matches!(
            point.fields.get("value"),
            Some(FieldValue::Float(v)) if (*v - 42.0).abs() < f64::EPSILON
        ));
        assert_eq!(point.timestamp, Some(1234567890));
    }

    #[test]
    fn test_parsed_point_is_valid() {
        let valid = ParsedPoint::new("cpu").with_field("value", FieldValue::Float(42.0));
        assert!(valid.is_valid());

        let no_fields = ParsedPoint::new("cpu");
        assert!(!no_fields.is_valid());

        let empty_measurement = ParsedPoint::new("").with_field("value", FieldValue::Float(42.0));
        assert!(!empty_measurement.is_valid());
    }

    #[test]
    fn test_parsed_point_series_key() {
        let point = ParsedPoint::new("cpu")
            .with_tag("host", "server01")
            .with_tag("region", "us-east")
            .with_field("value", FieldValue::Float(42.0));

        // Tags should be sorted alphabetically
        let key = point.series_key();
        assert_eq!(key, "cpu,host=server01,region=us-east");
    }

    #[test]
    fn test_parsed_point_into_owned() {
        let input = String::from("cpu");
        let point: ParsedPoint<'_> = ParsedPoint::new(&input[..])
            .with_tag("host", "server01")
            .with_field("value", FieldValue::Float(42.0));

        let owned = point.into_owned();
        assert_eq!(owned.measurement, "cpu");
        assert!(owned.is_valid());
    }

    #[test]
    fn test_field_value_types() {
        assert_eq!(FieldValue::Float(1.0).type_name(), "float");
        assert_eq!(FieldValue::Integer(1).type_name(), "integer");
        assert_eq!(FieldValue::UInteger(1).type_name(), "uinteger");
        assert_eq!(FieldValue::String("test".to_string()).type_name(), "string");
        assert_eq!(FieldValue::Boolean(true).type_name(), "boolean");
    }

    #[test]
    fn test_field_value_as_f64() {
        assert_eq!(FieldValue::Float(42.5).as_f64(), Some(42.5));
        assert_eq!(FieldValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(FieldValue::UInteger(42).as_f64(), Some(42.0));
        assert_eq!(FieldValue::Boolean(true).as_f64(), None);
    }

    #[test]
    fn test_field_value_display() {
        assert_eq!(format!("{}", FieldValue::Float(42.5)), "42.5");
        assert_eq!(format!("{}", FieldValue::Integer(42)), "42i");
        assert_eq!(format!("{}", FieldValue::UInteger(42)), "42u");
        assert_eq!(
            format!("{}", FieldValue::String("test".to_string())),
            "\"test\""
        );
        assert_eq!(format!("{}", FieldValue::Boolean(true)), "true");
    }

    #[test]
    fn test_protocol_from_str() {
        assert_eq!(
            Protocol::parse("line-protocol"),
            Some(Protocol::LineProtocol)
        );
        assert_eq!(Protocol::parse("json"), Some(Protocol::Json));
        assert_eq!(Protocol::parse("protobuf"), Some(Protocol::Protobuf));
        assert_eq!(Protocol::parse("proto"), Some(Protocol::Protobuf));
        assert_eq!(Protocol::parse("unknown"), None);
    }

    #[test]
    fn test_protocol_content_type() {
        assert_eq!(Protocol::LineProtocol.content_type(), "text/plain");
        assert_eq!(Protocol::Json.content_type(), "application/json");
        assert_eq!(Protocol::Protobuf.content_type(), "application/x-protobuf");
    }

    #[test]
    fn test_parsed_point_estimated_size() {
        let point = ParsedPoint::new("cpu")
            .with_tag("host", "server01")
            .with_field("value", FieldValue::Float(42.0));

        let size = point.estimated_size();
        assert!(size > 0);
        assert!(size > point.measurement.len());
    }
}
