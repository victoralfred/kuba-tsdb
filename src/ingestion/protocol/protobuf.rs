//! Protobuf protocol parser
//!
//! Parses Protocol Buffer formatted data points for ingestion.
//! Uses a simple wire format compatible with common time series schemas.
//!
//! # Wire Format
//!
//! The protobuf schema is defined as:
//! ```protobuf
//! message WriteRequest {
//!   repeated TimeSeries timeseries = 1;
//! }
//!
//! message TimeSeries {
//!   string measurement = 1;
//!   repeated Label labels = 2;
//!   repeated Sample samples = 3;
//! }
//!
//! message Label {
//!   string name = 1;
//!   string value = 2;
//! }
//!
//! message Sample {
//!   double value = 1;
//!   int64 timestamp = 2;
//! }
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::protocol::{ProtobufParser, ProtocolParser};
//!
//! let parser = ProtobufParser::new();
//!
//! // Empty input returns empty result (valid protobuf with no data)
//! let empty_bytes: &[u8] = &[];
//! let result = parser.parse(empty_bytes);
//! // Note: parsing empty protobuf may succeed or fail depending on schema
//! ```

use std::borrow::Cow;
use std::collections::HashMap;

use prost::Message;

use super::error::{ParseError, ParseErrorKind};
use super::{FieldValue, ParsedPoint, ProtocolParser};

/// Maximum number of time series in a single WriteRequest (DoS protection)
const MAX_TIMESERIES: usize = 10_000;

/// Maximum number of samples per time series (DoS protection)
const MAX_SAMPLES_PER_SERIES: usize = 100_000;

/// Maximum number of labels per time series (DoS protection)
const MAX_LABELS_PER_SERIES: usize = 256;

/// Protobuf protocol parser
///
/// Parses Protocol Buffer formatted time series data into ParsedPoints.
///
/// # Size Limits
///
/// To prevent DoS attacks, the parser enforces the following limits:
/// - Maximum 10,000 time series per request
/// - Maximum 100,000 samples per time series
/// - Maximum 256 labels per time series
#[derive(Debug, Clone, Default)]
pub struct ProtobufParser {
    /// Default field name for samples (default: "value")
    default_field_name: String,
    /// Maximum number of time series (default: 10,000)
    max_timeseries: usize,
    /// Maximum samples per series (default: 100,000)
    max_samples_per_series: usize,
}

impl ProtobufParser {
    /// Create a new Protobuf parser with default settings
    pub fn new() -> Self {
        Self {
            default_field_name: "value".to_string(),
            max_timeseries: MAX_TIMESERIES,
            max_samples_per_series: MAX_SAMPLES_PER_SERIES,
        }
    }

    /// Set the default field name for samples
    pub fn with_field_name(mut self, name: impl Into<String>) -> Self {
        self.default_field_name = name.into();
        self
    }

    /// Set maximum number of time series per request
    pub fn with_max_timeseries(mut self, max: usize) -> Self {
        self.max_timeseries = max;
        self
    }

    /// Set maximum samples per time series
    pub fn with_max_samples_per_series(mut self, max: usize) -> Self {
        self.max_samples_per_series = max;
        self
    }

    /// Parse a WriteRequest message
    fn parse_write_request<'a>(
        &self,
        request: &WriteRequest,
    ) -> Result<Vec<ParsedPoint<'a>>, ParseError> {
        // Check time series count limit
        if request.timeseries.len() > self.max_timeseries {
            return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                message: format!(
                    "too many time series: {} exceeds maximum {}",
                    request.timeseries.len(),
                    self.max_timeseries
                ),
            }));
        }

        let mut points = Vec::new();

        for ts in &request.timeseries {
            let measurement: Cow<'a, str> = if ts.measurement.is_empty() {
                return Err(ParseError::new(ParseErrorKind::EmptyMeasurement)
                    .with_context("timeseries missing measurement"));
            } else {
                Cow::Owned(ts.measurement.clone())
            };

            // Check labels limit
            if ts.labels.len() > MAX_LABELS_PER_SERIES {
                return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                    message: format!(
                        "too many labels: {} exceeds maximum {}",
                        ts.labels.len(),
                        MAX_LABELS_PER_SERIES
                    ),
                }));
            }

            // Check samples limit
            if ts.samples.len() > self.max_samples_per_series {
                return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                    message: format!(
                        "too many samples: {} exceeds maximum {}",
                        ts.samples.len(),
                        self.max_samples_per_series
                    ),
                }));
            }

            // Convert labels to tags
            let tags: HashMap<Cow<'a, str>, Cow<'a, str>> = ts
                .labels
                .iter()
                .map(|l| (Cow::Owned(l.name.clone()), Cow::Owned(l.value.clone())))
                .collect();

            // Each sample becomes a point
            for sample in &ts.samples {
                let mut fields = HashMap::new();
                fields.insert(
                    Cow::Owned(self.default_field_name.clone()),
                    FieldValue::Float(sample.value),
                );

                points.push(ParsedPoint {
                    measurement: measurement.clone(),
                    tags: tags.clone(),
                    fields,
                    timestamp: if sample.timestamp != 0 {
                        Some(sample.timestamp)
                    } else {
                        None
                    },
                });
            }
        }

        Ok(points)
    }
}

impl<'a> ProtocolParser<'a> for ProtobufParser {
    fn parse(&self, input: &'a [u8]) -> Result<Vec<ParsedPoint<'a>>, ParseError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        // Try to decode as WriteRequest
        let request = WriteRequest::decode(input).map_err(|e| {
            ParseError::new(ParseErrorKind::InvalidSyntax {
                message: format!("protobuf decode error: {}", e),
            })
        })?;

        self.parse_write_request(&request)
    }

    fn parse_single(&self, input: &'a [u8]) -> Result<ParsedPoint<'a>, ParseError> {
        let points = self.parse(input)?;
        points.into_iter().next().ok_or_else(|| {
            ParseError::new(ParseErrorKind::MissingFields)
                .with_context("no points in protobuf message")
        })
    }

    fn protocol_name(&self) -> &'static str {
        "protobuf"
    }

    fn can_parse(&self, input: &[u8]) -> bool {
        // Protobuf messages typically start with a field tag
        // Field 1 with wire type 2 (length-delimited) = 0x0A
        // This is a heuristic - not 100% accurate
        if input.is_empty() {
            return false;
        }

        // Check for common protobuf field tags
        matches!(input[0], 0x0A | 0x08 | 0x10 | 0x12 | 0x1A)
    }
}

// Protobuf message definitions using prost
// These match a common time series wire format

/// Write request containing multiple time series
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    /// List of time series to write
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// A single time series with labels and samples
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    /// Measurement/metric name
    #[prost(string, tag = "1")]
    pub measurement: String,
    /// Labels (tags) for this time series
    #[prost(message, repeated, tag = "2")]
    pub labels: Vec<Label>,
    /// Sample data points
    #[prost(message, repeated, tag = "3")]
    pub samples: Vec<Sample>,
}

/// A label (tag) key-value pair
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    /// Label name (tag key)
    #[prost(string, tag = "1")]
    pub name: String,
    /// Label value (tag value)
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A single sample (data point)
#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    /// Sample value (float64)
    #[prost(double, tag = "1")]
    pub value: f64,
    /// Timestamp in nanoseconds
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

impl WriteRequest {
    /// Create a new empty write request
    pub fn new() -> Self {
        Self {
            timeseries: Vec::new(),
        }
    }

    /// Add a time series to the request
    pub fn add_timeseries(mut self, ts: TimeSeries) -> Self {
        self.timeseries.push(ts);
        self
    }

    /// Encode to protobuf bytes
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).expect("encoding should not fail");
        buf
    }
}

impl TimeSeries {
    /// Create a new time series with the given measurement name
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            labels: Vec::new(),
            samples: Vec::new(),
        }
    }

    /// Add a label (tag) to the time series
    pub fn add_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push(Label {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    /// Add a sample to the time series
    pub fn add_sample(mut self, value: f64, timestamp: i64) -> Self {
        self.samples.push(Sample { value, timestamp });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_request() -> WriteRequest {
        WriteRequest::new().add_timeseries(
            TimeSeries::new("cpu")
                .add_label("host", "server01")
                .add_label("region", "us-east")
                .add_sample(42.5, 1234567890000000000),
        )
    }

    #[test]
    fn test_parse_single_timeseries() {
        let parser = ProtobufParser::new();
        let request = create_test_request();
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].measurement, "cpu");
        assert_eq!(
            points[0].tags.get("host").map(|v| v.as_ref()),
            Some("server01")
        );
        assert_eq!(points[0].timestamp, Some(1234567890000000000));
    }

    #[test]
    fn test_parse_multiple_samples() {
        let parser = ProtobufParser::new();
        let request = WriteRequest::new().add_timeseries(
            TimeSeries::new("cpu")
                .add_sample(42.5, 1000000000)
                .add_sample(43.5, 2000000000)
                .add_sample(44.5, 3000000000),
        );
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].timestamp, Some(1000000000));
        assert_eq!(points[1].timestamp, Some(2000000000));
        assert_eq!(points[2].timestamp, Some(3000000000));
    }

    #[test]
    fn test_parse_multiple_timeseries() {
        let parser = ProtobufParser::new();
        let request = WriteRequest::new()
            .add_timeseries(TimeSeries::new("cpu").add_sample(42.5, 1000))
            .add_timeseries(TimeSeries::new("memory").add_sample(1024.0, 1000));
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].measurement, "cpu");
        assert_eq!(points[1].measurement, "memory");
    }

    #[test]
    fn test_custom_field_name() {
        let parser = ProtobufParser::new().with_field_name("temperature");
        let request =
            WriteRequest::new().add_timeseries(TimeSeries::new("sensor").add_sample(25.5, 1000));
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        assert!(points[0].fields.contains_key("temperature"));
        assert!(!points[0].fields.contains_key("value"));
    }

    #[test]
    fn test_empty_input() {
        let parser = ProtobufParser::new();
        let points = parser.parse(&[]).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn test_missing_measurement() {
        let parser = ProtobufParser::new();
        let request =
            WriteRequest::new().add_timeseries(TimeSeries::new("").add_sample(42.5, 1000));
        let encoded = request.encode_to_vec();

        let result = parser.parse(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_timestamp() {
        let parser = ProtobufParser::new();
        let request =
            WriteRequest::new().add_timeseries(TimeSeries::new("cpu").add_sample(42.5, 0));
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        // Zero timestamp should be treated as None
        assert_eq!(points[0].timestamp, None);
    }

    #[test]
    fn test_invalid_protobuf() {
        let parser = ProtobufParser::new();
        let result = parser.parse(b"not valid protobuf data");
        assert!(result.is_err());
    }

    #[test]
    fn test_can_parse() {
        let parser = ProtobufParser::new();

        // Valid protobuf typically starts with field tags
        assert!(parser.can_parse(&[0x0A, 0x10]));
        assert!(parser.can_parse(&[0x08, 0x01]));

        // Empty input
        assert!(!parser.can_parse(&[]));

        // Text-like input
        assert!(!parser.can_parse(b"cpu value=42"));
    }

    #[test]
    fn test_protocol_name() {
        let parser = ProtobufParser::new();
        assert_eq!(parser.protocol_name(), "protobuf");
    }

    #[test]
    fn test_parse_single() {
        let parser = ProtobufParser::new();
        let request = create_test_request();
        let encoded = request.encode_to_vec();

        let point = parser.parse_single(&encoded).unwrap();
        assert_eq!(point.measurement, "cpu");
    }

    #[test]
    fn test_labels_preserved() {
        let parser = ProtobufParser::new();
        let request = WriteRequest::new().add_timeseries(
            TimeSeries::new("metric")
                .add_label("env", "production")
                .add_label("service", "api")
                .add_label("version", "1.0.0")
                .add_sample(100.0, 1000),
        );
        let encoded = request.encode_to_vec();

        let points = parser.parse(&encoded).unwrap();
        assert_eq!(points[0].tags.len(), 3);
        assert_eq!(
            points[0].tags.get("env").map(|v| v.as_ref()),
            Some("production")
        );
        assert_eq!(
            points[0].tags.get("service").map(|v| v.as_ref()),
            Some("api")
        );
        assert_eq!(
            points[0].tags.get("version").map(|v| v.as_ref()),
            Some("1.0.0")
        );
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = WriteRequest::new()
            .add_timeseries(
                TimeSeries::new("cpu")
                    .add_label("host", "server01")
                    .add_sample(42.5, 1234567890),
            )
            .add_timeseries(
                TimeSeries::new("memory")
                    .add_label("host", "server01")
                    .add_sample(1024.0, 1234567890),
            );

        let encoded = original.encode_to_vec();
        let decoded = WriteRequest::decode(&encoded[..]).unwrap();

        assert_eq!(original, decoded);
    }
}
