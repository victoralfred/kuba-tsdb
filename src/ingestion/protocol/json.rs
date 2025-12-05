//! JSON protocol parser
//!
//! Parses JSON-formatted data points for ingestion. Supports both
//! single point objects and arrays of points.
//!
//! # JSON Format
//!
//! Single point:
//! ```json
//! {
//!   "measurement": "cpu",
//!   "tags": {"host": "server01", "region": "us-east"},
//!   "fields": {"value": 42.5, "count": 100},
//!   "timestamp": 1234567890000000000
//! }
//! ```
//!
//! Multiple points:
//! ```json
//! [
//!   {"measurement": "cpu", "fields": {"value": 42.5}},
//!   {"measurement": "memory", "fields": {"used": 1024}}
//! ]
//! ```
//!
//! # Example
//!
//! ```rust
//! use kuba_tsdb::ingestion::protocol::{JsonParser, ProtocolParser};
//!
//! let parser = JsonParser::new();
//! let input = br#"{"measurement": "cpu", "fields": {"value": 42.5}}"#;
//! let points = parser.parse(input).unwrap();
//! assert_eq!(points.len(), 1);
//! ```

use std::borrow::Cow;
use std::collections::HashMap;

use serde::Deserialize;

use super::error::{ParseError, ParseErrorKind};
use super::{FieldValue, ParsedPoint, ProtocolParser};

/// Maximum number of points allowed in a single JSON array (DoS protection)
const MAX_ARRAY_POINTS: usize = 100_000;

/// Maximum number of tags per point (DoS protection)
const MAX_TAGS_PER_POINT: usize = 256;

/// Maximum number of fields per point (DoS protection)
const MAX_FIELDS_PER_POINT: usize = 256;

/// JSON protocol parser
///
/// Parses JSON-formatted time series data into ParsedPoints.
/// Supports both single objects and arrays of objects.
///
/// # Size Limits
///
/// To prevent DoS attacks, the parser enforces the following limits:
/// - Maximum 100,000 points per array
/// - Maximum 256 tags per point
/// - Maximum 256 fields per point
#[derive(Debug, Clone, Default)]
pub struct JsonParser {
    /// Whether to allow missing timestamps (default: true)
    allow_missing_timestamp: bool,
    /// Whether to allow missing tags (default: true)
    /// When false, points must have at least one tag defined
    allow_missing_tags: bool,
    /// Default measurement name if not specified
    default_measurement: Option<String>,
    /// Maximum number of points in array (default: 100,000)
    max_array_size: usize,
}

impl JsonParser {
    /// Create a new JSON parser with default settings
    pub fn new() -> Self {
        Self {
            allow_missing_timestamp: true,
            allow_missing_tags: true,
            default_measurement: None,
            max_array_size: MAX_ARRAY_POINTS,
        }
    }

    /// Create a parser that requires timestamps
    pub fn require_timestamp(mut self) -> Self {
        self.allow_missing_timestamp = false;
        self
    }

    /// Create a parser that requires tags
    ///
    /// When enabled, points without tags (or with empty tags) will be rejected.
    /// This is useful for enforcing proper metric labeling in production.
    pub fn require_tags(mut self) -> Self {
        self.allow_missing_tags = false;
        self
    }

    /// Set a default measurement name for points without one
    pub fn with_default_measurement(mut self, measurement: impl Into<String>) -> Self {
        self.default_measurement = Some(measurement.into());
        self
    }

    /// Set maximum array size (for DoS protection)
    pub fn with_max_array_size(mut self, max_size: usize) -> Self {
        self.max_array_size = max_size;
        self
    }

    /// Validate that tags are present if required
    fn validate_tags(&self, tags: &HashMap<Cow<'_, str>, Cow<'_, str>>) -> Result<(), ParseError> {
        if !self.allow_missing_tags && tags.is_empty() {
            return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                message: "tags are required but none were provided".to_string(),
            }));
        }
        Ok(())
    }

    /// Parse a JSON value into a ParsedPoint
    fn parse_json_point<'a>(&self, json: &JsonPoint) -> Result<ParsedPoint<'a>, ParseError> {
        // Get measurement name
        let measurement: Cow<'a, str> = match &json.measurement {
            Some(m) => Cow::Owned(m.clone()),
            None => match &self.default_measurement {
                Some(default) => Cow::Owned(default.clone()),
                None => {
                    return Err(ParseError::new(ParseErrorKind::MissingMeasurement)
                        .with_context("JSON point missing 'measurement' field"))
                },
            },
        };

        // Validate measurement
        if measurement.is_empty() {
            return Err(ParseError::new(ParseErrorKind::EmptyMeasurement)
                .with_context("measurement cannot be empty"));
        }

        // Parse tags with size limit check
        let tags: HashMap<Cow<'a, str>, Cow<'a, str>> = match &json.tags {
            Some(t) => {
                if t.len() > MAX_TAGS_PER_POINT {
                    return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                        message: format!(
                            "too many tags: {} exceeds maximum {}",
                            t.len(),
                            MAX_TAGS_PER_POINT
                        ),
                    }));
                }
                t.iter()
                    .map(|(k, v)| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                    .collect()
            },
            None => HashMap::new(),
        };

        // Validate tags are present if required
        self.validate_tags(&tags)?;

        // Parse fields (required) with size limit check
        let fields: HashMap<Cow<'a, str>, FieldValue> = match &json.fields {
            Some(f) if !f.is_empty() => {
                if f.len() > MAX_FIELDS_PER_POINT {
                    return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                        message: format!(
                            "too many fields: {} exceeds maximum {}",
                            f.len(),
                            MAX_FIELDS_PER_POINT
                        ),
                    }));
                }
                f.iter()
                    .map(|(k, v)| (Cow::Owned(k.clone()), convert_json_value(v)))
                    .collect()
            },
            _ => {
                return Err(ParseError::new(ParseErrorKind::MissingFields)
                    .with_context("at least one field required"))
            },
        };

        // Parse timestamp
        let timestamp = json.timestamp;
        if !self.allow_missing_timestamp && timestamp.is_none() {
            return Err(ParseError::new(ParseErrorKind::InvalidTimestamp {
                value: "missing".to_string(),
            })
            .with_context("timestamp is required"));
        }

        Ok(ParsedPoint {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }
}

impl<'a> ProtocolParser<'a> for JsonParser {
    fn parse(&self, input: &'a [u8]) -> Result<Vec<ParsedPoint<'a>>, ParseError> {
        // Convert to string
        let input_str = std::str::from_utf8(input).map_err(|e| {
            ParseError::new(ParseErrorKind::InvalidUtf8)
                .with_context(format!("invalid UTF-8: {}", e))
        })?;

        // Skip whitespace
        let trimmed = input_str.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        // Detect if it's an array or single object
        if trimmed.starts_with('[') {
            // Parse as array
            let json_points: Vec<JsonPoint> = serde_json::from_str(trimmed).map_err(|e| {
                ParseError::new(ParseErrorKind::InvalidSyntax {
                    message: e.to_string(),
                })
                .at_column(e.column())
            })?;

            // Check array size limit to prevent DoS
            if json_points.len() > self.max_array_size {
                return Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                    message: format!(
                        "array too large: {} points exceeds maximum {}",
                        json_points.len(),
                        self.max_array_size
                    ),
                }));
            }

            let mut points = Vec::with_capacity(json_points.len());
            for json in &json_points {
                points.push(self.parse_json_point(json)?);
            }
            Ok(points)
        } else if trimmed.starts_with('{') {
            // Parse as single object
            let json_point: JsonPoint = serde_json::from_str(trimmed).map_err(|e| {
                ParseError::new(ParseErrorKind::InvalidSyntax {
                    message: e.to_string(),
                })
                .at_column(e.column())
            })?;

            Ok(vec![self.parse_json_point(&json_point)?])
        } else {
            Err(ParseError::new(ParseErrorKind::InvalidSyntax {
                message: "expected JSON object or array".to_string(),
            }))
        }
    }

    fn parse_single(&self, input: &'a [u8]) -> Result<ParsedPoint<'a>, ParseError> {
        let points = self.parse(input)?;
        points.into_iter().next().ok_or_else(|| {
            ParseError::new(ParseErrorKind::MissingFields).with_context("no points in input")
        })
    }

    fn protocol_name(&self) -> &'static str {
        "json"
    }

    fn can_parse(&self, input: &[u8]) -> bool {
        // Look for JSON markers
        let trimmed = input.iter().skip_while(|&&b| b.is_ascii_whitespace());
        matches!(trimmed.clone().next(), Some(b'{') | Some(b'['))
    }
}

/// Internal JSON structure for deserialization
#[derive(Debug, Deserialize)]
struct JsonPoint {
    /// Measurement name
    measurement: Option<String>,
    /// Tag key-value pairs
    tags: Option<HashMap<String, String>>,
    /// Field key-value pairs
    fields: Option<HashMap<String, serde_json::Value>>,
    /// Timestamp in nanoseconds
    timestamp: Option<i64>,
    /// Alternative: time field (alias for timestamp)
    #[serde(alias = "time")]
    _time: Option<i64>,
}

/// Convert a serde_json::Value to our FieldValue type
fn convert_json_value(value: &serde_json::Value) -> FieldValue {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Integer(i)
            } else if let Some(u) = n.as_u64() {
                FieldValue::UInteger(u)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::Float(f64::NAN)
            }
        },
        serde_json::Value::Bool(b) => FieldValue::Boolean(*b),
        serde_json::Value::String(s) => FieldValue::String(s.clone()),
        // For null/array/object, convert to string representation
        _ => FieldValue::String(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_point() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "cpu", "fields": {"value": 42.5}}"#;

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].measurement, "cpu");
        assert!(points[0].fields.contains_key("value"));
    }

    #[test]
    fn test_parse_point_with_tags() {
        let parser = JsonParser::new();
        let input = br#"{
            "measurement": "cpu",
            "tags": {"host": "server01", "region": "us-east"},
            "fields": {"value": 42.5}
        }"#;

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(
            points[0].tags.get("host").map(|v| v.as_ref()),
            Some("server01")
        );
        assert_eq!(
            points[0].tags.get("region").map(|v| v.as_ref()),
            Some("us-east")
        );
    }

    #[test]
    fn test_parse_point_with_timestamp() {
        let parser = JsonParser::new();
        let input = br#"{
            "measurement": "cpu",
            "fields": {"value": 42.5},
            "timestamp": 1234567890000000000
        }"#;

        let points = parser.parse(input).unwrap();
        assert_eq!(points[0].timestamp, Some(1234567890000000000));
    }

    #[test]
    fn test_parse_array_of_points() {
        let parser = JsonParser::new();
        let input = br#"[
            {"measurement": "cpu", "fields": {"value": 42.5}},
            {"measurement": "memory", "fields": {"used": 1024}}
        ]"#;

        let points = parser.parse(input).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].measurement, "cpu");
        assert_eq!(points[1].measurement, "memory");
    }

    #[test]
    fn test_parse_various_field_types() {
        let parser = JsonParser::new();
        let input = br#"{
            "measurement": "test",
            "fields": {
                "float_val": 42.5,
                "int_val": 100,
                "bool_val": true,
                "string_val": "hello"
            }
        }"#;

        let points = parser.parse(input).unwrap();
        let fields = &points[0].fields;

        assert!(matches!(
            fields.get("float_val"),
            Some(FieldValue::Float(_))
        ));
        assert!(matches!(
            fields.get("int_val"),
            Some(FieldValue::Integer(100))
        ));
        assert!(matches!(
            fields.get("bool_val"),
            Some(FieldValue::Boolean(true))
        ));
        assert!(matches!(
            fields.get("string_val"),
            Some(FieldValue::String(_))
        ));
    }

    #[test]
    fn test_missing_measurement_error() {
        let parser = JsonParser::new();
        let input = br#"{"fields": {"value": 42.5}}"#;

        let result = parser.parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_fields_error() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "cpu"}"#;

        let result = parser.parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_fields_error() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "cpu", "fields": {}}"#;

        let result = parser.parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_measurement() {
        let parser = JsonParser::new().with_default_measurement("default_metric");
        let input = br#"{"fields": {"value": 42.5}}"#;

        let points = parser.parse(input).unwrap();
        assert_eq!(points[0].measurement, "default_metric");
    }

    #[test]
    fn test_require_timestamp() {
        let parser = JsonParser::new().require_timestamp();
        let input = br#"{"measurement": "cpu", "fields": {"value": 42.5}}"#;

        let result = parser.parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let parser = JsonParser::new();
        let points = parser.parse(b"").unwrap();
        assert!(points.is_empty());

        let points = parser.parse(b"   ").unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn test_invalid_json() {
        let parser = JsonParser::new();
        let result = parser.parse(b"not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_can_parse() {
        let parser = JsonParser::new();

        assert!(parser.can_parse(br#"{"measurement": "cpu"}"#));
        assert!(parser.can_parse(br#"[{"measurement": "cpu"}]"#));
        assert!(parser.can_parse(b"  {"));
        assert!(!parser.can_parse(b"cpu,host=server01 value=42"));
    }

    #[test]
    fn test_protocol_name() {
        let parser = JsonParser::new();
        assert_eq!(parser.protocol_name(), "json");
    }

    #[test]
    fn test_parse_single() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "cpu", "fields": {"value": 42.5}}"#;

        let point = parser.parse_single(input).unwrap();
        assert_eq!(point.measurement, "cpu");
    }

    #[test]
    fn test_negative_integer() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "temp", "fields": {"value": -10}}"#;

        let points = parser.parse(input).unwrap();
        assert!(matches!(
            points[0].fields.get("value"),
            Some(FieldValue::Integer(-10))
        ));
    }

    #[test]
    fn test_large_numbers() {
        let parser = JsonParser::new();
        let input = br#"{"measurement": "test", "fields": {"big": 9223372036854775807}}"#;

        let points = parser.parse(input).unwrap();
        assert!(matches!(
            points[0].fields.get("big"),
            Some(FieldValue::Integer(9223372036854775807))
        ));
    }
}
