//! Line Protocol Parser
//!
//! Implements a high-performance, zero-copy parser for InfluxDB Line Protocol.
//!
//! # Format
//!
//! ```text
//! <measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]
//! ```
//!
//! # Examples
//!
//! ```text
//! cpu,host=server01,region=us-west usage=0.64 1234567890000000000
//! temperature,sensor=sensor1 value=25.5
//! events message="system started",code=200i 1234567890
//! ```
//!
//! # Escaping
//!
//! - Measurement: escape `,`, ` ` (space)
//! - Tag keys/values: escape `,`, `=`, ` ` (space)
//! - Field keys: escape `,`, `=`, ` ` (space)
//! - Field string values: escape `"`, `\`
//!
//! # Performance
//!
//! This parser is designed for high throughput:
//! - Zero-copy string references where possible (using Cow)
//! - Single-pass parsing
//! - Minimal allocations
//! - SIMD-friendly byte scanning (future optimization)

use std::borrow::Cow;
use std::collections::HashMap;

use super::error::{ParseError, ParseErrorKind};
use super::{FieldValue, ParsedPoint, ProtocolParser};

/// Line protocol parser configuration
#[derive(Debug, Clone)]
pub struct LineProtocolConfig {
    /// Maximum line length in bytes (default: 64KB)
    pub max_line_length: usize,
    /// Maximum number of tags per point (default: 256)
    pub max_tags: usize,
    /// Maximum number of fields per point (default: 256)
    pub max_fields: usize,
    /// Whether to allow timestamps in the past (default: true)
    pub allow_past_timestamps: bool,
    /// Whether to allow timestamps in the future (default: true)
    pub allow_future_timestamps: bool,
}

impl Default for LineProtocolConfig {
    fn default() -> Self {
        Self {
            max_line_length: 64 * 1024, // 64 KB
            max_tags: 256,
            max_fields: 256,
            allow_past_timestamps: true,
            allow_future_timestamps: true,
        }
    }
}

/// Parsed line protocol line (before conversion to ParsedPoint)
///
/// This intermediate representation preserves the exact byte positions
/// for zero-copy string references.
#[derive(Debug, Clone)]
pub struct ParsedLine<'a> {
    /// The measurement name
    pub measurement: Cow<'a, str>,
    /// Tag key-value pairs
    pub tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
    /// Field key-value pairs
    pub fields: Vec<(Cow<'a, str>, FieldValue)>,
    /// Optional timestamp
    pub timestamp: Option<i64>,
}

impl<'a> ParsedLine<'a> {
    /// Convert to ParsedPoint
    pub fn into_point(self) -> ParsedPoint<'a> {
        let mut tags = HashMap::with_capacity(self.tags.len());
        for (k, v) in self.tags {
            tags.insert(k, v);
        }

        let mut fields = HashMap::with_capacity(self.fields.len());
        for (k, v) in self.fields {
            fields.insert(k, v);
        }

        ParsedPoint {
            measurement: self.measurement,
            tags,
            fields,
            timestamp: self.timestamp,
        }
    }
}

/// High-performance Line Protocol parser
///
/// Parses InfluxDB Line Protocol format with zero-copy semantics
/// where possible.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::ingestion::protocol::{LineProtocolParser, ProtocolParser};
///
/// let parser = LineProtocolParser::new();
/// let input = b"cpu,host=server01 value=42.0 1234567890";
/// let points = parser.parse(input).unwrap();
/// assert_eq!(points.len(), 1);
/// assert_eq!(points[0].measurement, "cpu");
/// ```
#[derive(Debug, Clone)]
pub struct LineProtocolParser {
    config: LineProtocolConfig,
}

impl LineProtocolParser {
    /// Create a new parser with default configuration
    pub fn new() -> Self {
        Self {
            config: LineProtocolConfig::default(),
        }
    }

    /// Create a new parser with custom configuration
    pub fn with_config(config: LineProtocolConfig) -> Self {
        Self { config }
    }

    /// Parse a single line into a ParsedLine structure
    pub fn parse_line<'a>(&self, line: &'a str) -> Result<ParsedLine<'a>, ParseError> {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            return Err(ParseError::new(ParseErrorKind::EmptyInput));
        }

        // Check line length
        if line.len() > self.config.max_line_length {
            return Err(ParseError::new(ParseErrorKind::InputTooLarge));
        }

        let mut parser = LineParser::new(line);

        // Parse measurement (required)
        let measurement = parser.parse_measurement()?;

        // Parse tags (optional, starts with comma after measurement)
        let tags = if parser.peek() == Some(',') {
            parser.advance(); // consume comma
            parser.parse_tags(self.config.max_tags)?
        } else {
            Vec::new()
        };

        // Expect space before fields
        if parser.peek() != Some(' ') {
            return Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                char: parser.peek().unwrap_or('\0'),
                expected: "space before fields".to_string(),
            })
            .at_column(parser.position + 1));
        }
        parser.skip_whitespace();

        // Parse fields (required, at least one)
        let fields = parser.parse_fields(self.config.max_fields)?;

        if fields.is_empty() {
            return Err(ParseError::new(ParseErrorKind::MissingFields));
        }

        // Parse optional timestamp
        parser.skip_whitespace();
        let timestamp = if !parser.is_eof() {
            Some(parser.parse_timestamp()?)
        } else {
            None
        };

        Ok(ParsedLine {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }
}

impl Default for LineProtocolParser {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> ProtocolParser<'a> for LineProtocolParser {
    fn parse(&self, input: &'a [u8]) -> Result<Vec<ParsedPoint<'a>>, ParseError> {
        // Convert to string, validating UTF-8
        let input_str =
            std::str::from_utf8(input).map_err(|_| ParseError::new(ParseErrorKind::InvalidUtf8))?;

        let mut points = Vec::new();

        for (line_num, line) in input_str.lines().enumerate() {
            match self.parse_line(line) {
                Ok(parsed) => points.push(parsed.into_point()),
                Err(e) if matches!(e.kind, ParseErrorKind::EmptyInput) => {
                    // Skip empty lines silently
                    continue;
                },
                Err(e) => {
                    return Err(e.at_line(line_num + 1));
                },
            }
        }

        Ok(points)
    }

    fn parse_single(&self, input: &'a [u8]) -> Result<ParsedPoint<'a>, ParseError> {
        let input_str =
            std::str::from_utf8(input).map_err(|_| ParseError::new(ParseErrorKind::InvalidUtf8))?;

        self.parse_line(input_str).map(|l| l.into_point())
    }

    fn protocol_name(&self) -> &'static str {
        "line-protocol"
    }

    fn can_parse(&self, input: &[u8]) -> bool {
        // Line protocol heuristic:
        // - Usually starts with alphanumeric (measurement name)
        // - Contains spaces (between tags/fields/timestamp)
        // - Contains = signs (tag=value, field=value)
        // - Does NOT start with { or [ (would be JSON)
        // - Is valid UTF-8

        if input.is_empty() {
            return false;
        }

        // Quick checks
        let first = input[0];
        if first == b'{' || first == b'[' || first == 0x08 {
            return false; // JSON or Protobuf
        }

        // Check for UTF-8 and presence of = and space
        if let Ok(s) = std::str::from_utf8(&input[..input.len().min(200)]) {
            s.contains('=') && s.contains(' ')
        } else {
            false
        }
    }
}

/// Internal parser state machine for line protocol
struct LineParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> LineParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    /// Peek at the next character without consuming it
    fn peek(&self) -> Option<char> {
        self.input[self.position..].chars().next()
    }

    /// Advance position by one character
    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.position += c.len_utf8();
        }
    }

    /// Check if we're at end of input
    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Skip whitespace characters
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c == ' ' || c == '\t' {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Parse measurement name (up to first comma or space)
    fn parse_measurement(&mut self) -> Result<Cow<'a, str>, ParseError> {
        let start = self.position;
        let mut needs_unescape = false;

        while let Some(c) = self.peek() {
            match c {
                '\\' => {
                    needs_unescape = true;
                    self.advance();
                    if self.peek().is_some() {
                        self.advance();
                    }
                },
                ',' | ' ' => break,
                _ => self.advance(),
            }
        }

        let end = self.position;
        if start == end {
            return Err(ParseError::new(ParseErrorKind::MissingMeasurement));
        }

        let slice = &self.input[start..end];
        if needs_unescape {
            Ok(Cow::Owned(unescape_identifier(slice)))
        } else {
            Ok(Cow::Borrowed(slice))
        }
    }

    /// Parse tag set (comma-separated key=value pairs)
    #[allow(clippy::type_complexity)]
    fn parse_tags(
        &mut self,
        max_tags: usize,
    ) -> Result<Vec<(Cow<'a, str>, Cow<'a, str>)>, ParseError> {
        let mut tags = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        loop {
            // Parse tag key
            let (key, key_unescaped) = self.parse_identifier_until(&['=', ',', ' '])?;

            if self.peek() != Some('=') {
                return Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                    char: self.peek().unwrap_or('\0'),
                    expected: "= in tag".to_string(),
                })
                .at_column(self.position + 1));
            }
            self.advance(); // consume '='

            // Parse tag value
            let (value, value_unescaped) = self.parse_identifier_until(&[',', ' '])?;

            // Check for duplicate
            let key_str = if key_unescaped {
                unescape_identifier(key)
            } else {
                key.to_string()
            };

            if !seen_keys.insert(key_str.clone()) {
                return Err(ParseError::new(ParseErrorKind::DuplicateTag {
                    tag: key_str,
                }));
            }

            let key_cow: Cow<'a, str> = if key_unescaped {
                Cow::Owned(key_str)
            } else {
                Cow::Borrowed(key)
            };

            let value_cow: Cow<'a, str> = if value_unescaped {
                Cow::Owned(unescape_identifier(value))
            } else {
                Cow::Borrowed(value)
            };

            tags.push((key_cow, value_cow));

            if tags.len() > max_tags {
                return Err(ParseError::new(ParseErrorKind::InputTooLarge));
            }

            // Check for more tags or end of tag set
            match self.peek() {
                Some(',') => self.advance(),
                Some(' ') | None => break,
                Some(c) => {
                    return Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                        char: c,
                        expected: "comma or space after tag".to_string(),
                    })
                    .at_column(self.position + 1));
                },
            }
        }

        Ok(tags)
    }

    /// Parse field set (comma-separated key=value pairs)
    fn parse_fields(
        &mut self,
        max_fields: usize,
    ) -> Result<Vec<(Cow<'a, str>, FieldValue)>, ParseError> {
        let mut fields = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        loop {
            // Parse field key
            let (key, key_unescaped) = self.parse_identifier_until(&['=', ',', ' '])?;

            if self.peek() != Some('=') {
                return Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                    char: self.peek().unwrap_or('\0'),
                    expected: "= in field".to_string(),
                })
                .at_column(self.position + 1));
            }
            self.advance(); // consume '='

            // Parse field value
            let value = self.parse_field_value()?;

            // Check for duplicate
            let key_str = if key_unescaped {
                unescape_identifier(key)
            } else {
                key.to_string()
            };

            if !seen_keys.insert(key_str.clone()) {
                return Err(ParseError::new(ParseErrorKind::DuplicateField {
                    field: key_str,
                }));
            }

            let key_cow: Cow<'a, str> = if key_unescaped {
                Cow::Owned(key_str)
            } else {
                Cow::Borrowed(key)
            };

            fields.push((key_cow, value));

            if fields.len() > max_fields {
                return Err(ParseError::new(ParseErrorKind::InputTooLarge));
            }

            // Check for more fields or end of field set
            match self.peek() {
                Some(',') => self.advance(),
                Some(' ') | None => break,
                Some(c) => {
                    return Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                        char: c,
                        expected: "comma or space after field".to_string(),
                    })
                    .at_column(self.position + 1));
                },
            }
        }

        Ok(fields)
    }

    /// Parse an identifier (tag/field key or tag value)
    /// Returns the slice and whether it needs unescaping
    fn parse_identifier_until(
        &mut self,
        terminators: &[char],
    ) -> Result<(&'a str, bool), ParseError> {
        let start = self.position;
        let mut needs_unescape = false;

        while let Some(c) = self.peek() {
            if terminators.contains(&c) {
                break;
            }
            if c == '\\' {
                needs_unescape = true;
                self.advance();
                if self.peek().is_some() {
                    self.advance();
                }
            } else {
                self.advance();
            }
        }

        let slice = &self.input[start..self.position];
        if slice.is_empty() {
            return Err(ParseError::new(ParseErrorKind::SyntaxError {
                message: "empty identifier".to_string(),
            })
            .at_column(self.position + 1));
        }

        Ok((slice, needs_unescape))
    }

    /// Parse a field value (various types)
    fn parse_field_value(&mut self) -> Result<FieldValue, ParseError> {
        match self.peek() {
            // String value
            Some('"') => self.parse_string_value(),
            // Boolean true
            Some('t') | Some('T') => self.parse_boolean_true(),
            // Boolean false
            Some('f') | Some('F') => self.parse_boolean_false(),
            // Number (integer, unsigned, or float)
            Some(c) if c == '-' || c == '+' || c == '.' || c.is_ascii_digit() => {
                self.parse_number_value()
            },
            Some(c) => Err(ParseError::new(ParseErrorKind::UnexpectedChar {
                char: c,
                expected: "field value".to_string(),
            })
            .at_column(self.position + 1)),
            None => Err(ParseError::new(ParseErrorKind::UnexpectedEof)),
        }
    }

    /// Parse a string field value (double-quoted)
    fn parse_string_value(&mut self) -> Result<FieldValue, ParseError> {
        assert_eq!(self.peek(), Some('"'));
        self.advance(); // consume opening quote

        let mut value = String::new();

        loop {
            match self.peek() {
                Some('"') => {
                    self.advance(); // consume closing quote
                    break;
                },
                Some('\\') => {
                    self.advance(); // consume backslash
                    match self.peek() {
                        Some('"') => {
                            value.push('"');
                            self.advance();
                        },
                        Some('\\') => {
                            value.push('\\');
                            self.advance();
                        },
                        Some('n') => {
                            value.push('\n');
                            self.advance();
                        },
                        Some('r') => {
                            value.push('\r');
                            self.advance();
                        },
                        Some('t') => {
                            value.push('\t');
                            self.advance();
                        },
                        Some(c) => {
                            return Err(ParseError::new(ParseErrorKind::InvalidEscape {
                                sequence: format!("\\{}", c),
                            })
                            .at_column(self.position));
                        },
                        None => {
                            return Err(ParseError::new(ParseErrorKind::UnterminatedString));
                        },
                    }
                },
                Some(c) => {
                    value.push(c);
                    self.advance();
                },
                None => {
                    return Err(ParseError::new(ParseErrorKind::UnterminatedString));
                },
            }
        }

        Ok(FieldValue::String(value))
    }

    /// Parse boolean true value
    fn parse_boolean_true(&mut self) -> Result<FieldValue, ParseError> {
        let start = self.position;

        // Consume characters
        while let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() {
                self.advance();
            } else {
                break;
            }
        }

        let value = &self.input[start..self.position];
        match value.to_lowercase().as_str() {
            "t" | "true" => Ok(FieldValue::Boolean(true)),
            _ => Err(ParseError::new(ParseErrorKind::InvalidFieldValue {
                field: "".to_string(),
                reason: format!("invalid boolean: {}", value),
            })
            .at_column(start + 1)),
        }
    }

    /// Parse boolean false value
    fn parse_boolean_false(&mut self) -> Result<FieldValue, ParseError> {
        let start = self.position;

        // Consume characters
        while let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() {
                self.advance();
            } else {
                break;
            }
        }

        let value = &self.input[start..self.position];
        match value.to_lowercase().as_str() {
            "f" | "false" => Ok(FieldValue::Boolean(false)),
            _ => Err(ParseError::new(ParseErrorKind::InvalidFieldValue {
                field: "".to_string(),
                reason: format!("invalid boolean: {}", value),
            })
            .at_column(start + 1)),
        }
    }

    /// Parse numeric value (float, integer, or unsigned integer)
    fn parse_number_value(&mut self) -> Result<FieldValue, ParseError> {
        let start = self.position;

        // Parse the number part
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
                self.advance();
            } else {
                break;
            }
        }

        let num_str = &self.input[start..self.position];

        // Check for type suffix
        match self.peek() {
            Some('i') => {
                self.advance();
                // Signed integer
                num_str
                    .parse::<i64>()
                    .map(FieldValue::Integer)
                    .map_err(|_| {
                        ParseError::new(ParseErrorKind::InvalidFieldValue {
                            field: "".to_string(),
                            reason: format!("invalid integer: {}", num_str),
                        })
                        .at_column(start + 1)
                    })
            },
            Some('u') => {
                self.advance();
                // Unsigned integer
                num_str
                    .parse::<u64>()
                    .map(FieldValue::UInteger)
                    .map_err(|_| {
                        ParseError::new(ParseErrorKind::InvalidFieldValue {
                            field: "".to_string(),
                            reason: format!("invalid unsigned integer: {}", num_str),
                        })
                        .at_column(start + 1)
                    })
            },
            _ => {
                // Float (default)
                num_str.parse::<f64>().map(FieldValue::Float).map_err(|_| {
                    ParseError::new(ParseErrorKind::InvalidFieldValue {
                        field: "".to_string(),
                        reason: format!("invalid float: {}", num_str),
                    })
                    .at_column(start + 1)
                })
            },
        }
    }

    /// Parse timestamp
    fn parse_timestamp(&mut self) -> Result<i64, ParseError> {
        let start = self.position;

        // Optional sign
        if self.peek() == Some('-') {
            self.advance();
        }

        // Digits
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }

        let ts_str = &self.input[start..self.position];
        ts_str.parse::<i64>().map_err(|_| {
            ParseError::new(ParseErrorKind::InvalidTimestamp {
                value: ts_str.to_string(),
            })
            .at_column(start + 1)
        })
    }
}

/// Unescape an identifier (measurement, tag key/value, field key)
fn unescape_identifier(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some(&',') | Some(&'=') | Some(&' ') | Some(&'\\') => {
                    result.push(chars.next().unwrap());
                },
                _ => result.push(c),
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_line() {
        let parser = LineProtocolParser::new();
        let line = "cpu,host=server01 value=42.0 1234567890";

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.measurement, "cpu");
        assert_eq!(result.tags.len(), 1);
        assert_eq!(result.tags[0].0, "host");
        assert_eq!(result.tags[0].1, "server01");
        assert_eq!(result.fields.len(), 1);
        assert!(
            matches!(result.fields[0].1, FieldValue::Float(v) if (v - 42.0).abs() < f64::EPSILON)
        );
        assert_eq!(result.timestamp, Some(1234567890));
    }

    #[test]
    fn test_parse_no_tags() {
        let parser = LineProtocolParser::new();
        let line = "cpu value=42.0";

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.measurement, "cpu");
        assert!(result.tags.is_empty());
        assert_eq!(result.fields.len(), 1);
        assert!(result.timestamp.is_none());
    }

    #[test]
    fn test_parse_multiple_tags() {
        let parser = LineProtocolParser::new();
        let line = "cpu,host=server01,region=us-west,env=prod value=42.0";

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.tags.len(), 3);
    }

    #[test]
    fn test_parse_multiple_fields() {
        let parser = LineProtocolParser::new();
        let line = "cpu usage=0.64,temperature=75.5,count=42i";

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.fields.len(), 3);
        assert!(matches!(result.fields[0].1, FieldValue::Float(_)));
        assert!(matches!(result.fields[1].1, FieldValue::Float(_)));
        assert!(matches!(result.fields[2].1, FieldValue::Integer(42)));
    }

    #[test]
    fn test_parse_string_field() {
        let parser = LineProtocolParser::new();
        let line = r#"events message="hello world",status="ok""#;

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.fields.len(), 2);
        assert!(matches!(&result.fields[0].1, FieldValue::String(s) if s == "hello world"));
        assert!(matches!(&result.fields[1].1, FieldValue::String(s) if s == "ok"));
    }

    #[test]
    fn test_parse_escaped_string() {
        let parser = LineProtocolParser::new();
        let line = r#"events message="hello \"world\"" 1234567890"#;

        let result = parser.parse_line(line).unwrap();

        assert!(matches!(&result.fields[0].1, FieldValue::String(s) if s == r#"hello "world""#));
    }

    #[test]
    fn test_parse_boolean_fields() {
        let parser = LineProtocolParser::new();
        let line = "status active=true,enabled=false,on=T,off=F";

        let result = parser.parse_line(line).unwrap();

        assert!(matches!(result.fields[0].1, FieldValue::Boolean(true)));
        assert!(matches!(result.fields[1].1, FieldValue::Boolean(false)));
        assert!(matches!(result.fields[2].1, FieldValue::Boolean(true)));
        assert!(matches!(result.fields[3].1, FieldValue::Boolean(false)));
    }

    #[test]
    fn test_parse_integer_field() {
        let parser = LineProtocolParser::new();
        let line = "cpu count=42i,negative=-10i";

        let result = parser.parse_line(line).unwrap();

        assert!(matches!(result.fields[0].1, FieldValue::Integer(42)));
        assert!(matches!(result.fields[1].1, FieldValue::Integer(-10)));
    }

    #[test]
    fn test_parse_unsigned_integer_field() {
        let parser = LineProtocolParser::new();
        let line = "cpu count=42u";

        let result = parser.parse_line(line).unwrap();

        assert!(matches!(result.fields[0].1, FieldValue::UInteger(42)));
    }

    #[test]
    fn test_parse_scientific_notation() {
        let parser = LineProtocolParser::new();
        let line = "measurements value=1.5e10";

        let result = parser.parse_line(line).unwrap();

        assert!(matches!(result.fields[0].1, FieldValue::Float(v) if (v - 1.5e10).abs() < 1.0));
    }

    #[test]
    fn test_parse_escaped_tag() {
        let parser = LineProtocolParser::new();
        let line = r"cpu,host=server\ 01,path=/var/log value=42.0";

        let result = parser.parse_line(line).unwrap();

        assert_eq!(result.tags[0].1, "server 01");
    }

    #[test]
    fn test_parse_multiple_lines() {
        let parser = LineProtocolParser::new();
        let input = b"cpu,host=a value=1.0 1234567890\ncpu,host=b value=2.0 1234567891\n";

        let points = parser.parse(input).unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].tags.get("host").map(|v| v.as_ref()), Some("a"));
        assert_eq!(points[1].tags.get("host").map(|v| v.as_ref()), Some("b"));
    }

    #[test]
    fn test_parse_empty_input() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line("");

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::EmptyInput
        ));
    }

    #[test]
    fn test_parse_comment() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line("# this is a comment");

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::EmptyInput
        ));
    }

    #[test]
    fn test_parse_missing_fields() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line("cpu,host=server01");

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::UnexpectedChar { .. }
        ));
    }

    #[test]
    fn test_parse_duplicate_tag() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line("cpu,host=a,host=b value=1.0");

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::DuplicateTag { .. }
        ));
    }

    #[test]
    fn test_parse_duplicate_field() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line("cpu value=1.0,value=2.0");

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::DuplicateField { .. }
        ));
    }

    #[test]
    fn test_can_parse_detection() {
        let parser = LineProtocolParser::new();

        // Should detect line protocol
        assert!(parser.can_parse(b"cpu,host=a value=1.0"));
        assert!(parser.can_parse(b"measurement field=1"));

        // Should not detect JSON
        assert!(!parser.can_parse(b"{\"measurement\": \"cpu\"}"));
        assert!(!parser.can_parse(b"[{\"value\": 1}]"));

        // Should not detect empty
        assert!(!parser.can_parse(b""));
    }

    #[test]
    fn test_parsed_line_into_point() {
        let parser = LineProtocolParser::new();
        let line = "cpu,host=server01 value=42.0 1234567890";

        let parsed = parser.parse_line(line).unwrap();
        let point = parsed.into_point();

        assert_eq!(point.measurement, "cpu");
        assert!(point.is_valid());
        assert_eq!(point.series_key(), "cpu,host=server01");
    }

    #[test]
    fn test_parse_negative_timestamp() {
        let parser = LineProtocolParser::new();
        let line = "cpu value=42.0 -1234567890";

        let result = parser.parse_line(line).unwrap();
        assert_eq!(result.timestamp, Some(-1234567890));
    }

    #[test]
    fn test_unterminated_string() {
        let parser = LineProtocolParser::new();
        let result = parser.parse_line(r#"events message="unterminated"#);

        assert!(matches!(
            result.unwrap_err().kind,
            ParseErrorKind::UnterminatedString
        ));
    }
}
