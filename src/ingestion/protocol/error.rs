//! Protocol parsing error types
//!
//! Provides detailed error types for protocol parsing failures
//! with context about the error location and nature.

use std::fmt;

/// Parse error with location and context
#[derive(Debug, Clone)]
pub struct ParseError {
    /// Kind of error that occurred
    pub kind: ParseErrorKind,
    /// Line number where error occurred (1-indexed)
    pub line: Option<usize>,
    /// Column/byte offset where error occurred (1-indexed)
    pub column: Option<usize>,
    /// The problematic input snippet (truncated if too long)
    pub context: Option<String>,
}

impl ParseError {
    /// Create a new parse error
    pub fn new(kind: ParseErrorKind) -> Self {
        Self {
            kind,
            line: None,
            column: None,
            context: None,
        }
    }

    /// Add line number to error
    pub fn at_line(mut self, line: usize) -> Self {
        self.line = Some(line);
        self
    }

    /// Add column/offset to error
    pub fn at_column(mut self, column: usize) -> Self {
        self.column = Some(column);
        self
    }

    /// Add context snippet to error
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        let ctx = context.into();
        // Truncate long context
        self.context = if ctx.len() > 50 {
            Some(format!("{}...", &ctx[..47]))
        } else {
            Some(ctx)
        };
        self
    }

    /// Check if this is a validation error
    pub fn is_validation_error(&self) -> bool {
        matches!(
            self.kind,
            ParseErrorKind::MissingMeasurement
                | ParseErrorKind::MissingFields
                | ParseErrorKind::InvalidTimestamp
                | ParseErrorKind::InvalidFieldValue { .. }
                | ParseErrorKind::DuplicateTag { .. }
                | ParseErrorKind::DuplicateField { .. }
        )
    }

    /// Check if this is a syntax error
    pub fn is_syntax_error(&self) -> bool {
        matches!(
            self.kind,
            ParseErrorKind::UnexpectedChar { .. }
                | ParseErrorKind::UnterminatedString
                | ParseErrorKind::InvalidEscape { .. }
                | ParseErrorKind::UnexpectedEof
        )
    }

    /// Check if this error is recoverable (parsing can continue)
    pub fn is_recoverable(&self) -> bool {
        // Most line-based errors are recoverable - we can skip the line
        !matches!(
            self.kind,
            ParseErrorKind::InvalidUtf8 | ParseErrorKind::InputTooLarge
        )
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if let Some(line) = self.line {
            if let Some(col) = self.column {
                write!(f, " at line {}, column {}", line, col)?;
            } else {
                write!(f, " at line {}", line)?;
            }
        }

        if let Some(ref ctx) = self.context {
            write!(f, " near '{}'", ctx)?;
        }

        Ok(())
    }
}

impl std::error::Error for ParseError {}

/// Kinds of parse errors that can occur
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorKind {
    /// Input is not valid UTF-8
    InvalidUtf8,

    /// Input exceeds maximum allowed size
    InputTooLarge,

    /// Empty input provided
    EmptyInput,

    /// Missing measurement name
    MissingMeasurement,

    /// Missing field set (at least one field required)
    MissingFields,

    /// Invalid timestamp format or out of range
    InvalidTimestamp,

    /// Invalid field value
    InvalidFieldValue {
        /// The field name
        field: String,
        /// Description of what's wrong
        reason: String,
    },

    /// Duplicate tag key found
    DuplicateTag {
        /// The duplicated tag name
        tag: String,
    },

    /// Duplicate field key found
    DuplicateField {
        /// The duplicated field name
        field: String,
    },

    /// Unexpected character encountered
    UnexpectedChar {
        /// The unexpected character
        char: char,
        /// What was expected
        expected: String,
    },

    /// Unterminated string literal
    UnterminatedString,

    /// Invalid escape sequence
    InvalidEscape {
        /// The invalid escape sequence
        sequence: String,
    },

    /// Unexpected end of input
    UnexpectedEof,

    /// Generic syntax error
    SyntaxError {
        /// Description of the error
        message: String,
    },

    /// Protocol mismatch (e.g., expected JSON but got line protocol)
    ProtocolMismatch {
        /// Expected protocol
        expected: String,
        /// What was detected
        detected: String,
    },
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseErrorKind::InvalidUtf8 => write!(f, "Invalid UTF-8 encoding"),
            ParseErrorKind::InputTooLarge => write!(f, "Input exceeds maximum size"),
            ParseErrorKind::EmptyInput => write!(f, "Empty input"),
            ParseErrorKind::MissingMeasurement => write!(f, "Missing measurement name"),
            ParseErrorKind::MissingFields => write!(f, "Missing field set"),
            ParseErrorKind::InvalidTimestamp => write!(f, "Invalid timestamp"),
            ParseErrorKind::InvalidFieldValue { field, reason } => {
                write!(f, "Invalid value for field '{}': {}", field, reason)
            }
            ParseErrorKind::DuplicateTag { tag } => {
                write!(f, "Duplicate tag key: {}", tag)
            }
            ParseErrorKind::DuplicateField { field } => {
                write!(f, "Duplicate field key: {}", field)
            }
            ParseErrorKind::UnexpectedChar { char, expected } => {
                write!(f, "Unexpected character '{}', expected {}", char, expected)
            }
            ParseErrorKind::UnterminatedString => write!(f, "Unterminated string literal"),
            ParseErrorKind::InvalidEscape { sequence } => {
                write!(f, "Invalid escape sequence: {}", sequence)
            }
            ParseErrorKind::UnexpectedEof => write!(f, "Unexpected end of input"),
            ParseErrorKind::SyntaxError { message } => write!(f, "Syntax error: {}", message),
            ParseErrorKind::ProtocolMismatch { expected, detected } => {
                write!(
                    f,
                    "Protocol mismatch: expected {}, got {}",
                    expected, detected
                )
            }
        }
    }
}

/// Result type for parse operations with partial success support
#[derive(Debug)]
pub struct ParseResult<'a, T> {
    /// Successfully parsed items
    pub items: Vec<T>,
    /// Errors encountered during parsing
    pub errors: Vec<ParseError>,
    /// Total bytes consumed from input
    pub bytes_consumed: usize,
    /// Reference to remaining unparsed input
    pub remaining: &'a [u8],
}

impl<'a, T> ParseResult<'a, T> {
    /// Create a new parse result
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            errors: Vec::new(),
            bytes_consumed: 0,
            remaining: &[],
        }
    }

    /// Check if parsing was completely successful (no errors)
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Check if any items were successfully parsed
    pub fn has_items(&self) -> bool {
        !self.items.is_empty()
    }

    /// Get the number of errors
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Get the number of successfully parsed items
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Convert to standard Result (fails if any errors)
    pub fn into_result(self) -> Result<Vec<T>, ParseError> {
        if self.errors.is_empty() {
            Ok(self.items)
        } else {
            Err(self.errors.into_iter().next().unwrap())
        }
    }
}

impl<'a, T> Default for ParseResult<'a, T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_display() {
        let err = ParseError::new(ParseErrorKind::MissingMeasurement)
            .at_line(5)
            .at_column(10)
            .with_context("bad input here");

        let msg = format!("{}", err);
        assert!(msg.contains("Missing measurement name"));
        assert!(msg.contains("line 5"));
        assert!(msg.contains("column 10"));
        assert!(msg.contains("bad input here"));
    }

    #[test]
    fn test_parse_error_truncates_context() {
        let long_context = "a".repeat(100);
        let err = ParseError::new(ParseErrorKind::SyntaxError {
            message: "test".to_string(),
        })
        .with_context(long_context);

        assert!(err.context.as_ref().unwrap().len() <= 50);
        assert!(err.context.as_ref().unwrap().ends_with("..."));
    }

    #[test]
    fn test_parse_error_is_validation() {
        assert!(ParseError::new(ParseErrorKind::MissingMeasurement).is_validation_error());
        assert!(ParseError::new(ParseErrorKind::MissingFields).is_validation_error());
        assert!(!ParseError::new(ParseErrorKind::InvalidUtf8).is_validation_error());
    }

    #[test]
    fn test_parse_error_is_syntax() {
        assert!(ParseError::new(ParseErrorKind::UnterminatedString).is_syntax_error());
        assert!(ParseError::new(ParseErrorKind::UnexpectedEof).is_syntax_error());
        assert!(!ParseError::new(ParseErrorKind::MissingFields).is_syntax_error());
    }

    #[test]
    fn test_parse_error_is_recoverable() {
        assert!(ParseError::new(ParseErrorKind::MissingMeasurement).is_recoverable());
        assert!(!ParseError::new(ParseErrorKind::InvalidUtf8).is_recoverable());
    }

    #[test]
    fn test_parse_result() {
        let mut result: ParseResult<'_, i32> = ParseResult::new();
        result.items.push(1);
        result.items.push(2);

        assert!(result.is_ok());
        assert!(result.has_items());
        assert_eq!(result.item_count(), 2);
        assert_eq!(result.error_count(), 0);

        let items = result.into_result().unwrap();
        assert_eq!(items, vec![1, 2]);
    }

    #[test]
    fn test_parse_result_with_errors() {
        let mut result: ParseResult<'_, i32> = ParseResult::new();
        result.items.push(1);
        result
            .errors
            .push(ParseError::new(ParseErrorKind::EmptyInput));

        assert!(!result.is_ok());
        assert!(result.has_items());

        let err = result.into_result().unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::EmptyInput));
    }

    #[test]
    fn test_parse_error_kind_display() {
        assert_eq!(
            format!("{}", ParseErrorKind::InvalidUtf8),
            "Invalid UTF-8 encoding"
        );

        let field_err = ParseErrorKind::InvalidFieldValue {
            field: "value".to_string(),
            reason: "not a number".to_string(),
        };
        assert!(format!("{}", field_err).contains("value"));
        assert!(format!("{}", field_err).contains("not a number"));
    }
}
