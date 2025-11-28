//! Query error types
//!
//! Provides structured error handling for all query operations including
//! parsing, validation, planning, and execution phases.

use std::fmt;

/// Query error with context
#[derive(Debug)]
pub struct QueryError {
    /// Error kind for programmatic handling
    pub kind: QueryErrorKind,
    /// Human-readable message
    pub message: String,
    /// Optional source error
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl QueryError {
    /// Create a new query error
    pub fn new(kind: QueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Add source error for error chaining
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Create a parse error
    pub fn parse(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::ParseError, message)
    }

    /// Create a validation error
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::ValidationError, message)
    }

    /// Create a planning error
    pub fn planning(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::PlanningError, message)
    }

    /// Create an execution error
    pub fn execution(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::ExecutionError, message)
    }

    /// Create a timeout error
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::Timeout, message)
    }

    /// Create a resource limit error
    pub fn resource_limit(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::ResourceLimit, message)
    }

    /// Create a not found error
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(QueryErrorKind::NotFound, message)
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Categories of query errors for programmatic handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorKind {
    /// Query syntax error during parsing
    ParseError,
    /// Query validation failed (invalid types, unknown series, etc.)
    ValidationError,
    /// Query planning failed (no valid execution plan)
    PlanningError,
    /// Query execution failed (I/O error, internal error)
    ExecutionError,
    /// Query exceeded time limit
    Timeout,
    /// Query exceeded resource limits (memory, result size)
    ResourceLimit,
    /// Requested series or data not found
    NotFound,
    /// Query was cancelled by user or system
    Cancelled,
    /// Internal error (bug, unexpected state)
    Internal,
}

impl fmt::Display for QueryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryErrorKind::ParseError => write!(f, "ParseError"),
            QueryErrorKind::ValidationError => write!(f, "ValidationError"),
            QueryErrorKind::PlanningError => write!(f, "PlanningError"),
            QueryErrorKind::ExecutionError => write!(f, "ExecutionError"),
            QueryErrorKind::Timeout => write!(f, "Timeout"),
            QueryErrorKind::ResourceLimit => write!(f, "ResourceLimit"),
            QueryErrorKind::NotFound => write!(f, "NotFound"),
            QueryErrorKind::Cancelled => write!(f, "Cancelled"),
            QueryErrorKind::Internal => write!(f, "Internal"),
        }
    }
}

/// Result type alias for query operations
pub type QueryResult<T> = std::result::Result<T, QueryError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = QueryError::parse("unexpected token 'foo'");
        assert_eq!(err.kind, QueryErrorKind::ParseError);
        assert!(err.message.contains("foo"));
    }

    #[test]
    fn test_error_display() {
        let err = QueryError::validation("series 'cpu' not found");
        let display = format!("{}", err);
        assert!(display.contains("ValidationError"));
        assert!(display.contains("cpu"));
    }

    #[test]
    fn test_error_with_source() {
        use std::error::Error;
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = QueryError::execution("failed to read chunk").with_source(io_err);
        assert!(err.source().is_some());
    }
}
