//! Spill error types
//!
//! Defines error types for spill operations.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Result type for spill operations
pub type SpillResult<T> = Result<T, SpillError>;

/// Spill error types
#[derive(Debug)]
pub enum SpillError {
    /// I/O error during spill operations
    Io {
        /// The I/O error
        source: io::Error,
        /// Context about what operation failed
        context: String,
    },
    /// Compression error
    Compression {
        /// Description of the compression error
        message: String,
    },
    /// Decompression error
    Decompression {
        /// Description of the decompression error
        message: String,
    },
    /// Spill file corruption detected
    Corruption {
        /// Path to the corrupted file
        path: PathBuf,
        /// Description of the corruption
        message: String,
    },
    /// Configuration error
    Config {
        /// Description of the configuration error
        message: String,
    },
    /// Maximum files reached
    MaxFilesReached {
        /// Current file count
        count: usize,
        /// Maximum allowed
        max: usize,
    },
    /// Recovery failed
    RecoveryFailed {
        /// Description of the failure
        message: String,
    },
    /// File not found
    FileNotFound {
        /// Path to the missing file
        path: PathBuf,
    },
}

impl fmt::Display for SpillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpillError::Io { source, context } => {
                write!(f, "Spill I/O error ({}): {}", context, source)
            }
            SpillError::Compression { message } => {
                write!(f, "Spill compression error: {}", message)
            }
            SpillError::Decompression { message } => {
                write!(f, "Spill decompression error: {}", message)
            }
            SpillError::Corruption { path, message } => {
                write!(f, "Spill file corruption at {:?}: {}", path, message)
            }
            SpillError::Config { message } => {
                write!(f, "Spill configuration error: {}", message)
            }
            SpillError::MaxFilesReached { count, max } => {
                write!(f, "Maximum spill files reached ({}/{})", count, max)
            }
            SpillError::RecoveryFailed { message } => {
                write!(f, "Spill recovery failed: {}", message)
            }
            SpillError::FileNotFound { path } => {
                write!(f, "Spill file not found: {:?}", path)
            }
        }
    }
}

impl std::error::Error for SpillError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpillError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for SpillError {
    fn from(err: io::Error) -> Self {
        SpillError::Io {
            source: err,
            context: "unknown operation".to_string(),
        }
    }
}

impl From<String> for SpillError {
    fn from(message: String) -> Self {
        SpillError::Config { message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SpillError::MaxFilesReached {
            count: 100,
            max: 100,
        };
        assert!(err.to_string().contains("100"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let spill_err: SpillError = io_err.into();
        assert!(matches!(spill_err, SpillError::Io { .. }));
    }
}
