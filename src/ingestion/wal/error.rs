//! WAL error types
//!
//! Defines error types for all WAL operations including I/O errors,
//! corruption detection, and configuration errors.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Result type for WAL operations
pub type WalResult<T> = Result<T, WalError>;

/// WAL error types
#[derive(Debug)]
pub enum WalError {
    /// I/O error during WAL operations
    Io {
        /// The I/O error
        source: io::Error,
        /// Context about what operation failed
        context: String,
    },
    /// Segment file corruption detected
    Corruption {
        /// Segment ID where corruption was found
        segment_id: u64,
        /// Offset in segment
        offset: u64,
        /// Description of the corruption
        message: String,
    },
    /// CRC checksum mismatch
    ChecksumMismatch {
        /// Expected checksum
        expected: u32,
        /// Actual checksum
        actual: u32,
        /// Segment ID
        segment_id: u64,
        /// Offset in segment
        offset: u64,
    },
    /// Invalid record format
    InvalidRecord {
        /// Description of the format error
        message: String,
        /// Segment ID
        segment_id: u64,
        /// Offset in segment
        offset: u64,
    },
    /// Segment not found
    SegmentNotFound {
        /// Path to the missing segment
        path: PathBuf,
    },
    /// Configuration error
    Config {
        /// Description of the configuration error
        message: String,
    },
    /// Channel closed (writer task stopped)
    ChannelClosed,
    /// Segment is full
    SegmentFull {
        /// Segment ID
        segment_id: u64,
        /// Current size
        current_size: usize,
        /// Maximum size
        max_size: usize,
    },
    /// Maximum segments reached
    MaxSegmentsReached {
        /// Current segment count
        count: usize,
        /// Maximum allowed
        max: usize,
    },
    /// Recovery failed
    RecoveryFailed {
        /// Description of the failure
        message: String,
    },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io { source, context } => {
                write!(f, "WAL I/O error ({}): {}", context, source)
            },
            WalError::Corruption {
                segment_id,
                offset,
                message,
            } => {
                write!(
                    f,
                    "WAL corruption in segment {} at offset {}: {}",
                    segment_id, offset, message
                )
            },
            WalError::ChecksumMismatch {
                expected,
                actual,
                segment_id,
                offset,
            } => {
                write!(
                    f,
                    "WAL checksum mismatch in segment {} at offset {}: expected {:08x}, got {:08x}",
                    segment_id, offset, expected, actual
                )
            },
            WalError::InvalidRecord {
                message,
                segment_id,
                offset,
            } => {
                write!(
                    f,
                    "Invalid WAL record in segment {} at offset {}: {}",
                    segment_id, offset, message
                )
            },
            WalError::SegmentNotFound { path } => {
                write!(f, "WAL segment not found: {:?}", path)
            },
            WalError::Config { message } => {
                write!(f, "WAL configuration error: {}", message)
            },
            WalError::ChannelClosed => {
                write!(f, "WAL writer channel closed")
            },
            WalError::SegmentFull {
                segment_id,
                current_size,
                max_size,
            } => {
                write!(
                    f,
                    "WAL segment {} is full ({}/{} bytes)",
                    segment_id, current_size, max_size
                )
            },
            WalError::MaxSegmentsReached { count, max } => {
                write!(f, "Maximum WAL segments reached ({}/{})", count, max)
            },
            WalError::RecoveryFailed { message } => {
                write!(f, "WAL recovery failed: {}", message)
            },
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::Io {
            source: err,
            context: "unknown operation".to_string(),
        }
    }
}

impl From<String> for WalError {
    fn from(message: String) -> Self {
        WalError::Config { message }
    }
}

/// Extension trait for adding context to I/O errors
pub trait WalIoResultExt<T> {
    /// Add context to an I/O error
    fn with_context(self, context: impl Into<String>) -> WalResult<T>;
}

impl<T> WalIoResultExt<T> for Result<T, io::Error> {
    fn with_context(self, context: impl Into<String>) -> WalResult<T> {
        self.map_err(|e| WalError::Io {
            source: e,
            context: context.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = WalError::Corruption {
            segment_id: 1,
            offset: 100,
            message: "truncated record".to_string(),
        };
        assert!(err.to_string().contains("segment 1"));
        assert!(err.to_string().contains("offset 100"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let wal_err: WalError = io_err.into();
        assert!(matches!(wal_err, WalError::Io { .. }));
    }

    #[test]
    fn test_checksum_error() {
        let err = WalError::ChecksumMismatch {
            expected: 0xdeadbeef,
            actual: 0xcafebabe,
            segment_id: 5,
            offset: 1000,
        };
        assert!(err.to_string().contains("deadbeef"));
        assert!(err.to_string().contains("cafebabe"));
    }
}
