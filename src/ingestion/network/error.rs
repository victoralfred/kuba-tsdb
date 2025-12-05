//! Network layer error types
//!
//! Provides comprehensive error types for all network-related operations
//! including connection handling, TLS, and protocol parsing.

use std::io;
use std::net::SocketAddr;
use thiserror::Error;

/// Network layer errors
#[derive(Error, Debug)]
pub enum NetworkError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// IO error (binding, accepting, reading, writing)
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// TLS configuration error
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    /// TLS handshake failed
    #[error("TLS handshake failed with {peer}: {reason}")]
    TlsHandshake {
        /// Peer address that failed TLS handshake
        peer: SocketAddr,
        /// Reason for the handshake failure
        reason: String,
    },

    /// Connection limit exceeded
    #[error("Connection limit exceeded: {current}/{max}")]
    ConnectionLimit {
        /// Current number of connections
        current: usize,
        /// Maximum allowed connections
        max: usize,
    },

    /// Per-IP connection limit exceeded
    #[error("Connection limit exceeded for {ip}: {current}/{max}")]
    PerIpLimit {
        /// IP address that exceeded limit
        ip: std::net::IpAddr,
        /// Current number of connections from this IP
        current: usize,
        /// Maximum allowed connections per IP
        max: usize,
    },

    /// Rate limit exceeded
    #[error("Rate limit exceeded for {identifier}")]
    RateLimited {
        /// Identifier (IP or tenant) that exceeded rate limit
        identifier: String,
    },

    /// Connection timeout
    #[error("Connection timeout after {duration_ms}ms")]
    Timeout {
        /// Timeout duration in milliseconds
        duration_ms: u64,
    },

    /// Connection closed by peer
    #[error("Connection closed by peer: {peer}")]
    ConnectionClosed {
        /// Peer address that closed the connection
        peer: SocketAddr,
    },

    /// Invalid data received
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Line too long
    #[error("Line exceeds maximum length: {length} > {max}")]
    LineTooLong {
        /// Actual line length in bytes
        length: usize,
        /// Maximum allowed line length
        max: usize,
    },

    /// Request body too large
    #[error("Request body too large: {size} > {max}")]
    BodyTooLarge {
        /// Actual body size in bytes
        size: usize,
        /// Maximum allowed body size
        max: usize,
    },

    /// Bind failed
    #[error("Failed to bind to {addr}: {reason}")]
    BindFailed {
        /// Address that failed to bind
        addr: SocketAddr,
        /// Reason for the bind failure
        reason: String,
    },

    /// Listener already running
    #[error("Listener already running on {addr}")]
    AlreadyRunning {
        /// Address where listener is already running
        addr: SocketAddr,
    },

    /// Shutdown error
    #[error("Shutdown error: {0}")]
    Shutdown(String),

    /// Certificate error
    #[error("Certificate error: {0}")]
    Certificate(String),

    /// Private key error
    #[error("Private key error: {0}")]
    PrivateKey(String),
}

impl NetworkError {
    /// Check if this error is retryable
    ///
    /// Returns true for transient errors that may succeed if retried,
    /// false for permanent errors that should not be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            // Transient errors - may succeed on retry
            NetworkError::Io(e) => {
                matches!(
                    e.kind(),
                    io::ErrorKind::TimedOut
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::Interrupted
                        | io::ErrorKind::WouldBlock
                )
            },
            NetworkError::Timeout { .. } => true,
            NetworkError::RateLimited { .. } => true,
            NetworkError::ConnectionClosed { .. } => true,

            // Permanent errors - should not retry
            NetworkError::Config(_) => false,
            NetworkError::TlsConfig(_) => false,
            NetworkError::TlsHandshake { .. } => false,
            NetworkError::ConnectionLimit { .. } => false,
            NetworkError::PerIpLimit { .. } => false,
            NetworkError::InvalidData(_) => false,
            NetworkError::LineTooLong { .. } => false,
            NetworkError::BodyTooLarge { .. } => false,
            NetworkError::BindFailed { .. } => false,
            NetworkError::AlreadyRunning { .. } => false,
            NetworkError::Shutdown(_) => false,
            NetworkError::Certificate(_) => false,
            NetworkError::PrivateKey(_) => false,
        }
    }

    /// Check if this error indicates the connection should be closed
    pub fn should_close_connection(&self) -> bool {
        matches!(
            self,
            NetworkError::TlsHandshake { .. }
                | NetworkError::ConnectionClosed { .. }
                | NetworkError::InvalidData(_)
                | NetworkError::LineTooLong { .. }
                | NetworkError::BodyTooLarge { .. }
                | NetworkError::Timeout { .. }
                | NetworkError::ConnectionLimit { .. }
                | NetworkError::PerIpLimit { .. }
                | NetworkError::RateLimited { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retryable() {
        assert!(NetworkError::Timeout { duration_ms: 1000 }.is_retryable());
        assert!(NetworkError::RateLimited {
            identifier: "test".to_string()
        }
        .is_retryable());
        assert!(!NetworkError::Config("bad config".to_string()).is_retryable());
        assert!(!NetworkError::InvalidData("bad data".to_string()).is_retryable());
    }

    #[test]
    fn test_error_should_close() {
        assert!(NetworkError::InvalidData("bad".to_string()).should_close_connection());
        assert!(NetworkError::LineTooLong {
            length: 100,
            max: 50
        }
        .should_close_connection());
        assert!(!NetworkError::Config("config".to_string()).should_close_connection());
    }
}
