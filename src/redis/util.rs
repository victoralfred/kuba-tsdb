//! Utility functions for Redis module
//!
//! Provides helper functions for:
//! - URL sanitization to prevent credential leakage in error messages
//! - Safe error message construction
//! - Connection string parsing

use url::Url;

/// Sanitizes a Redis URL by redacting credentials
///
/// This function masks any username and password present in the URL
/// to prevent credential leakage in logs and error messages.
///
/// # Arguments
///
/// * `url` - The Redis URL to sanitize (e.g., "redis://user:pass@host:6379")
///
/// # Returns
///
/// A sanitized URL string with credentials replaced by "***"
///
/// # Examples
///
/// ```rust
/// use kuba_tsdb::redis::util::sanitize_url;
///
/// // URL with credentials gets them redacted
/// let url = "redis://admin:secret123@localhost:6379/0";
/// let sanitized = sanitize_url(url);
/// assert!(sanitized.contains("***"));
/// assert!(!sanitized.contains("secret123"));
///
/// // URL without credentials remains similar (may add trailing slash)
/// let url = "redis://localhost:6379";
/// let sanitized = sanitize_url(url);
/// assert!(sanitized.contains("localhost:6379"));
///
/// // Invalid URL returns placeholder
/// assert_eq!(sanitize_url("not-a-valid-url"), "[invalid-url]");
/// ```
pub fn sanitize_url(url: &str) -> String {
    // Try to parse as URL
    match Url::parse(url) {
        Ok(mut parsed) => {
            // Redact password if present
            if parsed.password().is_some() {
                // set_password returns Result<(), ()>, ignore the result
                let _ = parsed.set_password(Some("***"));
            }
            // Redact username if present (but preserve structure)
            if !parsed.username().is_empty() {
                // set_username returns Result<(), ()>, ignore the result
                let _ = parsed.set_username("***");
            }
            parsed.to_string()
        },
        Err(_) => {
            // If not a valid URL, return a safe placeholder
            "[invalid-url]".to_string()
        },
    }
}

/// Extracts host and port from a Redis URL for safe display
///
/// # Arguments
///
/// * `url` - The Redis URL to parse
///
/// # Returns
///
/// A tuple of (host, port) or None if parsing fails
///
/// # Examples
///
/// ```rust
/// use kuba_tsdb::redis::util::extract_host_port;
///
/// let (host, port) = extract_host_port("redis://user:pass@localhost:6379").unwrap();
/// assert_eq!(host, "localhost");
/// assert_eq!(port, 6379);
/// ```
pub fn extract_host_port(url: &str) -> Option<(String, u16)> {
    Url::parse(url).ok().and_then(|parsed| {
        let host = parsed.host_str()?.to_string();
        let port = parsed.port().unwrap_or(6379); // Default Redis port
        Some((host, port))
    })
}

/// Creates a safe error message for connection failures
///
/// This function constructs an error message that includes the sanitized
/// connection target (host:port) without exposing credentials.
///
/// # Arguments
///
/// * `url` - The Redis URL (credentials will be redacted)
/// * `err` - The underlying Redis error
///
/// # Returns
///
/// A safe error message string suitable for logging and display
///
/// # Examples
///
/// ```rust
/// use kuba_tsdb::redis::util::connection_error_message;
///
/// // Error message shows host:port but not credentials
/// let msg = connection_error_message(
///     "redis://admin:secret@db.example.com:6379",
///     "Connection refused"
/// );
/// assert!(msg.contains("db.example.com:6379"));
/// assert!(!msg.contains("secret"));
/// ```
pub fn connection_error_message(url: &str, error_description: &str) -> String {
    // Try to extract just host:port for cleaner messages
    if let Some((host, port)) = extract_host_port(url) {
        format!(
            "Redis connection failed to {}:{}: {}",
            host, port, error_description
        )
    } else {
        // Fallback to sanitized URL if parsing fails
        format!(
            "Redis connection failed to {}: {}",
            sanitize_url(url),
            error_description
        )
    }
}

/// Creates a safe error message from a Redis error
///
/// This variant takes a redis::RedisError and extracts only
/// the error kind, not the full error message which might
/// contain sensitive information.
///
/// # Arguments
///
/// * `url` - The Redis URL (credentials will be redacted)
/// * `err` - The Redis error
///
/// # Returns
///
/// A safe error message string
pub fn safe_redis_error(url: &str, err: &redis::RedisError) -> String {
    // Only expose the error kind, not the full message
    // which might contain connection details
    let kind = match err.kind() {
        redis::ErrorKind::ResponseError => "Response error",
        redis::ErrorKind::AuthenticationFailed => "Authentication failed",
        redis::ErrorKind::TypeError => "Type error",
        redis::ErrorKind::ExecAbortError => "Transaction aborted",
        redis::ErrorKind::BusyLoadingError => "Server loading data",
        redis::ErrorKind::NoScriptError => "Script not found",
        redis::ErrorKind::InvalidClientConfig => "Invalid client config",
        redis::ErrorKind::Moved => "Key moved (cluster)",
        redis::ErrorKind::Ask => "Ask redirect (cluster)",
        redis::ErrorKind::TryAgain => "Try again",
        redis::ErrorKind::ClusterDown => "Cluster down",
        redis::ErrorKind::CrossSlot => "Cross-slot operation",
        redis::ErrorKind::MasterDown => "Master down",
        redis::ErrorKind::IoError => "IO error",
        redis::ErrorKind::ClientError => "Client error",
        redis::ErrorKind::ExtensionError => "Extension error",
        redis::ErrorKind::ReadOnly => "Read-only operation",
        redis::ErrorKind::MasterNameNotFoundBySentinel => "Master not found by sentinel",
        redis::ErrorKind::NoValidReplicasFoundBySentinel => "No valid replicas found",
        redis::ErrorKind::EmptySentinelList => "Empty sentinel list",
        redis::ErrorKind::NotBusy => "Not busy",
        redis::ErrorKind::ClusterConnectionNotFound => "Cluster connection not found",
        redis::ErrorKind::NoSub => "Not subscribed",
        redis::ErrorKind::ParseError => "Parse error",
        redis::ErrorKind::RESP3NotSupported => "RESP3 not supported",
        // Use wildcard for feature-gated variants (e.g., Serialize with json feature)
        _ => "Unknown error",
    };

    connection_error_message(url, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_url_with_credentials() {
        // Full credentials
        let url = "redis://admin:supersecretpassword@localhost:6379/0";
        let sanitized = sanitize_url(url);
        assert!(sanitized.contains("***:***@"));
        assert!(sanitized.contains("localhost:6379"));
        assert!(!sanitized.contains("supersecretpassword"));
        assert!(!sanitized.contains("admin"));
    }

    #[test]
    fn test_sanitize_url_password_only() {
        // Some Redis URLs use just password without username
        let url = "redis://:mysecret@localhost:6379";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("mysecret"));
        assert!(sanitized.contains("localhost:6379"));
    }

    #[test]
    fn test_sanitize_url_no_credentials() {
        let url = "redis://localhost:6379";
        let sanitized = sanitize_url(url);
        // Should remain mostly unchanged
        assert!(sanitized.contains("localhost:6379"));
        assert!(!sanitized.contains("***"));
    }

    #[test]
    fn test_sanitize_url_with_database() {
        let url = "redis://user:pass@host:6379/5";
        let sanitized = sanitize_url(url);
        assert!(sanitized.contains("/5"));
        assert!(sanitized.contains("***:***@"));
    }

    #[test]
    fn test_sanitize_url_invalid() {
        let url = "not-a-valid-url";
        assert_eq!(sanitize_url(url), "[invalid-url]");
    }

    #[test]
    fn test_sanitize_url_empty() {
        let url = "";
        assert_eq!(sanitize_url(url), "[invalid-url]");
    }

    #[test]
    fn test_sanitize_url_tls() {
        // rediss:// is the TLS scheme
        let url = "rediss://user:pass@secure.redis.cloud:6380/0";
        let sanitized = sanitize_url(url);
        assert!(sanitized.starts_with("rediss://"));
        assert!(sanitized.contains("***:***@"));
        assert!(!sanitized.contains("pass"));
    }

    #[test]
    fn test_extract_host_port() {
        let (host, port) = extract_host_port("redis://user:pass@myhost.com:6380").unwrap();
        assert_eq!(host, "myhost.com");
        assert_eq!(port, 6380);
    }

    #[test]
    fn test_extract_host_port_default() {
        // Default port when not specified
        let (host, port) = extract_host_port("redis://localhost").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 6379);
    }

    #[test]
    fn test_extract_host_port_invalid() {
        assert!(extract_host_port("not-a-url").is_none());
    }

    #[test]
    fn test_connection_error_message() {
        let msg = connection_error_message(
            "redis://admin:secret123@db.example.com:6379",
            "Connection refused",
        );

        assert!(msg.contains("db.example.com:6379"));
        assert!(msg.contains("Connection refused"));
        assert!(!msg.contains("secret123"));
        assert!(!msg.contains("admin"));
    }

    #[test]
    fn test_connection_error_message_invalid_url() {
        let msg = connection_error_message("invalid", "Some error");
        assert!(msg.contains("[invalid-url]"));
        assert!(msg.contains("Some error"));
    }
}
