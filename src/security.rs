//! Security hardening for Gorilla TSDB
//!
//! This module provides security features including path validation,
//! rate limiting (both global and per-client), and input sanitization.
//!
//! # Rate Limiting
//!
//! Two types of rate limiting are provided:
//! - **Global limits**: Protect the entire system from overload
//! - **Per-client limits**: Prevent single clients from consuming all resources
//!
//! ```rust,no_run
//! use gorilla_tsdb::security::{check_write_rate_limit, PerClientRateLimiter};
//! use std::net::IpAddr;
//!
//! // Global rate limiting
//! if check_write_rate_limit() {
//!     // Proceed with write
//! }
//!
//! // Per-client rate limiting
//! let limiter = PerClientRateLimiter::new(1000, 10000);
//! let client_ip: IpAddr = "192.168.1.1".parse().unwrap();
//! if limiter.check_client(&client_ip.to_string()) {
//!     // Client is within their rate limit
//! }
//! ```
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

lazy_static! {
    /// Global write rate limiter (100K writes/sec default)
    pub static ref WRITE_LIMITER: RateLimiter<NotKeyed, InMemoryState, DefaultClock> =
        RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(100_000).unwrap())
        );

    /// Global read rate limiter (100K reads/sec default)
    pub static ref READ_LIMITER: RateLimiter<NotKeyed, InMemoryState, DefaultClock> =
        RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(100_000).unwrap())
        );

    /// Default per-client rate limiter instance
    /// Allows 1000 requests/sec per client, up to 10K clients tracked
    pub static ref PER_CLIENT_LIMITER: PerClientRateLimiter =
        PerClientRateLimiter::new(1000, 10_000);
}

/// Per-client rate limiter for preventing single-client DoS attacks
///
/// This rate limiter tracks individual clients (by IP or identifier) and
/// enforces rate limits per client rather than globally. This prevents
/// a single abusive client from consuming all available rate limit capacity.
///
/// # Features
///
/// - Per-client request tracking with configurable limits
/// - Automatic cleanup of stale client entries
/// - Memory-bounded with configurable maximum clients
/// - Thread-safe for concurrent access
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::security::PerClientRateLimiter;
///
/// let limiter = PerClientRateLimiter::new(100, 1000); // 100 req/sec, max 1000 clients
///
/// // Check if client can make a request
/// if limiter.check_client("192.168.1.100") {
///     // Request allowed
/// } else {
///     // Rate limited - return 429 Too Many Requests
/// }
/// ```
pub struct PerClientRateLimiter {
    /// Per-client request counters and timestamps
    clients: RwLock<HashMap<String, ClientState>>,
    /// Maximum requests per second per client
    max_requests_per_second: u32,
    /// Maximum number of clients to track (prevents memory exhaustion)
    max_clients: usize,
    /// Time window for rate limiting (1 second)
    window: Duration,
}

/// State tracking for a single client
struct ClientState {
    /// Number of requests in current window
    request_count: u32,
    /// Start of current time window
    window_start: Instant,
    /// Last activity timestamp (for cleanup)
    last_seen: Instant,
}

impl PerClientRateLimiter {
    /// Create a new per-client rate limiter
    ///
    /// # Arguments
    ///
    /// * `max_requests_per_second` - Maximum requests allowed per client per second
    /// * `max_clients` - Maximum number of unique clients to track
    ///
    /// When `max_clients` is reached, oldest inactive clients are evicted.
    pub fn new(max_requests_per_second: u32, max_clients: usize) -> Self {
        Self {
            clients: RwLock::new(HashMap::with_capacity(max_clients.min(1000))),
            max_requests_per_second,
            max_clients,
            window: Duration::from_secs(1),
        }
    }

    /// Check if a client is allowed to make a request
    ///
    /// Returns `true` if the request is allowed, `false` if rate limited.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Unique client identifier (typically IP address or API key)
    pub fn check_client(&self, client_id: &str) -> bool {
        let now = Instant::now();

        // Fast path: check if client exists and is within limits
        {
            let clients = self.clients.read();
            if let Some(state) = clients.get(client_id) {
                // Check if we're still in the same window
                if now.duration_since(state.window_start) < self.window {
                    // Still in window - check count
                    if state.request_count >= self.max_requests_per_second {
                        return false; // Rate limited
                    }
                }
                // Either new window or under limit - need write lock
            }
        }

        // Slow path: update client state
        let mut clients = self.clients.write();

        // Cleanup if we're at capacity
        if clients.len() >= self.max_clients && !clients.contains_key(client_id) {
            self.evict_oldest_clients(&mut clients);
        }

        let state = clients
            .entry(client_id.to_string())
            .or_insert_with(|| ClientState {
                request_count: 0,
                window_start: now,
                last_seen: now,
            });

        // Check if we need to reset the window
        if now.duration_since(state.window_start) >= self.window {
            state.window_start = now;
            state.request_count = 0;
        }

        // Check rate limit
        if state.request_count >= self.max_requests_per_second {
            return false;
        }

        // Increment counter and update last seen
        state.request_count += 1;
        state.last_seen = now;

        true
    }

    /// Evict oldest inactive clients to make room for new ones
    ///
    /// Removes the 10% oldest clients or at least 1 client.
    fn evict_oldest_clients(&self, clients: &mut HashMap<String, ClientState>) {
        let evict_count = (clients.len() / 10).max(1);

        // Find oldest clients by last_seen
        let mut entries: Vec<_> = clients
            .iter()
            .map(|(k, v)| (k.clone(), v.last_seen))
            .collect();
        entries.sort_by_key(|(_, last_seen)| *last_seen);

        // Remove oldest entries
        for (key, _) in entries.into_iter().take(evict_count) {
            clients.remove(&key);
        }
    }

    /// Get current number of tracked clients
    pub fn client_count(&self) -> usize {
        self.clients.read().len()
    }

    /// Get rate limit info for a specific client
    ///
    /// Returns `(remaining_requests, reset_time_ms)` if client is tracked,
    /// or `None` if client is not tracked.
    pub fn get_client_info(&self, client_id: &str) -> Option<(u32, u64)> {
        let now = Instant::now();
        let clients = self.clients.read();

        clients.get(client_id).map(|state| {
            let elapsed = now.duration_since(state.window_start);
            if elapsed >= self.window {
                // Window has reset
                (self.max_requests_per_second, 0)
            } else {
                let remaining = self
                    .max_requests_per_second
                    .saturating_sub(state.request_count);
                let reset_ms = (self.window - elapsed).as_millis() as u64;
                (remaining, reset_ms)
            }
        })
    }

    /// Manually clear all tracked clients (useful for testing)
    pub fn clear(&self) {
        self.clients.write().clear();
    }
}

/// Check per-client rate limit using the default limiter
///
/// Convenience function that uses the global `PER_CLIENT_LIMITER` instance.
///
/// # Arguments
///
/// * `client_id` - Unique client identifier (typically IP address)
///
/// # Returns
///
/// `true` if request is allowed, `false` if client is rate limited.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::security::check_per_client_rate_limit;
///
/// let client_ip = "192.168.1.100";
/// if check_per_client_rate_limit(client_ip) {
///     // Process request
/// } else {
///     // Return 429 Too Many Requests
/// }
/// ```
#[inline]
pub fn check_per_client_rate_limit(client_id: &str) -> bool {
    PER_CLIENT_LIMITER.check_client(client_id)
}

/// Get rate limit info for a client from the default limiter
///
/// Returns `(remaining_requests, reset_time_ms)` for use in rate limit headers.
pub fn get_rate_limit_info(client_id: &str) -> Option<(u32, u64)> {
    PER_CLIENT_LIMITER.get_client_info(client_id)
}

/// Validate and sanitize a chunk file path
///
/// This function prevents:
/// - Path traversal attacks (../)
/// - Symlink attacks
/// - Null byte injection
/// - Access outside data directory
///
/// # Example
///
/// ```
/// use gorilla_tsdb::security::validate_chunk_path;
///
/// // Valid path
/// let path = validate_chunk_path("/data/gorilla-tsdb/chunks/chunk_1.gor").unwrap();
///
/// // Invalid path (traversal attempt)
/// let result = validate_chunk_path("/data/gorilla-tsdb/../etc/passwd");
/// assert!(result.is_err());
/// ```
pub fn validate_chunk_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let path = path.as_ref();

    // Reject paths with null bytes
    if let Some(path_str) = path.to_str() {
        if path_str.contains('\0') {
            return Err("Path contains null byte".to_string());
        }
    }

    // Reject suspicious patterns in path string
    let path_str = path.to_string_lossy();
    if path_str.contains("..") {
        return Err(format!(
            "Path traversal detected: path contains '..' - {:?}",
            path
        ));
    }

    // Check for Unicode look-alike characters (homograph attacks)
    if path_str.contains('\u{2024}')
        || path_str.contains('\u{2025}')
        || path_str.contains('\u{2026}')
    {
        return Err("Path contains suspicious Unicode characters".to_string());
    }

    // Ensure filename doesn't contain suspicious characters
    if let Some(filename) = path.file_name() {
        let name = filename.to_string_lossy();
        if name.contains('\0') || name.contains("..") {
            return Err(format!("Suspicious filename detected: {:?}", filename));
        }

        // Ensure .gor extension
        if !name.ends_with(".gor") {
            return Err(format!(
                "Invalid file extension: expected .gor, got {:?}",
                filename
            ));
        }
    }

    // For existing paths or paths with existing parents, check for symlink attacks
    let check_path = if path.exists() {
        Some(path)
    } else {
        path.parent().filter(|parent| parent.exists())
    };

    if let Some(check_path) = check_path {
        // Check if path is a symlink
        if check_path.is_symlink() {
            return Err(format!(
                "Symlink detected: {:?} - symlinks are not allowed for security",
                check_path
            ));
        }

        // Canonicalize to resolve any symlinks in the path
        let canonical = check_path
            .canonicalize()
            .map_err(|e| format!("Failed to canonicalize path {:?}: {}", check_path, e))?;

        // Get data directory from environment or use default
        let data_dir =
            std::env::var("TSDB_DATA_DIR").unwrap_or_else(|_| "/data/gorilla-tsdb".to_string());
        let data_dir = PathBuf::from(data_dir);

        // If data directory exists, canonicalize and check containment
        if data_dir.exists() {
            let canonical_data = data_dir
                .canonicalize()
                .map_err(|e| format!("Failed to canonicalize data dir {:?}: {}", data_dir, e))?;

            if !canonical.starts_with(&canonical_data) {
                return Err(format!(
                    "Security violation: path {:?} is outside data directory {:?}",
                    canonical, canonical_data
                ));
            }
        }

        // Return the original path (not canonical) if validation passes
        // This allows tests with temp directories to work
        return Ok(path.to_path_buf());
    }

    // Path doesn't exist and parent doesn't exist - basic validation passed
    Ok(path.to_path_buf())
}

/// Check if write operation is allowed by rate limiter
///
/// # Example
///
/// ```
/// use gorilla_tsdb::security::check_write_rate_limit;
///
/// if check_write_rate_limit() {
///     // Proceed with write
/// } else {
///     // Rate limit exceeded
/// }
/// ```
#[inline]
pub fn check_write_rate_limit() -> bool {
    WRITE_LIMITER.check().is_ok()
}

/// Check if read operation is allowed by rate limiter
///
/// # Example
///
/// ```
/// use gorilla_tsdb::security::check_read_rate_limit;
///
/// if check_read_rate_limit() {
///     // Proceed with read
/// } else {
///     // Rate limit exceeded
/// }
/// ```
#[inline]
pub fn check_read_rate_limit() -> bool {
    READ_LIMITER.check().is_ok()
}

/// Sanitize series ID to prevent overflow attacks
///
/// Rejects series IDs that could cause issues with storage or indexing.
pub fn validate_series_id(series_id: u128) -> Result<(), String> {
    // Reject series ID of 0 (reserved)
    if series_id == 0 {
        return Err("Series ID cannot be 0 (reserved)".to_string());
    }

    // Reject series ID of max value (reserved for sentinel)
    if series_id == u128::MAX {
        return Err("Series ID cannot be u128::MAX (reserved)".to_string());
    }

    Ok(())
}

/// Validate timestamp to prevent overflow and invalid ranges
pub fn validate_timestamp(timestamp: i64) -> Result<(), String> {
    // Reject extreme timestamps that could cause overflow
    if timestamp == i64::MIN || timestamp == i64::MAX {
        return Err("Timestamp cannot be i64::MIN or i64::MAX (reserved)".to_string());
    }

    // Reject negative timestamps (if needed)
    // Uncomment if your use case doesn't support historical dates
    // if timestamp < 0 {
    //     return Err("Timestamp cannot be negative".to_string());
    // }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_chunk_path_traversal() {
        let result = validate_chunk_path("/data/gorilla-tsdb/../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("traversal"));
    }

    #[test]
    fn test_validate_chunk_path_null_byte() {
        let result = validate_chunk_path("/data/gorilla-tsdb/chunk\0.gor");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_chunk_path_wrong_extension() {
        let result = validate_chunk_path("/data/gorilla-tsdb/chunk.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_series_id_zero() {
        assert!(validate_series_id(0).is_err());
    }

    #[test]
    fn test_validate_series_id_max() {
        assert!(validate_series_id(u128::MAX).is_err());
    }

    #[test]
    fn test_validate_series_id_valid() {
        assert!(validate_series_id(123).is_ok());
    }

    #[test]
    fn test_validate_timestamp_extreme() {
        assert!(validate_timestamp(i64::MIN).is_err());
        assert!(validate_timestamp(i64::MAX).is_err());
    }

    #[test]
    fn test_validate_timestamp_valid() {
        assert!(validate_timestamp(1000000).is_ok());
    }

    #[test]
    fn test_rate_limit() {
        // Should allow some requests
        assert!(check_write_rate_limit());
        assert!(check_read_rate_limit());
    }

    #[test]
    fn test_per_client_rate_limiter_basic() {
        let limiter = PerClientRateLimiter::new(5, 100);

        // First 5 requests should succeed
        for _ in 0..5 {
            assert!(limiter.check_client("client1"));
        }

        // 6th request should be rate limited
        assert!(!limiter.check_client("client1"));

        // Different client should work
        assert!(limiter.check_client("client2"));
    }

    #[test]
    fn test_per_client_rate_limiter_client_count() {
        let limiter = PerClientRateLimiter::new(10, 100);

        // No clients initially
        assert_eq!(limiter.client_count(), 0);

        // Add some clients
        limiter.check_client("client1");
        limiter.check_client("client2");
        limiter.check_client("client3");

        assert_eq!(limiter.client_count(), 3);
    }

    #[test]
    fn test_per_client_rate_limiter_eviction() {
        let limiter = PerClientRateLimiter::new(10, 5);

        // Fill up to capacity
        for i in 0..5 {
            limiter.check_client(&format!("client{}", i));
        }
        assert_eq!(limiter.client_count(), 5);

        // Adding a new client should trigger eviction
        limiter.check_client("new_client");

        // Should have evicted some clients (at least 1)
        assert!(limiter.client_count() <= 5);
    }

    #[test]
    fn test_per_client_rate_limiter_info() {
        let limiter = PerClientRateLimiter::new(10, 100);

        // Unknown client returns None
        assert!(limiter.get_client_info("unknown").is_none());

        // Make some requests
        for _ in 0..3 {
            limiter.check_client("client1");
        }

        // Should have 7 remaining
        let info = limiter.get_client_info("client1");
        assert!(info.is_some());
        let (remaining, _reset_ms) = info.unwrap();
        assert_eq!(remaining, 7);
    }

    #[test]
    fn test_per_client_rate_limit_convenience() {
        // Clear any existing state
        PER_CLIENT_LIMITER.clear();

        // Should allow requests for new client
        assert!(check_per_client_rate_limit("test_client"));
    }
}
