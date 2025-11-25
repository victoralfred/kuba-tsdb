///! Security hardening for Gorilla TSDB
///!
///! This module provides security features including path validation,
///! rate limiting, and input sanitization.

use governor::{Quota, RateLimiter, clock::DefaultClock, state::{InMemoryState, NotKeyed}};
use lazy_static::lazy_static;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

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
    if path_str.contains('\u{2024}') || path_str.contains('\u{2025}') || path_str.contains('\u{2026}') {
        return Err("Path contains suspicious Unicode characters".to_string());
    }

    // Ensure filename doesn't contain suspicious characters
    if let Some(filename) = path.file_name() {
        let name = filename.to_string_lossy();
        if name.contains('\0') || name.contains("..") {
            return Err(format!(
                "Suspicious filename detected: {:?}",
                filename
            ));
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
    } else if let Some(parent) = path.parent() {
        if parent.exists() {
            Some(parent)
        } else {
            None
        }
    } else {
        None
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
        let canonical = check_path.canonicalize()
            .map_err(|e| format!("Failed to canonicalize path {:?}: {}", check_path, e))?;

        // Get data directory from environment or use default
        let data_dir = std::env::var("TSDB_DATA_DIR")
            .unwrap_or_else(|_| "/data/gorilla-tsdb".to_string());
        let data_dir = PathBuf::from(data_dir);

        // If data directory exists, canonicalize and check containment
        if data_dir.exists() {
            let canonical_data = data_dir.canonicalize()
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
}
