//! Buffer configuration and constants
//!
//! Defines configuration options and validation for write buffers.

use std::time::Duration;

/// Minimum valid timestamp (Unix epoch 1970-01-01 in milliseconds)
pub const MIN_VALID_TIMESTAMP: i64 = 0;

/// Maximum valid timestamp (year 2100 in milliseconds)
pub const MAX_VALID_TIMESTAMP: i64 = 4_102_444_800_000;

/// Maximum allowed series count to prevent memory exhaustion
pub const MAX_SERIES_COUNT: usize = 10_000_000;

/// Maximum points per series limit
pub const MAX_POINTS_PER_SERIES: usize = 1_000_000;

/// Configuration for the write buffer manager
///
/// Controls buffering behavior including size limits, flush timing,
/// and memory constraints.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::ingestion::BufferConfig;
/// use std::time::Duration;
///
/// let config = BufferConfig {
///     max_points_per_series: 5_000,
///     max_total_memory: 256 * 1024 * 1024, // 256 MB
///     flush_interval: Duration::from_millis(500),
///     max_buffer_age: Duration::from_secs(5),
///     initial_series_capacity: 500,
///     max_series_count: 50_000,
/// };
///
/// assert!(config.validate().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum points to buffer per series before flushing
    pub max_points_per_series: usize,
    /// Maximum total memory for all buffers (bytes)
    pub max_total_memory: usize,
    /// Flush interval for time-based flushing
    pub flush_interval: Duration,
    /// Maximum age of buffered points before forced flush
    pub max_buffer_age: Duration,
    /// Initial capacity for series buffers
    pub initial_series_capacity: usize,
    /// Maximum number of unique series to buffer (prevents memory exhaustion)
    pub max_series_count: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_points_per_series: 10_000,
            max_total_memory: 512 * 1024 * 1024, // 512 MB
            flush_interval: Duration::from_secs(1),
            max_buffer_age: Duration::from_secs(10),
            initial_series_capacity: 1_000,
            max_series_count: 100_000, // 100K series default limit
        }
    }
}

impl BufferConfig {
    /// Validate the configuration
    ///
    /// Checks that all values are within acceptable bounds.
    ///
    /// # Returns
    ///
    /// Ok(()) if valid, Err with description if invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.max_points_per_series == 0 {
            return Err("max_points_per_series must be > 0".to_string());
        }
        if self.max_points_per_series > MAX_POINTS_PER_SERIES {
            return Err(format!(
                "max_points_per_series {} exceeds maximum {}",
                self.max_points_per_series, MAX_POINTS_PER_SERIES
            ));
        }
        if self.max_total_memory == 0 {
            return Err("max_total_memory must be > 0".to_string());
        }
        if self.flush_interval.is_zero() {
            return Err("flush_interval must be > 0".to_string());
        }
        if self.max_series_count == 0 {
            return Err("max_series_count must be > 0".to_string());
        }
        if self.max_series_count > MAX_SERIES_COUNT {
            return Err(format!(
                "max_series_count {} exceeds maximum {}",
                self.max_series_count, MAX_SERIES_COUNT
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.max_points_per_series, 10_000);
        assert_eq!(config.max_total_memory, 512 * 1024 * 1024);
        assert_eq!(config.flush_interval, Duration::from_secs(1));
        assert_eq!(config.max_buffer_age, Duration::from_secs(10));
        assert_eq!(config.initial_series_capacity, 1_000);
        assert_eq!(config.max_series_count, 100_000);
    }

    #[test]
    fn test_buffer_config_validation() {
        let config = BufferConfig {
            max_points_per_series: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_buffer_config_validation_zero_memory() {
        let config = BufferConfig {
            max_total_memory: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_total_memory must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_zero_flush_interval() {
        let config = BufferConfig {
            flush_interval: Duration::ZERO,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("flush_interval must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_zero_series_count() {
        let config = BufferConfig {
            max_series_count: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_series_count must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_exceeds_max_points() {
        let config = BufferConfig {
            max_points_per_series: MAX_POINTS_PER_SERIES + 1,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_buffer_config_validation_exceeds_max_series() {
        let config = BufferConfig {
            max_series_count: MAX_SERIES_COUNT + 1,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_buffer_config_clone() {
        let config = BufferConfig {
            max_points_per_series: 5000,
            max_total_memory: 1024,
            flush_interval: Duration::from_millis(500),
            max_buffer_age: Duration::from_secs(5),
            initial_series_capacity: 100,
            max_series_count: 1000,
        };

        let cloned = config.clone();
        assert_eq!(cloned.max_points_per_series, 5000);
        assert_eq!(cloned.max_total_memory, 1024);
        assert_eq!(cloned.flush_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_buffer_config_debug() {
        let config = BufferConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("BufferConfig"));
        assert!(debug_str.contains("max_points_per_series"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(MIN_VALID_TIMESTAMP, 0);
        assert_eq!(MAX_SERIES_COUNT, 10_000_000);
        assert_eq!(MAX_POINTS_PER_SERIES, 1_000_000);
    }

    #[test]
    fn test_max_valid_timestamp_is_reasonable() {
        // MAX_VALID_TIMESTAMP should be year 2100 in milliseconds
        // 2100-01-01 is approximately 4102444800 seconds since epoch
        assert_eq!(MAX_VALID_TIMESTAMP, 4_102_444_800_000);
    }
}
