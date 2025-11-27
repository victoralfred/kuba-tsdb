//! WAL configuration
//!
//! Provides configuration options for the Write-Ahead Log including
//! segment sizing, sync behavior, and retention policies.

use std::path::PathBuf;

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL segment files
    pub directory: PathBuf,
    /// Maximum segment size in bytes (default: 64MB)
    pub segment_size: usize,
    /// Sync mode for durability
    pub sync_mode: SyncMode,
    /// Write buffer size (number of pending writes)
    pub write_buffer_size: usize,
    /// Maximum number of segments to retain
    pub max_segments: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./wal"),
            segment_size: 64 * 1024 * 1024, // 64MB
            sync_mode: SyncMode::Interval(100),
            write_buffer_size: 10_000,
            max_segments: 100,
        }
    }
}

impl WalConfig {
    /// Create a new WAL config with the specified directory
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            ..Default::default()
        }
    }

    /// Set the segment size
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the sync mode
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set the write buffer size
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Set the maximum number of segments
    pub fn max_segments(mut self, max: usize) -> Self {
        self.max_segments = max;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.segment_size < 1024 {
            return Err("segment_size must be at least 1KB".to_string());
        }

        if self.segment_size > 1024 * 1024 * 1024 {
            return Err("segment_size cannot exceed 1GB".to_string());
        }

        if self.write_buffer_size == 0 {
            return Err("write_buffer_size must be > 0".to_string());
        }

        if self.max_segments == 0 {
            return Err("max_segments must be > 0".to_string());
        }

        Ok(())
    }

    /// Get the segment file path for a given segment ID
    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.directory.join(format!("wal-{:08}.log", segment_id))
    }
}

/// Sync mode determines when data is flushed to disk
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Sync after every write (safest, slowest)
    Immediate,
    /// Sync after N writes
    BatchSize(usize),
    /// Sync every N milliseconds
    Interval(u64),
    /// Never explicitly sync (OS handles it)
    None,
}

impl SyncMode {
    /// Check if this mode should sync after a write
    pub fn should_sync_after_write(&self) -> bool {
        matches!(self, SyncMode::Immediate)
    }

    /// Get batch size for BatchSize mode
    pub fn batch_size(&self) -> Option<usize> {
        match self {
            SyncMode::BatchSize(n) => Some(*n),
            _ => None,
        }
    }

    /// Get interval for Interval mode
    pub fn interval_ms(&self) -> Option<u64> {
        match self {
            SyncMode::Interval(ms) => Some(*ms),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WalConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.segment_size, 64 * 1024 * 1024);
        assert_eq!(config.write_buffer_size, 10_000);
    }

    #[test]
    fn test_config_builder() {
        let config = WalConfig::new("/tmp/wal")
            .segment_size(32 * 1024 * 1024)
            .sync_mode(SyncMode::Immediate)
            .write_buffer_size(5000)
            .max_segments(50);

        assert!(config.validate().is_ok());
        assert_eq!(config.segment_size, 32 * 1024 * 1024);
        assert_eq!(config.sync_mode, SyncMode::Immediate);
    }

    #[test]
    fn test_validation() {
        let mut config = WalConfig::default();

        config.segment_size = 100; // Too small
        assert!(config.validate().is_err());

        config.segment_size = 64 * 1024 * 1024;
        config.write_buffer_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_segment_path() {
        let config = WalConfig::new("/data/wal");
        assert_eq!(
            config.segment_path(1),
            PathBuf::from("/data/wal/wal-00000001.log")
        );
        assert_eq!(
            config.segment_path(12345),
            PathBuf::from("/data/wal/wal-00012345.log")
        );
    }

    #[test]
    fn test_sync_mode() {
        assert!(SyncMode::Immediate.should_sync_after_write());
        assert!(!SyncMode::BatchSize(100).should_sync_after_write());
        assert!(!SyncMode::Interval(100).should_sync_after_write());
        assert!(!SyncMode::None.should_sync_after_write());

        assert_eq!(SyncMode::BatchSize(100).batch_size(), Some(100));
        assert_eq!(SyncMode::Interval(50).interval_ms(), Some(50));
    }
}
