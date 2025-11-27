//! Spill configuration
//!
//! Configuration options for the emergency spill-to-disk system.

use std::path::PathBuf;

/// Spill configuration
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// Directory for spill files
    pub directory: PathBuf,
    /// Memory threshold (bytes) to trigger spill
    pub memory_threshold: usize,
    /// Memory threshold percentage (0.0-1.0) to trigger spill
    pub memory_threshold_percent: f64,
    /// Maximum spill file size in bytes
    pub max_file_size: usize,
    /// Enable LZ4 compression
    pub compression_enabled: bool,
    /// LZ4 compression level (1-12, higher = better compression)
    pub compression_level: u32,
    /// Maximum number of spill files to retain
    pub max_files: usize,
    /// Automatic cleanup after recovery
    pub auto_cleanup: bool,
    /// Number of parallel recovery workers
    pub recovery_parallelism: usize,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./spill"),
            memory_threshold: 1024 * 1024 * 1024, // 1GB
            memory_threshold_percent: 0.8,        // 80%
            max_file_size: 256 * 1024 * 1024,     // 256MB per file
            compression_enabled: true,
            compression_level: 1, // Fast compression
            max_files: 100,
            auto_cleanup: true,
            recovery_parallelism: 4,
        }
    }
}

impl SpillConfig {
    /// Create a new config with the specified directory
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            ..Default::default()
        }
    }

    /// Set memory threshold in bytes
    pub fn memory_threshold(mut self, threshold: usize) -> Self {
        self.memory_threshold = threshold;
        self
    }

    /// Set memory threshold as percentage
    pub fn memory_threshold_percent(mut self, percent: f64) -> Self {
        self.memory_threshold_percent = percent.clamp(0.0, 1.0);
        self
    }

    /// Set maximum file size
    pub fn max_file_size(mut self, size: usize) -> Self {
        self.max_file_size = size;
        self
    }

    /// Enable/disable compression
    pub fn compression(mut self, enabled: bool) -> Self {
        self.compression_enabled = enabled;
        self
    }

    /// Set compression level
    pub fn compression_level(mut self, level: u32) -> Self {
        self.compression_level = level.clamp(1, 12);
        self
    }

    /// Set maximum number of files
    pub fn max_files(mut self, max: usize) -> Self {
        self.max_files = max;
        self
    }

    /// Enable/disable auto cleanup
    pub fn auto_cleanup(mut self, enabled: bool) -> Self {
        self.auto_cleanup = enabled;
        self
    }

    /// Set recovery parallelism
    pub fn recovery_parallelism(mut self, parallelism: usize) -> Self {
        self.recovery_parallelism = parallelism.max(1);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_file_size < 1024 {
            return Err("max_file_size must be at least 1KB".to_string());
        }

        if self.max_files == 0 {
            return Err("max_files must be > 0".to_string());
        }

        if self.memory_threshold == 0 {
            return Err("memory_threshold must be > 0".to_string());
        }

        Ok(())
    }

    /// Get the spill file path for a given file ID
    pub fn file_path(&self, file_id: u64) -> PathBuf {
        let extension = if self.compression_enabled {
            "lz4"
        } else {
            "spill"
        };
        self.directory
            .join(format!("spill-{:08}.{}", file_id, extension))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpillConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.compression_enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = SpillConfig::new("/tmp/spill")
            .memory_threshold(512 * 1024 * 1024)
            .compression(false)
            .max_files(50);

        assert!(config.validate().is_ok());
        assert_eq!(config.memory_threshold, 512 * 1024 * 1024);
        assert!(!config.compression_enabled);
    }

    #[test]
    fn test_file_path() {
        let config = SpillConfig::default();
        assert_eq!(
            config.file_path(1),
            PathBuf::from("./spill/spill-00000001.lz4")
        );

        let config = SpillConfig::default().compression(false);
        assert_eq!(
            config.file_path(1),
            PathBuf::from("./spill/spill-00000001.spill")
        );
    }

    #[test]
    fn test_validation() {
        let mut config = SpillConfig::default();

        config.max_file_size = 100; // Too small
        assert!(config.validate().is_err());

        config.max_file_size = 1024 * 1024;
        config.max_files = 0;
        assert!(config.validate().is_err());
    }
}
