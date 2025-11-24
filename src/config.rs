///! Configuration management for Gorilla TSDB
///!
///! This module provides configuration file support with TOML format,
///! environment variable overrides, and sensible defaults.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Server configuration
    pub server: ServerConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Performance tuning
    pub performance: PerformanceConfig,

    /// Monitoring and observability
    pub monitoring: MonitoringConfig,

    /// Security settings
    #[serde(default)]
    pub security: SecurityConfig,
}

/// Server configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// Server listen address
    #[serde(default = "default_host")]
    pub host: String,

    /// Server port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of worker threads
    #[serde(default = "default_workers")]
    pub workers: usize,
}

/// Storage configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Maximum points per chunk
    #[serde(default = "default_max_chunk_points")]
    pub max_chunk_points: u32,

    /// Maximum chunk duration in milliseconds
    #[serde(default = "default_seal_duration_ms")]
    pub seal_duration_ms: i64,

    /// Maximum chunk size in bytes
    #[serde(default = "default_seal_size_bytes")]
    pub seal_size_bytes: usize,

    /// Enable mmap for sealed chunks
    #[serde(default = "default_true")]
    pub enable_mmap: bool,
}

/// Performance configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    /// Write buffer size
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,

    /// Number of parallel seal operations
    #[serde(default = "default_seal_parallelism")]
    pub seal_parallelism: usize,

    /// Mmap cache size (number of chunks)
    #[serde(default = "default_mmap_cache_size")]
    pub mmap_cache_size: usize,

    /// Enable batch operations
    #[serde(default = "default_true")]
    pub enable_batch_operations: bool,
}

/// Monitoring configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringConfig {
    /// Enable Prometheus metrics
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,

    /// Metrics endpoint port
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,

    /// Log level (error, warn, info, debug, trace)
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Enable structured logging
    #[serde(default = "default_true")]
    pub structured_logging: bool,
}

/// Security configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    /// Enable path validation
    #[serde(default = "default_true")]
    pub enable_path_validation: bool,

    /// Enable rate limiting
    #[serde(default = "default_true")]
    pub enable_rate_limiting: bool,

    /// Maximum writes per second (0 = unlimited)
    #[serde(default = "default_rate_limit")]
    pub max_writes_per_second: u32,

    /// Maximum reads per second (0 = unlimited)
    #[serde(default = "default_rate_limit")]
    pub max_reads_per_second: u32,
}

// Default value functions
fn default_host() -> String { "0.0.0.0".to_string() }
fn default_port() -> u16 { 8080 }
fn default_workers() -> usize { num_cpus::get() }
fn default_data_dir() -> PathBuf { PathBuf::from("/data/gorilla-tsdb") }
fn default_max_chunk_points() -> u32 { 10_000_000 }
fn default_seal_duration_ms() -> i64 { 3_600_000 }
fn default_seal_size_bytes() -> usize { 104_857_600 }
fn default_write_buffer_size() -> usize { 10_000 }
fn default_seal_parallelism() -> usize { 4 }
fn default_mmap_cache_size() -> usize { 1000 }
fn default_metrics_port() -> u16 { 9090 }
fn default_log_level() -> String { "info".to_string() }
fn default_rate_limit() -> u32 { 100_000 }
fn default_true() -> bool { true }

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            performance: PerformanceConfig::default(),
            monitoring: MonitoringConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            workers: default_workers(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            max_chunk_points: default_max_chunk_points(),
            seal_duration_ms: default_seal_duration_ms(),
            seal_size_bytes: default_seal_size_bytes(),
            enable_mmap: true,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: default_write_buffer_size(),
            seal_parallelism: default_seal_parallelism(),
            mmap_cache_size: default_mmap_cache_size(),
            enable_batch_operations: true,
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            metrics_port: default_metrics_port(),
            log_level: default_log_level(),
            structured_logging: true,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_path_validation: true,
            enable_rate_limiting: true,
            max_writes_per_second: default_rate_limit(),
            max_reads_per_second: default_rate_limit(),
        }
    }
}

impl Config {
    /// Load configuration from TOML file
    pub fn from_file(path: &str) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file {}: {}", path, e))?;

        toml::from_str(&contents)
            .map_err(|e| format!("Failed to parse config file {}: {}", path, e))
    }

    /// Load configuration with environment variable overrides
    pub fn from_file_with_env(path: &str) -> Result<Self, String> {
        let mut config = Self::from_file(path)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Load from environment variables only
    pub fn from_env() -> Self {
        let mut config = Self::default();
        config.apply_env_overrides();
        config
    }

    /// Apply environment variable overrides
    pub fn apply_env_overrides(&mut self) {
        // Server
        if let Ok(host) = std::env::var("TSDB_HOST") {
            self.server.host = host;
        }
        if let Ok(port) = std::env::var("TSDB_PORT") {
            if let Ok(p) = port.parse() {
                self.server.port = p;
            }
        }

        // Storage
        if let Ok(data_dir) = std::env::var("TSDB_DATA_DIR") {
            self.storage.data_dir = PathBuf::from(data_dir);
        }
        if let Ok(max_points) = std::env::var("TSDB_MAX_CHUNK_POINTS") {
            if let Ok(p) = max_points.parse() {
                self.storage.max_chunk_points = p;
            }
        }

        // Monitoring
        if let Ok(log_level) = std::env::var("RUST_LOG") {
            self.monitoring.log_level = log_level;
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate port range
        if self.server.port == 0 {
            return Err("Server port cannot be 0".to_string());
        }

        // Validate data directory
        if self.storage.data_dir.as_os_str().is_empty() {
            return Err("Data directory cannot be empty".to_string());
        }

        // Validate chunk limits
        if self.storage.max_chunk_points == 0 {
            return Err("Max chunk points must be > 0".to_string());
        }
        if self.storage.max_chunk_points > 10_000_000 {
            return Err("Max chunk points cannot exceed 10M".to_string());
        }

        // Validate performance settings
        if self.performance.seal_parallelism == 0 {
            return Err("Seal parallelism must be > 0".to_string());
        }

        Ok(())
    }

    /// Save configuration to TOML file
    pub fn save_to_file(&self, path: &str) -> Result<(), String> {
        let contents = toml::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize config: {}", e))?;

        std::fs::write(path, contents)
            .map_err(|e| format!("Failed to write config file {}: {}", path, e))
    }
}

// Add num_cpus dependency helper
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.port, 8080);
        assert!(config.monitoring.metrics_enabled);
        assert!(config.security.enable_rate_limiting);
    }

    #[test]
    fn test_config_validation() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_port() {
        let mut config = Config::default();
        config.server.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_env_override() {
        std::env::set_var("TSDB_PORT", "9999");
        let config = Config::from_env();
        assert_eq!(config.server.port, 9999);
        std::env::remove_var("TSDB_PORT");
    }
}
