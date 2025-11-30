//! Configuration management for Gorilla TSDB
//!
//! This module provides configuration file support with TOML format,
//! environment variable overrides, and sensible defaults.
//!
//! # Configuration Priority (Highest to Lowest)
//!
//! 1. Environment Variables (GORILLA_*)
//! 2. Command-line Arguments
//! 3. Configuration File (application.toml)
//! 4. Built-in Defaults
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::config::ApplicationConfig;
//!
//! // Load from file with environment overrides
//! let config = ApplicationConfig::load("application.toml").unwrap();
//!
//! // Or use defaults
//! let config = ApplicationConfig::default();
//! ```

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

/// Configuration loading and validation errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Failed to read configuration file from disk
    #[error("Failed to read config file: {0}")]
    FileRead(#[from] std::io::Error),

    /// Failed to parse TOML configuration syntax
    #[error("Failed to parse TOML: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// Configuration value failed validation
    #[error("Validation error: {field} - {message}")]
    Validation {
        /// Configuration field that failed validation
        field: String,
        /// Description of the validation failure
        message: String,
    },

    /// Error processing environment variable override
    #[error("Environment variable error: {0}")]
    EnvVar(String),
}

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

// ============================================================================
// Main Application Configuration
// ============================================================================

/// Root application configuration
///
/// Contains all configuration sections for the Gorilla TSDB server.
/// All fields have sensible defaults and can be overridden via TOML
/// file or environment variables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApplicationConfig {
    /// Server network settings
    pub server: ServerConfig,
    /// HTTP listener settings
    pub http: HttpConfig,
    /// Storage engine settings
    pub storage: StorageConfig,
    /// Ingestion pipeline settings
    pub ingestion: IngestionConfig,
    /// Backpressure control settings
    pub backpressure: BackpressureConfig,
    /// Write-ahead log settings
    pub wal: WalConfig,
    /// Compression settings
    pub compression: CompressionConfig,
    /// Compaction settings
    pub compaction: CompactionConfig,
    /// Rate limiting settings
    pub rate_limit: RateLimitConfig,
    /// Query engine settings
    pub query: QueryConfig,
    /// Emergency spill settings
    pub spill: SpillConfig,
    /// Redis integration settings
    pub redis: RedisConfig,
    /// Health check settings
    pub health: HealthConfig,
    /// Monitoring and metrics settings
    pub monitoring: MonitoringConfig,
    /// Protocol parser settings
    pub protocol: ProtocolConfig,
    /// Schema validation settings
    pub schema: SchemaConfig,
    /// Timestamp validation settings
    pub timestamp: TimestampConfig,
    /// Performance tuning settings
    pub performance: PerformanceConfig,
    /// Security settings
    pub security: SecurityConfig,
}

impl ApplicationConfig {
    /// Load configuration from file with environment variable overrides
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Errors
    ///
    /// Returns error if file cannot be read, parsed, or fails validation.
    pub fn load(path: &str) -> ConfigResult<Self> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: ApplicationConfig = toml::from_str(&contents)?;
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from file, falling back to defaults if not found
    ///
    /// Logs a warning if the file cannot be loaded but continues with defaults.
    pub fn load_or_default(path: &str) -> Self {
        match Self::load(path) {
            Ok(config) => config,
            Err(e) => {
                tracing::warn!(
                    "Failed to load config from {}: {}. Using defaults.",
                    path,
                    e
                );
                Self::default()
            }
        }
    }

    /// Apply environment variable overrides
    ///
    /// Environment variables use the GORILLA_ prefix and underscore-separated
    /// section names. For example: GORILLA_SERVER_LISTEN_ADDR
    pub fn apply_env_overrides(&mut self) -> ConfigResult<()> {
        // Server overrides
        if let Ok(val) = std::env::var("GORILLA_SERVER_LISTEN_ADDR") {
            self.server.listen_addr = val;
        }
        if let Ok(val) = std::env::var("GORILLA_SERVER_WORKERS") {
            self.server.workers = val.parse().map_err(|_| {
                ConfigError::EnvVar("GORILLA_SERVER_WORKERS must be a number".into())
            })?;
        }
        if let Ok(val) = std::env::var("GORILLA_SERVER_LOG_LEVEL") {
            self.server.log_level = val;
        }

        // Storage overrides
        if let Ok(val) = std::env::var("GORILLA_STORAGE_DATA_DIR") {
            self.storage.data_dir = PathBuf::from(val);
        }

        // Ingestion memory overrides
        if let Ok(val) = std::env::var("GORILLA_INGESTION_MAX_MEMORY") {
            self.ingestion.max_total_memory_bytes = parse_size(&val).ok_or_else(|| {
                ConfigError::EnvVar(
                    "GORILLA_INGESTION_MAX_MEMORY must be a valid size (e.g., 512MB)".into(),
                )
            })?;
        }

        // Rate limit overrides
        if let Ok(val) = std::env::var("GORILLA_RATE_LIMIT_ENABLED") {
            self.rate_limit.enabled = val.parse().unwrap_or(true);
        }

        // Redis overrides
        if let Ok(val) = std::env::var("GORILLA_REDIS_URL") {
            self.redis.url = val;
            self.redis.enabled = true;
        }

        // Query timeout override
        if let Ok(val) = std::env::var("GORILLA_QUERY_TIMEOUT_SECS") {
            self.query.timeout_secs = val.parse().map_err(|_| {
                ConfigError::EnvVar("GORILLA_QUERY_TIMEOUT_SECS must be a number".into())
            })?;
        }

        Ok(())
    }

    /// Validate all configuration values
    ///
    /// Checks that all settings are within valid ranges and consistent.
    pub fn validate(&self) -> ConfigResult<()> {
        self.server.validate()?;
        self.http.validate()?;
        self.storage.validate()?;
        self.ingestion.validate()?;
        self.backpressure.validate()?;
        self.wal.validate()?;
        self.compression.validate()?;
        self.compaction.validate()?;
        self.rate_limit.validate()?;
        self.query.validate()?;
        self.health.validate()?;
        self.schema.validate()?;
        self.timestamp.validate()?;
        Ok(())
    }

    /// Save configuration to TOML file
    pub fn save_to_file(&self, path: &str) -> ConfigResult<()> {
        let contents = toml::to_string_pretty(self).map_err(|e| ConfigError::Validation {
            field: "serialization".into(),
            message: e.to_string(),
        })?;

        std::fs::write(path, contents)?;
        Ok(())
    }
}

// ============================================================================
// Server Configuration
// ============================================================================

/// Server network and runtime settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Network binding address (e.g., "0.0.0.0:8080")
    pub listen_addr: String,
    /// Number of worker threads (0 = auto-detect CPU count)
    pub workers: usize,
    /// Graceful shutdown timeout in seconds
    pub shutdown_timeout_secs: u64,
    /// Enable request logging
    pub request_logging: bool,
    /// Log level: trace, debug, info, warn, error
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            workers: 0, // Auto-detect
            shutdown_timeout_secs: 30,
            request_logging: true,
            log_level: "info".to_string(),
        }
    }
}

impl ServerConfig {
    /// Validate server configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        // Validate listen address format
        if self.listen_addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(ConfigError::Validation {
                field: "server.listen_addr".into(),
                message: "Invalid socket address format".into(),
            });
        }

        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.log_level.to_lowercase().as_str()) {
            return Err(ConfigError::Validation {
                field: "server.log_level".into(),
                message: format!("Must be one of: {:?}", valid_levels),
            });
        }

        Ok(())
    }

    /// Get worker count, auto-detecting if set to 0
    pub fn effective_workers(&self) -> usize {
        if self.workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            self.workers
        }
    }

    /// Get shutdown timeout as Duration
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_secs)
    }
}

// ============================================================================
// HTTP Configuration
// ============================================================================

/// HTTP listener settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    /// Maximum request body size in bytes (default: 10MB)
    pub max_body_size_bytes: usize,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    /// Keep-alive timeout in seconds
    pub keep_alive_secs: u64,
    /// Maximum concurrent connections (0 = unlimited)
    pub max_connections: usize,
    /// Enable gzip compression for responses
    pub enable_compression: bool,
    /// Minimum response size to compress (bytes)
    pub compression_threshold_bytes: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_body_size_bytes: 10 * 1024 * 1024, // 10MB
            request_timeout_secs: 30,
            keep_alive_secs: 60,
            max_connections: 10_000,
            enable_compression: true,
            compression_threshold_bytes: 1024,
        }
    }
}

impl HttpConfig {
    /// Validate HTTP configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.max_body_size_bytes == 0 {
            return Err(ConfigError::Validation {
                field: "http.max_body_size_bytes".into(),
                message: "Must be greater than 0".into(),
            });
        }
        if self.max_body_size_bytes > 1024 * 1024 * 1024 {
            return Err(ConfigError::Validation {
                field: "http.max_body_size_bytes".into(),
                message: "Cannot exceed 1GB".into(),
            });
        }
        Ok(())
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    /// Get keep-alive timeout as Duration
    pub fn keep_alive(&self) -> Duration {
        Duration::from_secs(self.keep_alive_secs)
    }
}

// ============================================================================
// Storage Configuration
// ============================================================================

/// Storage engine settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Base directory for all data files
    pub data_dir: PathBuf,
    /// Maximum chunk size in bytes before rotation
    pub max_chunk_size_bytes: usize,
    /// Maximum points per chunk before sealing
    pub max_chunk_points: usize,
    /// Time-based sealing threshold in milliseconds
    pub seal_duration_ms: i64,
    /// Size-based sealing threshold in bytes
    pub seal_size_bytes: usize,
    /// Enable data checksums for integrity verification
    pub enable_checksums: bool,
    /// Sync writes to disk (true = safer, false = faster)
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/data/gorilla-tsdb"),
            max_chunk_size_bytes: 1024 * 1024,  // 1MB
            max_chunk_points: 10_000_000,       // 10M
            seal_duration_ms: 3_600_000,        // 1 hour
            seal_size_bytes: 100 * 1024 * 1024, // 100MB
            enable_checksums: true,
            sync_writes: false,
        }
    }
}

impl StorageConfig {
    /// Validate storage configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.max_chunk_size_bytes < 1024 {
            return Err(ConfigError::Validation {
                field: "storage.max_chunk_size_bytes".into(),
                message: "Must be at least 1KB".into(),
            });
        }
        if self.max_chunk_points == 0 {
            return Err(ConfigError::Validation {
                field: "storage.max_chunk_points".into(),
                message: "Must be greater than 0".into(),
            });
        }
        if self.seal_duration_ms <= 0 {
            return Err(ConfigError::Validation {
                field: "storage.seal_duration_ms".into(),
                message: "Must be greater than 0".into(),
            });
        }
        Ok(())
    }

    /// Get seal duration as Duration
    pub fn seal_duration(&self) -> Duration {
        Duration::from_millis(self.seal_duration_ms as u64)
    }
}

// ============================================================================
// Ingestion Configuration
// ============================================================================

/// Ingestion pipeline settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestionConfig {
    /// Maximum memory for all series buffers combined
    pub max_total_memory_bytes: usize,
    /// Maximum points buffered per series before flush
    pub max_points_per_series: usize,
    /// Maximum number of unique series in memory
    pub max_series_count: usize,
    /// Automatic flush interval in seconds
    pub flush_interval_secs: u64,
    /// Maximum time a buffer can exist before forced flush
    pub max_buffer_age_secs: u64,
    /// Number of shards for parallel processing
    pub shard_count: usize,
    /// Maximum batch size for single ingest operation
    pub max_batch_size: usize,
    /// Channel buffer size for async processing
    pub channel_buffer_size: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_total_memory_bytes: 512 * 1024 * 1024, // 512MB
            max_points_per_series: 10_000,
            max_series_count: 100_000,
            flush_interval_secs: 1,
            max_buffer_age_secs: 10,
            shard_count: 16,
            max_batch_size: 100_000,
            channel_buffer_size: 10_000,
        }
    }
}

impl IngestionConfig {
    /// Validate ingestion configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.max_total_memory_bytes < 1024 * 1024 {
            return Err(ConfigError::Validation {
                field: "ingestion.max_total_memory_bytes".into(),
                message: "Must be at least 1MB".into(),
            });
        }
        if self.shard_count == 0 || !self.shard_count.is_power_of_two() {
            return Err(ConfigError::Validation {
                field: "ingestion.shard_count".into(),
                message: "Must be a power of 2".into(),
            });
        }
        Ok(())
    }

    /// Get flush interval as Duration
    pub fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_interval_secs)
    }

    /// Get maximum buffer age as Duration
    pub fn max_buffer_age(&self) -> Duration {
        Duration::from_secs(self.max_buffer_age_secs)
    }
}

// ============================================================================
// Backpressure Configuration
// ============================================================================

/// Backpressure control settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BackpressureConfig {
    /// Memory limit that triggers backpressure (bytes)
    pub memory_limit_bytes: usize,
    /// Queue depth limit that triggers backpressure
    pub queue_limit: usize,
    /// How often to check backpressure conditions (ms)
    pub check_interval_ms: u64,
    /// Maximum time to block a write under backpressure (seconds)
    pub max_block_time_secs: u64,
    /// Percentage of limit at which to start throttling (0.0-1.0)
    pub soft_limit_percent: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 1024 * 1024 * 1024, // 1GB
            queue_limit: 1_000_000,
            check_interval_ms: 10,
            max_block_time_secs: 30,
            soft_limit_percent: 0.8,
        }
    }
}

impl BackpressureConfig {
    /// Validate backpressure configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.soft_limit_percent <= 0.0 || self.soft_limit_percent > 1.0 {
            return Err(ConfigError::Validation {
                field: "backpressure.soft_limit_percent".into(),
                message: "Must be between 0.0 and 1.0".into(),
            });
        }
        Ok(())
    }

    /// Get check interval as Duration
    pub fn check_interval(&self) -> Duration {
        Duration::from_millis(self.check_interval_ms)
    }

    /// Get maximum block time as Duration
    pub fn max_block_time(&self) -> Duration {
        Duration::from_secs(self.max_block_time_secs)
    }
}

// ============================================================================
// WAL Configuration
// ============================================================================

/// Write-ahead log settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// WAL directory (relative to data_dir if not absolute)
    pub directory: PathBuf,
    /// Maximum segment file size
    pub segment_size_bytes: usize,
    /// Sync mode: "immediate", "interval", "none"
    pub sync_mode: String,
    /// Sync interval in milliseconds (when sync_mode = "interval")
    pub sync_interval_ms: u64,
    /// Buffer size for pending writes
    pub write_buffer_size: usize,
    /// Maximum number of segments to retain
    pub max_segments: usize,
    /// Enable WAL compression
    pub compression_enabled: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./wal"),
            segment_size_bytes: 64 * 1024 * 1024, // 64MB
            sync_mode: "interval".to_string(),
            sync_interval_ms: 100,
            write_buffer_size: 10_000,
            max_segments: 100,
            compression_enabled: false,
        }
    }
}

impl WalConfig {
    /// Validate WAL configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        let valid_modes = ["immediate", "interval", "none"];
        if !valid_modes.contains(&self.sync_mode.to_lowercase().as_str()) {
            return Err(ConfigError::Validation {
                field: "wal.sync_mode".into(),
                message: format!("Must be one of: {:?}", valid_modes),
            });
        }
        if self.segment_size_bytes < 1024 {
            return Err(ConfigError::Validation {
                field: "wal.segment_size_bytes".into(),
                message: "Must be at least 1KB".into(),
            });
        }
        if self.segment_size_bytes > 1024 * 1024 * 1024 {
            return Err(ConfigError::Validation {
                field: "wal.segment_size_bytes".into(),
                message: "Cannot exceed 1GB".into(),
            });
        }
        Ok(())
    }

    /// Get sync interval as Duration
    pub fn sync_interval(&self) -> Duration {
        Duration::from_millis(self.sync_interval_ms)
    }
}

// ============================================================================
// Compression Configuration
// ============================================================================

/// Compression settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// Minimum age before chunk is eligible for compression (seconds)
    pub min_age_seconds: u64,
    /// Number of parallel compression workers
    pub worker_count: usize,
    /// How often to scan for compressible chunks (seconds)
    pub scan_interval_seconds: u64,
    /// Maximum chunks to process per scan
    pub max_chunks_per_scan: usize,
    /// Delete original files after compression
    pub delete_original: bool,
    /// Compression algorithm: "lz4", "zstd", "snappy"
    pub algorithm: String,
    /// Compression level (algorithm-specific)
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            min_age_seconds: 3600, // 1 hour
            worker_count: 4,
            scan_interval_seconds: 300, // 5 minutes
            max_chunks_per_scan: 100,
            delete_original: true,
            algorithm: "lz4".to_string(),
            level: 1,
        }
    }
}

impl CompressionConfig {
    /// Validate compression configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        let valid_algorithms = ["lz4", "zstd", "snappy"];
        if !valid_algorithms.contains(&self.algorithm.to_lowercase().as_str()) {
            return Err(ConfigError::Validation {
                field: "compression.algorithm".into(),
                message: format!("Must be one of: {:?}", valid_algorithms),
            });
        }
        Ok(())
    }

    /// Get minimum chunk age before compression as Duration
    pub fn min_age(&self) -> Duration {
        Duration::from_secs(self.min_age_seconds)
    }

    /// Get scan interval as Duration
    pub fn scan_interval(&self) -> Duration {
        Duration::from_secs(self.scan_interval_seconds)
    }
}

// ============================================================================
// Compaction Configuration
// ============================================================================

/// Compaction settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactionConfig {
    /// Compaction strategy: "size_based", "time_based", "leveled"
    pub strategy: String,
    /// Minimum chunks needed to trigger compaction
    pub min_chunks_to_compact: usize,
    /// Maximum chunks to compact in single job
    pub max_chunks_per_job: usize,
    /// Target size for compacted chunks
    pub target_chunk_size_bytes: usize,
    /// Number of parallel compaction workers
    pub worker_count: usize,
    /// How often to check for compaction opportunities (seconds)
    pub check_interval_secs: u64,
    /// Maximum pending jobs in queue
    pub max_queue_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: "size_based".to_string(),
            min_chunks_to_compact: 4,
            max_chunks_per_job: 10,
            target_chunk_size_bytes: 64 * 1024 * 1024, // 64MB
            worker_count: 2,
            check_interval_secs: 60,
            max_queue_size: 1000,
        }
    }
}

impl CompactionConfig {
    /// Validate compaction configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        let valid_strategies = ["size_based", "time_based", "leveled"];
        if !valid_strategies.contains(&self.strategy.to_lowercase().as_str()) {
            return Err(ConfigError::Validation {
                field: "compaction.strategy".into(),
                message: format!("Must be one of: {:?}", valid_strategies),
            });
        }
        Ok(())
    }

    /// Get check interval as Duration
    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_secs)
    }
}

// ============================================================================
// Rate Limit Configuration
// ============================================================================

/// Rate limiting settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Enable rate limiting
    pub enabled: bool,
    /// Maximum points per second per IP address
    pub points_per_sec_per_ip: u64,
    /// Maximum points per second per tenant
    pub points_per_sec_per_tenant: u64,
    /// Burst capacity multiplier (allows temporary spikes)
    pub burst_multiplier: f64,
    /// Time-to-live for rate limit buckets (seconds)
    pub bucket_ttl_secs: u64,
    /// IP whitelist (bypass rate limiting)
    pub ip_whitelist: Vec<String>,
    /// IPv6 prefix length for grouping (/64 recommended)
    pub ipv6_prefix_length: u8,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            points_per_sec_per_ip: 100_000,
            points_per_sec_per_tenant: 1_000_000,
            burst_multiplier: 2.0,
            bucket_ttl_secs: 300,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
            ipv6_prefix_length: 64,
        }
    }
}

impl RateLimitConfig {
    /// Validate rate limit configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.burst_multiplier < 1.0 {
            return Err(ConfigError::Validation {
                field: "rate_limit.burst_multiplier".into(),
                message: "Must be at least 1.0".into(),
            });
        }
        if self.ipv6_prefix_length > 128 {
            return Err(ConfigError::Validation {
                field: "rate_limit.ipv6_prefix_length".into(),
                message: "Must be between 0 and 128".into(),
            });
        }
        Ok(())
    }

    /// Get bucket time-to-live as Duration
    pub fn bucket_ttl(&self) -> Duration {
        Duration::from_secs(self.bucket_ttl_secs)
    }
}

// ============================================================================
// Query Configuration
// ============================================================================

/// Query engine settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    /// Enable query result cache
    pub cache_enabled: bool,
    /// Maximum cache size in bytes
    pub cache_max_size_bytes: usize,
    /// Maximum number of cached entries
    pub cache_max_entries: usize,
    /// Cache entry time-to-live (seconds)
    pub cache_default_ttl_secs: u64,
    /// Default limit for query results (if not specified)
    pub default_limit: usize,
    /// Maximum allowed limit for query results
    pub max_limit: usize,
    /// Query timeout in seconds
    pub timeout_secs: u64,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Enable parallel query execution
    pub parallel_execution: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            cache_enabled: true,
            cache_max_size_bytes: 128 * 1024 * 1024, // 128MB
            cache_max_entries: 10_000,
            cache_default_ttl_secs: 60,
            default_limit: 10_000,
            max_limit: 1_000_000,
            timeout_secs: 30,
            max_concurrent_queries: 100,
            parallel_execution: true,
        }
    }
}

impl QueryConfig {
    /// Validate query engine configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.default_limit > self.max_limit {
            return Err(ConfigError::Validation {
                field: "query.default_limit".into(),
                message: "Cannot exceed max_limit".into(),
            });
        }
        Ok(())
    }

    /// Get cache time-to-live as Duration
    pub fn cache_ttl(&self) -> Duration {
        Duration::from_secs(self.cache_default_ttl_secs)
    }

    /// Get query timeout as Duration
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

// ============================================================================
// Spill Configuration
// ============================================================================

/// Emergency overflow (spill) settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SpillConfig {
    /// Spill directory (relative to data_dir if not absolute)
    pub directory: PathBuf,
    /// Memory threshold that triggers spill (bytes)
    pub memory_threshold_bytes: usize,
    /// Memory threshold as percentage of system RAM (0.0-1.0)
    pub memory_threshold_percent: f64,
    /// Maximum size per spill file
    pub max_file_size_bytes: usize,
    /// Enable compression for spill files
    pub compression_enabled: bool,
    /// Compression level (1=fast, 12=best)
    pub compression_level: u32,
    /// Maximum number of spill files to retain
    pub max_files: usize,
    /// Automatically clean up spill files after recovery
    pub auto_cleanup: bool,
    /// Number of parallel workers for recovery
    pub recovery_parallelism: usize,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./spill"),
            memory_threshold_bytes: 1024 * 1024 * 1024, // 1GB
            memory_threshold_percent: 0.8,
            max_file_size_bytes: 256 * 1024 * 1024, // 256MB
            compression_enabled: true,
            compression_level: 1,
            max_files: 100,
            auto_cleanup: true,
            recovery_parallelism: 4,
        }
    }
}

// ============================================================================
// Redis Configuration
// ============================================================================

/// Redis integration settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    /// Enable Redis integration
    pub enabled: bool,
    /// Redis connection URL
    pub url: String,
    /// Connection pool size
    pub pool_size: usize,
    /// Connection timeout (seconds)
    pub connection_timeout_secs: u64,
    /// Command timeout (seconds)
    pub command_timeout_secs: u64,
    /// Health check interval (seconds)
    pub health_check_interval_secs: u64,
    /// Enable TLS
    pub tls_enabled: bool,
    /// TLS certificate path (if tls_enabled)
    pub tls_cert_path: Option<PathBuf>,
    /// TLS key path (if tls_enabled)
    pub tls_key_path: Option<PathBuf>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "redis://127.0.0.1:6379".to_string(),
            pool_size: 16,
            connection_timeout_secs: 5,
            command_timeout_secs: 1,
            health_check_interval_secs: 30,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

impl RedisConfig {
    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.connection_timeout_secs)
    }

    /// Get command timeout as Duration
    pub fn command_timeout(&self) -> Duration {
        Duration::from_secs(self.command_timeout_secs)
    }
}

// ============================================================================
// Health Configuration
// ============================================================================

/// Health check settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    /// Health check interval (seconds)
    pub check_interval_secs: u64,
    /// Timeout for individual health checks (seconds)
    pub check_timeout_secs: u64,
    /// Minimum free disk space percentage (0.0-100.0)
    pub min_free_disk_percent: f64,
    /// Maximum write latency before unhealthy (microseconds)
    pub max_write_latency_us: u64,
    /// Maximum read latency before unhealthy (microseconds)
    pub max_read_latency_us: u64,
    /// Consecutive failures before marking unhealthy
    pub failure_threshold: u32,
    /// Consecutive successes before marking healthy again
    pub recovery_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 30,
            check_timeout_secs: 5,
            min_free_disk_percent: 10.0,
            max_write_latency_us: 10_000, // 10ms
            max_read_latency_us: 5_000,   // 5ms
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

impl HealthConfig {
    /// Validate health check configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.min_free_disk_percent < 0.0 || self.min_free_disk_percent > 100.0 {
            return Err(ConfigError::Validation {
                field: "health.min_free_disk_percent".into(),
                message: "Must be between 0.0 and 100.0".into(),
            });
        }
        Ok(())
    }

    /// Get check interval as Duration
    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_secs)
    }

    /// Get check timeout as Duration
    pub fn check_timeout(&self) -> Duration {
        Duration::from_secs(self.check_timeout_secs)
    }
}

// ============================================================================
// Monitoring Configuration
// ============================================================================

/// Monitoring and metrics settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MonitoringConfig {
    /// Metrics collection interval (seconds)
    pub collection_interval_secs: u64,
    /// Maximum samples to retain per metric
    pub max_samples: usize,
    /// Maximum custom metrics allowed
    pub max_custom_metrics: usize,
    /// Enable Prometheus metrics endpoint
    pub prometheus_enabled: bool,
    /// Prometheus metrics path
    pub prometheus_path: String,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            collection_interval_secs: 10,
            max_samples: 1000,
            max_custom_metrics: 10_000,
            prometheus_enabled: true,
            prometheus_path: "/metrics".to_string(),
        }
    }
}

impl MonitoringConfig {
    /// Get metrics collection interval as Duration
    pub fn collection_interval(&self) -> Duration {
        Duration::from_secs(self.collection_interval_secs)
    }
}

// ============================================================================
// Protocol Configuration
// ============================================================================

/// Protocol parser settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProtocolConfig {
    /// Line protocol settings
    pub line: LineProtocolConfig,
    /// JSON protocol settings
    pub json: JsonProtocolConfig,
    /// Protobuf protocol settings
    pub protobuf: ProtobufProtocolConfig,
}

/// Line protocol specific settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LineProtocolConfig {
    /// Maximum line length in bytes
    pub max_line_length_bytes: usize,
}

impl Default for LineProtocolConfig {
    fn default() -> Self {
        Self {
            max_line_length_bytes: 64 * 1024, // 64KB
        }
    }
}

/// JSON protocol specific settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JsonProtocolConfig {
    /// Maximum points in a single JSON array
    pub max_array_points: usize,
}

impl Default for JsonProtocolConfig {
    fn default() -> Self {
        Self {
            max_array_points: 100_000,
        }
    }
}

/// Protobuf protocol specific settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProtobufProtocolConfig {
    /// Maximum timeseries in a single request
    pub max_timeseries: usize,
    /// Maximum samples per timeseries
    pub max_samples_per_series: usize,
}

impl Default for ProtobufProtocolConfig {
    fn default() -> Self {
        Self {
            max_timeseries: 10_000,
            max_samples_per_series: 100_000,
        }
    }
}

// ============================================================================
// Schema Configuration
// ============================================================================

/// Schema validation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaConfig {
    /// Maximum measurement/metric name length
    pub max_measurement_length: usize,
    /// Maximum tag key length
    pub max_tag_key_length: usize,
    /// Maximum tag value length
    pub max_tag_value_length: usize,
    /// Maximum field key length
    pub max_field_key_length: usize,
    /// Maximum field string value length
    pub max_field_string_length: usize,
    /// Maximum tags per point
    pub max_tags_per_point: usize,
    /// Maximum fields per point
    pub max_fields_per_point: usize,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            max_measurement_length: 256,
            max_tag_key_length: 256,
            max_tag_value_length: 256,
            max_field_key_length: 256,
            max_field_string_length: 64 * 1024, // 64KB
            max_tags_per_point: 256,
            max_fields_per_point: 256,
        }
    }
}

impl SchemaConfig {
    /// Validate schema configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.max_measurement_length == 0 {
            return Err(ConfigError::Validation {
                field: "schema.max_measurement_length".into(),
                message: "Must be greater than 0".into(),
            });
        }
        Ok(())
    }
}

// ============================================================================
// Timestamp Configuration
// ============================================================================

/// Timestamp validation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TimestampConfig {
    /// Minimum valid timestamp (milliseconds since epoch)
    pub min_valid_ms: i64,
    /// Maximum valid timestamp (milliseconds since epoch)
    pub max_valid_ms: i64,
    /// Reject future timestamps beyond this threshold (seconds, 0 = allow any)
    pub max_future_secs: u64,
}

impl Default for TimestampConfig {
    fn default() -> Self {
        Self {
            min_valid_ms: 0,                 // 1970-01-01
            max_valid_ms: 4_102_444_800_000, // 2100-01-01
            max_future_secs: 3600,           // 1 hour
        }
    }
}

impl TimestampConfig {
    /// Validate timestamp configuration settings
    pub fn validate(&self) -> ConfigResult<()> {
        if self.min_valid_ms >= self.max_valid_ms {
            return Err(ConfigError::Validation {
                field: "timestamp".into(),
                message: "min_valid_ms must be less than max_valid_ms".into(),
            });
        }
        Ok(())
    }
}

// ============================================================================
// Performance Configuration
// ============================================================================

/// Performance tuning settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// Parallel seal workers
    pub seal_parallelism: usize,
    /// Mmap cache size (number of chunks)
    pub mmap_cache_size: usize,
    /// Enable memory-mapped file reads
    pub use_mmap: bool,
    /// Read-ahead buffer size for sequential reads
    pub read_ahead_bytes: usize,
    /// Enable SIMD optimizations (if available)
    pub enable_simd: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            seal_parallelism: 4,
            mmap_cache_size: 1000,
            use_mmap: true,
            read_ahead_bytes: 1024 * 1024, // 1MB
            enable_simd: true,
        }
    }
}

// ============================================================================
// Security Configuration
// ============================================================================

/// Security settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Enable TLS for HTTP server
    pub tls_enabled: bool,
    /// TLS certificate path
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private key path
    pub tls_key_path: Option<PathBuf>,
    /// Enable client certificate authentication
    pub tls_client_auth: bool,
    /// Trusted CA certificates for client auth
    pub tls_ca_path: Option<PathBuf>,
    /// Enable authentication
    pub auth_enabled: bool,
    /// Authentication token (if auth_enabled)
    pub auth_token: Option<String>,
    /// CORS allowed origins (empty = disabled)
    pub cors_allowed_origins: Vec<String>,
    /// Enable request validation
    pub validate_requests: bool,
    /// Path traversal protection
    pub path_validation_enabled: bool,
    /// Enable rate limiting for writes/reads (used by storage writer)
    pub enable_rate_limiting: bool,
    /// Maximum writes per second (0 = unlimited)
    pub max_writes_per_second: u32,
    /// Maximum reads per second (0 = unlimited)
    pub max_reads_per_second: u32,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_client_auth: false,
            tls_ca_path: None,
            auth_enabled: false,
            auth_token: None,
            cors_allowed_origins: vec![],
            validate_requests: true,
            path_validation_enabled: true,
            enable_rate_limiting: true,
            max_writes_per_second: 0, // Unlimited by default
            max_reads_per_second: 0,  // Unlimited by default
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse size strings like "512MB", "1GB", "1024" (bytes)
///
/// Supports suffixes: B, KB, MB, GB (case-insensitive)
pub fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim().to_uppercase();

    if let Ok(bytes) = s.parse::<usize>() {
        return Some(bytes);
    }

    let (num_str, multiplier) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('B') {
        (&s[..s.len() - 1], 1)
    } else {
        return None;
    };

    num_str.trim().parse::<usize>().ok().map(|n| n * multiplier)
}

// ============================================================================
// Legacy Compatibility (for existing code)
// ============================================================================

/// Legacy Config type alias for backward compatibility
pub type Config = ApplicationConfig;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("1MB"), Some(1024 * 1024));
        assert_eq!(parse_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("512MB"), Some(512 * 1024 * 1024));
        assert_eq!(parse_size("invalid"), None);
    }

    #[test]
    fn test_default_config() {
        let config = ApplicationConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_server_workers() {
        let config = ServerConfig {
            workers: 0,
            ..Default::default()
        };
        assert!(config.effective_workers() > 0);

        let config = ServerConfig {
            workers: 8,
            ..Default::default()
        };
        assert_eq!(config.effective_workers(), 8);
    }

    #[test]
    fn test_invalid_listen_addr() {
        let config = ServerConfig {
            listen_addr: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_log_level() {
        let config = ServerConfig {
            log_level: "invalid_level".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_http_body_size_validation() {
        let config = HttpConfig {
            max_body_size_bytes: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = HttpConfig {
            max_body_size_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_storage_validation() {
        let config = StorageConfig {
            max_chunk_size_bytes: 100, // Too small
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = StorageConfig {
            seal_duration_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_ingestion_shard_validation() {
        let config = IngestionConfig {
            shard_count: 3, // Not power of 2
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = IngestionConfig {
            shard_count: 16,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rate_limit_validation() {
        let config = RateLimitConfig {
            burst_multiplier: 0.5, // Less than 1
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = RateLimitConfig {
            burst_multiplier: 2.0,
            ipv6_prefix_length: 200, // Invalid
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_query_limit_validation() {
        let config = QueryConfig {
            default_limit: 2_000_000,
            max_limit: 1_000_000,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_wal_sync_mode_validation() {
        let config = WalConfig {
            sync_mode: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = WalConfig {
            sync_mode: "immediate".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_compression_algorithm_validation() {
        let config = CompressionConfig {
            algorithm: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = CompressionConfig {
            algorithm: "zstd".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_timestamp_validation() {
        let mut config = TimestampConfig::default();
        config.min_valid_ms = config.max_valid_ms + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_env_override() {
        std::env::set_var("GORILLA_SERVER_WORKERS", "16");
        let mut config = ApplicationConfig::default();
        config.apply_env_overrides().unwrap();
        assert_eq!(config.server.workers, 16);
        std::env::remove_var("GORILLA_SERVER_WORKERS");
    }

    #[test]
    fn test_duration_helpers() {
        let server = ServerConfig::default();
        assert_eq!(server.shutdown_timeout(), Duration::from_secs(30));

        let http = HttpConfig::default();
        assert_eq!(http.request_timeout(), Duration::from_secs(30));

        let query = QueryConfig::default();
        assert_eq!(query.timeout(), Duration::from_secs(30));
    }
}
