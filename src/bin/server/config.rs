//! Server Configuration
//!
//! This module handles loading and managing server configuration.

use kuba_tsdb::config::ApplicationConfig;
use std::path::PathBuf;

/// Server runtime configuration derived from ApplicationConfig
///
/// This is a simplified view of the configuration used by the HTTP server.
/// Some fields are reserved for future features (metrics, subscriptions).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerConfig {
    /// HTTP server address
    pub listen_addr: String,

    /// Data directory path
    pub data_dir: PathBuf,

    /// Maximum chunk size in bytes
    pub max_chunk_size: usize,

    /// Data retention in days (None = forever)
    pub retention_days: Option<u32>,

    /// Enable Prometheus metrics endpoint
    pub enable_metrics: bool,

    /// Maximum points per write request
    pub max_write_points: usize,

    /// Subscription cleanup interval in seconds (0 = disabled)
    pub subscription_cleanup_interval_secs: u64,

    /// Maximum age of stale subscriptions in seconds before cleanup
    pub subscription_max_age_secs: u64,

    /// Metrics histogram reset interval in seconds (0 = disabled)
    pub metrics_reset_interval_secs: u64,

    /// CORS allowed origins (empty = allow all origins for development)
    pub cors_allowed_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::from(ApplicationConfig::default())
    }
}

impl From<ApplicationConfig> for ServerConfig {
    /// Convert from ApplicationConfig to ServerConfig
    fn from(app_config: ApplicationConfig) -> Self {
        Self {
            listen_addr: app_config.server.listen_addr,
            data_dir: app_config.storage.data_dir,
            max_chunk_size: app_config.storage.max_chunk_size_bytes,
            retention_days: None,
            enable_metrics: app_config.monitoring.prometheus_enabled,
            max_write_points: app_config.ingestion.max_batch_size,
            subscription_cleanup_interval_secs: 300,
            subscription_max_age_secs: 3600,
            metrics_reset_interval_secs: 0,
            cors_allowed_origins: app_config.security.cors_allowed_origins,
        }
    }
}

/// Load configuration from file or environment
///
/// Priority:
/// 1. TSDB_CONFIG environment variable
/// 2. application.toml
/// 3. tsdb.toml (legacy)
/// 4. Default configuration
pub fn load_config_with_app() -> (ServerConfig, ApplicationConfig) {
    // Check for TSDB_CONFIG environment variable
    if let Ok(path) = std::env::var("TSDB_CONFIG") {
        match ApplicationConfig::load(&path) {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from: {}", path);
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            },
            Err(e) => {
                eprintln!(
                    "[config] Failed to load config from {}: {}. Trying defaults.",
                    path, e
                );
            },
        }
    }

    // Try application.toml (new default)
    let app_toml_path = std::path::Path::new("application.toml");
    if app_toml_path.exists() {
        match ApplicationConfig::load("application.toml") {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from application.toml");
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            },
            Err(e) => {
                eprintln!(
                    "[config] Failed to parse application.toml: {}. Trying tsdb.toml.",
                    e
                );
            },
        }
    } else {
        eprintln!(
            "[config] application.toml not found at: {:?}",
            app_toml_path
                .canonicalize()
                .unwrap_or_else(|_| app_toml_path.to_path_buf())
        );
    }

    // Try tsdb.toml (legacy compatibility)
    let tsdb_toml_path = std::path::Path::new("tsdb.toml");
    if tsdb_toml_path.exists() {
        match ApplicationConfig::load("tsdb.toml") {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from tsdb.toml (legacy)");
                let server_config = ServerConfig::from(config.clone());
                return (server_config, config);
            },
            Err(e) => {
                eprintln!("[config] Failed to parse tsdb.toml: {}. Using defaults.", e);
            },
        }
    }

    // Use defaults with environment variable overrides
    eprintln!("[config] Using default configuration");
    let mut app_config = ApplicationConfig::default();
    if let Err(e) = app_config.apply_env_overrides() {
        eprintln!("[config] Failed to apply environment overrides: {}", e);
    }
    let server_config = ServerConfig::from(app_config.clone());
    (server_config, app_config)
}

/// Load configuration (simplified version for backward compatibility)
#[allow(dead_code)]
pub fn load_config() -> ServerConfig {
    let (server_config, _) = load_config_with_app();
    server_config
}
