//! Network layer for ingestion pipeline
//!
//! Provides TCP, UDP, and HTTP listeners for accepting data from external
//! clients with support for TLS encryption, connection pooling, and rate limiting.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//! │ TCP/TLS     │  │ UDP         │  │ HTTP        │
//! │ Listener    │  │ Listener    │  │ Listener    │
//! └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
//!        └─────────────────┴─────────────────┘
//!                          │
//!               ┌──────────▼──────────┐
//!               │  Connection Manager │◄── Rate Limiting
//!               │  (per-IP limits)    │
//!               └──────────┬──────────┘
//!                          │
//!               ┌──────────▼──────────┐
//!               │  Protocol Parser    │
//!               │  (auto-detect)      │
//!               └──────────┬──────────┘
//!                          │
//!               ┌──────────▼──────────┐
//!               │  Ingestion Pipeline │
//!               └─────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::ingestion::network::{NetworkConfig, NetworkListener};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = NetworkConfig::default();
//! let listener = NetworkListener::new(config).await?;
//! listener.start().await?;
//! # Ok(())
//! # }
//! ```

pub mod connection;
pub mod error;
pub mod http;
pub mod rate_limit;
pub mod tcp;
pub mod tls;
pub mod udp;

pub use connection::{ConnectionConfig, ConnectionManager};
pub use error::NetworkError;
pub use http::{HttpConfig, HttpListener};
pub use rate_limit::{RateLimitConfig, RateLimiter};
pub use tcp::TcpListener;
pub use tls::{TlsConfig, TlsVersion};
pub use udp::UdpListener;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Network layer configuration
///
/// Configures all aspects of the network listeners including:
/// - TCP/UDP/HTTP bind addresses
/// - TLS settings
/// - Connection limits
/// - Rate limiting
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// TCP listener address (e.g., "0.0.0.0:8086")
    /// Set to None to disable TCP listener
    pub tcp_addr: Option<SocketAddr>,

    /// UDP listener address (e.g., "0.0.0.0:8087")
    /// Set to None to disable UDP listener (disabled by default)
    pub udp_addr: Option<SocketAddr>,

    /// HTTP listener address for REST API (e.g., "0.0.0.0:8088")
    /// Set to None to disable HTTP listener
    pub http_addr: Option<SocketAddr>,

    /// TLS configuration (None = plaintext connections)
    pub tls: Option<TlsConfig>,

    /// Connection pool and limits configuration
    pub connection: ConnectionConfig,

    /// Rate limiting configuration
    pub rate_limit: RateLimitConfig,

    /// Read buffer size for incoming data (default: 64 KB)
    pub read_buffer_size: usize,

    /// Write buffer size for responses (default: 16 KB)
    pub write_buffer_size: usize,

    /// Maximum line length for line protocol (default: 64 KB)
    /// Lines longer than this will be rejected
    pub max_line_length: usize,

    /// Maximum request body size for HTTP (default: 10 MB)
    pub max_body_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            tcp_addr: Some("0.0.0.0:8086".parse().expect("valid address")),
            udp_addr: None, // Disabled by default
            http_addr: Some("0.0.0.0:8087".parse().expect("valid address")),
            tls: None,
            connection: ConnectionConfig::default(),
            rate_limit: RateLimitConfig::default(),
            read_buffer_size: 64 * 1024,     // 64 KB
            write_buffer_size: 16 * 1024,    // 16 KB
            max_line_length: 64 * 1024,      // 64 KB
            max_body_size: 10 * 1024 * 1024, // 10 MB
        }
    }
}

impl NetworkConfig {
    /// Create a new network config builder
    pub fn builder() -> NetworkConfigBuilder {
        NetworkConfigBuilder::default()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // At least one listener should be enabled
        if self.tcp_addr.is_none() && self.udp_addr.is_none() && self.http_addr.is_none() {
            return Err("At least one listener (TCP, UDP, or HTTP) must be enabled".to_string());
        }

        // Validate TLS config if present
        if let Some(ref tls) = self.tls {
            tls.validate()?;
        }

        // Validate sub-configs
        self.connection.validate()?;
        self.rate_limit.validate()?;

        // Validate buffer sizes
        if self.read_buffer_size == 0 {
            return Err("read_buffer_size must be > 0".to_string());
        }
        if self.write_buffer_size == 0 {
            return Err("write_buffer_size must be > 0".to_string());
        }
        if self.max_line_length == 0 {
            return Err("max_line_length must be > 0".to_string());
        }
        if self.max_body_size == 0 {
            return Err("max_body_size must be > 0".to_string());
        }

        Ok(())
    }
}

/// Builder for NetworkConfig
#[derive(Debug, Default)]
pub struct NetworkConfigBuilder {
    config: NetworkConfig,
}

impl NetworkConfigBuilder {
    /// Set TCP listener address
    pub fn tcp_addr(mut self, addr: SocketAddr) -> Self {
        self.config.tcp_addr = Some(addr);
        self
    }

    /// Disable TCP listener
    pub fn disable_tcp(mut self) -> Self {
        self.config.tcp_addr = None;
        self
    }

    /// Set UDP listener address
    pub fn udp_addr(mut self, addr: SocketAddr) -> Self {
        self.config.udp_addr = Some(addr);
        self
    }

    /// Set HTTP listener address
    pub fn http_addr(mut self, addr: SocketAddr) -> Self {
        self.config.http_addr = Some(addr);
        self
    }

    /// Disable HTTP listener
    pub fn disable_http(mut self) -> Self {
        self.config.http_addr = None;
        self
    }

    /// Set TLS configuration
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Set connection configuration
    pub fn connection(mut self, config: ConnectionConfig) -> Self {
        self.config.connection = config;
        self
    }

    /// Set rate limit configuration
    pub fn rate_limit(mut self, config: RateLimitConfig) -> Self {
        self.config.rate_limit = config;
        self
    }

    /// Set read buffer size
    pub fn read_buffer_size(mut self, size: usize) -> Self {
        self.config.read_buffer_size = size;
        self
    }

    /// Set max line length
    pub fn max_line_length(mut self, size: usize) -> Self {
        self.config.max_line_length = size;
        self
    }

    /// Set max body size
    pub fn max_body_size(mut self, size: usize) -> Self {
        self.config.max_body_size = size;
        self
    }

    /// Build and validate the configuration
    pub fn build(self) -> Result<NetworkConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }
}

/// Network listener coordinator
///
/// Manages all network listeners (TCP, UDP, HTTP) and coordinates
/// their lifecycle including startup and graceful shutdown.
pub struct NetworkListener {
    config: NetworkConfig,
    connection_manager: Arc<ConnectionManager>,
    rate_limiter: Arc<RateLimiter>,
    shutdown_tx: broadcast::Sender<()>,
}

impl NetworkListener {
    /// Create a new network listener
    ///
    /// # Arguments
    ///
    /// * `config` - Network configuration
    ///
    /// # Errors
    ///
    /// Returns error if configuration is invalid
    pub async fn new(config: NetworkConfig) -> Result<Self, NetworkError> {
        config.validate().map_err(NetworkError::Config)?;

        let connection_manager = Arc::new(ConnectionManager::new(config.connection.clone()));
        let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit.clone()));
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            connection_manager,
            rate_limiter,
            shutdown_tx,
        })
    }

    /// Get a reference to the connection manager
    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }

    /// Get a reference to the rate limiter
    pub fn rate_limiter(&self) -> &Arc<RateLimiter> {
        &self.rate_limiter
    }

    /// Get a shutdown receiver for spawned tasks
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Start all configured listeners
    ///
    /// This method spawns listener tasks and returns immediately.
    /// Use `shutdown()` to stop all listeners gracefully.
    pub async fn start(&self) -> Result<(), NetworkError> {
        info!("Starting network listeners");

        // Start TCP listener if configured
        if let Some(addr) = self.config.tcp_addr {
            let tcp_listener = TcpListener::bind(
                addr,
                self.config.tls.as_ref(),
                self.config.connection.clone(),
            )
            .await?;

            let conn_mgr = Arc::clone(&self.connection_manager);
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let shutdown_rx = self.shutdown_tx.subscribe();

            tokio::spawn(async move {
                if let Err(e) = tcp_listener.run(conn_mgr, rate_limiter, shutdown_rx).await {
                    warn!("TCP listener error: {}", e);
                }
            });

            info!("TCP listener started on {}", addr);
        }

        // Start UDP listener if configured
        if let Some(addr) = self.config.udp_addr {
            let udp_listener = UdpListener::bind(addr, self.config.read_buffer_size).await?;

            let rate_limiter = Arc::clone(&self.rate_limiter);
            let shutdown_rx = self.shutdown_tx.subscribe();

            tokio::spawn(async move {
                if let Err(e) = udp_listener.run(rate_limiter, shutdown_rx).await {
                    warn!("UDP listener error: {}", e);
                }
            });

            info!("UDP listener started on {}", addr);
        }

        // HTTP listener would be started here
        // (TODO: implement HTTP listener in separate task)

        info!("Network listeners started");
        Ok(())
    }

    /// Shutdown all listeners gracefully
    pub fn shutdown(&self) {
        info!("Shutting down network listeners");
        let _ = self.shutdown_tx.send(());
    }

    /// Get current statistics
    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            total_connections: self.connection_manager.total(),
            rate_limit_rejections: self.rate_limiter.total_rejections(),
        }
    }
}

/// Network listener statistics
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total active connections
    pub total_connections: usize,
    /// Total requests rejected by rate limiting
    pub rate_limit_rejections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== NetworkConfig tests =====

    #[test]
    fn test_network_config_default() {
        let config = NetworkConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.tcp_addr.is_some());
        assert!(config.udp_addr.is_none()); // Disabled by default
        assert!(config.http_addr.is_some());
    }

    #[test]
    fn test_network_config_default_values() {
        let config = NetworkConfig::default();
        assert_eq!(config.read_buffer_size, 64 * 1024);
        assert_eq!(config.write_buffer_size, 16 * 1024);
        assert_eq!(config.max_line_length, 64 * 1024);
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert!(config.tls.is_none());
    }

    #[test]
    fn test_network_config_validation_no_listeners() {
        let config = NetworkConfig {
            tcp_addr: None,
            udp_addr: None,
            http_addr: None,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("At least one listener"));
    }

    #[test]
    fn test_network_config_validation_zero_read_buffer() {
        let config = NetworkConfig {
            read_buffer_size: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("read_buffer_size must be > 0"));
    }

    #[test]
    fn test_network_config_validation_zero_write_buffer() {
        let config = NetworkConfig {
            write_buffer_size: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("write_buffer_size must be > 0"));
    }

    #[test]
    fn test_network_config_validation_zero_max_line_length() {
        let config = NetworkConfig {
            max_line_length: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_line_length must be > 0"));
    }

    #[test]
    fn test_network_config_validation_zero_max_body_size() {
        let config = NetworkConfig {
            max_body_size: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_body_size must be > 0"));
    }

    #[test]
    fn test_network_config_tcp_only() {
        let config = NetworkConfig {
            tcp_addr: Some("127.0.0.1:8086".parse().unwrap()),
            udp_addr: None,
            http_addr: None,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_network_config_udp_only() {
        let config = NetworkConfig {
            tcp_addr: None,
            udp_addr: Some("127.0.0.1:8087".parse().unwrap()),
            http_addr: None,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_network_config_http_only() {
        let config = NetworkConfig {
            tcp_addr: None,
            udp_addr: None,
            http_addr: Some("127.0.0.1:8088".parse().unwrap()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_network_config_clone() {
        let config = NetworkConfig::default();
        let cloned = config.clone();

        assert_eq!(cloned.tcp_addr, config.tcp_addr);
        assert_eq!(cloned.read_buffer_size, config.read_buffer_size);
    }

    #[test]
    fn test_network_config_debug() {
        let config = NetworkConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("NetworkConfig"));
        assert!(debug_str.contains("tcp_addr"));
    }

    // ===== NetworkConfigBuilder tests =====

    #[test]
    fn test_network_config_builder() {
        let config = NetworkConfig::builder()
            .tcp_addr("127.0.0.1:8086".parse().unwrap())
            .disable_http()
            .read_buffer_size(128 * 1024)
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.tcp_addr.unwrap().port(), 8086);
        assert!(config.http_addr.is_none());
        assert_eq!(config.read_buffer_size, 128 * 1024);
    }

    #[test]
    fn test_network_config_builder_default() {
        let builder = NetworkConfigBuilder::default();
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("NetworkConfigBuilder"));
    }

    #[test]
    fn test_network_config_builder_tcp_addr() {
        let addr: SocketAddr = "192.168.1.1:9000".parse().unwrap();
        let config = NetworkConfig::builder().tcp_addr(addr).build().unwrap();
        assert_eq!(config.tcp_addr, Some(addr));
    }

    #[test]
    fn test_network_config_builder_disable_tcp() {
        let config = NetworkConfig::builder()
            .http_addr("127.0.0.1:8088".parse().unwrap())
            .disable_tcp()
            .build()
            .unwrap();
        assert!(config.tcp_addr.is_none());
    }

    #[test]
    fn test_network_config_builder_udp_addr() {
        let addr: SocketAddr = "0.0.0.0:8087".parse().unwrap();
        let config = NetworkConfig::builder()
            .udp_addr(addr)
            .disable_tcp()
            .disable_http()
            .build()
            .unwrap();
        assert_eq!(config.udp_addr, Some(addr));
    }

    #[test]
    fn test_network_config_builder_http_addr() {
        let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let config = NetworkConfig::builder()
            .http_addr(addr)
            .disable_tcp()
            .build()
            .unwrap();
        assert_eq!(config.http_addr, Some(addr));
    }

    #[test]
    fn test_network_config_builder_connection() {
        let conn_config = ConnectionConfig {
            max_connections: 500,
            ..Default::default()
        };
        let config = NetworkConfig::builder()
            .connection(conn_config.clone())
            .build()
            .unwrap();
        assert_eq!(config.connection.max_connections, 500);
    }

    #[test]
    fn test_network_config_builder_rate_limit() {
        let rate_config = RateLimitConfig::default();
        let config = NetworkConfig::builder()
            .rate_limit(rate_config)
            .build()
            .unwrap();
        // Just verify it builds successfully
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_network_config_builder_max_line_length() {
        let config = NetworkConfig::builder()
            .max_line_length(1024)
            .build()
            .unwrap();
        assert_eq!(config.max_line_length, 1024);
    }

    #[test]
    fn test_network_config_builder_max_body_size() {
        let config = NetworkConfig::builder()
            .max_body_size(5 * 1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(config.max_body_size, 5 * 1024 * 1024);
    }

    #[test]
    fn test_network_config_builder_validation_fails() {
        let result = NetworkConfig::builder()
            .disable_tcp()
            .disable_http()
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_network_config_builder_chaining() {
        let config = NetworkConfig::builder()
            .tcp_addr("127.0.0.1:8086".parse().unwrap())
            .http_addr("127.0.0.1:8088".parse().unwrap())
            .read_buffer_size(32 * 1024)
            .max_line_length(16 * 1024)
            .max_body_size(1024 * 1024)
            .build()
            .unwrap();

        assert_eq!(config.tcp_addr.unwrap().port(), 8086);
        assert_eq!(config.http_addr.unwrap().port(), 8088);
        assert_eq!(config.read_buffer_size, 32 * 1024);
        assert_eq!(config.max_line_length, 16 * 1024);
        assert_eq!(config.max_body_size, 1024 * 1024);
    }

    // ===== NetworkStats tests =====

    #[test]
    fn test_network_stats_default() {
        let stats = NetworkStats::default();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.rate_limit_rejections, 0);
    }

    #[test]
    fn test_network_stats_clone() {
        let stats = NetworkStats {
            total_connections: 42,
            rate_limit_rejections: 10,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.total_connections, 42);
        assert_eq!(cloned.rate_limit_rejections, 10);
    }

    #[test]
    fn test_network_stats_debug() {
        let stats = NetworkStats {
            total_connections: 5,
            rate_limit_rejections: 2,
        };
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("NetworkStats"));
        assert!(debug_str.contains("total_connections: 5"));
        assert!(debug_str.contains("rate_limit_rejections: 2"));
    }

    // ===== NetworkListener tests =====

    #[tokio::test]
    async fn test_network_listener_new() {
        let config = NetworkConfig::default();
        let listener = NetworkListener::new(config).await;
        assert!(listener.is_ok());
    }

    #[tokio::test]
    async fn test_network_listener_new_invalid_config() {
        let config = NetworkConfig {
            tcp_addr: None,
            udp_addr: None,
            http_addr: None,
            ..Default::default()
        };
        let result = NetworkListener::new(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_network_listener_accessors() {
        let config = NetworkConfig::default();
        let listener = NetworkListener::new(config).await.unwrap();

        // Test accessors
        let _ = listener.connection_manager();
        let _ = listener.rate_limiter();
        let _ = listener.shutdown_receiver();
        let _ = listener.stats();
    }

    #[tokio::test]
    async fn test_network_listener_stats() {
        let config = NetworkConfig::default();
        let listener = NetworkListener::new(config).await.unwrap();

        let stats = listener.stats();
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_network_listener_shutdown() {
        let config = NetworkConfig::default();
        let listener = NetworkListener::new(config).await.unwrap();

        // Test shutdown doesn't panic
        listener.shutdown();
    }
}
