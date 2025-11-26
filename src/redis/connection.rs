//! Redis connection pool with health checking and retry logic
//!
//! Provides a robust connection pool for Redis operations with:
//! - Configurable pool size and timeouts
//! - Automatic connection health checking
//! - Exponential backoff retry logic
//! - Connection metrics tracking
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisPool};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Use the builder pattern for convenience
//! let config = RedisConfig::default();
//!
//! // Or use the full struct initialization
//! let config = RedisConfig {
//!     url: "redis://localhost:6379".to_string(),
//!     pool_size: 16,
//!     connection_timeout: Duration::from_secs(5),
//!     command_timeout: Duration::from_secs(1),
//!     retry_policy: Default::default(),
//!     health_check_interval: Duration::from_secs(30),
//!     tls_enabled: false,
//! };
//!
//! let pool = RedisPool::new(config).await?;
//!
//! // Get a connection from the pool
//! let mut conn = pool.get().await?;
//! # Ok(())
//! # }
//! ```

use crate::error::IndexError;
use parking_lot::RwLock;
use redis::aio::MultiplexedConnection;
use redis::{Client, RedisError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Configuration for Redis connection pool
///
/// Controls connection behavior, timeouts, and retry logic.
#[derive(Clone, Debug)]
pub struct RedisConfig {
    /// Redis server URL (e.g., "redis://localhost:6379")
    pub url: String,

    /// Maximum number of connections in the pool
    /// Default: 16
    pub pool_size: u32,

    /// Timeout for establishing new connections
    /// Default: 5 seconds
    pub connection_timeout: Duration,

    /// Timeout for individual Redis commands
    /// Default: 1 second
    pub command_timeout: Duration,

    /// Retry policy for failed operations
    pub retry_policy: RetryPolicy,

    /// Enable TLS for connections
    /// Default: false
    pub tls_enabled: bool,

    /// Health check interval
    /// Default: 30 seconds
    pub health_check_interval: Duration,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            pool_size: 16,
            connection_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(1),
            retry_policy: RetryPolicy::default(),
            tls_enabled: false,
            health_check_interval: Duration::from_secs(30),
        }
    }
}

impl RedisConfig {
    /// Create a new config with the specified URL
    pub fn with_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// Set the pool size
    pub fn pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }

    /// Set the connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set the command timeout
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Set the retry policy
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("Redis URL cannot be empty".to_string());
        }
        if self.pool_size == 0 {
            return Err("Pool size must be greater than 0".to_string());
        }
        if self.pool_size > 1000 {
            return Err("Pool size cannot exceed 1000".to_string());
        }
        Ok(())
    }
}

/// Retry policy with exponential backoff
///
/// Controls how failed operations are retried.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    /// Default: 3
    pub max_retries: u32,

    /// Initial delay between retries
    /// Default: 100ms
    pub initial_delay: Duration,

    /// Maximum delay between retries
    /// Default: 5 seconds
    pub max_delay: Duration,

    /// Multiplier for exponential backoff
    /// Default: 2.0
    pub multiplier: f64,

    /// Add random jitter to delays
    /// Default: true
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for a given attempt number (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay = self.initial_delay.as_millis() as f64
            * self.multiplier.powi(attempt as i32);

        let delay_ms = base_delay.min(self.max_delay.as_millis() as f64);

        let final_delay = if self.jitter {
            // Add up to 25% jitter
            let jitter = rand::random::<f64>() * 0.25;
            delay_ms * (1.0 + jitter)
        } else {
            delay_ms
        };

        Duration::from_millis(final_delay as u64)
    }

    /// Check if we should retry after the given attempt
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

/// Connection pool metrics
#[derive(Debug, Default)]
pub struct PoolMetrics {
    /// Total number of successful connections
    pub connections_created: AtomicU64,

    /// Total number of connection failures
    pub connection_failures: AtomicU64,

    /// Total number of commands executed
    pub commands_executed: AtomicU64,

    /// Total number of command failures
    pub command_failures: AtomicU64,

    /// Total number of retries
    pub retries: AtomicU64,

    /// Total command latency in microseconds
    pub total_latency_us: AtomicU64,
}

impl PoolMetrics {
    /// Record a successful connection
    pub fn record_connection(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection failure
    pub fn record_connection_failure(&self) {
        self.connection_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful command with latency
    pub fn record_command(&self, latency: Duration) {
        self.commands_executed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a command failure
    pub fn record_command_failure(&self) {
        self.command_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average command latency in microseconds
    pub fn average_latency_us(&self) -> f64 {
        let total = self.total_latency_us.load(Ordering::Relaxed);
        let count = self.commands_executed.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            total as f64 / count as f64
        }
    }

    /// Get a snapshot of the metrics
    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connection_failures: self.connection_failures.load(Ordering::Relaxed),
            commands_executed: self.commands_executed.load(Ordering::Relaxed),
            command_failures: self.command_failures.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            average_latency_us: self.average_latency_us(),
        }
    }
}

/// Snapshot of pool metrics at a point in time
#[derive(Debug, Clone)]
pub struct PoolMetricsSnapshot {
    /// Total number of connections created during pool lifetime
    pub connections_created: u64,
    /// Total number of connection failures during pool lifetime
    pub connection_failures: u64,
    /// Total number of commands executed through the pool
    pub commands_executed: u64,
    /// Total number of command failures encountered
    pub command_failures: u64,
    /// Total number of retry attempts made for failed operations
    pub retries: u64,
    /// Average command latency in microseconds
    pub average_latency_us: f64,
}

/// Health status of the Redis connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Connection is healthy
    Healthy,
    /// Connection is degraded (slow but working)
    Degraded,
    /// Connection is unhealthy
    Unhealthy,
    /// Health status unknown (not yet checked)
    Unknown,
}

/// Redis connection pool
///
/// Manages a pool of multiplexed connections to Redis with health checking
/// and automatic reconnection.
pub struct RedisPool {
    /// Redis client for creating connections
    client: Client,

    /// The multiplexed connection (Redis handles multiplexing internally)
    connection: RwLock<Option<MultiplexedConnection>>,

    /// Pool configuration
    config: RedisConfig,

    /// Connection metrics
    metrics: Arc<PoolMetrics>,

    /// Semaphore to limit concurrent operations
    semaphore: Arc<Semaphore>,

    /// Current health status
    health_status: RwLock<HealthStatus>,

    /// Last health check time
    last_health_check: RwLock<Option<Instant>>,
}

impl RedisPool {
    /// Create a new Redis connection pool
    ///
    /// # Arguments
    ///
    /// * `config` - Pool configuration
    ///
    /// # Returns
    ///
    /// A new RedisPool instance or an error if connection fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use gorilla_tsdb::redis::{RedisConfig, RedisPool};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = RedisPool::new(RedisConfig::default()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: RedisConfig) -> Result<Self, IndexError> {
        // Validate configuration
        config
            .validate()
            .map_err(|e| IndexError::ConnectionError(e))?;

        // Create Redis client
        let client = Client::open(config.url.as_str())
            .map_err(|e| IndexError::ConnectionError(format!("Failed to create Redis client: {}", e)))?;

        let metrics = Arc::new(PoolMetrics::default());
        let semaphore = Arc::new(Semaphore::new(config.pool_size as usize));

        let pool = Self {
            client,
            connection: RwLock::new(None),
            config,
            metrics,
            semaphore,
            health_status: RwLock::new(HealthStatus::Unknown),
            last_health_check: RwLock::new(None),
        };

        // Establish initial connection
        pool.connect().await?;

        info!("Redis connection pool initialized");
        Ok(pool)
    }

    /// Establish or re-establish the connection
    async fn connect(&self) -> Result<(), IndexError> {
        let start = Instant::now();

        // Create connection with timeout
        let conn_future = self.client.get_multiplexed_async_connection();
        let conn = tokio::time::timeout(self.config.connection_timeout, conn_future)
            .await
            .map_err(|_| {
                self.metrics.record_connection_failure();
                IndexError::ConnectionError("Connection timeout".to_string())
            })?
            .map_err(|e| {
                self.metrics.record_connection_failure();
                IndexError::ConnectionError(format!("Failed to connect: {}", e))
            })?;

        // Store the connection
        {
            let mut guard = self.connection.write();
            *guard = Some(conn);
        }

        self.metrics.record_connection();
        *self.health_status.write() = HealthStatus::Healthy;

        debug!("Redis connection established in {:?}", start.elapsed());
        Ok(())
    }

    /// Get a connection from the pool
    ///
    /// This acquires a permit from the semaphore and returns a connection guard.
    /// The connection is automatically returned when the guard is dropped.
    pub async fn get(&self) -> Result<PooledConnection<'_>, IndexError> {
        // Acquire semaphore permit
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IndexError::ConnectionError("Semaphore closed".to_string()))?;

        // Check if we need to reconnect
        let conn = {
            let guard = self.connection.read();
            guard.clone()
        };

        let conn = match conn {
            Some(c) => c,
            None => {
                // Try to reconnect
                self.connect().await?;
                let guard = self.connection.read();
                guard
                    .clone()
                    .ok_or_else(|| IndexError::ConnectionError("No connection available".to_string()))?
            }
        };

        Ok(PooledConnection {
            conn,
            pool: self,
            _permit: permit,
        })
    }

    /// Execute a command with retry logic
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that takes a connection and returns a result
    ///
    /// # Returns
    ///
    /// The result of the command or an error after all retries are exhausted
    pub async fn execute<F, Fut, T>(&self, f: F) -> Result<T, IndexError>
    where
        F: Fn(MultiplexedConnection) -> Fut,
        Fut: std::future::Future<Output = Result<T, RedisError>>,
    {
        let mut attempt = 0;

        loop {
            let conn = self.get().await?;
            let start = Instant::now();

            // Execute with timeout
            let result = tokio::time::timeout(
                self.config.command_timeout,
                f(conn.conn.clone()),
            )
            .await;

            match result {
                Ok(Ok(value)) => {
                    self.metrics.record_command(start.elapsed());
                    return Ok(value);
                }
                Ok(Err(e)) => {
                    self.metrics.record_command_failure();

                    // Check if we should retry
                    if self.config.retry_policy.should_retry(attempt) && is_retriable_error(&e) {
                        self.metrics.record_retry();
                        let delay = self.config.retry_policy.delay_for_attempt(attempt);
                        warn!(
                            "Redis command failed (attempt {}), retrying in {:?}: {}",
                            attempt + 1,
                            delay,
                            e
                        );
                        tokio::time::sleep(delay).await;

                        // Try to reconnect on connection errors
                        if is_connection_error(&e) {
                            let _ = self.connect().await;
                        }

                        attempt += 1;
                        continue;
                    }

                    return Err(IndexError::ConnectionError(format!(
                        "Redis command failed: {}",
                        e
                    )));
                }
                Err(_) => {
                    self.metrics.record_command_failure();

                    if self.config.retry_policy.should_retry(attempt) {
                        self.metrics.record_retry();
                        let delay = self.config.retry_policy.delay_for_attempt(attempt);
                        warn!(
                            "Redis command timeout (attempt {}), retrying in {:?}",
                            attempt + 1,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }

                    return Err(IndexError::ConnectionError("Command timeout".to_string()));
                }
            }
        }
    }

    /// Perform a health check
    ///
    /// Sends a PING command to Redis and updates the health status.
    pub async fn health_check(&self) -> HealthStatus {
        let start = Instant::now();

        let result = self
            .execute(|mut conn| async move {
                redis::cmd("PING")
                    .query_async::<String>(&mut conn)
                    .await
            })
            .await;

        let status = match result {
            Ok(_) => {
                let latency = start.elapsed();
                // Degraded if latency > 100ms
                if latency > Duration::from_millis(100) {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Healthy
                }
            }
            Err(_) => HealthStatus::Unhealthy,
        };

        *self.health_status.write() = status.clone();
        *self.last_health_check.write() = Some(Instant::now());

        status
    }

    /// Get the current health status
    pub fn health_status(&self) -> HealthStatus {
        self.health_status.read().clone()
    }

    /// Get pool metrics
    pub fn metrics(&self) -> PoolMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get the pool configuration
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    /// Check if the pool needs a health check
    pub fn needs_health_check(&self) -> bool {
        let last_check = self.last_health_check.read();
        match *last_check {
            None => true,
            Some(instant) => instant.elapsed() > self.config.health_check_interval,
        }
    }
}

/// A pooled connection that returns to the pool when dropped
pub struct PooledConnection<'a> {
    conn: MultiplexedConnection,
    /// Reference to parent pool (for future use in connection recycling)
    #[allow(dead_code)]
    pool: &'a RedisPool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a> PooledConnection<'a> {
    /// Get a reference to the underlying connection
    pub fn connection(&mut self) -> &mut MultiplexedConnection {
        &mut self.conn
    }
}

impl<'a> std::ops::Deref for PooledConnection<'a> {
    type Target = MultiplexedConnection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<'a> std::ops::DerefMut for PooledConnection<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

/// Check if an error is retriable
fn is_retriable_error(e: &RedisError) -> bool {
    // Retry on connection errors, timeouts, and temporary failures
    e.is_connection_dropped()
        || e.is_timeout()
        || e.is_io_error()
        || matches!(e.kind(), redis::ErrorKind::BusyLoadingError)
}

/// Check if an error is a connection error that requires reconnection
fn is_connection_error(e: &RedisError) -> bool {
    e.is_connection_dropped() || e.is_io_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RedisConfig::default();
        assert_eq!(config.pool_size, 16);
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = RedisConfig::default();

        // Empty URL
        config.url = "".to_string();
        assert!(config.validate().is_err());

        // Zero pool size
        config.url = "redis://localhost".to_string();
        config.pool_size = 0;
        assert!(config.validate().is_err());

        // Pool size too large
        config.pool_size = 1001;
        assert!(config.validate().is_err());

        // Valid config
        config.pool_size = 16;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_retry_policy_delay() {
        let policy = RetryPolicy {
            initial_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(5),
            jitter: false,
            ..Default::default()
        };

        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));

        // Should cap at max_delay
        assert_eq!(policy.delay_for_attempt(10), Duration::from_secs(5));
    }

    #[test]
    fn test_retry_policy_should_retry() {
        let policy = RetryPolicy {
            max_retries: 3,
            ..Default::default()
        };

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }

    #[test]
    fn test_pool_metrics() {
        let metrics = PoolMetrics::default();

        metrics.record_connection();
        metrics.record_command(Duration::from_micros(100));
        metrics.record_command(Duration::from_micros(200));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connections_created, 1);
        assert_eq!(snapshot.commands_executed, 2);
        assert_eq!(snapshot.average_latency_us, 150.0);
    }

    #[test]
    fn test_config_builder() {
        let config = RedisConfig::with_url("redis://localhost:6380")
            .pool_size(32)
            .connection_timeout(Duration::from_secs(10));

        assert_eq!(config.url, "redis://localhost:6380");
        assert_eq!(config.pool_size, 32);
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
    }
}
