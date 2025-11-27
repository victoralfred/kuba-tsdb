//! Connection pool manager
//!
//! Manages connection limits to prevent resource exhaustion from too many
//! concurrent connections, with support for per-IP limits to mitigate
//! connection-based DoS attacks.

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Maximum total concurrent connections (default: 10,000)
    pub max_connections: usize,

    /// Maximum connections per IP address (default: 100)
    /// Helps prevent single-source connection flooding
    pub max_per_ip: usize,

    /// Connection idle timeout (default: 60 seconds)
    /// Connections idle longer than this will be closed
    pub idle_timeout: Duration,

    /// Enable TCP_NODELAY for low-latency writes (default: true)
    pub tcp_nodelay: bool,

    /// Enable SO_REUSEPORT for multi-listener scaling (default: true)
    /// Allows multiple listener threads to accept on the same port
    pub so_reuseport: bool,

    /// TCP keepalive interval (default: 30 seconds)
    /// Set to None to disable keepalives
    pub keepalive_interval: Option<Duration>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_per_ip: 100,
            idle_timeout: Duration::from_secs(60),
            tcp_nodelay: true,
            so_reuseport: true,
            keepalive_interval: Some(Duration::from_secs(30)),
        }
    }
}

impl ConnectionConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_connections == 0 {
            return Err("max_connections must be > 0".to_string());
        }
        if self.max_per_ip == 0 {
            return Err("max_per_ip must be > 0".to_string());
        }
        if self.max_per_ip > self.max_connections {
            return Err("max_per_ip cannot exceed max_connections".to_string());
        }
        if self.idle_timeout.is_zero() {
            return Err("idle_timeout must be > 0".to_string());
        }
        Ok(())
    }
}

/// Connection manager with per-IP tracking
///
/// Thread-safe connection tracking using atomic counters for the global count
/// and a concurrent hashmap for per-IP counts.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::ingestion::network::{ConnectionManager, ConnectionConfig};
/// use std::net::IpAddr;
///
/// let manager = ConnectionManager::new(ConnectionConfig::default());
/// let ip: IpAddr = "192.168.1.1".parse().unwrap();
///
/// // Try to acquire a connection slot
/// if manager.try_acquire(ip) {
///     // Connection accepted
///     // ... handle connection ...
///
///     // Release when done
///     manager.release(ip);
/// } else {
///     // Connection limit exceeded, reject
/// }
/// ```
pub struct ConnectionManager {
    config: ConnectionConfig,
    /// Total active connections across all IPs
    total_connections: AtomicUsize,
    /// Per-IP connection counts
    per_ip_counts: DashMap<IpAddr, AtomicUsize>,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            total_connections: AtomicUsize::new(0),
            per_ip_counts: DashMap::new(),
        }
    }

    /// Try to acquire a connection slot for an IP
    ///
    /// Returns true if the connection is accepted, false if limits are exceeded.
    /// If true is returned, the caller MUST call `release()` when the connection
    /// is closed.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address of the connecting client
    pub fn try_acquire(&self, ip: IpAddr) -> bool {
        // Check total limit first (cheap check)
        let current_total = self.total_connections.load(Ordering::Relaxed);
        if current_total >= self.config.max_connections {
            return false;
        }

        // Check per-IP limit
        let ip_entry = self
            .per_ip_counts
            .entry(ip)
            .or_insert_with(|| AtomicUsize::new(0));

        let current_ip = ip_entry.load(Ordering::Relaxed);
        if current_ip >= self.config.max_per_ip {
            return false;
        }

        // Try to acquire slots atomically
        // Note: There's a small race window between check and increment,
        // but this is acceptable for connection limits (soft limits)

        // Increment total
        let new_total = self.total_connections.fetch_add(1, Ordering::Relaxed) + 1;
        if new_total > self.config.max_connections {
            // Rollback - we exceeded the limit
            self.total_connections.fetch_sub(1, Ordering::Relaxed);
            return false;
        }

        // Increment per-IP
        let new_ip = ip_entry.fetch_add(1, Ordering::Relaxed) + 1;
        if new_ip > self.config.max_per_ip {
            // Rollback both counters
            ip_entry.fetch_sub(1, Ordering::Relaxed);
            self.total_connections.fetch_sub(1, Ordering::Relaxed);
            return false;
        }

        true
    }

    /// Release a connection slot
    ///
    /// Must be called exactly once for each successful `try_acquire()` call.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address of the disconnecting client
    pub fn release(&self, ip: IpAddr) {
        // Decrement total (with underflow protection)
        let _ = self
            .total_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });

        // Decrement per-IP
        if let Some(ip_count) = self.per_ip_counts.get(&ip) {
            let _ = ip_count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        }
    }

    /// Get total active connection count
    pub fn total(&self) -> usize {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Get connection count for a specific IP
    pub fn count_for_ip(&self, ip: IpAddr) -> usize {
        self.per_ip_counts
            .get(&ip)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get remaining capacity (total)
    pub fn remaining_capacity(&self) -> usize {
        self.config
            .max_connections
            .saturating_sub(self.total_connections.load(Ordering::Relaxed))
    }

    /// Get remaining capacity for a specific IP
    pub fn remaining_capacity_for_ip(&self, ip: IpAddr) -> usize {
        let current = self.count_for_ip(ip);
        self.config.max_per_ip.saturating_sub(current)
    }

    /// Check if connections are at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.total_connections.load(Ordering::Relaxed) >= self.config.max_connections
    }

    /// Check if a specific IP is at capacity
    pub fn is_ip_at_capacity(&self, ip: IpAddr) -> bool {
        self.count_for_ip(ip) >= self.config.max_per_ip
    }

    /// Get configuration
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Cleanup stale entries from the per-IP map
    ///
    /// Call periodically to remove entries for IPs with zero connections.
    /// This prevents memory growth from many unique IPs over time.
    pub fn cleanup_stale(&self) {
        self.per_ip_counts
            .retain(|_, count| count.load(Ordering::Relaxed) > 0);
    }

    /// Get number of unique IPs with active connections
    pub fn unique_ips(&self) -> usize {
        self.per_ip_counts
            .iter()
            .filter(|entry| entry.value().load(Ordering::Relaxed) > 0)
            .count()
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            total_connections: self.total(),
            max_connections: self.config.max_connections,
            unique_ips: self.unique_ips(),
            max_per_ip: self.config.max_per_ip,
        }
    }
}

/// Connection manager statistics
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Current total connections
    pub total_connections: usize,
    /// Maximum allowed connections
    pub max_connections: usize,
    /// Number of unique IPs with connections
    pub unique_ips: usize,
    /// Maximum connections per IP
    pub max_per_ip: usize,
}

impl ConnectionStats {
    /// Get utilization as a percentage (0.0 - 100.0)
    pub fn utilization_percent(&self) -> f64 {
        if self.max_connections == 0 {
            return 0.0;
        }
        (self.total_connections as f64 / self.max_connections as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.max_per_ip, 100);
    }

    #[test]
    fn test_connection_config_validation() {
        let mut config = ConnectionConfig::default();

        config.max_connections = 0;
        assert!(config.validate().is_err());

        config.max_connections = 100;
        config.max_per_ip = 0;
        assert!(config.validate().is_err());

        config.max_per_ip = 200; // Greater than max_connections
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_connection_manager_basic() {
        let config = ConnectionConfig {
            max_connections: 10,
            max_per_ip: 3,
            ..Default::default()
        };
        let manager = ConnectionManager::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Should accept connections up to per-IP limit
        assert!(manager.try_acquire(ip));
        assert_eq!(manager.total(), 1);
        assert_eq!(manager.count_for_ip(ip), 1);

        assert!(manager.try_acquire(ip));
        assert!(manager.try_acquire(ip));
        assert_eq!(manager.count_for_ip(ip), 3);

        // Should reject - per-IP limit reached
        assert!(!manager.try_acquire(ip));
        assert_eq!(manager.count_for_ip(ip), 3);

        // Release one
        manager.release(ip);
        assert_eq!(manager.count_for_ip(ip), 2);

        // Should accept again
        assert!(manager.try_acquire(ip));
        assert_eq!(manager.count_for_ip(ip), 3);
    }

    #[test]
    fn test_connection_manager_total_limit() {
        let config = ConnectionConfig {
            max_connections: 5,
            max_per_ip: 10, // Higher than total, so total limit triggers first
            ..Default::default()
        };
        let manager = ConnectionManager::new(config);

        // Use different IPs to not hit per-IP limit
        for i in 0..5 {
            let ip: IpAddr = format!("192.168.1.{}", i).parse().unwrap();
            assert!(manager.try_acquire(ip), "Should accept connection {}", i);
        }

        assert_eq!(manager.total(), 5);

        // Should reject - total limit reached
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        assert!(!manager.try_acquire(ip));
    }

    #[test]
    fn test_connection_manager_release() {
        let config = ConnectionConfig {
            max_connections: 10,
            max_per_ip: 5,
            ..Default::default()
        };
        let manager = ConnectionManager::new(config);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // Acquire and release
        assert!(manager.try_acquire(ip));
        assert!(manager.try_acquire(ip));
        assert_eq!(manager.total(), 2);

        manager.release(ip);
        assert_eq!(manager.total(), 1);
        assert_eq!(manager.count_for_ip(ip), 1);

        manager.release(ip);
        assert_eq!(manager.total(), 0);
        assert_eq!(manager.count_for_ip(ip), 0);

        // Multiple releases shouldn't underflow
        manager.release(ip);
        assert_eq!(manager.total(), 0);
        assert_eq!(manager.count_for_ip(ip), 0);
    }

    #[test]
    fn test_connection_manager_stats() {
        let config = ConnectionConfig {
            max_connections: 100,
            max_per_ip: 10,
            ..Default::default()
        };
        let manager = ConnectionManager::new(config);

        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        manager.try_acquire(ip1);
        manager.try_acquire(ip1);
        manager.try_acquire(ip2);

        let stats = manager.stats();
        assert_eq!(stats.total_connections, 3);
        assert_eq!(stats.unique_ips, 2);
        assert!((stats.utilization_percent() - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_connection_manager_cleanup() {
        let config = ConnectionConfig::default();
        let manager = ConnectionManager::new(config);

        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        manager.try_acquire(ip);
        manager.release(ip);

        // Entry still exists but count is 0
        assert_eq!(manager.per_ip_counts.len(), 1);

        // Cleanup should remove it
        manager.cleanup_stale();
        assert_eq!(manager.per_ip_counts.len(), 0);
    }
}
