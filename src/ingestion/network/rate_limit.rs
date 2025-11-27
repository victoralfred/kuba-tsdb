//! Token bucket rate limiter
//!
//! Provides rate limiting for network ingestion using the token bucket algorithm.
//! Supports per-IP and per-tenant rate limits with configurable burst capacity.

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Enable rate limiting (default: true)
    pub enabled: bool,

    /// Maximum points per second per IP (default: 100,000)
    pub points_per_sec_per_ip: usize,

    /// Maximum points per second per tenant (default: 1,000,000)
    pub points_per_sec_per_tenant: usize,

    /// Burst multiplier - allows short bursts above rate limit (default: 2.0)
    /// A value of 2.0 means the bucket can hold 2x the per-second rate
    pub burst_multiplier: f64,

    /// Time-to-live for inactive buckets (default: 5 minutes)
    /// Buckets not accessed within this time are cleaned up
    pub bucket_ttl: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            points_per_sec_per_ip: 100_000,
            points_per_sec_per_tenant: 1_000_000,
            burst_multiplier: 2.0,
            bucket_ttl: Duration::from_secs(300),
        }
    }
}

impl RateLimitConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            if self.points_per_sec_per_ip == 0 {
                return Err("points_per_sec_per_ip must be > 0 when enabled".to_string());
            }
            if self.points_per_sec_per_tenant == 0 {
                return Err("points_per_sec_per_tenant must be > 0 when enabled".to_string());
            }
            if self.burst_multiplier < 1.0 {
                return Err("burst_multiplier must be >= 1.0".to_string());
            }
        }
        Ok(())
    }

    /// Get capacity for per-IP bucket
    pub fn ip_bucket_capacity(&self) -> u64 {
        (self.points_per_sec_per_ip as f64 * self.burst_multiplier) as u64
    }

    /// Get capacity for per-tenant bucket
    pub fn tenant_bucket_capacity(&self) -> u64 {
        (self.points_per_sec_per_tenant as f64 * self.burst_multiplier) as u64
    }
}

/// Token bucket for rate limiting
struct TokenBucket {
    /// Available tokens
    tokens: AtomicU64,
    /// Last refill time
    last_refill: Mutex<Instant>,
    /// Last access time (for TTL)
    last_access: Mutex<Instant>,
    /// Bucket capacity (maximum tokens)
    capacity: u64,
    /// Refill rate (tokens per second)
    refill_rate: u64,
}

impl TokenBucket {
    /// Create a new token bucket
    fn new(rate_per_sec: u64, burst_multiplier: f64) -> Self {
        let capacity = (rate_per_sec as f64 * burst_multiplier) as u64;
        let now = Instant::now();

        Self {
            tokens: AtomicU64::new(capacity), // Start full
            last_refill: Mutex::new(now),
            last_access: Mutex::new(now),
            capacity,
            refill_rate: rate_per_sec,
        }
    }

    /// Try to consume tokens from the bucket
    ///
    /// Returns true if tokens were consumed, false if insufficient tokens.
    fn try_consume(&self, tokens_needed: u64) -> bool {
        self.refill();

        // Update last access time
        *self.last_access.lock().unwrap() = Instant::now();

        // Try to consume tokens atomically
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < tokens_needed {
                return false; // Insufficient tokens
            }

            match self.tokens.compare_exchange_weak(
                current,
                current - tokens_needed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry on contention
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        // Only refill if enough time has passed (avoid lock contention)
        if elapsed < Duration::from_millis(1) {
            return;
        }

        // Calculate tokens to add
        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
        if tokens_to_add > 0 {
            let _ = self
                .tokens
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(tokens_to_add).min(self.capacity))
                });
            *last_refill = now;
        }
    }

    /// Check if bucket has expired (no recent access)
    fn is_expired(&self, ttl: Duration) -> bool {
        let last_access = *self.last_access.lock().unwrap();
        last_access.elapsed() > ttl
    }

    /// Get current token count
    fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }
}

/// Token bucket rate limiter
///
/// Provides rate limiting using the token bucket algorithm with support for:
/// - Per-IP rate limiting
/// - Per-tenant rate limiting
/// - Burst capacity
/// - Automatic bucket cleanup
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::ingestion::network::{RateLimiter, RateLimitConfig};
/// use std::net::IpAddr;
///
/// let limiter = RateLimiter::new(RateLimitConfig::default());
/// let ip: IpAddr = "192.168.1.1".parse().unwrap();
///
/// // Check if request is allowed
/// if limiter.check_ip(ip, 100) {
///     // Request allowed
/// } else {
///     // Rate limited
/// }
/// ```
pub struct RateLimiter {
    config: RateLimitConfig,
    /// Per-IP buckets
    ip_buckets: DashMap<IpAddr, TokenBucket>,
    /// Per-tenant buckets
    tenant_buckets: DashMap<String, TokenBucket>,
    /// Total rejections counter
    total_rejections: AtomicU64,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            ip_buckets: DashMap::new(),
            tenant_buckets: DashMap::new(),
            total_rejections: AtomicU64::new(0),
        }
    }

    /// Check if request is allowed for an IP and consume tokens
    ///
    /// # Arguments
    ///
    /// * `ip` - Client IP address
    /// * `points` - Number of points/tokens to consume
    ///
    /// # Returns
    ///
    /// `true` if allowed, `false` if rate limited
    pub fn check_ip(&self, ip: IpAddr, points: u64) -> bool {
        if !self.config.enabled {
            return true;
        }

        let bucket = self.ip_buckets.entry(ip).or_insert_with(|| {
            TokenBucket::new(
                self.config.points_per_sec_per_ip as u64,
                self.config.burst_multiplier,
            )
        });

        if bucket.try_consume(points) {
            true
        } else {
            self.total_rejections.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Check if request is allowed for a tenant and consume tokens
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant identifier
    /// * `points` - Number of points/tokens to consume
    ///
    /// # Returns
    ///
    /// `true` if allowed, `false` if rate limited
    pub fn check_tenant(&self, tenant_id: &str, points: u64) -> bool {
        if !self.config.enabled {
            return true;
        }

        let bucket = self
            .tenant_buckets
            .entry(tenant_id.to_string())
            .or_insert_with(|| {
                TokenBucket::new(
                    self.config.points_per_sec_per_tenant as u64,
                    self.config.burst_multiplier,
                )
            });

        if bucket.try_consume(points) {
            true
        } else {
            self.total_rejections.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Check both IP and tenant limits
    ///
    /// Returns true only if both checks pass.
    pub fn check_both(&self, ip: IpAddr, tenant_id: &str, points: u64) -> bool {
        // Check IP first
        if !self.check_ip(ip, points) {
            return false;
        }

        // Check tenant
        if !self.check_tenant(tenant_id, points) {
            // Note: IP tokens already consumed, but this is acceptable
            // for rate limiting (slight over-counting is fine)
            return false;
        }

        true
    }

    /// Get available tokens for an IP
    pub fn available_for_ip(&self, ip: IpAddr) -> u64 {
        self.ip_buckets
            .get(&ip)
            .map(|b| b.available())
            .unwrap_or(self.config.ip_bucket_capacity())
    }

    /// Get available tokens for a tenant
    pub fn available_for_tenant(&self, tenant_id: &str) -> u64 {
        self.tenant_buckets
            .get(tenant_id)
            .map(|b| b.available())
            .unwrap_or(self.config.tenant_bucket_capacity())
    }

    /// Get total number of rejections
    pub fn total_rejections(&self) -> u64 {
        self.total_rejections.load(Ordering::Relaxed)
    }

    /// Cleanup expired buckets
    ///
    /// Call periodically to remove buckets that haven't been accessed recently.
    /// This prevents memory growth from many unique IPs/tenants over time.
    pub fn cleanup(&self) {
        let ttl = self.config.bucket_ttl;

        self.ip_buckets.retain(|_, bucket| !bucket.is_expired(ttl));
        self.tenant_buckets
            .retain(|_, bucket| !bucket.is_expired(ttl));
    }

    /// Get statistics
    pub fn stats(&self) -> RateLimitStats {
        RateLimitStats {
            enabled: self.config.enabled,
            ip_buckets: self.ip_buckets.len(),
            tenant_buckets: self.tenant_buckets.len(),
            total_rejections: self.total_rejections(),
            ip_rate_limit: self.config.points_per_sec_per_ip,
            tenant_rate_limit: self.config.points_per_sec_per_tenant,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    /// Whether rate limiting is enabled
    pub enabled: bool,
    /// Number of active IP buckets
    pub ip_buckets: usize,
    /// Number of active tenant buckets
    pub tenant_buckets: usize,
    /// Total number of rejected requests
    pub total_rejections: u64,
    /// Per-IP rate limit (points/sec)
    pub ip_rate_limit: usize,
    /// Per-tenant rate limit (points/sec)
    pub tenant_rate_limit: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.enabled);
        assert_eq!(config.points_per_sec_per_ip, 100_000);
    }

    #[test]
    fn test_rate_limit_config_validation() {
        let mut config = RateLimitConfig::default();

        config.points_per_sec_per_ip = 0;
        assert!(config.validate().is_err());

        config.points_per_sec_per_ip = 100;
        config.burst_multiplier = 0.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rate_limiter_basic() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 1000,
            burst_multiplier: 1.0, // No burst, capacity = rate
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Should allow up to 100 points (capacity)
        assert!(limiter.check_ip(ip, 50));
        assert!(limiter.check_ip(ip, 50));

        // Should reject - bucket empty
        assert!(!limiter.check_ip(ip, 1));
        assert_eq!(limiter.total_rejections(), 1);
    }

    #[test]
    fn test_rate_limiter_burst() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 1000,
            burst_multiplier: 2.0, // 2x burst, capacity = 200
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Should allow up to 200 points (2x burst)
        assert!(limiter.check_ip(ip, 200));

        // Should reject
        assert!(!limiter.check_ip(ip, 1));
    }

    #[test]
    fn test_rate_limiter_refill() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 1000, // 1000/sec = 1/ms
            points_per_sec_per_tenant: 10000,
            burst_multiplier: 1.0,
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Consume all tokens
        assert!(limiter.check_ip(ip, 1000));
        assert!(!limiter.check_ip(ip, 1));

        // Wait for refill (100ms should give ~100 tokens)
        thread::sleep(Duration::from_millis(100));

        // Should have some tokens now
        assert!(limiter.check_ip(ip, 50));
    }

    #[test]
    fn test_rate_limiter_disabled() {
        let config = RateLimitConfig {
            enabled: false,
            ..Default::default()
        };
        let limiter = RateLimiter::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Should always allow when disabled
        assert!(limiter.check_ip(ip, 1_000_000_000));
        assert!(limiter.check_ip(ip, 1_000_000_000));
        assert_eq!(limiter.total_rejections(), 0);
    }

    #[test]
    fn test_rate_limiter_tenant() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 50, // Lower tenant limit
            burst_multiplier: 1.0,
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);

        // Tenant limit of 50
        assert!(limiter.check_tenant("tenant1", 50));
        assert!(!limiter.check_tenant("tenant1", 1));

        // Different tenant has separate bucket
        assert!(limiter.check_tenant("tenant2", 50));
    }

    #[test]
    fn test_rate_limiter_cleanup() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 1000,
            burst_multiplier: 1.0,
            bucket_ttl: Duration::from_millis(10), // Very short TTL for testing
        };
        let limiter = RateLimiter::new(config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Create a bucket
        limiter.check_ip(ip, 1);
        assert_eq!(limiter.ip_buckets.len(), 1);

        // Wait for expiry
        thread::sleep(Duration::from_millis(50));

        // Cleanup should remove expired bucket
        limiter.cleanup();
        assert_eq!(limiter.ip_buckets.len(), 0);
    }

    #[test]
    fn test_rate_limiter_stats() {
        let config = RateLimitConfig::default();
        let limiter = RateLimiter::new(config);

        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        limiter.check_ip(ip1, 1);
        limiter.check_ip(ip2, 1);
        limiter.check_tenant("tenant1", 1);

        let stats = limiter.stats();
        assert_eq!(stats.ip_buckets, 2);
        assert_eq!(stats.tenant_buckets, 1);
        assert!(stats.enabled);
    }
}
