//! Token bucket rate limiter
//!
//! Provides rate limiting for network ingestion using the token bucket algorithm.
//! Supports per-IP and per-tenant rate limits with configurable burst capacity.
//!
//! # IPv6 Rate Limiting
//!
//! For IPv6 addresses, rate limiting is applied per /64 prefix to prevent
//! bypass attacks where clients use different addresses from the same subnet.

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
///
/// Uses atomic operations for token consumption and standard mutex for timing.
/// Mutex poisoning is handled gracefully to prevent panics.
struct TokenBucket {
    /// Available tokens
    tokens: AtomicU64,
    /// Last refill time (using std Mutex - blocking is fine for short duration)
    last_refill: std::sync::Mutex<Instant>,
    /// Last access time (for TTL)
    last_access: std::sync::Mutex<Instant>,
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
            last_refill: std::sync::Mutex::new(now),
            last_access: std::sync::Mutex::new(now),
            capacity,
            refill_rate: rate_per_sec,
        }
    }

    /// Try to consume tokens from the bucket
    ///
    /// Returns true if tokens were consumed, false if insufficient tokens.
    /// Handles mutex poisoning gracefully.
    fn try_consume(&self, tokens_needed: u64) -> bool {
        self.refill();

        // Update last access time - handle poisoned mutex
        if let Ok(mut last_access) = self.last_access.lock() {
            *last_access = Instant::now();
        }
        // If mutex is poisoned, we still continue - rate limiting degrades gracefully

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
    ///
    /// Handles mutex poisoning gracefully - on poison, tokens are not refilled
    /// but the bucket remains functional.
    fn refill(&self) {
        // Try to acquire lock - if poisoned, skip refill
        let mut last_refill = match self.last_refill.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Recover from poisoned mutex
                poisoned.into_inner()
            }
        };

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
    ///
    /// Returns true if expired or if mutex is poisoned (treat as stale).
    fn is_expired(&self, ttl: Duration) -> bool {
        match self.last_access.lock() {
            Ok(last_access) => last_access.elapsed() > ttl,
            Err(_) => true, // Treat poisoned mutex as expired
        }
    }

    /// Get current token count
    fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }
}

/// Key type for IP-based rate limiting
///
/// For IPv4: uses the full address
/// For IPv6: uses the /64 prefix to prevent bypass attacks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IpKey {
    V4(std::net::Ipv4Addr),
    V6Prefix(u64), // First 64 bits of IPv6 address
}

impl From<IpAddr> for IpKey {
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => IpKey::V4(v4),
            IpAddr::V6(v6) => {
                // Extract first 64 bits (/64 prefix)
                let segments = v6.segments();
                let prefix = ((segments[0] as u64) << 48)
                    | ((segments[1] as u64) << 32)
                    | ((segments[2] as u64) << 16)
                    | (segments[3] as u64);
                IpKey::V6Prefix(prefix)
            }
        }
    }
}

/// Token bucket rate limiter
///
/// Provides rate limiting using the token bucket algorithm with support for:
/// - Per-IP rate limiting (IPv6 uses /64 prefix to prevent bypass)
/// - Per-tenant rate limiting
/// - Burst capacity
/// - Automatic bucket cleanup via background task
///
/// # IPv6 Considerations
///
/// IPv6 addresses are rate-limited by their /64 prefix to prevent attackers
/// from bypassing rate limits by using different addresses from the same subnet.
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
    /// Per-IP buckets (uses IpKey for IPv6 /64 prefix handling)
    ip_buckets: DashMap<IpKey, TokenBucket>,
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

    /// Create a rate limiter wrapped in Arc for sharing across tasks
    ///
    /// Also spawns a background cleanup task that runs periodically.
    pub fn new_with_cleanup(config: RateLimitConfig) -> Arc<Self> {
        let limiter = Arc::new(Self::new(config.clone()));

        // Spawn background cleanup task
        let cleanup_limiter = Arc::clone(&limiter);
        let cleanup_interval = config.bucket_ttl / 2; // Run at half TTL interval

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                cleanup_limiter.cleanup();
            }
        });

        limiter
    }

    /// Check if request is allowed for an IP and consume tokens
    ///
    /// # Arguments
    ///
    /// * `ip` - Client IP address (IPv6 addresses are grouped by /64 prefix)
    /// * `points` - Number of points/tokens to consume
    ///
    /// # Returns
    ///
    /// `true` if allowed, `false` if rate limited
    pub fn check_ip(&self, ip: IpAddr, points: u64) -> bool {
        if !self.config.enabled {
            return true;
        }

        // Convert IP to key (IPv6 uses /64 prefix)
        let ip_key = IpKey::from(ip);

        let bucket = self.ip_buckets.entry(ip_key).or_insert_with(|| {
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
        let ip_key = IpKey::from(ip);
        self.ip_buckets
            .get(&ip_key)
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
        let config = RateLimitConfig {
            points_per_sec_per_ip: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = RateLimitConfig {
            points_per_sec_per_ip: 100,
            burst_multiplier: 0.5,
            ..Default::default()
        };
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

        // Consume all 100 tokens in one call to avoid refill race
        assert!(limiter.check_ip(ip, 100));

        // Immediately try to consume more than could have been refilled
        // At 100 tokens/sec, even 10ms would only refill 1 token
        // Requesting 100 should fail
        assert!(!limiter.check_ip(ip, 100));
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

        // Immediately try to consume more than could have been refilled
        // At 100 tokens/sec, even 10ms would only refill 1 token
        // Requesting 200 should fail
        assert!(!limiter.check_ip(ip, 200));
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

        // Immediately requesting all tokens again should fail
        assert!(!limiter.check_ip(ip, 1000));

        // Wait for refill (100ms should give ~100 tokens)
        thread::sleep(Duration::from_millis(150));

        // Should have some tokens now (at least 100 after 100ms at 1000/sec)
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

    #[test]
    fn test_ipv6_prefix_rate_limiting() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 1000,
            burst_multiplier: 1.0,
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);

        // Two different IPv6 addresses from the same /64 prefix
        let ip1: IpAddr = "2001:db8:1234:5678::1".parse().unwrap();
        let ip2: IpAddr = "2001:db8:1234:5678::2".parse().unwrap();
        // Different /64 prefix
        let ip3: IpAddr = "2001:db8:1234:5679::1".parse().unwrap();

        // Consume all tokens from first IP
        assert!(limiter.check_ip(ip1, 100));

        // Second IP from same /64 should be rejected when requesting all tokens
        // (shared bucket, so requesting 100 more should fail)
        assert!(!limiter.check_ip(ip2, 100));

        // Third IP from different /64 should have its own bucket
        assert!(limiter.check_ip(ip3, 100));

        // Only 2 buckets should exist (one per /64 prefix)
        assert_eq!(limiter.ip_buckets.len(), 2);
    }

    #[test]
    fn test_ipv4_full_address_rate_limiting() {
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 100,
            points_per_sec_per_tenant: 1000,
            burst_multiplier: 1.0,
            bucket_ttl: Duration::from_secs(60),
        };
        let limiter = RateLimiter::new(config);

        // Two different IPv4 addresses from the same /24 subnet
        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();

        // Consume all tokens from first IP
        assert!(limiter.check_ip(ip1, 100));

        // Second IPv4 should have its own bucket (no prefix grouping)
        assert!(limiter.check_ip(ip2, 100));

        // 2 buckets for IPv4 (one per address)
        assert_eq!(limiter.ip_buckets.len(), 2);
    }

    #[test]
    fn test_ip_key_conversion() {
        // IPv4
        let ip4: IpAddr = "192.168.1.1".parse().unwrap();
        let key4 = IpKey::from(ip4);
        assert!(matches!(key4, IpKey::V4(_)));

        // IPv6 from same /64 should produce same prefix
        let ip6a: IpAddr = "2001:db8:1234:5678:aaaa:bbbb:cccc:dddd".parse().unwrap();
        let ip6b: IpAddr = "2001:db8:1234:5678:1111:2222:3333:4444".parse().unwrap();
        let key6a = IpKey::from(ip6a);
        let key6b = IpKey::from(ip6b);

        assert!(matches!(key6a, IpKey::V6Prefix(_)));
        assert_eq!(key6a, key6b); // Same /64 prefix should produce same key
    }
}
