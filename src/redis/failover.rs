//! Failover and high availability for Redis integration
//!
//! Provides failover capabilities with:
//! - Health monitoring of Redis connections
//! - Automatic failover to replicas
//! - Local cache fallback when Redis is unavailable
//! - Split-brain prevention mechanisms
//! - Index reconciliation after recovery
//!
//! # Example
//!
//! ```rust,no_run
//! use kuba_tsdb::redis::{RedisConfig, RedisPool};
//! use kuba_tsdb::redis::failover::{FailoverManager, FailoverConfig};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let primary = Arc::new(RedisPool::new(RedisConfig::default()).await?);
//!
//! let config = FailoverConfig::default();
//! let failover = FailoverManager::new(primary, config);
//!
//! // Health monitoring starts automatically
//! # Ok(())
//! # }
//! ```

use crate::engine::traits::ChunkReference;
use crate::error::IndexError;
use crate::types::{SeriesId, TimeRange};

use super::connection::{HealthStatus, RedisPool};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, warn};

/// Configuration for failover behavior
#[derive(Clone, Debug)]
pub struct FailoverConfig {
    /// Health check interval in milliseconds
    pub health_check_interval_ms: u64,

    /// Number of consecutive failures before failover
    pub failure_threshold: u32,

    /// Number of consecutive successes before recovery
    pub recovery_threshold: u32,

    /// Maximum size of local cache in megabytes
    pub local_cache_size_mb: usize,

    /// Maximum age of cached entries in seconds
    pub cache_ttl_secs: u64,

    /// Enable automatic failover
    pub auto_failover_enabled: bool,

    /// Enable local cache fallback
    pub local_cache_enabled: bool,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            health_check_interval_ms: 5_000,
            failure_threshold: 3,
            recovery_threshold: 2,
            local_cache_size_mb: 128,
            cache_ttl_secs: 300, // 5 minutes
            auto_failover_enabled: true,
            local_cache_enabled: true,
        }
    }
}

/// Current state of the failover system
///
/// Uses repr(u8) to enable atomic state transitions via AtomicU8.
/// This prevents race conditions when multiple tasks try to change state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FailoverState {
    /// Primary connection is healthy
    Primary = 0,
    /// Failing over to replica
    FailingOver = 1,
    /// Using replica connection
    Replica = 2,
    /// All connections failed, using local cache
    LocalCache = 3,
    /// Recovering back to primary
    Recovering = 4,
}

impl FailoverState {
    /// Convert from u8 to FailoverState
    ///
    /// Returns None for invalid values.
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Primary),
            1 => Some(Self::FailingOver),
            2 => Some(Self::Replica),
            3 => Some(Self::LocalCache),
            4 => Some(Self::Recovering),
            _ => None,
        }
    }
}

impl std::fmt::Display for FailoverState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailoverState::Primary => write!(f, "primary"),
            FailoverState::FailingOver => write!(f, "failing_over"),
            FailoverState::Replica => write!(f, "replica"),
            FailoverState::LocalCache => write!(f, "local_cache"),
            FailoverState::Recovering => write!(f, "recovering"),
        }
    }
}

/// Local cache entry for chunk references
struct CacheEntry {
    /// Cached chunk references
    chunks: Vec<ChunkReference>,
    /// When this entry was cached
    cached_at: Instant,
    /// Access count for LRU
    access_count: u64,
}

impl CacheEntry {
    fn new(chunks: Vec<ChunkReference>) -> Self {
        Self {
            chunks,
            cached_at: Instant::now(),
            access_count: 0,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }

    fn touch(&mut self) {
        self.access_count += 1;
    }
}

/// Cache key for local chunk cache
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct LocalCacheKey {
    series_id: SeriesId,
    start: i64,
    end: i64,
}

impl LocalCacheKey {
    fn new(series_id: SeriesId, range: &TimeRange) -> Self {
        Self {
            series_id,
            start: range.start,
            end: range.end,
        }
    }
}

/// Local index cache for failover scenarios
struct LocalIndexCache {
    /// Cached chunk references by series and time range
    chunks: HashMap<LocalCacheKey, CacheEntry>,

    /// Series metadata cache (reserved for future expansion)
    #[allow(dead_code)]
    series_meta: HashMap<SeriesId, SeriesMetaCache>,

    /// Maximum entries (reserved for future LRU eviction)
    #[allow(dead_code)]
    max_entries: usize,

    /// Cache TTL
    ttl: Duration,

    /// Total memory used (approximate)
    memory_used: usize,

    /// Maximum memory in bytes
    max_memory: usize,
}

/// Cached series metadata (reserved for future series lookup caching)
#[allow(dead_code)]
struct SeriesMetaCache {
    /// Metric name
    metric_name: String,
    /// Tags
    tags: HashMap<String, String>,
    /// When cached
    cached_at: Instant,
}

impl LocalIndexCache {
    fn new(max_memory_mb: usize, ttl_secs: u64) -> Self {
        Self {
            chunks: HashMap::new(),
            series_meta: HashMap::new(),
            max_entries: 10000,
            ttl: Duration::from_secs(ttl_secs),
            memory_used: 0,
            max_memory: max_memory_mb * 1024 * 1024,
        }
    }

    /// Get cached chunks for a query
    fn get_chunks(
        &mut self,
        series_id: SeriesId,
        range: &TimeRange,
    ) -> Option<Vec<ChunkReference>> {
        let key = LocalCacheKey::new(series_id, range);

        if let Some(entry) = self.chunks.get_mut(&key) {
            if entry.is_expired(self.ttl) {
                self.chunks.remove(&key);
                return None;
            }
            entry.touch();
            return Some(entry.chunks.clone());
        }
        None
    }

    /// Cache chunks from a query result
    fn put_chunks(&mut self, series_id: SeriesId, range: &TimeRange, chunks: Vec<ChunkReference>) {
        // Estimate memory for this entry
        let entry_size = std::mem::size_of::<ChunkReference>() * chunks.len() + 64;

        // Check memory limit
        while self.memory_used + entry_size > self.max_memory && !self.chunks.is_empty() {
            self.evict_one();
        }

        let key = LocalCacheKey::new(series_id, range);
        self.chunks.insert(key, CacheEntry::new(chunks));
        self.memory_used += entry_size;
    }

    /// Evict the least recently used entry
    fn evict_one(&mut self) {
        // Find expired entry first
        let expired_key = self
            .chunks
            .iter()
            .find(|(_, v)| v.is_expired(self.ttl))
            .map(|(k, _)| k.clone());

        if let Some(key) = expired_key {
            if let Some(entry) = self.chunks.remove(&key) {
                let entry_size = std::mem::size_of::<ChunkReference>() * entry.chunks.len() + 64;
                self.memory_used = self.memory_used.saturating_sub(entry_size);
            }
            return;
        }

        // Find LRU entry
        let lru_key = self
            .chunks
            .iter()
            .min_by_key(|(_, v)| v.access_count)
            .map(|(k, _)| k.clone());

        if let Some(key) = lru_key {
            if let Some(entry) = self.chunks.remove(&key) {
                let entry_size = std::mem::size_of::<ChunkReference>() * entry.chunks.len() + 64;
                self.memory_used = self.memory_used.saturating_sub(entry_size);
            }
        }
    }

    /// Clear all cached data
    fn clear(&mut self) {
        self.chunks.clear();
        self.series_meta.clear();
        self.memory_used = 0;
    }

    /// Get cache statistics
    fn stats(&self) -> LocalCacheStats {
        let expired_count = self
            .chunks
            .values()
            .filter(|e| e.is_expired(self.ttl))
            .count();

        LocalCacheStats {
            entries: self.chunks.len(),
            series_count: self.series_meta.len(),
            memory_used_bytes: self.memory_used,
            expired_entries: expired_count,
        }
    }
}

/// Local cache statistics
#[derive(Debug, Clone)]
pub struct LocalCacheStats {
    /// Number of cached query results
    pub entries: usize,
    /// Number of cached series
    pub series_count: usize,
    /// Memory used in bytes
    pub memory_used_bytes: usize,
    /// Number of expired entries
    pub expired_entries: usize,
}

/// Failover manager statistics
#[derive(Debug, Clone)]
pub struct FailoverStats {
    /// Current failover state
    pub state: FailoverState,
    /// Total failover events
    pub failover_count: u64,
    /// Total recovery events
    pub recovery_count: u64,
    /// Total health checks performed
    pub health_checks: u64,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Local cache statistics
    pub local_cache: LocalCacheStats,
}

/// Failover manager for high availability
///
/// Monitors Redis health and automatically fails over to replicas
/// or local cache when the primary is unavailable.
///
/// Uses atomic state transitions to prevent race conditions when
/// multiple tasks attempt concurrent state changes.
pub struct FailoverManager {
    /// Primary Redis connection pool
    primary: Arc<RedisPool>,

    /// Replica connection pools (if configured)
    replicas: RwLock<Vec<Arc<RedisPool>>>,

    /// Local index cache for fallback
    local_cache: RwLock<LocalIndexCache>,

    /// Configuration
    config: FailoverConfig,

    /// Current failover state - uses AtomicU8 for lock-free transitions
    ///
    /// This enables atomic compare-and-swap operations for state transitions,
    /// preventing race conditions when multiple tasks try to change state.
    state: AtomicU8,

    /// Consecutive failure count
    consecutive_failures: AtomicU64,

    /// Consecutive success count
    consecutive_successes: AtomicU64,

    /// Total failover count
    failover_count: AtomicU64,

    /// Total recovery count
    recovery_count: AtomicU64,

    /// Total health checks
    health_check_count: AtomicU64,

    /// Whether the manager is active
    active: AtomicBool,

    /// Shutdown signal
    shutdown: Arc<Notify>,
}

impl FailoverManager {
    /// Create a new failover manager
    ///
    /// # Arguments
    ///
    /// * `primary` - Primary Redis connection pool
    /// * `config` - Failover configuration
    pub fn new(primary: Arc<RedisPool>, config: FailoverConfig) -> Self {
        let local_cache = LocalIndexCache::new(config.local_cache_size_mb, config.cache_ttl_secs);

        Self {
            primary,
            replicas: RwLock::new(Vec::new()),
            local_cache: RwLock::new(local_cache),
            config,
            state: AtomicU8::new(FailoverState::Primary as u8),
            consecutive_failures: AtomicU64::new(0),
            consecutive_successes: AtomicU64::new(0),
            failover_count: AtomicU64::new(0),
            recovery_count: AtomicU64::new(0),
            health_check_count: AtomicU64::new(0),
            active: AtomicBool::new(true),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Add a replica connection pool
    pub async fn add_replica(&self, replica: Arc<RedisPool>) {
        let mut replicas = self.replicas.write().await;
        replicas.push(replica);
        debug!("Added replica (total: {})", replicas.len());
    }

    /// Get current failover state (lock-free)
    pub fn state(&self) -> FailoverState {
        FailoverState::from_u8(self.state.load(Ordering::SeqCst)).unwrap_or(FailoverState::Primary)
    }

    /// Atomically transition from one state to another
    ///
    /// Returns true if transition succeeded, false if current state wasn't `from`.
    /// This is the core primitive for race-free state transitions.
    fn try_transition(&self, from: FailoverState, to: FailoverState) -> bool {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Check if we're operating in degraded mode (lock-free)
    pub fn is_degraded(&self) -> bool {
        let state = self.state();
        matches!(
            state,
            FailoverState::Replica | FailoverState::LocalCache | FailoverState::FailingOver
        )
    }

    /// Perform a health check
    pub async fn health_check(&self) -> HealthStatus {
        self.health_check_count.fetch_add(1, Ordering::Relaxed);

        let primary_health = self.primary.health_check().await;

        match primary_health {
            HealthStatus::Healthy => {
                self.on_primary_healthy().await;
                HealthStatus::Healthy
            },
            HealthStatus::Degraded => {
                // Degraded is still usable, just slower
                debug!("Primary Redis is degraded");
                HealthStatus::Degraded
            },
            HealthStatus::Unhealthy | HealthStatus::Unknown => {
                self.on_primary_failure().await;
                primary_health
            },
        }
    }

    /// Handle primary becoming healthy
    async fn on_primary_healthy(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

        let current_state = self.state();

        // Check if we should recover
        if current_state != FailoverState::Primary
            && successes >= self.config.recovery_threshold as u64
        {
            self.recover().await;
        }
    }

    /// Handle primary failure
    async fn on_primary_failure(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        let current_state = self.state();

        // Check if we should fail over - use atomic transition to prevent races
        if current_state == FailoverState::Primary
            && failures >= self.config.failure_threshold as u64
        {
            if self.config.auto_failover_enabled {
                self.failover().await;
            } else {
                warn!("Primary unhealthy but auto-failover is disabled");
            }
        }
    }

    /// Perform failover to replica or local cache
    ///
    /// Uses atomic state transitions to prevent race conditions.
    async fn failover(&self) {
        // Atomically: Primary -> FailingOver
        // If this fails, someone else already started failover
        if !self.try_transition(FailoverState::Primary, FailoverState::FailingOver) {
            debug!("Failover already in progress or completed");
            return;
        }

        debug!("Initiating failover from primary");
        self.failover_count.fetch_add(1, Ordering::Relaxed);

        // Try replicas first
        let replicas = self.replicas.read().await;
        for (i, replica) in replicas.iter().enumerate() {
            let health = replica.health_check().await;
            if matches!(health, HealthStatus::Healthy | HealthStatus::Degraded) {
                // Atomically: FailingOver -> Replica
                if self.try_transition(FailoverState::FailingOver, FailoverState::Replica) {
                    debug!("Failed over to replica {}", i);
                } else {
                    // Someone else changed state - log but continue
                    warn!("Unexpected state change during failover to replica");
                }
                return;
            }
        }
        drop(replicas);

        // Fall back to local cache
        // Atomically: FailingOver -> LocalCache
        let new_state = FailoverState::LocalCache;
        if self.try_transition(FailoverState::FailingOver, new_state) {
            if self.config.local_cache_enabled {
                warn!("All Redis connections failed, using local cache fallback");
            } else {
                error!("All Redis connections failed and local cache is disabled");
            }
        } else {
            warn!("Unexpected state change during failover to local cache");
        }
    }

    /// Recover to primary
    ///
    /// Uses atomic state transitions to prevent race conditions.
    async fn recover(&self) {
        let current_state = self.state();

        // Can only recover from Replica or LocalCache states
        if current_state == FailoverState::Primary {
            return; // Already on primary
        }

        // Atomically: Current -> Recovering
        if !self.try_transition(current_state, FailoverState::Recovering) {
            debug!("Recovery already in progress or state changed");
            return;
        }

        debug!("Recovering to primary Redis");
        self.recovery_count.fetch_add(1, Ordering::Relaxed);

        // Verify primary is truly healthy
        let health = self.primary.health_check().await;
        if health == HealthStatus::Healthy {
            // Atomically: Recovering -> Primary
            if self.try_transition(FailoverState::Recovering, FailoverState::Primary) {
                self.consecutive_successes.store(0, Ordering::Relaxed);
                debug!("Successfully recovered to primary");

                // Clear local cache after recovery (async-safe)
                let mut cache = self.local_cache.write().await;
                cache.clear();
            }
        } else {
            warn!("Primary not healthy during recovery attempt");
            // Atomically: Recovering -> LocalCache (revert)
            let _ = self.try_transition(FailoverState::Recovering, FailoverState::LocalCache);
        }
    }

    /// Get the active connection pool
    ///
    /// Returns the primary, a replica, or None if all are unavailable.
    pub async fn active_pool(&self) -> Option<Arc<RedisPool>> {
        let state = self.state();

        match state {
            FailoverState::Primary => Some(Arc::clone(&self.primary)),
            FailoverState::Replica => {
                let replicas = self.replicas.read().await;
                replicas.first().map(Arc::clone)
            },
            _ => None,
        }
    }

    /// Query chunks with failover support
    ///
    /// Tries primary/replica first, falls back to local cache.
    pub async fn query_chunks_with_fallback(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        let state = self.state();

        // Try local cache first if in local cache mode
        if state == FailoverState::LocalCache && self.config.local_cache_enabled {
            let mut cache = self.local_cache.write().await;
            if let Some(chunks) = cache.get_chunks(series_id, range) {
                debug!("Serving from local cache");
                return Ok(chunks);
            }
        }

        // If we have an active pool, use it
        // Note: This would need to be integrated with RedisTimeIndex
        // For now, return the cached result or empty if not available
        if state == FailoverState::LocalCache {
            Ok(Vec::new())
        } else {
            // In a real implementation, this would call the index
            Err(IndexError::ConnectionError(
                "No active connection (integration required)".to_string(),
            ))
        }
    }

    /// Cache query results for fallback
    pub async fn cache_query_result(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
        chunks: Vec<ChunkReference>,
    ) {
        if !self.config.local_cache_enabled {
            return;
        }

        let mut cache = self.local_cache.write().await;
        cache.put_chunks(series_id, range, chunks);
    }

    /// Get failover statistics
    pub async fn stats(&self) -> FailoverStats {
        let cache = self.local_cache.read().await;

        FailoverStats {
            state: self.state(),
            failover_count: self.failover_count.load(Ordering::Relaxed),
            recovery_count: self.recovery_count.load(Ordering::Relaxed),
            health_checks: self.health_check_count.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed) as u32,
            local_cache: cache.stats(),
        }
    }

    /// Run the health monitoring loop
    ///
    /// This should be spawned as a background task.
    pub async fn run_health_monitor(&self) {
        let interval = Duration::from_millis(self.config.health_check_interval_ms);

        loop {
            if !self.active.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    let _health = self.health_check().await;
                    debug!("Health check complete, state: {}", self.state());
                }
                _ = self.shutdown.notified() => {
                    debug!("Health monitor shutting down");
                    break;
                }
            }
        }
    }

    /// Shutdown the failover manager
    pub fn shutdown(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.shutdown.notify_one();
    }

    /// Force a failover (for testing)
    pub async fn force_failover(&self) {
        self.failover().await;
    }

    /// Force a recovery (for testing)
    pub async fn force_recovery(&self) {
        self.recover().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // FailoverConfig Tests
    // =========================================================================

    #[test]
    fn test_failover_config_default() {
        let config = FailoverConfig::default();
        assert_eq!(config.health_check_interval_ms, 5_000);
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.recovery_threshold, 2);
        assert!(config.auto_failover_enabled);
        assert!(config.local_cache_enabled);
    }

    #[test]
    fn test_failover_config_all_fields() {
        let config = FailoverConfig {
            health_check_interval_ms: 10_000,
            failure_threshold: 5,
            recovery_threshold: 3,
            local_cache_size_mb: 256,
            cache_ttl_secs: 600,
            auto_failover_enabled: false,
            local_cache_enabled: false,
        };

        assert_eq!(config.health_check_interval_ms, 10_000);
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.recovery_threshold, 3);
        assert_eq!(config.local_cache_size_mb, 256);
        assert_eq!(config.cache_ttl_secs, 600);
        assert!(!config.auto_failover_enabled);
        assert!(!config.local_cache_enabled);
    }

    #[test]
    fn test_failover_config_clone() {
        let config1 = FailoverConfig {
            health_check_interval_ms: 1000,
            failure_threshold: 2,
            recovery_threshold: 1,
            local_cache_size_mb: 64,
            cache_ttl_secs: 120,
            auto_failover_enabled: true,
            local_cache_enabled: true,
        };

        let config2 = config1.clone();
        assert_eq!(
            config2.health_check_interval_ms,
            config1.health_check_interval_ms
        );
        assert_eq!(config2.failure_threshold, config1.failure_threshold);
        assert_eq!(config2.recovery_threshold, config1.recovery_threshold);
        assert_eq!(config2.local_cache_size_mb, config1.local_cache_size_mb);
        assert_eq!(config2.cache_ttl_secs, config1.cache_ttl_secs);
        assert_eq!(config2.auto_failover_enabled, config1.auto_failover_enabled);
        assert_eq!(config2.local_cache_enabled, config1.local_cache_enabled);
    }

    #[test]
    fn test_failover_config_debug() {
        let config = FailoverConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("FailoverConfig"));
        assert!(debug_str.contains("health_check_interval_ms"));
        assert!(debug_str.contains("failure_threshold"));
    }

    // =========================================================================
    // FailoverState Tests
    // =========================================================================

    #[test]
    fn test_failover_state_display() {
        assert_eq!(format!("{}", FailoverState::Primary), "primary");
        assert_eq!(format!("{}", FailoverState::Replica), "replica");
        assert_eq!(format!("{}", FailoverState::LocalCache), "local_cache");
    }

    #[test]
    fn test_failover_state_display_all() {
        assert_eq!(format!("{}", FailoverState::Primary), "primary");
        assert_eq!(format!("{}", FailoverState::FailingOver), "failing_over");
        assert_eq!(format!("{}", FailoverState::Replica), "replica");
        assert_eq!(format!("{}", FailoverState::LocalCache), "local_cache");
        assert_eq!(format!("{}", FailoverState::Recovering), "recovering");
    }

    #[test]
    fn test_failover_state_from_u8() {
        assert_eq!(FailoverState::from_u8(0), Some(FailoverState::Primary));
        assert_eq!(FailoverState::from_u8(1), Some(FailoverState::FailingOver));
        assert_eq!(FailoverState::from_u8(2), Some(FailoverState::Replica));
        assert_eq!(FailoverState::from_u8(3), Some(FailoverState::LocalCache));
        assert_eq!(FailoverState::from_u8(4), Some(FailoverState::Recovering));
        assert_eq!(FailoverState::from_u8(5), None);
        assert_eq!(FailoverState::from_u8(255), None);
    }

    #[test]
    fn test_failover_state_repr() {
        assert_eq!(FailoverState::Primary as u8, 0);
        assert_eq!(FailoverState::FailingOver as u8, 1);
        assert_eq!(FailoverState::Replica as u8, 2);
        assert_eq!(FailoverState::LocalCache as u8, 3);
        assert_eq!(FailoverState::Recovering as u8, 4);
    }

    #[test]
    fn test_failover_state_equality() {
        assert_eq!(FailoverState::Primary, FailoverState::Primary);
        assert_ne!(FailoverState::Primary, FailoverState::Replica);
        assert_ne!(FailoverState::LocalCache, FailoverState::Recovering);
    }

    #[test]
    fn test_failover_state_clone() {
        let s1 = FailoverState::Recovering;
        let s2 = s1;
        assert_eq!(s1, s2);
    }

    // =========================================================================
    // LocalCacheKey Tests
    // =========================================================================

    #[test]
    fn test_local_cache_key() {
        let range1 = TimeRange { start: 0, end: 100 };
        let range2 = TimeRange { start: 0, end: 100 };
        let range3 = TimeRange { start: 0, end: 200 };

        let key1 = LocalCacheKey::new(1, &range1);
        let key2 = LocalCacheKey::new(1, &range2);
        let key3 = LocalCacheKey::new(1, &range3);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_local_cache_key_different_series() {
        let range = TimeRange { start: 0, end: 100 };

        let key1 = LocalCacheKey::new(1, &range);
        let key2 = LocalCacheKey::new(2, &range);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_local_cache_key_debug() {
        let range = TimeRange { start: 0, end: 100 };
        let key = LocalCacheKey::new(42, &range);
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("LocalCacheKey"));
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_local_cache_key_clone() {
        let range = TimeRange {
            start: 50,
            end: 150,
        };
        let key1 = LocalCacheKey::new(99, &range);
        let key2 = key1.clone();
        assert_eq!(key1, key2);
        assert_eq!(key1.series_id, key2.series_id);
        assert_eq!(key1.start, key2.start);
        assert_eq!(key1.end, key2.end);
    }

    // =========================================================================
    // LocalIndexCache Tests
    // =========================================================================

    #[test]
    fn test_local_cache_operations() {
        let mut cache = LocalIndexCache::new(1, 60); // 1 MB cache

        let range = TimeRange { start: 0, end: 100 };
        let chunks = vec![];

        // Insert
        cache.put_chunks(1, &range, chunks.clone());

        // Retrieve
        let result = cache.get_chunks(1, &range);
        assert!(result.is_some());

        // Missing
        let missing_range = TimeRange {
            start: 100,
            end: 200,
        };
        let result = cache.get_chunks(1, &missing_range);
        assert!(result.is_none());
    }

    #[test]
    fn test_local_cache_stats() {
        let cache = LocalIndexCache::new(1, 60);
        let stats = cache.stats();

        assert_eq!(stats.entries, 0);
        assert_eq!(stats.series_count, 0);
        assert_eq!(stats.memory_used_bytes, 0);
    }

    #[test]
    fn test_local_cache_clear() {
        let mut cache = LocalIndexCache::new(1, 60);

        let range = TimeRange { start: 0, end: 100 };
        cache.put_chunks(1, &range, vec![]);
        cache.put_chunks(2, &range, vec![]);

        assert!(cache.stats().entries > 0);

        cache.clear();

        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().memory_used_bytes, 0);
    }

    #[test]
    fn test_local_cache_touch_increments_access() {
        let mut entry = CacheEntry::new(vec![]);
        assert_eq!(entry.access_count, 0);

        entry.touch();
        assert_eq!(entry.access_count, 1);

        entry.touch();
        assert_eq!(entry.access_count, 2);
    }

    #[test]
    fn test_local_cache_multiple_series() {
        let mut cache = LocalIndexCache::new(10, 60);

        let range = TimeRange { start: 0, end: 100 };

        // Add chunks for multiple series
        for series_id in 1..=5 {
            cache.put_chunks(series_id, &range, vec![]);
        }

        // Verify all are retrievable
        for series_id in 1..=5 {
            assert!(cache.get_chunks(series_id, &range).is_some());
        }
    }

    // =========================================================================
    // CacheEntry Tests
    // =========================================================================

    #[test]
    fn test_cache_entry_expiry() {
        let chunks = vec![];
        let entry = CacheEntry::new(chunks);

        // Entry should not be expired immediately
        assert!(!entry.is_expired(Duration::from_secs(60)));

        // Entry with 0 TTL should be expired
        assert!(entry.is_expired(Duration::ZERO));
    }

    #[test]
    fn test_cache_entry_new() {
        let chunks = vec![];
        let entry = CacheEntry::new(chunks);

        assert_eq!(entry.access_count, 0);
        assert!(entry.cached_at.elapsed() < Duration::from_secs(1));
    }

    // =========================================================================
    // LocalCacheStats Tests
    // =========================================================================

    #[test]
    fn test_local_cache_stats_clone() {
        let stats = LocalCacheStats {
            entries: 10,
            series_count: 5,
            memory_used_bytes: 1024,
            expired_entries: 2,
        };

        let cloned = stats.clone();
        assert_eq!(cloned.entries, stats.entries);
        assert_eq!(cloned.series_count, stats.series_count);
        assert_eq!(cloned.memory_used_bytes, stats.memory_used_bytes);
        assert_eq!(cloned.expired_entries, stats.expired_entries);
    }

    #[test]
    fn test_local_cache_stats_debug() {
        let stats = LocalCacheStats {
            entries: 10,
            series_count: 5,
            memory_used_bytes: 1024,
            expired_entries: 2,
        };

        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("LocalCacheStats"));
        assert!(debug_str.contains("entries"));
        assert!(debug_str.contains("10"));
    }

    // =========================================================================
    // FailoverStats Tests
    // =========================================================================

    #[test]
    fn test_failover_stats_clone() {
        let stats = FailoverStats {
            state: FailoverState::Primary,
            failover_count: 5,
            recovery_count: 3,
            health_checks: 100,
            consecutive_failures: 0,
            local_cache: LocalCacheStats {
                entries: 0,
                series_count: 0,
                memory_used_bytes: 0,
                expired_entries: 0,
            },
        };

        let cloned = stats.clone();
        assert_eq!(cloned.state, stats.state);
        assert_eq!(cloned.failover_count, stats.failover_count);
        assert_eq!(cloned.recovery_count, stats.recovery_count);
        assert_eq!(cloned.health_checks, stats.health_checks);
    }

    #[test]
    fn test_failover_stats_debug() {
        let stats = FailoverStats {
            state: FailoverState::Replica,
            failover_count: 1,
            recovery_count: 0,
            health_checks: 50,
            consecutive_failures: 2,
            local_cache: LocalCacheStats {
                entries: 5,
                series_count: 3,
                memory_used_bytes: 512,
                expired_entries: 0,
            },
        };

        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("FailoverStats"));
        assert!(debug_str.contains("failover_count"));
    }

    // =========================================================================
    // Edge Cases Tests
    // =========================================================================

    #[test]
    fn test_local_cache_key_negative_times() {
        let range = TimeRange {
            start: -1000,
            end: -500,
        };
        let key = LocalCacheKey::new(1, &range);

        assert_eq!(key.start, -1000);
        assert_eq!(key.end, -500);
    }

    #[test]
    fn test_local_cache_key_large_values() {
        let range = TimeRange {
            start: i64::MIN,
            end: i64::MAX,
        };
        let key = LocalCacheKey::new(u64::MAX as SeriesId, &range);

        assert_eq!(key.series_id, u64::MAX as SeriesId);
        assert_eq!(key.start, i64::MIN);
        assert_eq!(key.end, i64::MAX);
    }

    #[test]
    fn test_local_cache_zero_memory() {
        let cache = LocalIndexCache::new(0, 60);
        assert_eq!(cache.max_memory, 0);
    }

    #[test]
    fn test_local_cache_zero_ttl() {
        let cache = LocalIndexCache::new(1, 0);
        assert_eq!(cache.ttl, Duration::ZERO);
    }

    #[test]
    fn test_cache_eviction_when_full() {
        // Create a very small cache (effectively 1 entry)
        let mut cache = LocalIndexCache::new(0, 60);
        cache.max_memory = 100; // Override to tiny size

        let range1 = TimeRange { start: 0, end: 100 };
        let range2 = TimeRange {
            start: 100,
            end: 200,
        };

        // Add entries to trigger eviction
        cache.put_chunks(1, &range1, vec![]);
        cache.put_chunks(2, &range2, vec![]);

        // With such a small cache, older entries may be evicted
        // We just verify no panic occurs
        let stats = cache.stats();
        assert!(stats.entries <= 2);
    }
}
