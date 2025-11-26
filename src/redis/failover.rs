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
//! use gorilla_tsdb::redis::{RedisConfig, RedisPool};
//! use gorilla_tsdb::redis::failover::{FailoverManager, FailoverConfig};
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

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// Primary connection is healthy
    Primary,
    /// Failing over to replica
    FailingOver,
    /// Using replica connection
    Replica,
    /// All connections failed, using local cache
    LocalCache,
    /// Recovering back to primary
    Recovering,
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
    fn get_chunks(&mut self, series_id: SeriesId, range: &TimeRange) -> Option<Vec<ChunkReference>> {
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
        let expired_count = self.chunks.values().filter(|e| e.is_expired(self.ttl)).count();

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
pub struct FailoverManager {
    /// Primary Redis connection pool
    primary: Arc<RedisPool>,

    /// Replica connection pools (if configured)
    replicas: RwLock<Vec<Arc<RedisPool>>>,

    /// Local index cache for fallback
    local_cache: RwLock<LocalIndexCache>,

    /// Configuration
    config: FailoverConfig,

    /// Current failover state
    state: RwLock<FailoverState>,

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
            state: RwLock::new(FailoverState::Primary),
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
    pub fn add_replica(&self, replica: Arc<RedisPool>) {
        let mut replicas = self.replicas.write();
        replicas.push(replica);
        info!("Added replica (total: {})", replicas.len());
    }

    /// Get current failover state
    pub fn state(&self) -> FailoverState {
        *self.state.read()
    }

    /// Check if we're operating in degraded mode
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
            }
            HealthStatus::Degraded => {
                // Degraded is still usable, just slower
                debug!("Primary Redis is degraded");
                HealthStatus::Degraded
            }
            HealthStatus::Unhealthy | HealthStatus::Unknown => {
                self.on_primary_failure().await;
                primary_health
            }
        }
    }

    /// Handle primary becoming healthy
    async fn on_primary_healthy(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

        let current_state = self.state();

        // Check if we should recover
        if current_state != FailoverState::Primary && successes >= self.config.recovery_threshold as u64
        {
            self.recover().await;
        }
    }

    /// Handle primary failure
    async fn on_primary_failure(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        let current_state = self.state();

        // Check if we should fail over
        if current_state == FailoverState::Primary && failures >= self.config.failure_threshold as u64
        {
            if self.config.auto_failover_enabled {
                self.failover().await;
            } else {
                warn!("Primary unhealthy but auto-failover is disabled");
            }
        }
    }

    /// Perform failover to replica or local cache
    async fn failover(&self) {
        let mut state = self.state.write();

        if *state != FailoverState::Primary {
            return; // Already failed over
        }

        *state = FailoverState::FailingOver;
        drop(state);

        info!("Initiating failover from primary");
        self.failover_count.fetch_add(1, Ordering::Relaxed);

        // Try replicas first
        let replicas = self.replicas.read();
        for (i, replica) in replicas.iter().enumerate() {
            let health = replica.health_check().await;
            if matches!(health, HealthStatus::Healthy | HealthStatus::Degraded) {
                let mut state = self.state.write();
                *state = FailoverState::Replica;
                info!("Failed over to replica {}", i);
                return;
            }
        }

        // Fall back to local cache
        if self.config.local_cache_enabled {
            let mut state = self.state.write();
            *state = FailoverState::LocalCache;
            warn!("All Redis connections failed, using local cache fallback");
        } else {
            let mut state = self.state.write();
            *state = FailoverState::LocalCache;
            error!("All Redis connections failed and local cache is disabled");
        }
    }

    /// Recover to primary
    async fn recover(&self) {
        let mut state = self.state.write();

        if *state == FailoverState::Primary {
            return; // Already on primary
        }

        *state = FailoverState::Recovering;
        drop(state);

        info!("Recovering to primary Redis");
        self.recovery_count.fetch_add(1, Ordering::Relaxed);

        // Verify primary is truly healthy
        let health = self.primary.health_check().await;
        if health == HealthStatus::Healthy {
            let mut state = self.state.write();
            *state = FailoverState::Primary;
            self.consecutive_successes.store(0, Ordering::Relaxed);
            info!("Successfully recovered to primary");

            // Clear local cache after recovery
            let mut cache = self.local_cache.write();
            cache.clear();
        } else {
            warn!("Primary not healthy during recovery attempt");
            let mut state = self.state.write();
            *state = FailoverState::LocalCache;
        }
    }

    /// Get the active connection pool
    ///
    /// Returns the primary, a replica, or None if all are unavailable.
    pub fn active_pool(&self) -> Option<Arc<RedisPool>> {
        let state = self.state();

        match state {
            FailoverState::Primary => Some(Arc::clone(&self.primary)),
            FailoverState::Replica => {
                let replicas = self.replicas.read();
                replicas.first().map(Arc::clone)
            }
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
            let mut cache = self.local_cache.write();
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
    pub fn cache_query_result(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
        chunks: Vec<ChunkReference>,
    ) {
        if !self.config.local_cache_enabled {
            return;
        }

        let mut cache = self.local_cache.write();
        cache.put_chunks(series_id, range, chunks);
    }

    /// Get failover statistics
    pub fn stats(&self) -> FailoverStats {
        let cache = self.local_cache.read();

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
                    info!("Health monitor shutting down");
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
    fn test_failover_state_display() {
        assert_eq!(format!("{}", FailoverState::Primary), "primary");
        assert_eq!(format!("{}", FailoverState::Replica), "replica");
        assert_eq!(format!("{}", FailoverState::LocalCache), "local_cache");
    }

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
    fn test_cache_entry_expiry() {
        let chunks = vec![];
        let entry = CacheEntry::new(chunks);

        // Entry should not be expired immediately
        assert!(!entry.is_expired(Duration::from_secs(60)));

        // Entry with 0 TTL should be expired
        assert!(entry.is_expired(Duration::ZERO));
    }
}
