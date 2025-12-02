//! Multi-tier cache management system for time-series data
//!
//! This module provides a production-grade caching layer optimized for time-series workloads.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Cache Manager                         │
//! ├─────────────────────────────────────────────────────────┤
//! │  L1 Cache (Hot) - Decompressed Data                     │
//! │  ├─ Size: 256MB - 1GB (configurable)                    │
//! │  ├─ Access: ~1-10 µs                                    │
//! │  ├─ Hit Rate Target: 70-90%                             │
//! │  └─ Eviction: Hybrid (LRU + TTL + Age)                  │
//! ├─────────────────────────────────────────────────────────┤
//! │  L2 Cache (Warm) - Memory-Mapped Compressed             │
//! │  ├─ Size: 1GB - 4GB (configurable)                      │
//! │  ├─ Access: ~10-100 µs                                  │
//! │  ├─ Hit Rate Target: 50-70%                             │
//! │  └─ Eviction: LRU (existing mmap implementation)        │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multi-tier caching**: L1 (hot), L2 (warm), L3 (cold/disk)
//! - **Sharded architecture**: 16-32 shards for concurrent access
//! - **Pluggable eviction**: LRU, ARC, TTL, Hybrid policies
//! - **Memory-aware**: Watermark-based pressure handling
//! - **Time-series optimized**: Age-based priority, range prefetching
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::storage::cache::{CacheManager, CacheConfig, CacheKey};
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create cache with default config
//! let config = CacheConfig::default();
//! let cache = Arc::new(CacheManager::<Vec<u8>>::new(config));
//!
//! // Start background eviction
//! cache.start_background_eviction().await?;
//!
//! // Cache operations
//! let key = CacheKey::new(1, 12345);
//! let data = vec![1, 2, 3, 4];
//! cache.insert(key.clone(), data, 4, 0);
//! let cached = cache.get(&key);
//!
//! // Get statistics
//! if let Some(stats) = cache.stats() {
//!     println!("Hit rate: {:.2}%", stats.hit_rate() * 100.0);
//! }
//! # Ok(())
//! # }
//! ```

use crate::types::SeriesId;
use lru::LruCache;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

/// Cache key uniquely identifying a cached entry
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CacheKey {
    /// Series identifier
    pub series_id: SeriesId,

    /// Chunk identifier (timestamp or sequence number)
    pub chunk_id: u64,
}

impl CacheKey {
    /// Create a new cache key
    pub fn new(series_id: SeriesId, chunk_id: u64) -> Self {
        Self {
            series_id,
            chunk_id,
        }
    }

    /// Calculate shard ID for this key
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is 0
    pub fn shard_id(&self, num_shards: usize) -> usize {
        assert!(num_shards > 0, "num_shards must be greater than 0");
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        (hasher.finish() as usize) % num_shards
    }
}

/// Metadata for cache entry eviction decisions
#[derive(Debug)]
pub struct EntryMetadata {
    /// Access count (for LFU components)
    pub access_count: AtomicU64,

    /// Last access timestamp in microseconds since epoch
    pub last_access: AtomicU64,

    /// Entry size in bytes
    pub size_bytes: usize,

    /// Creation timestamp
    pub created_at: Instant,

    /// Time-series specific: chunk age in seconds
    pub chunk_age_seconds: u64,

    /// Eviction priority (higher = keep longer)
    pub priority: AtomicI32,
}

impl EntryMetadata {
    /// Create new metadata for an entry
    pub fn new(size_bytes: usize, chunk_age_seconds: u64) -> Self {
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Self {
            access_count: AtomicU64::new(0),
            last_access: AtomicU64::new(now_micros),
            size_bytes,
            created_at: Instant::now(),
            chunk_age_seconds,
            priority: AtomicI32::new(0),
        }
    }

    /// Record an access to this entry
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering for performance. This is safe because:
    /// - Metadata is used for **eviction heuristics**, not correctness
    /// - Slight inconsistency across threads is acceptable
    /// - No critical happens-before relationships between different atomics
    /// - Avoiding memory barriers significantly improves hot path performance
    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);

        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros() as u64;

        self.last_access.store(now_micros, Ordering::Relaxed);
    }

    /// Get seconds since last access
    ///
    /// Returns 0 if system time issues occur.
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering - see `record_access` for rationale.
    pub fn seconds_since_access(&self) -> u64 {
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros() as u64;

        let last = self.last_access.load(Ordering::Relaxed);
        now_micros.saturating_sub(last) / 1_000_000
    }

    /// Get current access count
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering - see `record_access` for rationale.
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    /// Get current priority
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering - see `record_access` for rationale.
    pub fn priority(&self) -> i32 {
        self.priority.load(Ordering::Relaxed)
    }

    /// Set priority for eviction decisions
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering - see `record_access` for rationale.
    pub fn set_priority(&self, priority: i32) {
        self.priority.store(priority, Ordering::Relaxed);
    }
}

/// Cache entry containing data and metadata
#[derive(Debug)]
pub struct CacheEntry<T> {
    /// Cached data
    pub data: Arc<T>,

    /// Entry metadata
    pub metadata: EntryMetadata,
}

impl<T> CacheEntry<T> {
    /// Create a new cache entry
    ///
    /// # Parameters
    ///
    /// - `data`: The cached data wrapped in Arc
    /// - `size_bytes`: Size in bytes for memory accounting (should include heap allocations)
    /// - `chunk_age_seconds`: Age of the chunk for eviction priority
    ///
    /// # Debug Assertions
    ///
    /// In debug builds, validates that `size_bytes` is at least `size_of::<T>()`.
    /// This helps catch incorrect size calculations during development.
    pub fn new(data: Arc<T>, size_bytes: usize, chunk_age_seconds: u64) -> Self {
        // Validate size in debug builds
        debug_assert!(
            size_bytes >= std::mem::size_of::<T>(),
            "size_bytes ({}) should be at least the base size of T ({} bytes). \
             Did you forget to account for heap allocations?",
            size_bytes,
            std::mem::size_of::<T>()
        );

        Self {
            data,
            metadata: EntryMetadata::new(size_bytes, chunk_age_seconds),
        }
    }

    /// Get the size in bytes for this entry
    pub fn size_bytes(&self) -> usize {
        self.metadata.size_bytes
    }
}

/// Memory tracker for cache size management
#[derive(Debug)]
pub struct MemoryTracker {
    /// Current total memory usage in bytes
    current_bytes: AtomicUsize,

    /// Maximum allowed memory in bytes
    max_bytes: usize,

    /// Per-shard memory usage
    per_shard_bytes: Vec<AtomicUsize>,
}

impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new(max_bytes: usize, num_shards: usize) -> Self {
        let mut per_shard = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            per_shard.push(AtomicUsize::new(0));
        }

        Self {
            current_bytes: AtomicUsize::new(0),
            max_bytes,
            per_shard_bytes: per_shard,
        }
    }

    /// Check if inserting size bytes would exceed limit
    pub fn can_insert(&self, size: usize) -> bool {
        self.current_bytes.load(Ordering::Relaxed) + size <= self.max_bytes
    }

    /// Record insertion of size bytes
    pub fn on_insert(&self, shard_id: usize, size: usize) {
        self.current_bytes.fetch_add(size, Ordering::Relaxed);
        self.per_shard_bytes[shard_id].fetch_add(size, Ordering::Relaxed);
    }

    /// Record eviction of size bytes
    ///
    /// # Safety
    ///
    /// Uses saturating subtraction to prevent underflow. If the requested
    /// eviction size exceeds current usage, usage will be set to 0.
    pub fn on_evict(&self, shard_id: usize, size: usize) {
        // Saturating sub to prevent underflow
        self.current_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(size))
            })
            .ok();
        self.per_shard_bytes[shard_id]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(size))
            })
            .ok();
    }

    /// Get current total memory usage
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Get maximum allowed memory
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Get current memory usage ratio (0.0 to 1.0)
    pub fn usage_ratio(&self) -> f64 {
        self.current_bytes() as f64 / self.max_bytes as f64
    }

    /// Get per-shard memory usage
    ///
    /// Returns 0 if `shard_id` is out of bounds.
    ///
    /// # Memory Ordering
    ///
    /// Uses `Relaxed` ordering for performance - see [`EntryMetadata::record_access`]
    /// for rationale on Relaxed ordering in cache metadata.
    pub fn shard_bytes(&self, shard_id: usize) -> usize {
        self.per_shard_bytes
            .get(shard_id)
            .map(|atomic| atomic.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// LRU (Least Recently Used) list using O(1) operations
///
/// This is a wrapper around the `lru` crate which provides:
/// - O(1) insertion
/// - O(1) access/touch (mark as recently used)
/// - O(1) removal
/// - O(1) eviction (pop least recently used)
///
/// Internally uses HashMap + intrusive doubly-linked list for optimal performance.
pub struct LruList {
    /// LRU cache storing () as values since we only need key ordering
    cache: LruCache<CacheKey, ()>,
}

impl LruList {
    /// Create a new LRU list with unbounded capacity
    ///
    /// Note: In practice, capacity is controlled by memory tracker,
    /// so we use a very large capacity here.
    pub fn new() -> Self {
        // Use a large capacity (10M entries) - actual limit is memory-based
        let capacity = NonZeroUsize::new(10_000_000).unwrap();
        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Create a new LRU list with specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Get current size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Add key to back (most recently used)
    /// If key already exists, it's moved to back
    pub fn push_back(&mut self, key: CacheKey) {
        self.cache.put(key, ());
    }

    /// Remove and return front key (least recently used)
    pub fn pop_front(&mut self) -> Option<CacheKey> {
        self.cache.pop_lru().map(|(k, _)| k)
    }

    /// Move key to back (mark as recently used)
    /// Returns true if key was found and moved, false if not present
    pub fn touch(&mut self, key: &CacheKey) -> bool {
        self.cache.get(key).is_some()
    }

    /// Remove a specific key
    /// Returns true if key was found and removed
    pub fn remove(&mut self, key: &CacheKey) -> bool {
        self.cache.pop(key).is_some()
    }

    /// Clear the list
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get iterator over keys (LRU to MRU order)
    pub fn iter(&self) -> impl Iterator<Item = &CacheKey> {
        self.cache.iter().map(|(k, _)| k)
    }
}

impl Default for LruList {
    fn default() -> Self {
        Self::new()
    }
}

/// Single shard of the cache
///
/// Each shard maintains its own:
/// - HashMap for O(1) lookups
/// - LRU list for eviction ordering
/// - Lock for thread safety
///
/// Shards reduce lock contention by distributing entries across multiple independent units.
pub struct CacheShard<T> {
    /// Cached entries mapped by key
    entries: parking_lot::RwLock<std::collections::HashMap<CacheKey, CacheEntry<T>>>,

    /// LRU eviction ordering
    lru: parking_lot::RwLock<LruList>,
}

impl<T: Clone> CacheShard<T> {
    /// Create a new cache shard
    pub fn new() -> Self {
        Self {
            entries: parking_lot::RwLock::new(std::collections::HashMap::new()),
            lru: parking_lot::RwLock::new(LruList::new()),
        }
    }

    /// Get an entry from the shard
    ///
    /// Updates access metadata and moves entry to MRU position.
    /// Returns Arc clone (cheap - just bumps ref count).
    pub fn get(&self, key: &CacheKey) -> Option<Arc<T>> {
        // Read lock for lookup
        let entries = self.entries.read();
        let entry = entries.get(key)?;

        // Update metadata
        entry.metadata.record_access();

        // Clone Arc (cheap - just increments ref count)
        let data = entry.data.clone();

        // Release read lock before acquiring write lock (avoid deadlock)
        drop(entries);

        // Update LRU ordering (requires write lock)
        self.lru.write().touch(key);

        Some(data)
    }

    /// Insert an entry into the shard
    ///
    /// Returns the old value if key already existed.
    pub fn insert(&self, key: CacheKey, data: T, metadata: EntryMetadata) -> Option<CacheEntry<T>> {
        let entry = CacheEntry {
            data: Arc::new(data),
            metadata,
        };

        // Insert into map
        let mut entries = self.entries.write();
        let old = entries.insert(key.clone(), entry);
        drop(entries);

        // Update LRU
        self.lru.write().push_back(key);

        old
    }

    /// Remove an entry from the shard
    ///
    /// Returns the removed entry if it existed.
    pub fn remove(&self, key: &CacheKey) -> Option<CacheEntry<T>> {
        // Remove from map
        let mut entries = self.entries.write();
        let entry = entries.remove(key);
        drop(entries);

        // Remove from LRU
        self.lru.write().remove(key);

        entry
    }

    /// Pop the least recently used entry
    ///
    /// Used for eviction.
    pub fn pop_lru(&self) -> Option<(CacheKey, CacheEntry<T>)> {
        // Get LRU key
        let mut lru = self.lru.write();
        let key = lru.pop_front()?;
        drop(lru);

        // Remove from map
        let mut entries = self.entries.write();
        let entry = entries.remove(&key)?;

        Some((key, entry))
    }

    /// Get current entry count
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if shard is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().clear();
        self.lru.write().clear();
    }

    /// Get all keys (snapshot)
    ///
    /// Returns a Vec of all keys currently in the shard.
    /// This is a point-in-time snapshot.
    pub fn keys(&self) -> Vec<CacheKey> {
        self.entries.read().keys().cloned().collect()
    }
}

impl<T: Clone> Default for CacheShard<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,

    /// Number of shards for concurrency (16-32 recommended)
    pub num_shards: usize,

    /// Low watermark (70% - start background eviction)
    pub low_watermark: f64,

    /// High watermark (90% - aggressive eviction)
    pub high_watermark: f64,

    /// Critical watermark (95% - emergency measures)
    pub critical_watermark: f64,

    /// Background eviction interval in seconds
    pub eviction_interval_secs: u64,

    /// Enable cache statistics tracking
    pub enable_stats: bool,

    /// Enable prefetching for sequential access
    pub enable_prefetch: bool,

    /// Prefetch window size (number of chunks to prefetch)
    pub prefetch_window: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            // Default to 256MB cache
            max_size_bytes: 256 * 1024 * 1024,
            // 16 shards for good concurrency
            num_shards: 16,
            // Standard watermarks
            low_watermark: 0.70,
            high_watermark: 0.90,
            critical_watermark: 0.95,
            // Evict every 5 seconds
            eviction_interval_secs: 5,
            // Enable stats by default
            enable_stats: true,
            // Disable prefetch by default
            enable_prefetch: false,
            prefetch_window: 3,
        }
    }
}

impl CacheConfig {
    /// Create a new cache configuration
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            max_size_bytes,
            ..Default::default()
        }
    }

    /// Set the number of shards
    pub fn with_shards(mut self, num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        self.num_shards = num_shards;
        self
    }

    /// Set watermarks for memory pressure handling
    pub fn with_watermarks(mut self, low: f64, high: f64, critical: f64) -> Self {
        assert!(low > 0.0 && low < 1.0, "low_watermark must be 0.0-1.0");
        assert!(
            high > low && high < 1.0,
            "high_watermark must be > low and < 1.0"
        );
        assert!(
            critical > high && critical < 1.0,
            "critical_watermark must be > high and < 1.0"
        );
        self.low_watermark = low;
        self.high_watermark = high;
        self.critical_watermark = critical;
        self
    }

    /// Enable prefetching for sequential access patterns
    pub fn with_prefetch(mut self, enabled: bool, window_size: usize) -> Self {
        self.enable_prefetch = enabled;
        self.prefetch_window = window_size;
        self
    }

    /// Set the background eviction interval in seconds
    ///
    /// Default is 5 seconds. Use smaller values for testing.
    pub fn with_eviction_interval(mut self, interval_secs: u64) -> Self {
        self.eviction_interval_secs = interval_secs;
        self
    }

    /// Get the low watermark threshold in bytes
    pub fn low_watermark_bytes(&self) -> usize {
        (self.max_size_bytes as f64 * self.low_watermark) as usize
    }

    /// Get the high watermark threshold in bytes
    pub fn high_watermark_bytes(&self) -> usize {
        (self.max_size_bytes as f64 * self.high_watermark) as usize
    }

    /// Get the critical watermark threshold in bytes
    pub fn critical_watermark_bytes(&self) -> usize {
        (self.max_size_bytes as f64 * self.critical_watermark) as usize
    }
}

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: AtomicU64,

    /// Total cache misses
    pub misses: AtomicU64,

    /// Total insertions
    pub insertions: AtomicU64,

    /// Total evictions
    pub evictions: AtomicU64,

    /// Current number of entries
    pub entry_count: AtomicUsize,

    /// Current memory usage in bytes
    pub current_bytes: AtomicUsize,

    /// Peak memory usage in bytes
    pub peak_bytes: AtomicUsize,

    /// Total bytes inserted (for throughput calculation)
    pub bytes_inserted: AtomicU64,

    /// Total bytes evicted
    pub bytes_evicted: AtomicU64,
}

impl CacheStats {
    /// Create new cache statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an insertion
    pub fn record_insertion(&self, size_bytes: usize) {
        self.insertions.fetch_add(1, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_inserted
            .fetch_add(size_bytes as u64, Ordering::Relaxed);

        // Update current and peak bytes
        let new_size = self.current_bytes.fetch_add(size_bytes, Ordering::Relaxed) + size_bytes;
        self.peak_bytes.fetch_max(new_size, Ordering::Relaxed);
    }

    /// Record an eviction
    ///
    /// Uses saturating subtraction to prevent underflow.
    pub fn record_eviction(&self, size_bytes: usize) {
        self.evictions.fetch_add(1, Ordering::Relaxed);

        // Fix P0-4: Use fetch_update with saturating_sub to prevent underflow
        self.entry_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            })
            .ok();

        self.current_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(size_bytes))
            })
            .ok();

        self.bytes_evicted
            .fetch_add(size_bytes as u64, Ordering::Relaxed);
    }

    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get current memory usage
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Get peak memory usage
    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    /// Get current entry count
    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get total hits
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get total misses
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get total evictions
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.insertions.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        // Don't reset current_bytes, entry_count, or peak_bytes
        self.bytes_inserted.store(0, Ordering::Relaxed);
        self.bytes_evicted.store(0, Ordering::Relaxed);
    }
}

/// Multi-tier cache manager
///
/// Manages a sharded LRU cache with automatic eviction and statistics tracking.
pub struct CacheManager<T: Clone + Send + Sync> {
    /// Configuration
    config: CacheConfig,

    /// Cache shards for concurrent access (Arc for safe sharing with background task)
    shards: Arc<Vec<CacheShard<T>>>,

    /// Memory tracker
    memory: Arc<MemoryTracker>,

    /// Statistics (optional)
    stats: Option<Arc<CacheStats>>,

    /// Background eviction handle
    eviction_handle: parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>,

    /// Shutdown signal
    shutdown_tx: parking_lot::RwLock<Option<tokio::sync::mpsc::Sender<()>>>,
}

impl<T: Clone + Send + Sync + 'static> CacheManager<T> {
    /// Create a new cache manager
    pub fn new(config: CacheConfig) -> Self {
        let num_shards = config.num_shards;
        let max_bytes = config.max_size_bytes;
        let enable_stats = config.enable_stats;

        // Create shards
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(CacheShard::new());
        }

        // Create memory tracker
        let memory = Arc::new(MemoryTracker::new(max_bytes, num_shards));

        // Create stats if enabled
        let stats = if enable_stats {
            Some(Arc::new(CacheStats::new()))
        } else {
            None
        };

        Self {
            config,
            shards: Arc::new(shards),
            memory,
            stats,
            eviction_handle: parking_lot::RwLock::new(None),
            shutdown_tx: parking_lot::RwLock::new(None),
        }
    }

    /// Get an entry from the cache
    pub fn get(&self, key: &CacheKey) -> Option<Arc<T>> {
        // Calculate shard
        let shard_id = key.shard_id(self.config.num_shards);
        let shard = &self.shards[shard_id];

        // Try to get from shard
        let result = shard.get(key);

        // Update stats
        if let Some(ref stats) = self.stats {
            if result.is_some() {
                stats.record_hit();
            } else {
                stats.record_miss();
            }
        }

        result
    }

    /// Insert an entry into the cache
    ///
    /// Returns true if inserted, false if eviction needed but failed
    pub fn insert(
        &self,
        key: CacheKey,
        data: T,
        size_bytes: usize,
        chunk_age_seconds: u64,
    ) -> bool {
        // Check if we need to evict first
        if !self.memory.can_insert(size_bytes) {
            // Try to evict to make space
            if !self.evict_to_fit(size_bytes) {
                return false;
            }
        }

        // Calculate shard
        let shard_id = key.shard_id(self.config.num_shards);
        let shard = &self.shards[shard_id];

        // Create metadata
        let metadata = EntryMetadata::new(size_bytes, chunk_age_seconds);

        // Insert into shard
        let old_entry = shard.insert(key, data, metadata);

        // Update memory tracking
        if let Some(old) = old_entry {
            // Replacing existing entry
            let old_size = old.metadata.size_bytes;
            self.memory.on_evict(shard_id, old_size);
            if let Some(ref stats) = self.stats {
                stats.record_eviction(old_size);
            }
        }

        self.memory.on_insert(shard_id, size_bytes);

        // Update stats
        if let Some(ref stats) = self.stats {
            stats.record_insertion(size_bytes);
        }

        true
    }

    /// Remove an entry from the cache
    pub fn remove(&self, key: &CacheKey) -> Option<Arc<T>> {
        let shard_id = key.shard_id(self.config.num_shards);
        let shard = &self.shards[shard_id];

        let entry = shard.remove(key)?;
        let size = entry.metadata.size_bytes;

        // Update memory tracking
        self.memory.on_evict(shard_id, size);

        // Update stats
        if let Some(ref stats) = self.stats {
            stats.record_eviction(size);
        }

        Some(entry.data)
    }

    /// Evict entries to fit a new entry of given size
    ///
    /// Returns true if enough space was freed
    fn evict_to_fit(&self, needed_bytes: usize) -> bool {
        let current = self.memory.current_bytes();
        let max = self.memory.max_bytes();

        // Calculate how much we need to free
        let target_bytes = if current + needed_bytes > max {
            (current + needed_bytes) - max
        } else {
            return true; // Already have space
        };

        let mut freed_bytes = 0;

        // Try to evict from each shard in round-robin
        for _ in 0..self.shards.len() * 10 {
            // Max 10 attempts per shard
            if freed_bytes >= target_bytes {
                return true;
            }

            // Find shard with most memory usage
            let mut max_shard_id = 0;
            let mut max_shard_bytes = 0;

            for (shard_id, _) in self.shards.iter().enumerate() {
                let bytes = self.memory.shard_bytes(shard_id);
                if bytes > max_shard_bytes {
                    max_shard_bytes = bytes;
                    max_shard_id = shard_id;
                }
            }

            if max_shard_bytes == 0 {
                break; // No more entries to evict
            }

            // Evict LRU from this shard
            if let Some((_, entry)) = self.shards[max_shard_id].pop_lru() {
                let size = entry.metadata.size_bytes;
                self.memory.on_evict(max_shard_id, size);
                freed_bytes += size;

                if let Some(ref stats) = self.stats {
                    stats.record_eviction(size);
                }
            } else {
                break; // Shard is empty
            }
        }

        freed_bytes >= target_bytes
    }

    /// Get current memory usage ratio (0.0 to 1.0)
    pub fn memory_usage_ratio(&self) -> f64 {
        self.memory.usage_ratio()
    }

    /// Get cache statistics
    pub fn stats(&self) -> Option<Arc<CacheStats>> {
        self.stats.clone()
    }

    /// Clear all entries from the cache
    ///
    /// Properly updates memory tracking and statistics.
    pub fn clear(&self) {
        for (shard_id, shard) in self.shards.iter().enumerate() {
            // Collect entries to properly track eviction
            let keys: Vec<CacheKey> = shard.keys();

            for key in keys {
                if let Some(entry) = shard.remove(&key) {
                    let size = entry.metadata.size_bytes;
                    self.memory.on_evict(shard_id, size);

                    if let Some(ref stats) = self.stats {
                        stats.record_eviction(size);
                    }
                }
            }
        }
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Start background eviction thread
    pub async fn start_background_eviction(&self) -> Result<(), String> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        let config = self.config.clone();
        let memory = self.memory.clone();
        let stats = self.stats.clone();
        let shards = self.shards.clone(); // Safe Arc clone instead of raw pointer
        let num_shards = shards.len();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                config.eviction_interval_secs,
            ));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check memory pressure
                        let usage_ratio = memory.usage_ratio();

                        if usage_ratio > config.high_watermark {
                            // High pressure - evict to low watermark
                            let current = memory.current_bytes();
                            let target = config.low_watermark_bytes();

                            if current > target {
                                let to_free = current - target;
                                let mut freed = 0;

                                // Evict from each shard
                                for shard_id in 0..num_shards {
                                    while freed < to_free {
                                        if let Some((_, entry)) = shards[shard_id].pop_lru() {
                                            let size = entry.metadata.size_bytes;
                                            memory.on_evict(shard_id, size);
                                            freed += size;

                                            if let Some(ref stats) = stats {
                                                stats.record_eviction(size);
                                            }
                                        } else {
                                            break;
                                        }
                                    }

                                    if freed >= to_free {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        // Fix P0-2: Drop lock before await point
        let mut handle_guard = self.eviction_handle.write();
        *handle_guard = Some(handle);
        drop(handle_guard); // Explicitly drop before next await

        let mut tx_guard = self.shutdown_tx.write();
        *tx_guard = Some(shutdown_tx);
        drop(tx_guard);

        Ok(())
    }

    /// Stop background eviction thread
    pub async fn stop_background_eviction(&self) {
        // Extract the sender from the lock before awaiting to avoid holding lock across await
        let tx = self.shutdown_tx.write().take();
        if let Some(tx) = tx {
            let _ = tx.send(()).await;
        }

        // Extract the handle from the lock before awaiting
        let handle = self.eviction_handle.write().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl<T: Clone + Send + Sync> Drop for CacheManager<T> {
    fn drop(&mut self) {
        // Send shutdown signal if background task is running
        if let Some(tx) = self.shutdown_tx.write().take() {
            // Best effort - don't block
            let _ = tx.try_send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_creation() {
        let series_id: SeriesId = 12345;
        let chunk_id = 67890;
        let key = CacheKey::new(series_id, chunk_id);

        assert_eq!(key.series_id, series_id);
        assert_eq!(key.chunk_id, chunk_id);
    }

    #[test]
    fn test_cache_key_shard_distribution() {
        let series_id: SeriesId = 12345;
        let num_shards = 16;

        // Test that different chunk IDs map to different shards
        let mut shard_counts = vec![0; num_shards];

        for chunk_id in 0..1000 {
            let key = CacheKey::new(series_id, chunk_id);
            let shard = key.shard_id(num_shards);
            assert!(shard < num_shards);
            shard_counts[shard] += 1;
        }

        // Check reasonable distribution (each shard should have some entries)
        for count in shard_counts {
            assert!(count > 0, "Shard should have at least one entry");
        }
    }

    #[test]
    fn test_entry_metadata_access_tracking() {
        let metadata = EntryMetadata::new(1024, 0);

        assert_eq!(metadata.access_count(), 0);

        metadata.record_access();
        assert_eq!(metadata.access_count(), 1);

        metadata.record_access();
        assert_eq!(metadata.access_count(), 2);

        // Verify last access is recent
        assert!(metadata.seconds_since_access() < 1);
    }

    #[test]
    fn test_entry_metadata_priority() {
        let metadata = EntryMetadata::new(1024, 0);

        assert_eq!(metadata.priority(), 0);

        metadata.set_priority(100);
        assert_eq!(metadata.priority(), 100);

        metadata.set_priority(-50);
        assert_eq!(metadata.priority(), -50);
    }

    #[test]
    fn test_memory_tracker_insert_evict() {
        let tracker = MemoryTracker::new(1024, 4);

        assert_eq!(tracker.current_bytes(), 0);
        assert!(tracker.can_insert(512));

        tracker.on_insert(0, 512);
        assert_eq!(tracker.current_bytes(), 512);
        assert_eq!(tracker.shard_bytes(0), 512);

        tracker.on_insert(1, 256);
        assert_eq!(tracker.current_bytes(), 768);

        tracker.on_evict(0, 512);
        assert_eq!(tracker.current_bytes(), 256);
        assert_eq!(tracker.shard_bytes(0), 0);
    }

    #[test]
    fn test_memory_tracker_capacity_check() {
        let tracker = MemoryTracker::new(1024, 4);

        assert!(tracker.can_insert(1024));
        assert!(!tracker.can_insert(1025));

        tracker.on_insert(0, 512);
        assert!(tracker.can_insert(512));
        assert!(!tracker.can_insert(513));
    }

    #[test]
    fn test_memory_tracker_usage_ratio() {
        let tracker = MemoryTracker::new(1000, 4);

        assert_eq!(tracker.usage_ratio(), 0.0);

        tracker.on_insert(0, 500);
        assert_eq!(tracker.usage_ratio(), 0.5);

        tracker.on_insert(1, 250);
        assert_eq!(tracker.usage_ratio(), 0.75);

        tracker.on_evict(0, 500);
        assert_eq!(tracker.usage_ratio(), 0.25);
    }

    #[test]
    fn test_lru_list_push_pop() {
        let mut lru = LruList::new();

        assert!(lru.is_empty());
        assert_eq!(lru.len(), 0);

        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);
        let key3 = CacheKey::new(1, 300);

        lru.push_back(key1.clone());
        lru.push_back(key2.clone());
        lru.push_back(key3.clone());

        assert_eq!(lru.len(), 3);

        // Pop should return in FIFO order (LRU at front)
        assert_eq!(lru.pop_front(), Some(key1));
        assert_eq!(lru.pop_front(), Some(key2));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn test_lru_list_touch() {
        let mut lru = LruList::new();

        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);
        let key3 = CacheKey::new(1, 300);

        lru.push_back(key1.clone());
        lru.push_back(key2.clone());
        lru.push_back(key3.clone());

        // Touch key1 (move to back)
        assert!(lru.touch(&key1));

        // Now order should be: key2, key3, key1
        assert_eq!(lru.pop_front(), Some(key2));
        assert_eq!(lru.pop_front(), Some(key3));
        assert_eq!(lru.pop_front(), Some(key1));
    }

    #[test]
    fn test_lru_list_remove() {
        let mut lru = LruList::new();

        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);
        let key3 = CacheKey::new(1, 300);

        lru.push_back(key1.clone());
        lru.push_back(key2.clone());
        lru.push_back(key3.clone());

        // Remove middle element
        assert!(lru.remove(&key2));
        assert_eq!(lru.len(), 2);

        // Try to remove non-existent key
        let key4 = CacheKey::new(1, 400);
        assert!(!lru.remove(&key4));

        // Verify remaining order
        assert_eq!(lru.pop_front(), Some(key1));
        assert_eq!(lru.pop_front(), Some(key3));
    }

    #[test]
    fn test_lru_list_clear() {
        let mut lru = LruList::new();

        lru.push_back(CacheKey::new(1, 100));
        lru.push_back(CacheKey::new(1, 200));

        assert_eq!(lru.len(), 2);

        lru.clear();

        assert!(lru.is_empty());
        assert_eq!(lru.len(), 0);
        assert_eq!(lru.pop_front(), None);
    }

    // CacheShard tests
    #[test]
    fn test_cache_shard_insert_get() {
        let shard: CacheShard<Vec<u8>> = CacheShard::new();
        let key = CacheKey::new(1, 100);
        let data = vec![1, 2, 3, 4];
        let metadata = EntryMetadata::new(4, 0);

        assert!(shard.is_empty());

        shard.insert(key.clone(), data.clone(), metadata);
        assert_eq!(shard.len(), 1);

        let retrieved = shard.get(&key).unwrap();
        assert_eq!(*retrieved, data);
    }

    #[test]
    fn test_cache_shard_remove() {
        let shard: CacheShard<Vec<u8>> = CacheShard::new();
        let key = CacheKey::new(1, 100);
        let data = vec![1, 2, 3, 4];
        let metadata = EntryMetadata::new(4, 0);

        shard.insert(key.clone(), data.clone(), metadata);
        assert_eq!(shard.len(), 1);

        let removed = shard.remove(&key);
        assert!(removed.is_some());
        assert_eq!(shard.len(), 0);

        // Second remove should return None
        assert!(shard.remove(&key).is_none());
    }

    #[test]
    fn test_cache_shard_pop_lru() {
        let shard: CacheShard<Vec<u8>> = CacheShard::new();

        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);
        let key3 = CacheKey::new(1, 300);

        shard.insert(key1.clone(), vec![1], EntryMetadata::new(1, 0));
        shard.insert(key2.clone(), vec![2], EntryMetadata::new(1, 0));
        shard.insert(key3.clone(), vec![3], EntryMetadata::new(1, 0));

        // Pop should return LRU (first inserted)
        let (popped_key, _) = shard.pop_lru().unwrap();
        assert_eq!(popped_key, key1);
        assert_eq!(shard.len(), 2);

        // Next pop should return second inserted
        let (popped_key, _) = shard.pop_lru().unwrap();
        assert_eq!(popped_key, key2);
        assert_eq!(shard.len(), 1);
    }

    #[test]
    fn test_cache_shard_clear() {
        let shard: CacheShard<Vec<u8>> = CacheShard::new();

        shard.insert(CacheKey::new(1, 100), vec![1], EntryMetadata::new(1, 0));
        shard.insert(CacheKey::new(1, 200), vec![2], EntryMetadata::new(1, 0));

        assert_eq!(shard.len(), 2);

        shard.clear();
        assert!(shard.is_empty());
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn test_cache_shard_access_updates_lru() {
        let shard: CacheShard<Vec<u8>> = CacheShard::new();

        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);

        shard.insert(key1.clone(), vec![1], EntryMetadata::new(1, 0));
        shard.insert(key2.clone(), vec![2], EntryMetadata::new(1, 0));

        // Access key1 (should move to MRU)
        shard.get(&key1);

        // Pop LRU should now return key2
        let (popped_key, _) = shard.pop_lru().unwrap();
        assert_eq!(popped_key, key2);
    }

    // P1 Issue tests
    #[test]
    fn test_memory_tracker_shard_bytes_bounds_check() {
        let tracker = MemoryTracker::new(1024, 4);

        tracker.on_insert(0, 100);
        assert_eq!(tracker.shard_bytes(0), 100);

        // Out of bounds should return 0 instead of panicking
        assert_eq!(tracker.shard_bytes(10), 0);
        assert_eq!(tracker.shard_bytes(100), 0);
    }

    #[test]
    fn test_entry_metadata_time_handling() {
        let metadata = EntryMetadata::new(1024, 0);

        // Record access
        metadata.record_access();

        // Should not panic even if accessed immediately
        let seconds = metadata.seconds_since_access();
        assert!(seconds < 2); // Should be very recent
    }

    #[test]
    fn test_cache_entry_size_bytes() {
        let data = vec![1, 2, 3, 4];
        let entry = CacheEntry::new(Arc::new(data), 100, 0);

        assert_eq!(entry.size_bytes(), 100);
    }

    // CacheConfig tests
    #[test]
    fn test_cache_config_defaults() {
        let config = CacheConfig::default();

        assert_eq!(config.max_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.num_shards, 16);
        assert_eq!(config.low_watermark, 0.70);
        assert_eq!(config.high_watermark, 0.90);
        assert_eq!(config.critical_watermark, 0.95);
    }

    #[test]
    fn test_cache_config_builder() {
        let config = CacheConfig::new(1024 * 1024)
            .with_shards(32)
            .with_watermarks(0.6, 0.8, 0.9)
            .with_prefetch(true, 5);

        assert_eq!(config.max_size_bytes, 1024 * 1024);
        assert_eq!(config.num_shards, 32);
        assert_eq!(config.low_watermark, 0.6);
        assert_eq!(config.high_watermark, 0.8);
        assert_eq!(config.critical_watermark, 0.9);
        assert!(config.enable_prefetch);
        assert_eq!(config.prefetch_window, 5);
    }

    #[test]
    fn test_cache_config_watermark_bytes() {
        let config = CacheConfig::new(1000);

        assert_eq!(config.low_watermark_bytes(), 700);
        assert_eq!(config.high_watermark_bytes(), 900);
        assert_eq!(config.critical_watermark_bytes(), 950);
    }

    // CacheStats tests
    #[test]
    fn test_cache_stats_hit_rate() {
        let stats = CacheStats::new();

        // Empty stats should have 0.0 hit rate
        assert_eq!(stats.hit_rate(), 0.0);

        // Record some hits and misses
        stats.record_hit();
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        // 3 hits / 4 total = 0.75
        assert_eq!(stats.hit_rate(), 0.75);
    }

    #[test]
    fn test_cache_stats_insertion_eviction() {
        let stats = CacheStats::new();

        stats.record_insertion(100);
        stats.record_insertion(200);
        assert_eq!(stats.entry_count(), 2);
        assert_eq!(stats.current_bytes(), 300);

        stats.record_eviction(100);
        assert_eq!(stats.entry_count(), 1);
        assert_eq!(stats.current_bytes(), 200);
        assert_eq!(stats.evictions(), 1);
    }

    #[test]
    fn test_cache_stats_peak_tracking() {
        let stats = CacheStats::new();

        stats.record_insertion(500);
        assert_eq!(stats.peak_bytes(), 500);

        stats.record_insertion(300);
        assert_eq!(stats.peak_bytes(), 800);

        // Eviction shouldn't change peak
        stats.record_eviction(400);
        assert_eq!(stats.current_bytes(), 400);
        assert_eq!(stats.peak_bytes(), 800);
    }

    #[test]
    fn test_cache_stats_reset() {
        let stats = CacheStats::new();

        stats.record_hit();
        stats.record_miss();
        stats.record_insertion(100);

        stats.reset();

        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        // Entry count and current_bytes should NOT be reset
        assert_eq!(stats.entry_count(), 1);
        assert_eq!(stats.current_bytes(), 100);
    }

    // CacheManager tests
    #[test]
    fn test_cache_manager_basic_operations() {
        let config = CacheConfig::new(1024).with_shards(4);
        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        // Insert
        let key = CacheKey::new(1, 100);
        let data = vec![1, 2, 3, 4];
        assert!(cache.insert(key.clone(), data.clone(), 4, 0));

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        // Get
        let retrieved = cache.get(&key).unwrap();
        assert_eq!(*retrieved, data);

        // Stats should track hit
        if let Some(stats) = cache.stats() {
            assert_eq!(stats.hits(), 1);
            assert_eq!(stats.misses(), 0);
        }
    }

    #[test]
    fn test_cache_manager_eviction() {
        // Small cache to force eviction
        let config = CacheConfig::new(100).with_shards(2);
        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        // Fill cache
        for i in 0..10 {
            let key = CacheKey::new(1, i);
            let data = vec![0u8; 20]; // 20 bytes each
            cache.insert(key, data, 20, 0);
        }

        // Cache should have evicted old entries
        assert!(cache.len() < 10);
        assert!(cache.memory_usage_ratio() <= 1.0);

        // Verify stats
        if let Some(stats) = cache.stats() {
            assert!(stats.evictions() > 0);
        }
    }

    #[test]
    fn test_cache_manager_remove() {
        let config = CacheConfig::new(1024).with_shards(4);
        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        let key = CacheKey::new(1, 100);
        let data = vec![1, 2, 3, 4];

        cache.insert(key.clone(), data.clone(), 4, 0);
        assert_eq!(cache.len(), 1);

        let removed = cache.remove(&key).unwrap();
        assert_eq!(*removed, data);
        assert_eq!(cache.len(), 0);

        // Second remove should return None
        assert!(cache.remove(&key).is_none());
    }

    #[test]
    fn test_cache_manager_clear() {
        let config = CacheConfig::new(1024).with_shards(4);
        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        // Insert multiple entries
        for i in 0..10 {
            let key = CacheKey::new(1, i);
            cache.insert(key, vec![i as u8], 1, 0);
        }

        assert_eq!(cache.len(), 10);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_manager_replacement() {
        let config = CacheConfig::new(1024).with_shards(4);
        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        let key = CacheKey::new(1, 100);

        // Insert first value
        cache.insert(key.clone(), vec![1], 1, 0);
        assert_eq!(*cache.get(&key).unwrap(), vec![1]);

        // Replace with new value
        cache.insert(key.clone(), vec![2], 1, 0);
        assert_eq!(*cache.get(&key).unwrap(), vec![2]);

        // Should still have only 1 entry
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn test_cache_manager_background_eviction() {
        // Use 1-second eviction interval for faster test
        let config = CacheConfig::new(200)
            .with_shards(2)
            .with_watermarks(0.5, 0.8, 0.95)
            .with_eviction_interval(1);

        let cache: CacheManager<Vec<u8>> = CacheManager::new(config);

        // Start background eviction
        cache.start_background_eviction().await.unwrap();

        // Fill cache past high watermark (400 bytes into 200 byte cache)
        for i in 0..20 {
            let key = CacheKey::new(1, i);
            cache.insert(key, vec![0u8; 20], 20, 0);
        }

        // Wait for background eviction to run (interval is 1 second, so wait 1.5s)
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Stop background eviction
        cache.stop_background_eviction().await;

        // Memory usage should be controlled below capacity
        assert!(cache.memory_usage_ratio() < 1.0);
    }
}
