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
//! use gorilla_tsdb::storage::cache::{CacheManager, CacheConfig};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CacheConfig::default();
//! let cache = Arc::new(CacheManager::new(config));
//!
//! // Start background eviction
//! cache.start().await?;
//!
//! // Cache operations
//! let key = CacheKey::new(series_id, chunk_id);
//! cache.insert(key.clone(), data).await;
//! let cached = cache.get(&key).await;
//!
//! // Get statistics
//! let stats = cache.stats().await;
//! println!("Hit rate: {:.2}%", stats.hit_rate() * 100.0);
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
    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);

        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        self.last_access.store(now_micros, Ordering::Relaxed);
    }

    /// Get seconds since last access
    pub fn seconds_since_access(&self) -> u64 {
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let last = self.last_access.load(Ordering::Relaxed);
        (now_micros - last) / 1_000_000
    }

    /// Get current access count
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    /// Get current priority
    pub fn priority(&self) -> i32 {
        self.priority.load(Ordering::Relaxed)
    }

    /// Set priority
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
    pub fn new(data: Arc<T>, size_bytes: usize, chunk_age_seconds: u64) -> Self {
        Self {
            data,
            metadata: EntryMetadata::new(size_bytes, chunk_age_seconds),
        }
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
    pub fn shard_bytes(&self, shard_id: usize) -> usize {
        self.per_shard_bytes[shard_id].load(Ordering::Relaxed)
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
}
