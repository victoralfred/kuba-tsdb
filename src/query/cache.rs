//! Query Result Cache
//!
//! Provides LRU-based caching of query results to avoid recomputation for
//! identical queries. Supports:
//! - LRU eviction based on memory limits
//! - TTL-based expiration for freshness
//! - Hash-based cache keys from query AST

use crate::query::ast::Query;
use crate::query::result::QueryResult;
use crate::types::SeriesId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// Cache Configuration
// ============================================================================

/// Configuration for query result caching
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in bytes (default: 128 MB)
    pub max_size_bytes: usize,

    /// Maximum number of cached entries (default: 10,000)
    pub max_entries: usize,

    /// Default TTL for cache entries (default: 60 seconds)
    pub default_ttl: Duration,

    /// Enable cache (default: true)
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 128 * 1024 * 1024, // 128 MB
            max_entries: 10_000,
            default_ttl: Duration::from_secs(60),
            enabled: true,
        }
    }
}

impl CacheConfig {
    /// Create config with custom size limit
    pub fn with_max_size(mut self, bytes: usize) -> Self {
        self.max_size_bytes = bytes;
        self
    }

    /// Set maximum entries
    pub fn with_max_entries(mut self, entries: usize) -> Self {
        self.max_entries = entries;
        self
    }

    /// Set default TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Disable caching
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

// ============================================================================
// Cache Key
// ============================================================================

/// Hash key derived from query AST
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey(u64);

impl CacheKey {
    /// Create a cache key from a query
    pub fn from_query(query: &Query) -> Self {
        use std::hash::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        format!("{:?}", query).hash(&mut hasher);
        CacheKey(hasher.finish())
    }
}

// ============================================================================
// Cache Entry
// ============================================================================

/// Cached query result with metadata
struct CacheEntry {
    /// The cached result
    result: QueryResult,

    /// When the entry was created
    created_at: Instant,

    /// TTL for this entry
    ttl: Duration,

    /// Approximate size in bytes
    size_bytes: usize,

    /// Last access time (for LRU)
    last_accessed: Instant,

    /// Series IDs this entry depends on (for invalidation)
    series_ids: HashSet<SeriesId>,
}

impl CacheEntry {
    /// Create a new cache entry
    fn new(result: QueryResult, ttl: Duration, series_ids: HashSet<SeriesId>) -> Self {
        let size_bytes = Self::estimate_size(&result);
        let now = Instant::now();
        Self {
            result,
            created_at: now,
            ttl,
            size_bytes,
            last_accessed: now,
            series_ids,
        }
    }

    /// Estimate the memory size of a result
    fn estimate_size(result: &QueryResult) -> usize {
        use crate::query::result::ResultData;

        let mut size = std::mem::size_of::<QueryResult>();

        match &result.data {
            ResultData::Rows(rows) => {
                size += rows.len() * 48;
            }
            ResultData::Series(series) => {
                for s in series {
                    size += s.values.len() * 16;
                }
            }
            ResultData::Scalar(_) => {
                size += 8;
            }
            ResultData::Explain(s) => {
                size += s.len();
            }
        }

        size
    }

    /// Check if entry has expired
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }

    /// Record an access
    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

// ============================================================================
// Query Cache
// ============================================================================

/// LRU cache for query results with series-based invalidation
pub struct QueryCache {
    /// Cache configuration
    config: CacheConfig,

    /// Cached entries
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,

    /// Reverse index: series_id -> cache keys that depend on it
    /// Used for efficient invalidation when series data changes
    series_index: RwLock<HashMap<SeriesId, HashSet<CacheKey>>>,

    /// Current total size in bytes
    current_size: AtomicU64,

    /// Statistics
    stats: CacheStats,
}

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: AtomicU64,

    /// Total cache misses
    pub misses: AtomicU64,

    /// Total evictions
    pub evictions: AtomicU64,

    /// Total invalidations triggered
    pub invalidations: AtomicU64,
}

impl QueryCache {
    /// Create a new query cache
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            series_index: RwLock::new(HashMap::new()),
            current_size: AtomicU64::new(0),
            stats: CacheStats::default(),
        }
    }

    /// Get a cached result for a query
    pub fn get(&self, query: &Query) -> Option<QueryResult> {
        if !self.config.enabled {
            return None;
        }

        let key = CacheKey::from_query(query);

        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(&key) {
            if entry.is_expired() {
                let size = entry.size_bytes as u64;
                entries.remove(&key);
                self.current_size.fetch_sub(size, Ordering::Relaxed);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            entry.touch();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.result.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache a query result
    pub fn put(&self, query: &Query, result: QueryResult) {
        if !self.config.enabled {
            return;
        }

        self.put_with_ttl(query, result, self.config.default_ttl)
    }

    /// Cache a query result with custom TTL
    pub fn put_with_ttl(&self, query: &Query, result: QueryResult, ttl: Duration) {
        if !self.config.enabled {
            return;
        }

        let key = CacheKey::from_query(query);

        // Extract series IDs from the query for invalidation tracking
        let series_ids = Self::extract_series_ids(query);

        let entry = CacheEntry::new(result, ttl, series_ids.clone());
        let entry_size = entry.size_bytes;

        self.evict_if_needed(entry_size);

        let mut entries = self.entries.write();

        // Remove old entry from series index if it exists
        if let Some(old_entry) = entries.get(&key) {
            let old_size = old_entry.size_bytes as u64;
            self.current_size.fetch_sub(old_size, Ordering::Relaxed);

            // Remove from series index
            let mut series_idx = self.series_index.write();
            for sid in &old_entry.series_ids {
                if let Some(keys) = series_idx.get_mut(sid) {
                    keys.remove(&key);
                }
            }
        }

        // Add to series index
        {
            let mut series_idx = self.series_index.write();
            for sid in &series_ids {
                series_idx.entry(*sid).or_default().insert(key);
            }
        }

        self.current_size
            .fetch_add(entry_size as u64, Ordering::Relaxed);
        entries.insert(key, entry);
    }

    /// Extract series IDs from a query for invalidation tracking
    fn extract_series_ids(query: &Query) -> HashSet<SeriesId> {
        let mut series_ids = HashSet::new();

        // Get the series selector from the query
        if let Some(series_id) = query.series_selector().series_id {
            series_ids.insert(series_id);
        }

        series_ids
    }

    /// Invalidate a specific query from cache
    pub fn invalidate(&self, query: &Query) {
        let key = CacheKey::from_query(query);
        self.invalidate_key(key);
    }

    /// Invalidate all cached queries that depend on a specific series
    ///
    /// Call this when data is written to a series to ensure cache freshness.
    /// This is O(n) where n is the number of queries cached for this series.
    pub fn invalidate_series(&self, series_id: SeriesId) {
        // Get all cache keys that depend on this series
        let keys_to_invalidate: Vec<CacheKey> = {
            let series_idx = self.series_index.read();
            series_idx
                .get(&series_id)
                .map(|keys| keys.iter().copied().collect())
                .unwrap_or_default()
        };

        // Invalidate each key
        for key in keys_to_invalidate {
            self.invalidate_key(key);
        }
    }

    /// Invalidate all cached queries for multiple series
    ///
    /// More efficient than calling invalidate_series multiple times.
    pub fn invalidate_series_batch(&self, series_ids: &[SeriesId]) {
        // Collect all unique keys to invalidate
        let keys_to_invalidate: HashSet<CacheKey> = {
            let series_idx = self.series_index.read();
            series_ids
                .iter()
                .filter_map(|sid| series_idx.get(sid))
                .flat_map(|keys| keys.iter().copied())
                .collect()
        };

        // Invalidate each key
        for key in keys_to_invalidate {
            self.invalidate_key(key);
        }
    }

    /// Internal method to invalidate a cache key
    fn invalidate_key(&self, key: CacheKey) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(&key) {
            self.current_size
                .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
            self.stats.invalidations.fetch_add(1, Ordering::Relaxed);

            // Remove from series index
            let mut series_idx = self.series_index.write();
            for sid in &entry.series_ids {
                if let Some(keys) = series_idx.get_mut(sid) {
                    keys.remove(&key);
                    if keys.is_empty() {
                        series_idx.remove(sid);
                    }
                }
            }
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.series_index.write().clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get current cache size in bytes
    pub fn size_bytes(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get number of cached entries
    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Evict entries if cache is over limits
    fn evict_if_needed(&self, new_entry_size: usize) {
        let mut entries = self.entries.write();

        while entries.len() >= self.config.max_entries {
            self.evict_lru(&mut entries);
        }

        let current = self.current_size.load(Ordering::Relaxed) as usize;
        while current + new_entry_size > self.config.max_size_bytes && !entries.is_empty() {
            self.evict_lru(&mut entries);
        }
    }

    /// Evict the least recently used entry
    fn evict_lru(&self, entries: &mut HashMap<CacheKey, CacheEntry>) {
        if entries.is_empty() {
            return;
        }

        let lru_key = entries
            .iter()
            .min_by_key(|(_, e)| e.last_accessed)
            .map(|(k, _)| *k);

        if let Some(key) = lru_key {
            if let Some(entry) = entries.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Thread-safe wrapper for shared cache access
pub type SharedQueryCache = Arc<QueryCache>;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{SelectQuery, SeriesSelector};
    use crate::types::TimeRange;

    fn make_select_query(series_id: u128) -> Query {
        Query::Select(SelectQuery::new(
            SeriesSelector::by_id(series_id),
            TimeRange {
                start: 0,
                end: 1000,
            },
        ))
    }

    #[test]
    fn test_cache_config_defaults() {
        let config = CacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_size_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn test_cache_key_generation() {
        let query1 = make_select_query(1);
        let query2 = make_select_query(1);
        let query3 = make_select_query(2);

        let key1 = CacheKey::from_query(&query1);
        let key2 = CacheKey::from_query(&query2);
        let key3 = CacheKey::from_query(&query3);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = QueryCache::new(CacheConfig::default());
        let query = make_select_query(1);
        let result = QueryResult::empty();

        assert!(cache.get(&query).is_none());
        cache.put(&query, result.clone());
        assert!(cache.get(&query).is_some());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = QueryCache::new(CacheConfig::default());
        let query = make_select_query(1);
        let result = QueryResult::empty();

        cache.put(&query, result);
        assert!(cache.get(&query).is_some());

        cache.invalidate(&query);
        assert!(cache.get(&query).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryCache::new(CacheConfig::default());

        for i in 0..10 {
            let query = make_select_query(i);
            cache.put(&query, QueryResult::empty());
        }

        assert_eq!(cache.entry_count(), 10);

        cache.clear();
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn test_cache_disabled() {
        let config = CacheConfig::default().disabled();
        let cache = QueryCache::new(config);
        let query = make_select_query(1);
        let result = QueryResult::empty();

        cache.put(&query, result);
        assert!(cache.get(&query).is_none());
    }

    #[test]
    fn test_invalidate_series() {
        let cache = QueryCache::new(CacheConfig::default());

        // Cache queries for series 1 and series 2
        let query1 = make_select_query(1);
        let query2 = make_select_query(2);

        cache.put(&query1, QueryResult::empty());
        cache.put(&query2, QueryResult::empty());

        assert!(cache.get(&query1).is_some());
        assert!(cache.get(&query2).is_some());

        // Invalidate series 1 - should only remove query1
        cache.invalidate_series(1);

        assert!(cache.get(&query1).is_none());
        assert!(cache.get(&query2).is_some());

        // Check invalidation count
        assert_eq!(cache.stats.invalidations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_invalidate_series_batch() {
        let cache = QueryCache::new(CacheConfig::default());

        // Cache queries for multiple series
        for i in 0..5 {
            let query = make_select_query(i);
            cache.put(&query, QueryResult::empty());
        }

        assert_eq!(cache.entry_count(), 5);

        // Invalidate series 1, 2, and 3
        cache.invalidate_series_batch(&[1, 2, 3]);

        assert_eq!(cache.entry_count(), 2);

        // Series 0 and 4 should still be cached
        assert!(cache.get(&make_select_query(0)).is_some());
        assert!(cache.get(&make_select_query(4)).is_some());

        // Series 1, 2, 3 should be invalidated
        assert!(cache.get(&make_select_query(1)).is_none());
        assert!(cache.get(&make_select_query(2)).is_none());
        assert!(cache.get(&make_select_query(3)).is_none());
    }

    #[test]
    fn test_series_index_cleanup() {
        let cache = QueryCache::new(CacheConfig::default());

        // Add and then invalidate a query
        let query = make_select_query(42);
        cache.put(&query, QueryResult::empty());

        // Verify series is in index
        {
            let idx = cache.series_index.read();
            assert!(idx.contains_key(&42));
        }

        // Invalidate the query
        cache.invalidate(&query);

        // Series should be removed from index since no queries depend on it
        {
            let idx = cache.series_index.read();
            assert!(!idx.contains_key(&42));
        }
    }
}
