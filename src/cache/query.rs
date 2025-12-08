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

    /// Maximum size per entry in bytes (DoS protection)
    /// Default: 10MB per query result
    pub max_entry_size_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 128 * 1024 * 1024, // 128 MB
            max_entries: 10_000,
            default_ttl: Duration::from_secs(60),
            enabled: true,
            max_entry_size_bytes: 10 * 1024 * 1024, // 10MB per entry (DoS protection)
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

    /// Set maximum entry size in bytes (DoS protection)
    ///
    /// Rejects entries larger than this limit to prevent DoS attacks
    /// via huge query results.
    ///
    /// # Arguments
    /// * `bytes` - Maximum size per entry in bytes
    pub fn with_max_entry_size(mut self, bytes: usize) -> Self {
        self.max_entry_size_bytes = bytes;
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
    ///
    /// # Security
    /// - Uses DefaultHasher (SipHash) which is resistant to hash collision attacks
    /// - Hash is derived from query structure, not user input directly
    ///
    /// # Performance
    /// - O(n) where n is query size (for Debug formatting)
    /// - Hash computation is fast (SipHash-1-3)
    pub fn from_query(query: &Query) -> Self {
        use std::hash::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        // SEC: DefaultHasher uses SipHash which is cryptographically secure
        // and resistant to hash collision attacks
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
    ///
    /// # Security
    /// - Uses checked arithmetic to prevent overflow
    /// - Caps estimate at reasonable limit to prevent DoS
    fn estimate_size(result: &QueryResult) -> usize {
        use crate::query::result::ResultData;

        let mut size = std::mem::size_of::<QueryResult>();

        match &result.data {
            ResultData::Rows(rows) => {
                // SEC: Use checked arithmetic to prevent overflow
                if let Some(rows_size) = rows.len().checked_mul(48) {
                    size = size.saturating_add(rows_size);
                } else {
                    // Overflow: cap at max
                    size = usize::MAX;
                }
            },
            ResultData::Series(series) => {
                for s in series {
                    // SEC: Use checked arithmetic
                    if let Some(series_size) = s.values.len().checked_mul(16) {
                        size = size.saturating_add(series_size);
                    } else {
                        size = usize::MAX;
                        break; // Stop on overflow
                    }
                }
            },
            ResultData::Scalar(_) => {
                size = size.saturating_add(8);
            },
            ResultData::Explain(s) => {
                size = size.saturating_add(s.len());
            },
        }

        // SEC: Cap at reasonable limit (1GB) to prevent DoS
        const MAX_ESTIMATED_SIZE: usize = 1024 * 1024 * 1024; // 1GB
        size.min(MAX_ESTIMATED_SIZE)
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
///
/// # Security
/// - Limits entry size to prevent DoS via huge query results
/// - Uses saturating arithmetic to prevent overflow
/// - Limits eviction attempts to prevent DoS
///
/// # Performance
/// - PERF: LRU eviction is currently O(n) - could be optimized to O(1)
///   with a proper LRU data structure (e.g., linked hash map)
/// - FUTURE: Add cache stampede protection (mutex per key for concurrent misses)
pub struct QueryCache {
    /// Cache configuration
    config: CacheConfig,

    /// Cached entries
    /// PERF: Currently uses HashMap - consider LinkedHashMap for O(1) LRU
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,

    /// Reverse index: series_id -> cache keys that depend on it
    /// Used for efficient invalidation when series data changes
    /// SEC: No explicit limit, but bounded by max_entries
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
    ///
    /// # Performance
    /// - Returns cached result if available and not expired
    /// - Automatically removes expired entries
    ///
    /// # Security
    /// - Uses saturating arithmetic to prevent underflow
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
                // SEC: Use saturating subtraction to prevent underflow
                let old_size = self.current_size.load(Ordering::Relaxed);
                self.current_size
                    .store(old_size.saturating_sub(size), Ordering::Relaxed);
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
    ///
    /// # Security
    /// - Validates entry size to prevent DoS via huge query results
    /// - Rejects entries larger than max_entry_size_bytes
    pub fn put_with_ttl(&self, query: &Query, result: QueryResult, ttl: Duration) {
        if !self.config.enabled {
            return;
        }

        let key = CacheKey::from_query(query);

        // Extract series IDs from the query for invalidation tracking
        let series_ids = Self::extract_series_ids(query);

        let entry = CacheEntry::new(result, ttl, series_ids.clone());
        let entry_size = entry.size_bytes;

        // SEC: Reject entries that are too large (DoS protection)
        if entry_size > self.config.max_entry_size_bytes {
            // Entry too large - don't cache it
            return;
        }

        // SEC: Reject entries larger than entire cache
        if entry_size > self.config.max_size_bytes {
            return;
        }

        self.evict_if_needed(entry_size);

        let mut entries = self.entries.write();

        // Remove old entry from series index if it exists
        if let Some(old_entry) = entries.get(&key) {
            let old_size = old_entry.size_bytes as u64;
            // SEC: Use saturating subtraction to prevent underflow
            let current = self.current_size.load(Ordering::Relaxed);
            self.current_size
                .store(current.saturating_sub(old_size), Ordering::Relaxed);

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

        // SEC: Use saturating addition to prevent overflow
        let current = self.current_size.load(Ordering::Relaxed);
        let new_size = current.saturating_add(entry_size as u64);
        // SEC: Cap at max_size_bytes to prevent DoS
        let capped_size = new_size.min(self.config.max_size_bytes as u64);
        self.current_size.store(capped_size, Ordering::Relaxed);
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
    ///
    /// # Security
    /// - Limits invalidation batch size to prevent DoS
    /// - Uses efficient batch operations
    pub fn invalidate_series(&self, series_id: SeriesId) {
        // Get all cache keys that depend on this series
        let keys_to_invalidate: Vec<CacheKey> = {
            let series_idx = self.series_index.read();
            series_idx
                .get(&series_id)
                .map(|keys| {
                    // SEC: Limit batch size to prevent DoS via invalidation storm
                    const MAX_INVALIDATION_BATCH: usize = 10_000;
                    keys.iter().copied().take(MAX_INVALIDATION_BATCH).collect()
                })
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
    ///
    /// # Security
    /// - Limits batch size to prevent DoS
    /// - Validates input size
    pub fn invalidate_series_batch(&self, series_ids: &[SeriesId]) {
        // SEC: Limit batch size to prevent DoS
        const MAX_BATCH_SIZE: usize = 10_000;
        let series_ids = if series_ids.len() > MAX_BATCH_SIZE {
            &series_ids[..MAX_BATCH_SIZE]
        } else {
            series_ids
        };

        // Collect all unique keys to invalidate
        let keys_to_invalidate: HashSet<CacheKey> = {
            let series_idx = self.series_index.read();
            series_ids
                .iter()
                .filter_map(|sid| series_idx.get(sid))
                .flat_map(|keys| keys.iter().copied())
                .take(MAX_BATCH_SIZE) // SEC: Limit total keys to invalidate
                .collect()
        };

        // Invalidate each key
        for key in keys_to_invalidate {
            self.invalidate_key(key);
        }
    }

    /// Internal method to invalidate a cache key
    ///
    /// # Security
    /// - Uses saturating arithmetic to prevent underflow
    fn invalidate_key(&self, key: CacheKey) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(&key) {
            // SEC: Use saturating subtraction to prevent underflow
            let current = self.current_size.load(Ordering::Relaxed);
            self.current_size.store(
                current.saturating_sub(entry.size_bytes as u64),
                Ordering::Relaxed,
            );
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
    ///
    /// # Security
    /// - Uses checked arithmetic to prevent overflow
    /// - Limits eviction attempts to prevent DoS
    /// - Handles race conditions in size tracking
    fn evict_if_needed(&self, new_entry_size: usize) {
        let mut entries = self.entries.write();

        // SEC: Limit eviction attempts to prevent DoS
        const MAX_EVICTION_ATTEMPTS: usize = 1000;
        let mut eviction_count = 0;

        // Evict by entry count limit
        while entries.len() >= self.config.max_entries && eviction_count < MAX_EVICTION_ATTEMPTS {
            self.evict_lru(&mut entries);
            eviction_count += 1;
        }

        // SEC: Use checked arithmetic to prevent overflow
        // Re-read current size inside the loop to handle race conditions
        let mut eviction_count_size = 0;
        loop {
            let current = self.current_size.load(Ordering::Relaxed);
            // SEC: Check for overflow when converting u64 to usize
            let current_usize = if current > usize::MAX as u64 {
                usize::MAX
            } else {
                current as usize
            };

            // SEC: Use checked arithmetic
            if let Some(total) = current_usize.checked_add(new_entry_size) {
                if total <= self.config.max_size_bytes || entries.is_empty() {
                    break;
                }
            } else {
                // Overflow: must evict
            }

            if eviction_count_size >= MAX_EVICTION_ATTEMPTS {
                break; // Prevent infinite loop
            }

            self.evict_lru(&mut entries);
            eviction_count_size += 1;
        }
    }

    /// Evict the least recently used entry
    ///
    /// # Performance
    /// - PERF: This is O(n) where n is the number of entries
    /// - Future optimization: Use a proper LRU data structure (e.g., linked hash map)
    ///   to make this O(1)
    ///
    /// # Security
    /// - Uses saturating subtraction to prevent underflow
    fn evict_lru(&self, entries: &mut HashMap<CacheKey, CacheEntry>) {
        if entries.is_empty() {
            return;
        }

        // PERF: O(n) scan to find LRU entry
        // TODO: Replace HashMap with LinkedHashMap or similar for O(1) LRU eviction
        let lru_key = entries
            .iter()
            .min_by_key(|(_, e)| e.last_accessed)
            .map(|(k, _)| *k);

        if let Some(key) = lru_key {
            if let Some(entry) = entries.remove(&key) {
                // SEC: Use saturating subtraction to prevent underflow
                let old_size = self.current_size.load(Ordering::Relaxed);
                let new_size = old_size.saturating_sub(entry.size_bytes as u64);
                self.current_size.store(new_size, Ordering::Relaxed);
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
