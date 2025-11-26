//! Query optimization for Redis-based time index
//!
//! Provides intelligent query planning and execution with:
//! - Query result caching
//! - Parallel chunk fetching
//! - Chunk pruning based on time ranges
//! - Optimization for common query patterns
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisTimeIndex};
//! use gorilla_tsdb::redis::query::{QueryPlanner, QueryConfig};
//! use gorilla_tsdb::types::TimeRange;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let index = RedisTimeIndex::new(RedisConfig::default()).await?;
//! let planner = QueryPlanner::new(Arc::new(index), QueryConfig::default());
//!
//! // Plan and execute a time range query
//! let chunks = planner.query_chunks(1, TimeRange::new(0, 1000)?).await?;
//! # Ok(())
//! # }
//! ```

use crate::engine::traits::{ChunkReference, TimeIndex};
use crate::error::IndexError;
use crate::types::{SeriesId, TimeRange};

use super::index::RedisTimeIndex;

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Configuration for query optimization
#[derive(Clone, Debug)]
pub struct QueryConfig {
    /// Maximum number of cached query results
    pub cache_max_entries: usize,

    /// Time-to-live for cached results in seconds
    pub cache_ttl_secs: u64,

    /// Maximum number of parallel chunk fetches
    pub max_parallel_fetches: usize,

    /// Enable query result caching
    pub enable_cache: bool,

    /// Minimum time range to cache (in milliseconds)
    pub min_cacheable_range_ms: i64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: 1000,
            cache_ttl_secs: 60,
            max_parallel_fetches: 8,
            enable_cache: true,
            min_cacheable_range_ms: 3_600_000, // 1 hour
        }
    }
}

/// Query result cache entry with TTL support
struct CacheEntry {
    /// Cached chunk references
    chunks: Vec<ChunkReference>,
    /// When this entry was cached
    cached_at: Instant,
}

impl CacheEntry {
    /// Create a new cache entry
    fn new(chunks: Vec<ChunkReference>) -> Self {
        Self {
            chunks,
            cached_at: Instant::now(),
        }
    }

    /// Check if the entry is expired based on the provided TTL
    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Cache key for query results
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct CacheKey {
    series_id: SeriesId,
    start: i64,
    end: i64,
}

impl CacheKey {
    fn new(series_id: SeriesId, range: &TimeRange) -> Self {
        Self {
            series_id,
            start: range.start,
            end: range.end,
        }
    }
}

/// LRU-based query result cache
///
/// Uses the `lru` crate for O(1) eviction instead of O(n) scanning.
/// This is a significant performance improvement for caches with many entries.
struct QueryCache {
    /// LRU cache for query results - provides O(1) get, put, and eviction
    entries: LruCache<CacheKey, CacheEntry>,
    /// Default TTL for entries
    default_ttl: Duration,
    /// Counter for expired entries (for stats)
    expired_evictions: u64,
}

impl QueryCache {
    /// Create a new LRU-based query cache
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries to cache
    /// * `default_ttl_secs` - Default time-to-live for cache entries in seconds
    fn new(max_entries: usize, default_ttl_secs: u64) -> Self {
        // NonZeroUsize ensures cache capacity is at least 1
        let capacity = NonZeroUsize::new(max_entries.max(1))
            .expect("Cache capacity must be non-zero");

        Self {
            entries: LruCache::new(capacity),
            default_ttl: Duration::from_secs(default_ttl_secs),
            expired_evictions: 0,
        }
    }

    /// Get a cached result if available and not expired - O(1)
    ///
    /// LruCache::get automatically promotes the entry to most-recently-used.
    fn get(&mut self, key: &CacheKey) -> Option<Vec<ChunkReference>> {
        // LruCache::get returns Option<&V> and promotes to MRU
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired(self.default_ttl) {
                // Remove expired entry - need to use pop since we have immutable ref
                self.entries.pop(key);
                self.expired_evictions += 1;
                return None;
            }
            return Some(entry.chunks.clone());
        }
        None
    }

    /// Cache a query result - O(1), auto-evicts LRU if at capacity
    ///
    /// LruCache::put automatically evicts the least-recently-used entry
    /// if the cache is at capacity.
    fn put(&mut self, key: CacheKey, chunks: Vec<ChunkReference>) {
        // LruCache::put handles eviction automatically
        self.entries.put(key, CacheEntry::new(chunks));
    }

    /// Invalidate all entries for a series - O(n) but rare operation
    ///
    /// This operation requires scanning all entries since we're filtering
    /// by series_id which is part of the composite key.
    fn invalidate_series(&mut self, series_id: SeriesId) {
        // Collect keys to remove (can't mutate while iterating)
        let keys_to_remove: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|(k, _)| k.series_id == series_id)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            self.entries.pop(&key);
        }
    }

    /// Clear all cached entries
    fn clear(&mut self) {
        self.entries.clear();
        self.expired_evictions = 0;
    }

    /// Get cache statistics
    fn stats(&self) -> CacheStats {
        // Count expired entries by iterating (O(n) but stats are infrequent)
        let expired_count = self
            .entries
            .iter()
            .filter(|(_, e)| e.is_expired(self.default_ttl))
            .count();

        CacheStats {
            entries: self.entries.len(),
            expired: expired_count,
            total_accesses: 0, // LRU doesn't track access counts
        }
    }
}

/// Cache statistics snapshot
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached entries
    pub entries: usize,
    /// Number of expired entries pending cleanup
    pub expired: usize,
    /// Total number of cache accesses
    pub total_accesses: u64,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Series to query
    pub series_id: SeriesId,
    /// Time range for the query
    pub time_range: TimeRange,
    /// Whether this query can use cached results
    pub use_cache: bool,
    /// Whether to prefetch adjacent chunks
    pub prefetch_adjacent: bool,
    /// Number of expected chunks (if known)
    pub estimated_chunks: Option<usize>,
}

/// Query planner and executor
///
/// Optimizes time range queries against the Redis index with
/// intelligent caching and parallel execution.
pub struct QueryPlanner {
    /// Underlying Redis time index
    index: Arc<RedisTimeIndex>,

    /// Query result cache
    cache: RwLock<QueryCache>,

    /// Configuration
    config: QueryConfig,

    /// Statistics: total queries executed
    queries_executed: AtomicU64,

    /// Statistics: cache hits
    cache_hits: AtomicU64,

    /// Statistics: cache misses
    cache_misses: AtomicU64,
}

impl QueryPlanner {
    /// Create a new query planner
    ///
    /// # Arguments
    ///
    /// * `index` - The Redis time index to query
    /// * `config` - Query optimization configuration
    pub fn new(index: Arc<RedisTimeIndex>, config: QueryConfig) -> Self {
        let cache = QueryCache::new(config.cache_max_entries, config.cache_ttl_secs);

        Self {
            index,
            cache: RwLock::new(cache),
            config,
            queries_executed: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Create a query plan for a time range query
    ///
    /// Analyzes the query and determines optimization strategies.
    pub fn plan_query(&self, series_id: SeriesId, time_range: TimeRange) -> QueryPlan {
        let duration = time_range.end - time_range.start;

        // Only cache queries with sufficient time range
        let use_cache = self.config.enable_cache && duration >= self.config.min_cacheable_range_ms;

        // Prefetch adjacent chunks for streaming queries
        let prefetch_adjacent = duration > 86_400_000; // > 1 day

        QueryPlan {
            series_id,
            time_range,
            use_cache,
            prefetch_adjacent,
            estimated_chunks: None,
        }
    }

    /// Execute a query and return matching chunk references
    ///
    /// Uses caching and optimization based on the query plan.
    pub async fn query_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);

        let plan = self.plan_query(series_id, time_range);

        // Check cache first if enabled (async-safe)
        if plan.use_cache {
            let cache_key = CacheKey::new(series_id, &time_range);
            let cached = {
                let mut cache = self.cache.write().await;
                cache.get(&cache_key)
            };

            if let Some(chunks) = cached {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                debug!("Cache hit for series {} range {:?}", series_id, time_range);
                return Ok(chunks);
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Execute query against Redis
        let chunks = self
            .index
            .query_chunks(series_id, time_range)
            .await?;

        // Cache the result if applicable (async-safe)
        if plan.use_cache {
            let cache_key = CacheKey::new(series_id, &time_range);
            let mut cache = self.cache.write().await;
            cache.put(cache_key, chunks.clone());
        }

        debug!(
            "Queried {} chunks for series {} range {:?}",
            chunks.len(),
            series_id,
            time_range
        );

        Ok(chunks)
    }

    /// Execute multiple queries in parallel
    ///
    /// Useful for multi-series queries or queries with multiple time ranges.
    pub async fn query_chunks_parallel(
        &self,
        queries: Vec<(SeriesId, TimeRange)>,
    ) -> Result<Vec<(SeriesId, Vec<ChunkReference>)>, IndexError> {
        use futures::future::join_all;

        // Limit parallel execution
        let chunk_size = self.config.max_parallel_fetches;

        let mut results = Vec::with_capacity(queries.len());

        for chunk in queries.chunks(chunk_size) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|(series_id, range)| async {
                    let chunks = self.query_chunks(*series_id, *range).await?;
                    Ok::<_, IndexError>((*series_id, chunks))
                })
                .collect();

            let chunk_results = join_all(futures).await;

            for result in chunk_results {
                results.push(result?);
            }
        }

        Ok(results)
    }

    /// Find chunks for a series without caching
    ///
    /// Use this for queries that should not pollute the cache.
    pub async fn query_chunks_uncached(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        self.index.query_chunks(series_id, time_range).await
    }

    /// Invalidate cached results for a series
    ///
    /// Call this when a series is modified to prevent stale cache reads.
    pub async fn invalidate_series(&self, series_id: SeriesId) {
        let mut cache = self.cache.write().await;
        cache.invalidate_series(series_id);
        info!("Invalidated cache for series {}", series_id);
    }

    /// Clear all cached results
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        info!("Cleared query cache");
    }

    /// Get query planner statistics
    pub async fn stats(&self) -> QueryPlannerStats {
        let cache = self.cache.read().await;
        let cache_stats = cache.stats();

        QueryPlannerStats {
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_entries: cache_stats.entries,
            cache_expired: cache_stats.expired,
        }
    }

    /// Get the hit rate for the query cache
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;

        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }
}

/// Query planner statistics snapshot
#[derive(Debug, Clone)]
pub struct QueryPlannerStats {
    /// Total queries executed
    pub queries_executed: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Current cache entries
    pub cache_entries: usize,
    /// Expired cache entries pending cleanup
    pub cache_expired: usize,
}

/// Time range analyzer for query optimization
pub struct TimeRangeAnalyzer;

impl TimeRangeAnalyzer {
    /// Analyze a time range and suggest optimal query strategy
    pub fn analyze(range: &TimeRange) -> TimeRangeAnalysis {
        let duration_ms = range.end - range.start;

        let granularity = if duration_ms < 3_600_000 {
            TimeGranularity::SubHour
        } else if duration_ms < 86_400_000 {
            TimeGranularity::Hour
        } else if duration_ms < 604_800_000 {
            TimeGranularity::Day
        } else {
            TimeGranularity::Week
        };

        // Estimate chunk count (assuming 2-hour chunks)
        let estimated_chunks = (duration_ms / 7_200_000).max(1) as usize;

        TimeRangeAnalysis {
            duration_ms,
            granularity,
            estimated_chunks,
            should_cache: duration_ms > 3_600_000,
            should_stream: estimated_chunks > 100,
        }
    }
}

/// Time granularity for query categorization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeGranularity {
    /// Less than 1 hour
    SubHour,
    /// 1 hour to 1 day
    Hour,
    /// 1 day to 1 week
    Day,
    /// More than 1 week
    Week,
}

/// Analysis of a time range query
#[derive(Debug, Clone)]
pub struct TimeRangeAnalysis {
    /// Duration of the range in milliseconds
    pub duration_ms: i64,
    /// Granularity category
    pub granularity: TimeGranularity,
    /// Estimated number of chunks
    pub estimated_chunks: usize,
    /// Whether results should be cached
    pub should_cache: bool,
    /// Whether results should be streamed
    pub should_stream: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_config_default() {
        let config = QueryConfig::default();
        assert_eq!(config.cache_max_entries, 1000);
        assert!(config.enable_cache);
        assert_eq!(config.max_parallel_fetches, 8);
    }

    #[test]
    fn test_cache_key_equality() {
        let range1 = TimeRange { start: 0, end: 100 };
        let range2 = TimeRange { start: 0, end: 100 };
        let range3 = TimeRange { start: 0, end: 200 };

        let key1 = CacheKey::new(1, &range1);
        let key2 = CacheKey::new(1, &range2);
        let key3 = CacheKey::new(1, &range3);
        let key4 = CacheKey::new(2, &range1);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_query_cache_operations() {
        let mut cache = QueryCache::new(2, 60);
        let range = TimeRange { start: 0, end: 100 };
        let key = CacheKey::new(1, &range);
        let chunks = vec![];

        // Insert and retrieve
        cache.put(key.clone(), chunks.clone());
        assert!(cache.get(&key).is_some());

        // Missing key returns None
        let missing_key = CacheKey::new(999, &range);
        assert!(cache.get(&missing_key).is_none());
    }

    #[test]
    fn test_query_cache_eviction() {
        let mut cache = QueryCache::new(2, 60);

        let range1 = TimeRange { start: 0, end: 100 };
        let range2 = TimeRange {
            start: 100,
            end: 200,
        };
        let range3 = TimeRange {
            start: 200,
            end: 300,
        };

        let key1 = CacheKey::new(1, &range1);
        let key2 = CacheKey::new(2, &range2);
        let key3 = CacheKey::new(3, &range3);

        cache.put(key1.clone(), vec![]);
        cache.put(key2.clone(), vec![]);

        // Access key1 to increase its access count
        cache.get(&key1);

        // Adding key3 should evict key2 (lower access count)
        cache.put(key3.clone(), vec![]);

        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key3).is_some());
    }

    #[test]
    fn test_cache_invalidate_series() {
        let mut cache = QueryCache::new(10, 60);

        let range = TimeRange { start: 0, end: 100 };

        let key1 = CacheKey::new(1, &range);
        let key2 = CacheKey::new(2, &range);

        cache.put(key1.clone(), vec![]);
        cache.put(key2.clone(), vec![]);

        cache.invalidate_series(1);

        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_time_range_analyzer() {
        // Sub-hour query
        let short_range = TimeRange {
            start: 0,
            end: 1800_000,
        }; // 30 min
        let analysis = TimeRangeAnalyzer::analyze(&short_range);
        assert_eq!(analysis.granularity, TimeGranularity::SubHour);
        assert!(!analysis.should_cache);

        // Day query
        let day_range = TimeRange {
            start: 0,
            end: 86400_000,
        }; // 1 day
        let analysis = TimeRangeAnalyzer::analyze(&day_range);
        assert_eq!(analysis.granularity, TimeGranularity::Day);
        assert!(analysis.should_cache);

        // Week query (1 week exactly = 604800000 ms)
        let week_range = TimeRange {
            start: 0,
            end: 604800_000,
        };
        let analysis = TimeRangeAnalyzer::analyze(&week_range);
        assert_eq!(analysis.granularity, TimeGranularity::Week);
        // Estimated chunks = 604800000 / 7200000 = 84 chunks, not enough for streaming
        assert!(analysis.should_cache);

        // Large range (30 days) that should trigger streaming
        let month_range = TimeRange {
            start: 0,
            end: 2592000_000, // 30 days
        };
        let analysis = TimeRangeAnalyzer::analyze(&month_range);
        assert_eq!(analysis.granularity, TimeGranularity::Week);
        // Estimated chunks = 2592000000 / 7200000 = 360 chunks, should stream
        assert!(analysis.should_stream);
    }

    #[test]
    fn test_query_plan_creation() {
        // We can't test the full planner without a Redis connection,
        // but we can test the configuration
        let config = QueryConfig {
            cache_max_entries: 100,
            cache_ttl_secs: 30,
            max_parallel_fetches: 4,
            enable_cache: true,
            min_cacheable_range_ms: 1000,
        };

        assert_eq!(config.cache_max_entries, 100);
        assert_eq!(config.max_parallel_fetches, 4);
    }
}
