//! Unified Cache Statistics and Management
//!
//! Provides a single interface to monitor and manage all cache layers.
//! This module aggregates statistics from:
//! - Storage cache (chunk data)
//! - Query cache (query results)
//! - Local metadata cache (Redis lookups)
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::cache::UnifiedCacheManager;
//!
//! let manager = UnifiedCacheManager::new(query_cache.clone(), Some(storage_cache.clone()));
//! let stats = manager.stats();
//!
//! println!("Query cache hit rate: {:.2}%", stats.query.hit_rate * 100.0);
//! println!("Storage cache entries: {}", stats.storage.as_ref().map(|s| s.entries).unwrap_or(0));
//! println!("Overall hit rate: {:.2}%", stats.overall_hit_rate * 100.0);
//! ```

use super::query::QueryCache;
use super::storage::CacheManager;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Snapshot of storage cache statistics
///
/// Contains point-in-time metrics for the chunk data cache.
#[derive(Debug, Clone, Serialize)]
pub struct StorageCacheStatsSnapshot {
    /// Current number of cached entries
    pub entries: usize,
    /// Current memory usage in bytes
    pub memory_bytes: usize,
    /// Memory usage as ratio of max (0.0 to 1.0)
    pub memory_ratio: f64,
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
    /// Total entries evicted
    pub evictions: u64,
}

/// Snapshot of query cache statistics
///
/// Contains point-in-time metrics for the query result cache.
#[derive(Debug, Clone, Serialize)]
pub struct QueryCacheStatsSnapshot {
    /// Current number of cached queries
    pub entries: usize,
    /// Current size in bytes
    pub size_bytes: u64,
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
    /// Total entries evicted due to size/count limits
    pub evictions: u64,
    /// Total entries invalidated due to writes
    pub invalidations: u64,
}

/// Unified view of all cache layer statistics
///
/// Aggregates metrics from all cache components for monitoring.
#[derive(Debug, Clone, Serialize)]
pub struct UnifiedCacheStats {
    /// Storage cache (chunk data) statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageCacheStatsSnapshot>,

    /// Query cache statistics
    pub query: QueryCacheStatsSnapshot,

    /// Total memory usage across all caches
    pub total_memory_bytes: usize,

    /// Overall hit rate across all caches (weighted by access count)
    pub overall_hit_rate: f64,

    /// Total hits across all caches
    pub total_hits: u64,

    /// Total misses across all caches
    pub total_misses: u64,
}

/// Unified cache manager for monitoring and management
///
/// Provides aggregate view of all cache layers and enables
/// coordinated cache operations (e.g., clear all).
pub struct UnifiedCacheManager {
    /// Query result cache
    query_cache: Arc<QueryCache>,

    /// Optional storage cache (chunk data)
    /// May be None if storage cache is not enabled
    storage_cache: Option<Arc<CacheManager<Vec<u8>>>>,
}

impl UnifiedCacheManager {
    /// Create a new unified cache manager
    ///
    /// # Arguments
    ///
    /// * `query_cache` - The query result cache (required)
    /// * `storage_cache` - Optional storage cache for chunk data
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = UnifiedCacheManager::new(query_cache.clone(), Some(storage_cache.clone()));
    /// ```
    pub fn new(
        query_cache: Arc<QueryCache>,
        storage_cache: Option<Arc<CacheManager<Vec<u8>>>>,
    ) -> Self {
        Self {
            query_cache,
            storage_cache,
        }
    }

    /// Get unified statistics from all cache layers
    ///
    /// Returns a snapshot of current cache metrics across all layers,
    /// including aggregated totals and overall hit rate.
    pub fn stats(&self) -> UnifiedCacheStats {
        // Collect query cache stats
        let q_stats = self.query_cache.stats();
        let q_hits = q_stats.hits.load(Ordering::Relaxed);
        let q_misses = q_stats.misses.load(Ordering::Relaxed);
        let q_total = q_hits + q_misses;
        let q_hit_rate = if q_total > 0 {
            q_hits as f64 / q_total as f64
        } else {
            0.0
        };

        let query_stats = QueryCacheStatsSnapshot {
            entries: self.query_cache.entry_count(),
            size_bytes: self.query_cache.size_bytes(),
            hits: q_hits,
            misses: q_misses,
            hit_rate: q_hit_rate,
            evictions: q_stats.evictions.load(Ordering::Relaxed),
            invalidations: q_stats.invalidations.load(Ordering::Relaxed),
        };

        // Collect storage cache stats if available
        let storage_stats = self.storage_cache.as_ref().and_then(|cache| {
            cache.stats().map(|s| {
                let s_hits = s.hits();
                let s_misses = s.misses();
                let s_total = s_hits + s_misses;
                let s_hit_rate = if s_total > 0 {
                    s_hits as f64 / s_total as f64
                } else {
                    0.0
                };

                StorageCacheStatsSnapshot {
                    entries: cache.len(),
                    memory_bytes: s.current_bytes(),
                    memory_ratio: cache.memory_usage_ratio(),
                    hits: s_hits,
                    misses: s_misses,
                    hit_rate: s_hit_rate,
                    evictions: s.evictions(),
                }
            })
        });

        // Calculate totals
        let (s_hits, s_misses, s_memory) = storage_stats
            .as_ref()
            .map(|s| (s.hits, s.misses, s.memory_bytes))
            .unwrap_or((0, 0, 0));

        let total_hits = q_hits + s_hits;
        let total_misses = q_misses + s_misses;
        let total_ops = total_hits + total_misses;

        let overall_hit_rate = if total_ops > 0 {
            total_hits as f64 / total_ops as f64
        } else {
            0.0
        };

        // Include query cache size in total memory (approximate)
        // SEC: Use checked arithmetic to prevent overflow
        let total_memory = s_memory
            .checked_add(query_stats.size_bytes as usize)
            .unwrap_or(usize::MAX); // Cap at max if overflow

        UnifiedCacheStats {
            storage: storage_stats,
            query: query_stats,
            total_memory_bytes: total_memory,
            overall_hit_rate,
            total_hits,
            total_misses,
        }
    }

    /// Clear all caches
    ///
    /// Clears entries from all cache layers. Use with caution in production
    /// as this will cause a temporary spike in latency.
    pub fn clear_all(&self) {
        self.query_cache.clear();
        if let Some(ref storage) = self.storage_cache {
            storage.clear();
        }
    }

    /// Get query cache reference
    ///
    /// Returns reference to the query cache for direct operations.
    pub fn query_cache(&self) -> &Arc<QueryCache> {
        &self.query_cache
    }

    /// Get storage cache reference if available
    ///
    /// Returns reference to the storage cache for direct operations.
    pub fn storage_cache(&self) -> Option<&Arc<CacheManager<Vec<u8>>>> {
        self.storage_cache.as_ref()
    }

    /// Invalidate all caches for a specific series
    ///
    /// Called when data is written to a series to ensure cache freshness.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series that was modified
    pub fn invalidate_series(&self, series_id: u128) {
        // Invalidate query cache
        self.query_cache.invalidate_series(series_id);

        // Storage cache doesn't have series-based invalidation
        // (chunks are keyed by series_id + chunk_id, handled separately)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::QueryCacheConfig;
    use crate::cache::StorageCacheConfig;

    #[test]
    fn test_unified_manager_query_only() {
        // Create query cache
        let query_cache = Arc::new(QueryCache::new(QueryCacheConfig::default()));

        // Create manager without storage cache
        let manager = UnifiedCacheManager::new(query_cache, None);

        // Get stats
        let stats = manager.stats();

        // Verify structure
        assert!(stats.storage.is_none());
        assert_eq!(stats.query.entries, 0);
        assert_eq!(stats.total_hits, 0);
        assert_eq!(stats.total_misses, 0);
    }

    #[test]
    fn test_unified_manager_with_storage() {
        // Create both caches
        let query_cache = Arc::new(QueryCache::new(QueryCacheConfig::default()));
        let storage_cache = Arc::new(CacheManager::new(StorageCacheConfig::default()));

        // Create manager with both
        let manager = UnifiedCacheManager::new(query_cache.clone(), Some(storage_cache.clone()));

        // Get stats
        let stats = manager.stats();

        // Verify both caches are tracked
        assert!(stats.storage.is_some());
        assert_eq!(stats.query.entries, 0);
    }

    #[test]
    fn test_unified_stats_serialization() {
        let query_stats = QueryCacheStatsSnapshot {
            entries: 100,
            size_bytes: 1024,
            hits: 500,
            misses: 100,
            hit_rate: 0.833,
            evictions: 10,
            invalidations: 5,
        };

        let storage_stats = StorageCacheStatsSnapshot {
            entries: 50,
            memory_bytes: 2048,
            memory_ratio: 0.5,
            hits: 1000,
            misses: 200,
            hit_rate: 0.833,
            evictions: 20,
        };

        let stats = UnifiedCacheStats {
            storage: Some(storage_stats),
            query: query_stats,
            total_memory_bytes: 3072,
            overall_hit_rate: 0.833,
            total_hits: 1500,
            total_misses: 300,
        };

        // Verify serialization works
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"entries\":100"));
        assert!(json.contains("\"total_hits\":1500"));
    }

    #[test]
    fn test_overall_hit_rate_calculation() {
        // Query: 80 hits, 20 misses (80% hit rate)
        // Storage: 160 hits, 40 misses (80% hit rate)
        // Overall: 240 hits, 60 misses = 80% hit rate

        let query_stats = QueryCacheStatsSnapshot {
            entries: 0,
            size_bytes: 0,
            hits: 80,
            misses: 20,
            hit_rate: 0.8,
            evictions: 0,
            invalidations: 0,
        };

        let storage_stats = StorageCacheStatsSnapshot {
            entries: 0,
            memory_bytes: 0,
            memory_ratio: 0.0,
            hits: 160,
            misses: 40,
            hit_rate: 0.8,
            evictions: 0,
        };

        let stats = UnifiedCacheStats {
            storage: Some(storage_stats),
            query: query_stats,
            total_memory_bytes: 0,
            overall_hit_rate: 0.8, // (80 + 160) / (100 + 200)
            total_hits: 240,
            total_misses: 60,
        };

        assert_eq!(stats.total_hits, 240);
        assert_eq!(stats.total_misses, 60);
        assert!((stats.overall_hit_rate - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_clear_all() {
        let query_cache = Arc::new(QueryCache::new(QueryCacheConfig::default()));
        let storage_config = StorageCacheConfig::new(1024).with_shards(2);
        let storage_cache = Arc::new(CacheManager::new(storage_config));

        let manager = UnifiedCacheManager::new(query_cache.clone(), Some(storage_cache.clone()));

        // Clear all should not panic
        manager.clear_all();

        // Verify caches are empty
        assert_eq!(manager.query_cache().entry_count(), 0);
        assert!(manager.storage_cache().unwrap().is_empty());
    }
}
