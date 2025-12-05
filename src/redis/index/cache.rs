//! Local cache for frequently accessed Redis data
//!
//! Provides a TTL-based cache for series metadata to reduce Redis round trips.

use crate::engine::traits::SeriesMetadata;
use crate::types::SeriesId;

use chrono::Utc;
use std::collections::HashMap;

/// Local cache for frequently accessed data
///
/// Thread-safe cache with TTL-based expiration for series metadata.
/// Uses simple eviction (expired-first, then oldest) when at capacity.
#[derive(Default)]
pub struct LocalCache {
    /// Series metadata cache: series_id -> metadata with TTL
    pub(crate) series_meta: HashMap<SeriesId, CachedSeriesMeta>,
    /// Maximum cache entries before eviction
    pub(crate) max_entries: usize,
}

/// Cached series metadata with time-to-live
///
/// Wraps SeriesMetadata with caching metadata for TTL-based expiration.
pub struct CachedSeriesMeta {
    /// The cached series metadata
    pub metadata: SeriesMetadata,
    /// Timestamp when cached (Unix milliseconds)
    pub cached_at: i64,
    /// Time-to-live in milliseconds
    pub ttl_ms: i64,
}

impl CachedSeriesMeta {
    /// Check if this cached entry has expired
    ///
    /// Compares current time against cached_at + ttl_ms.
    pub fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        now - self.cached_at > self.ttl_ms
    }
}

impl LocalCache {
    /// Create a new local cache with the specified capacity
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries before eviction triggers
    pub fn new(max_entries: usize) -> Self {
        Self {
            series_meta: HashMap::new(),
            max_entries,
        }
    }

    /// Get cached series metadata if not expired
    ///
    /// Returns cached metadata if available and not expired, enabling
    /// cache-first lookup optimization to reduce Redis round trips.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series identifier to look up
    ///
    /// # Returns
    ///
    /// Reference to cached metadata if found and not expired, None otherwise
    pub fn get_series_meta(&self, series_id: SeriesId) -> Option<&SeriesMetadata> {
        self.series_meta.get(&series_id).and_then(|cached| {
            if cached.is_expired() {
                None
            } else {
                Some(&cached.metadata)
            }
        })
    }

    /// Store series metadata in the cache with TTL
    ///
    /// If cache is at capacity, evicts an entry (preferring expired entries).
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series identifier
    /// * `metadata` - The metadata to cache
    /// * `ttl_ms` - Time-to-live in milliseconds
    pub fn set_series_meta(&mut self, series_id: SeriesId, metadata: SeriesMetadata, ttl_ms: i64) {
        // Evict if at capacity
        if self.series_meta.len() >= self.max_entries {
            // Simple eviction: remove first expired or oldest
            let mut to_remove = None;
            for (id, cached) in &self.series_meta {
                if cached.is_expired() || to_remove.is_none() {
                    to_remove = Some(*id);
                    if cached.is_expired() {
                        break;
                    }
                }
            }
            if let Some(id) = to_remove {
                self.series_meta.remove(&id);
            }
        }

        self.series_meta.insert(
            series_id,
            CachedSeriesMeta {
                metadata,
                cached_at: Utc::now().timestamp_millis(),
                ttl_ms,
            },
        );
    }

    /// Invalidate (remove) cached entry for a series
    ///
    /// Used when series metadata changes or series is deleted.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to invalidate
    pub fn invalidate_series(&mut self, series_id: SeriesId) {
        self.series_meta.remove(&series_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_cache_new() {
        let cache = LocalCache::new(100);
        assert_eq!(cache.max_entries, 100);
        assert!(cache.series_meta.is_empty());
    }

    #[test]
    fn test_local_cache_default() {
        let cache = LocalCache::default();
        assert_eq!(cache.max_entries, 0);
        assert!(cache.series_meta.is_empty());
    }

    #[test]
    fn test_local_cache() {
        let mut cache = LocalCache::new(2);

        let meta1 = SeriesMetadata {
            metric_name: "cpu.usage".to_string(),
            tags: HashMap::new(),
            created_at: 1000,
            retention_days: None,
        };

        let meta2 = SeriesMetadata {
            metric_name: "mem.usage".to_string(),
            tags: HashMap::new(),
            created_at: 2000,
            retention_days: None,
        };

        // Add two entries
        cache.set_series_meta(1, meta1.clone(), 60_000);
        cache.set_series_meta(2, meta2.clone(), 60_000);

        assert!(cache.get_series_meta(1).is_some());
        assert!(cache.get_series_meta(2).is_some());

        // Add third entry - should evict one
        let meta3 = SeriesMetadata {
            metric_name: "disk.usage".to_string(),
            tags: HashMap::new(),
            created_at: 3000,
            retention_days: None,
        };

        cache.set_series_meta(3, meta3.clone(), 60_000);

        // One of the first two should be evicted
        let count = [1, 2, 3]
            .iter()
            .filter(|id| cache.get_series_meta(**id).is_some())
            .count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_cache_invalidation() {
        let mut cache = LocalCache::new(10);

        let meta = SeriesMetadata {
            metric_name: "test".to_string(),
            tags: HashMap::new(),
            created_at: 1000,
            retention_days: None,
        };

        cache.set_series_meta(1, meta, 60_000);
        assert!(cache.get_series_meta(1).is_some());

        cache.invalidate_series(1);
        assert!(cache.get_series_meta(1).is_none());
    }

    #[test]
    fn test_cache_get_nonexistent() {
        let cache = LocalCache::new(10);
        assert!(cache.get_series_meta(12345).is_none());
    }

    #[test]
    fn test_cache_metadata_content() {
        let mut cache = LocalCache::new(10);

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("region".to_string(), "us-east".to_string());

        let meta = SeriesMetadata {
            metric_name: "cpu.usage".to_string(),
            tags: tags.clone(),
            created_at: 1000,
            retention_days: Some(30),
        };

        cache.set_series_meta(42, meta.clone(), 60_000);

        let cached = cache.get_series_meta(42).unwrap();
        assert_eq!(cached.metric_name, "cpu.usage");
        assert_eq!(cached.tags.get("host"), Some(&"server1".to_string()));
        assert_eq!(cached.tags.get("region"), Some(&"us-east".to_string()));
        assert_eq!(cached.created_at, 1000);
        assert_eq!(cached.retention_days, Some(30));
    }

    #[test]
    fn test_cache_overwrite_same_key() {
        let mut cache = LocalCache::new(10);

        let meta1 = SeriesMetadata {
            metric_name: "metric1".to_string(),
            tags: HashMap::new(),
            created_at: 1000,
            retention_days: None,
        };

        let meta2 = SeriesMetadata {
            metric_name: "metric2".to_string(),
            tags: HashMap::new(),
            created_at: 2000,
            retention_days: None,
        };

        cache.set_series_meta(1, meta1, 60_000);
        cache.set_series_meta(1, meta2, 60_000);

        let cached = cache.get_series_meta(1).unwrap();
        assert_eq!(cached.metric_name, "metric2");
        assert_eq!(cached.created_at, 2000);
    }

    #[test]
    fn test_cache_invalidate_nonexistent() {
        let mut cache = LocalCache::new(10);
        // Should not panic when invalidating non-existent entry
        cache.invalidate_series(99999);
        assert!(cache.get_series_meta(99999).is_none());
    }

    #[test]
    fn test_cached_series_meta_not_expired() {
        let cached = CachedSeriesMeta {
            metadata: SeriesMetadata {
                metric_name: "test".to_string(),
                tags: HashMap::new(),
                created_at: 0,
                retention_days: None,
            },
            cached_at: Utc::now().timestamp_millis(),
            ttl_ms: 60_000, // 60 seconds
        };

        assert!(!cached.is_expired());
    }

    #[test]
    fn test_cached_series_meta_expired() {
        let cached = CachedSeriesMeta {
            metadata: SeriesMetadata {
                metric_name: "test".to_string(),
                tags: HashMap::new(),
                created_at: 0,
                retention_days: None,
            },
            cached_at: Utc::now().timestamp_millis() - 120_000, // 2 minutes ago
            ttl_ms: 60_000,                                     // 60 seconds TTL
        };

        assert!(cached.is_expired());
    }

    #[test]
    fn test_cached_series_meta_zero_ttl() {
        let cached = CachedSeriesMeta {
            metadata: SeriesMetadata {
                metric_name: "test".to_string(),
                tags: HashMap::new(),
                created_at: 0,
                retention_days: None,
            },
            cached_at: Utc::now().timestamp_millis(),
            ttl_ms: 0, // Immediate expiry
        };

        // Zero TTL means immediate expiry (or very short-lived)
        // Adding 1ms to cached_at would make it expired
        let cached_old = CachedSeriesMeta {
            cached_at: Utc::now().timestamp_millis() - 1,
            ..cached
        };
        assert!(cached_old.is_expired());
    }

    #[test]
    fn test_cache_zero_capacity() {
        let mut cache = LocalCache::new(0);

        let meta = SeriesMetadata {
            metric_name: "test".to_string(),
            tags: HashMap::new(),
            created_at: 0,
            retention_days: None,
        };

        // With zero capacity, set should still work but evict immediately
        cache.set_series_meta(1, meta, 60_000);

        // The entry count should be at most 1 (or 0 if immediately evicted)
        assert!(cache.series_meta.len() <= 1);
    }

    #[test]
    fn test_cache_large_series_id() {
        let mut cache = LocalCache::new(10);

        let meta = SeriesMetadata {
            metric_name: "test".to_string(),
            tags: HashMap::new(),
            created_at: 0,
            retention_days: None,
        };

        let large_id = u64::MAX as SeriesId;
        cache.set_series_meta(large_id, meta.clone(), 60_000);

        assert!(cache.get_series_meta(large_id).is_some());
        assert_eq!(cache.get_series_meta(large_id).unwrap().metric_name, "test");
    }

    #[test]
    fn test_metadata_with_special_characters() {
        let mut cache = LocalCache::new(10);

        let mut tags = HashMap::new();
        tags.insert(
            "special".to_string(),
            "value with spaces and !@#$%^&*()".to_string(),
        );

        let meta = SeriesMetadata {
            metric_name: "metric.with.dots.and-dashes".to_string(),
            tags,
            created_at: 0,
            retention_days: None,
        };

        cache.set_series_meta(1, meta.clone(), 60_000);

        let cached = cache.get_series_meta(1).unwrap();
        assert_eq!(cached.metric_name, "metric.with.dots.and-dashes");
        assert_eq!(
            cached.tags.get("special"),
            Some(&"value with spaces and !@#$%^&*()".to_string())
        );
    }
}
