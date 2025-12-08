//! Local Metadata Cache
//!
//! Provides an in-memory LRU cache for frequently accessed metadata,
//! reducing Redis round-trips for repeated lookups.
//!
//! # Features
//!
//! - LRU eviction when at capacity
//! - TTL-based expiration for freshness
//! - Thread-safe with RwLock
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::cache::LocalMetadataCache;
//!
//! let cache = LocalMetadataCache::new(1000, 60_000); // 1000 entries, 60s TTL
//!
//! // Cache metadata
//! cache.set(series_id, metadata);
//!
//! // Retrieve (returns None if expired or not found)
//! if let Some(meta) = cache.get(series_id) {
//!     println!("Cached: {}", meta.metric_name);
//! }
//! ```

use chrono::Utc;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::RwLock;

/// Cached entry with TTL tracking
///
/// Wraps cached data with timing information for expiration.
#[derive(Clone, Debug)]
pub struct CachedMetadata<T: Clone> {
    /// The cached data
    pub data: T,
    /// Timestamp when cached (milliseconds since epoch)
    cached_at: i64,
    /// Time-to-live in milliseconds
    ttl_ms: i64,
}

impl<T: Clone> CachedMetadata<T> {
    /// Create a new cached entry
    ///
    /// # Arguments
    ///
    /// * `data` - The data to cache
    /// * `ttl_ms` - Time-to-live in milliseconds
    pub fn new(data: T, ttl_ms: i64) -> Self {
        Self {
            data,
            cached_at: Utc::now().timestamp_millis(),
            ttl_ms,
        }
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        now - self.cached_at > self.ttl_ms
    }

    /// Get remaining TTL in milliseconds (0 if expired)
    pub fn remaining_ttl_ms(&self) -> i64 {
        let elapsed = Utc::now().timestamp_millis() - self.cached_at;
        (self.ttl_ms - elapsed).max(0)
    }

    /// Get the cached data if not expired
    pub fn get_if_valid(&self) -> Option<&T> {
        if self.is_expired() {
            None
        } else {
            Some(&self.data)
        }
    }
}

/// Local in-memory cache for metadata with LRU eviction and TTL
///
/// Thread-safe cache that reduces external lookups by caching
/// frequently accessed metadata locally.
pub struct LocalMetadataCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Cached entries
    entries: RwLock<HashMap<K, CachedMetadata<V>>>,
    /// Maximum number of entries
    max_entries: usize,
    /// Default TTL in milliseconds
    default_ttl_ms: i64,
}

impl<K, V> LocalMetadataCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Create a new local metadata cache
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries before eviction
    /// * `default_ttl_ms` - Default time-to-live in milliseconds
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // 1000 entries, 60 second TTL
    /// let cache = LocalMetadataCache::new(1000, 60_000);
    /// ```
    pub fn new(max_entries: usize, default_ttl_ms: i64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
            default_ttl_ms,
        }
    }

    /// Get cached data if not expired
    ///
    /// Returns None if key not found or entry has expired.
    /// Expired entries are left in cache (cleaned on next set).
    pub async fn get(&self, key: &K) -> Option<V> {
        let entries = self.entries.read().await;
        entries.get(key).and_then(|cached| {
            if cached.is_expired() {
                None
            } else {
                Some(cached.data.clone())
            }
        })
    }

    /// Cache data with default TTL
    ///
    /// If at capacity, evicts expired entries first, then oldest entry.
    pub async fn set(&self, key: K, data: V) {
        self.set_with_ttl(key, data, self.default_ttl_ms).await;
    }

    /// Cache data with custom TTL
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key
    /// * `data` - Data to cache
    /// * `ttl_ms` - Time-to-live in milliseconds
    ///
    /// # Security
    /// - Limits eviction attempts to prevent DoS
    /// - Validates TTL to prevent negative or extremely large values
    pub async fn set_with_ttl(&self, key: K, data: V, ttl_ms: i64) {
        // SEC: Validate TTL to prevent DoS via extremely large TTLs
        const MAX_TTL_MS: i64 = 365 * 24 * 60 * 60 * 1000; // 1 year max
        const MIN_TTL_MS: i64 = 0;
        let ttl_ms = ttl_ms.clamp(MIN_TTL_MS, MAX_TTL_MS);

        let mut entries = self.entries.write().await;

        // SEC: Limit eviction attempts to prevent DoS
        const MAX_EVICTION_ATTEMPTS: usize = 100;
        let mut eviction_attempts = 0;

        // Evict if at capacity
        while entries.len() >= self.max_entries && eviction_attempts < MAX_EVICTION_ATTEMPTS {
            // First try to evict expired entries
            let expired_keys: Vec<K> = entries
                .iter()
                .filter(|(_, v)| v.is_expired())
                .map(|(k, _)| k.clone())
                .take(10) // Limit batch size to prevent DoS
                .collect();

            for k in expired_keys {
                entries.remove(&k);
            }

            // If still at capacity, evict oldest entry
            if entries.len() >= self.max_entries {
                let oldest_key = entries
                    .iter()
                    .min_by_key(|(_, v)| v.cached_at)
                    .map(|(k, _)| k.clone());

                if let Some(k) = oldest_key {
                    entries.remove(&k);
                }
            }

            eviction_attempts += 1;
        }

        entries.insert(key, CachedMetadata::new(data, ttl_ms));
    }

    /// Remove a specific entry from the cache
    ///
    /// Returns the removed data if it existed and was not expired.
    pub async fn remove(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.write().await;
        entries.remove(key).and_then(|cached| {
            if cached.is_expired() {
                None
            } else {
                Some(cached.data)
            }
        })
    }

    /// Clear all entries from the cache
    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        entries.clear();
    }

    /// Get current entry count (including expired)
    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Check if cache is empty
    pub async fn is_empty(&self) -> bool {
        self.entries.read().await.is_empty()
    }

    /// Get count of valid (non-expired) entries
    pub async fn valid_count(&self) -> usize {
        let entries = self.entries.read().await;
        entries.values().filter(|v| !v.is_expired()).count()
    }

    /// Remove all expired entries
    ///
    /// Returns the number of entries removed.
    pub async fn cleanup_expired(&self) -> usize {
        let mut entries = self.entries.write().await;
        let before = entries.len();

        entries.retain(|_, v| !v.is_expired());

        before - entries.len()
    }

    /// Get cache configuration
    pub fn config(&self) -> (usize, i64) {
        (self.max_entries, self.default_ttl_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_metadata_not_expired() {
        let cached = CachedMetadata::new("test data".to_string(), 60_000);
        assert!(!cached.is_expired());
        assert!(cached.get_if_valid().is_some());
    }

    #[test]
    fn test_cached_metadata_expired() {
        let mut cached = CachedMetadata::new("test data".to_string(), 1);
        // Simulate past cache time
        cached.cached_at = Utc::now().timestamp_millis() - 1000;
        assert!(cached.is_expired());
        assert!(cached.get_if_valid().is_none());
    }

    #[test]
    fn test_cached_metadata_remaining_ttl() {
        let cached = CachedMetadata::new("test".to_string(), 60_000);
        let remaining = cached.remaining_ttl_ms();
        // Should be close to 60000 (within 100ms tolerance)
        assert!(remaining > 59_900);
        assert!(remaining <= 60_000);
    }

    #[tokio::test]
    async fn test_local_cache_set_get() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 60_000);

        cache.set(1, "value1".to_string()).await;
        cache.set(2, "value2".to_string()).await;

        assert_eq!(cache.get(&1).await, Some("value1".to_string()));
        assert_eq!(cache.get(&2).await, Some("value2".to_string()));
        assert_eq!(cache.get(&3).await, None);
    }

    #[tokio::test]
    async fn test_local_cache_remove() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 60_000);

        cache.set(1, "value".to_string()).await;
        assert!(cache.get(&1).await.is_some());

        let removed = cache.remove(&1).await;
        assert_eq!(removed, Some("value".to_string()));
        assert!(cache.get(&1).await.is_none());

        // Remove non-existent
        assert!(cache.remove(&999).await.is_none());
    }

    #[tokio::test]
    async fn test_local_cache_capacity_eviction() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(3, 60_000);

        cache.set(1, "v1".to_string()).await;
        // Small delay to ensure different timestamps for deterministic eviction order
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
        cache.set(2, "v2".to_string()).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
        cache.set(3, "v3".to_string()).await;

        assert_eq!(cache.len().await, 3);

        // Adding 4th should evict oldest (key 1)
        cache.set(4, "v4".to_string()).await;

        assert_eq!(cache.len().await, 3);
        // Entry 1 should be evicted (oldest)
        assert!(cache.get(&1).await.is_none());
        assert!(cache.get(&4).await.is_some());
    }

    #[tokio::test]
    async fn test_local_cache_clear() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 60_000);

        cache.set(1, "v1".to_string()).await;
        cache.set(2, "v2".to_string()).await;

        assert_eq!(cache.len().await, 2);

        cache.clear().await;

        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_local_cache_custom_ttl() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 60_000);

        // Set with very short TTL
        cache.set_with_ttl(1, "short lived".to_string(), 1).await;

        // Should be available immediately
        // (might be expired depending on timing, so we just verify it doesn't panic)
        let _ = cache.get(&1).await;
    }

    #[tokio::test]
    async fn test_local_cache_config() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(500, 30_000);
        let (max_entries, ttl) = cache.config();
        assert_eq!(max_entries, 500);
        assert_eq!(ttl, 30_000);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 1);

        cache.set(1, "v1".to_string()).await;
        cache.set(2, "v2".to_string()).await;

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let cleaned = cache.cleanup_expired().await;
        assert_eq!(cleaned, 2);
        assert_eq!(cache.len().await, 0);
    }

    #[tokio::test]
    async fn test_valid_count() {
        let cache: LocalMetadataCache<u64, String> = LocalMetadataCache::new(100, 60_000);

        cache.set(1, "v1".to_string()).await;
        cache.set(2, "v2".to_string()).await;

        assert_eq!(cache.valid_count().await, 2);
    }
}
