//! Redis-based time index implementation
//!
//! Implements the `TimeIndex` trait using Redis Sorted Sets for
//! ultra-fast time-based chunk lookups.
//!
//! # Redis Key Schema
//!
//! ```text
//! ts:registry                    → SET of all series_ids
//! ts:series:{id}:index           → ZSET(start_timestamp → chunk_id)
//! ts:series:{id}:meta            → HASH {created_at, last_write, metric_name, tags...}
//! ts:chunks:{chunk_id}           → HASH {series_id, path, timestamps, status...}
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisTimeIndex};
//! use gorilla_tsdb::engine::traits::{TimeIndex, SeriesMetadata, ChunkStatus};
//! use gorilla_tsdb::types::{ChunkId, TimeRange};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let index = RedisTimeIndex::new(RedisConfig::default()).await?;
//!
//! // Query chunks in a time range
//! let chunks = index.query_chunks(1, TimeRange::new(0, 1000)?).await?;
//! # Ok(())
//! # }
//! ```

use crate::engine::traits::{
    ChunkLocation, ChunkReference, ChunkStatus, IndexConfig, IndexStats, SeriesMetadata, TimeIndex,
};
use crate::error::IndexError;
use crate::types::{ChunkId, SeriesId, TagFilter, TimeRange};

use super::connection::{HealthStatus, PoolMetricsSnapshot, RedisConfig, RedisPool};
use super::scripts::LuaScripts;

use async_trait::async_trait;
use chrono::Utc;
use parking_lot::RwLock;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Key prefixes for Redis data structures
const KEY_REGISTRY: &str = "ts:registry";
const KEY_SERIES_INDEX_PREFIX: &str = "ts:series:";
const KEY_SERIES_INDEX_SUFFIX: &str = ":index";
const KEY_SERIES_META_SUFFIX: &str = ":meta";
const KEY_CHUNKS_PREFIX: &str = "ts:chunks:";

/// Redis-based time index implementation
///
/// Uses Redis Sorted Sets for O(log N) time-based queries and provides
/// atomic operations via Lua scripts.
pub struct RedisTimeIndex {
    /// Redis connection pool
    pool: Arc<RedisPool>,

    /// Lua scripts for atomic operations
    scripts: Arc<LuaScripts>,

    /// Local cache for frequently accessed metadata
    local_cache: RwLock<LocalCache>,

    /// Index statistics (reserved for future live stats integration)
    #[allow(dead_code)]
    stats: IndexStats,

    /// Query counter for stats
    queries_served: AtomicU64,

    /// Cache hits counter
    cache_hits: AtomicU64,

    /// Cache misses counter
    cache_misses: AtomicU64,
}

/// Local cache for frequently accessed data
#[derive(Default)]
struct LocalCache {
    /// Series metadata cache: series_id -> metadata
    series_meta: HashMap<SeriesId, CachedSeriesMeta>,

    /// Maximum cache entries
    max_entries: usize,
}

/// Cached series metadata with TTL
struct CachedSeriesMeta {
    /// The cached series metadata
    #[allow(dead_code)]
    metadata: SeriesMetadata,
    /// Timestamp when cached (in milliseconds)
    cached_at: i64,
    /// Time-to-live in milliseconds
    ttl_ms: i64,
}

impl CachedSeriesMeta {
    fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        now - self.cached_at > self.ttl_ms
    }
}

impl LocalCache {
    fn new(max_entries: usize) -> Self {
        Self {
            series_meta: HashMap::new(),
            max_entries,
        }
    }

    /// Get cached series metadata if not expired
    /// Reserved for future cache-first lookup optimization
    #[allow(dead_code)]
    fn get_series_meta(&self, series_id: SeriesId) -> Option<&SeriesMetadata> {
        self.series_meta.get(&series_id).and_then(|cached| {
            if cached.is_expired() {
                None
            } else {
                Some(&cached.metadata)
            }
        })
    }

    fn set_series_meta(&mut self, series_id: SeriesId, metadata: SeriesMetadata, ttl_ms: i64) {
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

    fn invalidate_series(&mut self, series_id: SeriesId) {
        self.series_meta.remove(&series_id);
    }
}

/// Chunk metadata stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisChunkMetadata {
    series_id: String,
    path: String,
    start_time: i64,
    end_time: i64,
    point_count: usize,
    size_bytes: usize,
    compression: String,
    status: String,
    created_at: i64,
}

impl RedisTimeIndex {
    /// Create a new Redis time index
    ///
    /// # Arguments
    ///
    /// * `config` - Redis connection configuration
    ///
    /// # Returns
    ///
    /// A new RedisTimeIndex or an error if connection fails
    pub async fn new(config: RedisConfig) -> Result<Self, IndexError> {
        let pool = Arc::new(RedisPool::new(config).await?);
        let scripts = Arc::new(LuaScripts::new());

        info!("Redis time index initialized");

        Ok(Self {
            pool,
            scripts,
            local_cache: RwLock::new(LocalCache::new(1000)),
            stats: IndexStats::default(),
            queries_served: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        })
    }

    /// Get the Redis key for a series index
    fn series_index_key(series_id: SeriesId) -> String {
        format!(
            "{}{}{}",
            KEY_SERIES_INDEX_PREFIX, series_id, KEY_SERIES_INDEX_SUFFIX
        )
    }

    /// Get the Redis key for series metadata
    fn series_meta_key(series_id: SeriesId) -> String {
        format!(
            "{}{}{}",
            KEY_SERIES_INDEX_PREFIX, series_id, KEY_SERIES_META_SUFFIX
        )
    }

    /// Get the Redis key for chunk metadata
    fn chunk_key(chunk_id: &ChunkId) -> String {
        format!("{}{}", KEY_CHUNKS_PREFIX, chunk_id.0)
    }

    /// Get pool health status
    pub fn health_status(&self) -> HealthStatus {
        self.pool.health_status()
    }

    /// Get pool metrics
    pub fn pool_metrics(&self) -> PoolMetricsSnapshot {
        self.pool.metrics()
    }

    /// Perform health check
    pub async fn health_check(&self) -> HealthStatus {
        self.pool.health_check().await
    }

    /// Add multiple chunks in a batch
    pub async fn add_chunks_batch(
        &self,
        series_id: SeriesId,
        chunks: Vec<(ChunkId, TimeRange, ChunkLocation)>,
    ) -> Result<usize, IndexError> {
        if chunks.is_empty() {
            return Ok(0);
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();

        let series_index = Self::series_index_key(series_id);
        let current_time = Utc::now().timestamp_millis();

        for (chunk_id, time_range, location) in &chunks {
            // Add to sorted set
            pipe.zadd(&series_index, &chunk_id.0, time_range.start);

            // Store chunk metadata
            let chunk_key = Self::chunk_key(chunk_id);
            let metadata = RedisChunkMetadata {
                series_id: series_id.to_string(),
                path: location.path.clone(),
                start_time: time_range.start,
                end_time: time_range.end,
                point_count: 0,
                size_bytes: location.size.unwrap_or(0),
                compression: "gorilla".to_string(),
                status: "sealed".to_string(),
                created_at: current_time,
            };

            let metadata_json = serde_json::to_string(&metadata)
                .map_err(|e| IndexError::SerializationError(e.to_string()))?;

            pipe.hset(&chunk_key, "metadata", metadata_json);
        }

        // Execute pipeline
        pipe.query_async::<()>(&mut *conn)
            .await
            .map_err(|e| IndexError::ConnectionError(format!("Pipeline failed: {}", e)))?;

        Ok(chunks.len())
    }

    /// Get chunk metadata from Redis
    async fn get_chunk_metadata(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<Option<RedisChunkMetadata>, IndexError> {
        let chunk_key = Self::chunk_key(chunk_id);

        let result: Option<String> = self
            .pool
            .execute(|mut conn| {
                let chunk_key = chunk_key.clone();
                async move { conn.hget(&chunk_key, "metadata").await }
            })
            .await?;

        match result {
            Some(json) => {
                let metadata: RedisChunkMetadata = serde_json::from_str(&json)
                    .map_err(|e| IndexError::DeserializationError(e.to_string()))?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl TimeIndex for RedisTimeIndex {
    /// Get the index identifier
    fn index_id(&self) -> &str {
        "redis-time-index-v1"
    }

    /// Initialize the index (no-op for Redis as connection is established in new())
    async fn initialize(&self, _config: IndexConfig) -> Result<(), IndexError> {
        // Connection is already established in new()
        // Optionally verify connection here
        self.pool.health_check().await;
        Ok(())
    }

    /// Register a new time series
    async fn register_series(
        &self,
        series_id: SeriesId,
        metadata: SeriesMetadata,
    ) -> Result<(), IndexError> {
        let script = self.scripts.register_series();
        let current_time = Utc::now().timestamp_millis();

        let tags_json = serde_json::to_string(&metadata.tags)
            .map_err(|e| IndexError::SerializationError(e.to_string()))?;

        let retention = metadata.retention_days.unwrap_or(0);

        let result: i32 = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let metric_name = metadata.metric_name.clone();
                let tags_json = tags_json.clone();
                async move {
                    script
                        .key(KEY_REGISTRY)
                        .key(Self::series_meta_key(series_id))
                        .arg(series_id.to_string())
                        .arg(current_time)
                        .arg(metric_name)
                        .arg(tags_json)
                        .arg(retention)
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        if result == 1 {
            // Cache the metadata
            let mut cache = self.local_cache.write();
            cache.set_series_meta(series_id, metadata, 60_000); // 60 second TTL
            debug!("Registered new series: {}", series_id);
        } else {
            debug!("Series {} already exists", series_id);
        }

        Ok(())
    }

    /// Add a chunk reference to the index
    async fn add_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        time_range: TimeRange,
        location: ChunkLocation,
    ) -> Result<(), IndexError> {
        let script = self.scripts.add_chunk();
        let current_time = Utc::now().timestamp_millis();

        let chunk_metadata = RedisChunkMetadata {
            series_id: series_id.to_string(),
            path: location.path.clone(),
            start_time: time_range.start,
            end_time: time_range.end,
            point_count: 0,
            size_bytes: location.size.unwrap_or(0),
            compression: "gorilla".to_string(),
            status: "sealed".to_string(),
            created_at: current_time,
        };

        let metadata_json = serde_json::to_string(&chunk_metadata)
            .map_err(|e| IndexError::SerializationError(e.to_string()))?;

        // Clone chunk_id.0 for use in logging after the closure
        let chunk_id_str = chunk_id.0.clone();
        let chunk_key = Self::chunk_key(&chunk_id);

        let result: i32 = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let chunk_id_arg = chunk_id_str.clone();
                let metadata_json = metadata_json.clone();
                let chunk_key = chunk_key.clone();
                async move {
                    script
                        .key(Self::series_index_key(series_id))
                        .key(chunk_key)
                        .key(KEY_REGISTRY)
                        .key(Self::series_meta_key(series_id))
                        .arg(time_range.start)
                        .arg(chunk_id_arg)
                        .arg(series_id.to_string())
                        .arg(metadata_json)
                        .arg(current_time)
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        if result == 1 {
            debug!("Added chunk {} to series {}", chunk_id_str, series_id);
        } else {
            warn!("Chunk {} already exists", chunk_id_str);
        }

        Ok(())
    }

    /// Query chunks by time range
    async fn query_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        self.queries_served.fetch_add(1, Ordering::Relaxed);

        let script = self.scripts.find_chunks_in_range();
        let series_index = Self::series_index_key(series_id);

        // Get chunk IDs in range
        let chunk_ids: Vec<String> = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let series_index = series_index.clone();
                async move {
                    script
                        .key(series_index)
                        .arg(time_range.start)
                        .arg(time_range.end)
                        .arg(0) // No limit
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        // Fetch metadata for each chunk
        let mut references = Vec::with_capacity(chunk_ids.len());

        for chunk_id_str in chunk_ids {
            let chunk_id = ChunkId::from_string(&chunk_id_str);

            if let Some(metadata) = self.get_chunk_metadata(&chunk_id).await? {
                let status = match metadata.status.as_str() {
                    "active" => ChunkStatus::Active,
                    "sealed" => ChunkStatus::Sealed,
                    "compressed" => ChunkStatus::Compressed,
                    "archived" => ChunkStatus::Archived,
                    _ => ChunkStatus::Sealed,
                };

                references.push(ChunkReference {
                    chunk_id,
                    location: ChunkLocation {
                        engine_id: "local-disk-v1".to_string(),
                        path: metadata.path,
                        offset: None,
                        size: Some(metadata.size_bytes),
                    },
                    time_range: TimeRange {
                        start: metadata.start_time,
                        end: metadata.end_time,
                    },
                    status,
                });
            }
        }

        debug!(
            "Found {} chunks for series {} in range {:?}",
            references.len(),
            series_id,
            time_range
        );

        Ok(references)
    }

    /// Find series by metric name and tag filters
    async fn find_series(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, IndexError> {
        // Get all series from registry
        let all_series: Vec<String> = self
            .pool
            .execute(|mut conn| async move { conn.smembers(KEY_REGISTRY).await })
            .await?;

        let mut matching = Vec::new();

        for series_id_str in all_series {
            let series_id: SeriesId = series_id_str.parse().map_err(|_| {
                IndexError::ParseError(format!("Invalid series ID: {}", series_id_str))
            })?;

            let meta_key = Self::series_meta_key(series_id);

            // Get series metadata
            let meta: HashMap<String, String> = self
                .pool
                .execute(|mut conn| {
                    let meta_key = meta_key.clone();
                    async move { conn.hgetall(meta_key).await }
                })
                .await?;

            // Check metric name
            if let Some(stored_metric) = meta.get("metric_name") {
                if stored_metric != metric_name {
                    continue;
                }
            } else {
                continue;
            }

            // Check tag filter
            if let Some(tags_json) = meta.get("tags") {
                let tags: HashMap<String, String> =
                    serde_json::from_str(tags_json).unwrap_or_default();

                let matches = match tag_filter {
                    // All: match all series regardless of tags
                    TagFilter::All => true,
                    // Exact: series must have all specified tag key-value pairs
                    TagFilter::Exact(filter_tags) => {
                        filter_tags.iter().all(|(k, v)| tags.get(k) == Some(v))
                    }
                    // Pattern: wildcard pattern matching (basic implementation)
                    TagFilter::Pattern(pattern) => {
                        // Simple pattern matching: check if any tag value contains the pattern
                        tags.values().any(|v| v.contains(pattern))
                    }
                };

                if matches {
                    matching.push(series_id);
                }
            }
        }

        Ok(matching)
    }

    /// Update chunk status
    async fn update_chunk_status(
        &self,
        chunk_id: ChunkId,
        status: ChunkStatus,
    ) -> Result<(), IndexError> {
        let script = self.scripts.update_chunk_status();
        let current_time = Utc::now().timestamp_millis();

        let status_str = match status {
            ChunkStatus::Active => "active",
            ChunkStatus::Sealed => "sealed",
            ChunkStatus::Compressed => "compressed",
            ChunkStatus::Archived => "archived",
            ChunkStatus::Deleted => "deleted",
        };

        let result: i32 = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let chunk_key = Self::chunk_key(&chunk_id);
                async move {
                    script
                        .key(chunk_key)
                        .arg(status_str)
                        .arg(current_time)
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        if result == 0 {
            warn!("Chunk {} not found for status update", chunk_id.0);
        }

        Ok(())
    }

    /// Delete a series from the index
    async fn delete_series(&self, series_id: SeriesId) -> Result<(), IndexError> {
        let script = self.scripts.delete_series();

        let deleted: i32 = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                async move {
                    script
                        .key(KEY_REGISTRY)
                        .key(Self::series_index_key(series_id))
                        .key(Self::series_meta_key(series_id))
                        .arg(series_id.to_string())
                        .arg(KEY_CHUNKS_PREFIX)
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        // Invalidate cache
        {
            let mut cache = self.local_cache.write();
            cache.invalidate_series(series_id);
        }

        info!("Deleted series {} ({} chunks)", series_id, deleted);
        Ok(())
    }

    /// Get index statistics
    fn stats(&self) -> IndexStats {
        IndexStats {
            total_series: 0, // Would need to query Redis
            total_chunks: 0, // Would need to query Redis
            queries_served: self.queries_served.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }

    /// Rebuild index from storage
    async fn rebuild(&self) -> Result<(), IndexError> {
        // This would typically scan storage and rebuild the index
        // For now, we just clear the local cache
        {
            let mut cache = self.local_cache.write();
            cache.series_meta.clear();
        }
        info!("Index rebuild requested (cache cleared)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        assert_eq!(RedisTimeIndex::series_index_key(123), "ts:series:123:index");
        assert_eq!(RedisTimeIndex::series_meta_key(456), "ts:series:456:meta");
        assert_eq!(
            RedisTimeIndex::chunk_key(&ChunkId::from_string("abc-123")),
            "ts:chunks:abc-123"
        );
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
}
