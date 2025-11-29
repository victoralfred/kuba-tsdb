//! Redis-based time index implementation
//!
//! Implements the `TimeIndex` trait using Redis Sorted Sets for
//! ultra-fast time-based chunk lookups.
//!
//! # Redis Key Schema
//!
//! ```text
//! ts:registry                           → SET of all series_ids
//! ts:series:{id}:index                  → ZSET(start_timestamp → chunk_id)
//! ts:series:{id}:meta                   → HASH {created_at, last_write, metric_name, tags...}
//! ts:chunks:{chunk_id}                  → HASH {series_id, path, timestamps, status...}
//! ts:metric:{metric_name}:series        → SET of series_ids with this metric (secondary index)
//! ts:tag:{key}:{value}:series           → SET of series_ids with this tag (secondary index)
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
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
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
    ///
    /// Note: This method is kept for individual chunk lookups. For batch operations,
    /// prefer using pipelines in query_chunks to avoid N+1 query pattern.
    #[allow(dead_code)]
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

    /// Parse series ID strings into SeriesId values
    ///
    /// Helper method for secondary index queries that return string IDs.
    fn parse_series_ids(&self, series_ids: &[String]) -> Result<Vec<SeriesId>, IndexError> {
        series_ids
            .iter()
            .map(|s| {
                s.parse()
                    .map_err(|_| IndexError::ParseError(format!("Invalid series ID: {}", s)))
            })
            .collect()
    }

    /// Filter series by tag pattern (requires client-side lookup)
    ///
    /// Pattern matching cannot use set intersection, so we fetch metadata
    /// for series matching the metric and filter client-side.
    /// Uses pipeline batching to avoid N+1 queries.
    async fn filter_by_pattern(
        &self,
        series_ids: &[String],
        pattern: &str,
    ) -> Result<Vec<SeriesId>, IndexError> {
        let mut matching = Vec::new();

        // Batch fetch metadata using pipeline to avoid N+1
        const BATCH_SIZE: usize = 100;

        for batch in series_ids.chunks(BATCH_SIZE) {
            let batch_owned: Vec<String> = batch.to_vec();

            let metadata_results: Vec<Option<String>> = self
                .pool
                .execute(|mut conn| {
                    let batch = batch_owned.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for series_id_str in &batch {
                            // Parse series_id for key generation
                            if let Ok(series_id) = series_id_str.parse::<SeriesId>() {
                                let meta_key = Self::series_meta_key(series_id);
                                pipe.hget(&meta_key, "tags");
                            }
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await
                .map_err(|e| {
                    IndexError::ConnectionError(format!("Pipeline fetch failed: {}", e))
                })?;

            // Process results
            for (series_id_str, tags_opt) in batch.iter().zip(metadata_results) {
                if let Some(tags_json) = tags_opt {
                    let tags: HashMap<String, String> =
                        serde_json::from_str(&tags_json).unwrap_or_default();

                    // Pattern match: check if any tag value contains the pattern
                    if tags.values().any(|v| v.contains(pattern)) {
                        if let Ok(series_id) = series_id_str.parse::<SeriesId>() {
                            matching.push(series_id);
                        }
                    }
                }
            }
        }

        Ok(matching)
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
            // Cache the metadata (async-safe)
            let mut cache = self.local_cache.write().await;
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
    ///
    /// Uses Redis pipelines to fetch all chunk metadata in a single round trip,
    /// avoiding the N+1 query problem. For large result sets (>500 chunks),
    /// fetches are batched to avoid overwhelming Redis.
    async fn query_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        self.queries_served.fetch_add(1, Ordering::Relaxed);

        let script = self.scripts.find_chunks_in_range();
        let series_index = Self::series_index_key(series_id);

        // Step 1: Get chunk IDs in range (single Redis call)
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

        if chunk_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: Build chunk keys for pipeline batch fetch
        // Use unchecked since these are internal IDs from Redis
        let chunk_keys: Vec<String> = chunk_ids
            .iter()
            .map(|id| Self::chunk_key(&ChunkId::from_string_unchecked(id)))
            .collect();

        // Step 3: Fetch all metadata in batches using pipelines (avoids N+1 problem)
        // Batch size of 500 prevents overwhelming Redis with very large queries
        const PIPELINE_BATCH_SIZE: usize = 500;
        let mut all_metadata: Vec<Option<String>> = Vec::with_capacity(chunk_ids.len());

        for batch_keys in chunk_keys.chunks(PIPELINE_BATCH_SIZE) {
            let batch_keys_owned: Vec<String> = batch_keys.to_vec();

            let batch_results: Vec<Option<String>> = self
                .pool
                .execute(|mut conn| {
                    let keys = batch_keys_owned.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for key in &keys {
                            pipe.hget(key, "metadata");
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await
                .map_err(|e| {
                    IndexError::ConnectionError(format!("Pipeline fetch failed: {}", e))
                })?;

            all_metadata.extend(batch_results);
        }

        // Step 4: Process results in memory (no more Redis calls needed)
        let mut references = Vec::with_capacity(chunk_ids.len());

        for (chunk_id_str, metadata_opt) in chunk_ids.iter().zip(all_metadata) {
            if let Some(json) = metadata_opt {
                // Parse metadata JSON, skip invalid entries
                if let Ok(metadata) = serde_json::from_str::<RedisChunkMetadata>(&json) {
                    let status = match metadata.status.as_str() {
                        "active" => ChunkStatus::Active,
                        "sealed" => ChunkStatus::Sealed,
                        "compressed" => ChunkStatus::Compressed,
                        "archived" => ChunkStatus::Archived,
                        _ => ChunkStatus::Sealed,
                    };

                    references.push(ChunkReference {
                        // Use unchecked since these are internal IDs from Redis
                        chunk_id: ChunkId::from_string_unchecked(chunk_id_str),
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
                } else {
                    warn!(
                        "Failed to parse metadata for chunk {}, skipping",
                        chunk_id_str
                    );
                }
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

    /// Find series by metric name and tag filters using secondary indexes
    ///
    /// Uses Redis secondary indexes for efficient lookups:
    /// - `ts:metric:{metric_name}:series` - SET of series with this metric
    /// - `ts:tag:{key}:{value}:series` - SET of series with this tag k/v pair
    ///
    /// # Performance
    ///
    /// - Before (full scan): O(n) where n = total series count
    /// - After (secondary indexes):
    ///   - TagFilter::All: O(m) where m = series with metric
    ///   - TagFilter::Exact: O(min set size) using SINTER
    ///   - TagFilter::Pattern: O(m) with client-side filtering
    async fn find_series(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, IndexError> {
        // Use metric secondary index instead of scanning all series
        let metric_index_key = format!("ts:metric:{}:series", metric_name);

        match tag_filter {
            // Get all series for this metric - O(m) where m = series with metric
            TagFilter::All => {
                let series_ids: Vec<String> = self
                    .pool
                    .execute(|mut conn| {
                        let key = metric_index_key.clone();
                        async move { conn.smembers(key).await }
                    })
                    .await?;

                self.parse_series_ids(&series_ids)
            }

            // Intersect metric index with all tag indexes - O(min set size)
            TagFilter::Exact(tags) => {
                let mut keys = vec![metric_index_key];
                for (k, v) in tags {
                    keys.push(format!("ts:tag:{}:{}:series", k, v));
                }

                // Use SINTER to find series matching all criteria
                let series_ids: Vec<String> = self
                    .pool
                    .execute(|mut conn| {
                        let keys = keys.clone();
                        async move {
                            // SINTER requires &[&str] so we need to convert
                            redis::cmd("SINTER").arg(&keys).query_async(&mut conn).await
                        }
                    })
                    .await?;

                self.parse_series_ids(&series_ids)
            }

            // Get metric series, then filter client-side by pattern
            // Still much better than scanning ALL series
            TagFilter::Pattern(pattern) => {
                let series_ids: Vec<String> = self
                    .pool
                    .execute(|mut conn| {
                        let key = metric_index_key.clone();
                        async move { conn.smembers(key).await }
                    })
                    .await?;

                // Filter by pattern client-side (requires metadata lookup)
                self.filter_by_pattern(&series_ids, pattern).await
            }
        }
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

        // Invalidate cache (async-safe)
        {
            let mut cache = self.local_cache.write().await;
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
        // For now, we just clear the local cache (async-safe)
        {
            let mut cache = self.local_cache.write().await;
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
        // Use valid UUID for chunk_id
        let chunk_id = ChunkId::from_string("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            RedisTimeIndex::chunk_key(&chunk_id),
            "ts:chunks:550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_chunk_id_validation() {
        // Valid UUIDs should pass
        assert!(ChunkId::from_string("550e8400-e29b-41d4-a716-446655440000").is_ok());

        // Path traversal should fail
        assert!(ChunkId::from_string("../malicious").is_err());
        assert!(ChunkId::from_string("..\\malicious").is_err());
        assert!(ChunkId::from_string("/etc/passwd").is_err());

        // Invalid UUIDs should fail
        assert!(ChunkId::from_string("not-a-uuid").is_err());
        assert!(ChunkId::from_string("abc-123").is_err());
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
