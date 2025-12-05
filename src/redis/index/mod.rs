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
//! # Architecture
//!
//! This module is split into submodules for maintainability:
//!
//! - `cache` - Local TTL-based cache for series metadata
//! - `metadata` - Redis chunk metadata structures
//!
//! # Example
//!
//! ```rust,no_run
//! use kuba_tsdb::redis::{RedisConfig, RedisTimeIndex};
//! use kuba_tsdb::engine::traits::{TimeIndex, SeriesMetadata, ChunkStatus};
//! use kuba_tsdb::types::{ChunkId, TimeRange};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let index = RedisTimeIndex::new(RedisConfig::default()).await?;
//!
//! // Query chunks in a time range
//! let chunks = index.query_chunks(1, TimeRange::new(0, 1000)?).await?;
//! # Ok(())
//! # }
//! ```

mod cache;
mod metadata;

pub use cache::{CachedSeriesMeta, LocalCache};
pub use metadata::RedisChunkMetadata;

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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

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
///
/// # Features
///
/// - O(log N) time-range queries using ZRANGEBYSCORE
/// - Atomic operations via Lua scripts
/// - Local caching to reduce Redis round trips
/// - Pipeline batching to avoid N+1 queries
/// - Secondary indexes for metric/tag filtering
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

        debug!("Redis time index initialized");

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
    ///
    /// More efficient than adding chunks one at a time by using Redis pipelines.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to add chunks to
    /// * `chunks` - Vector of (chunk_id, time_range, location) tuples
    ///
    /// # Returns
    ///
    /// Number of chunks successfully added
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
                compression: "Kuba".to_string(),
                status: "sealed".to_string(),
                created_at: current_time,
                min_value: None,
                max_value: None,
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
    /// Retrieves metadata for a single chunk. For batch operations,
    /// prefer using pipelines in query_chunks to avoid N+1 query pattern.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - The chunk identifier
    ///
    /// # Returns
    ///
    /// Chunk metadata if found, None otherwise
    pub async fn get_chunk_metadata(
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
            },
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

    /// Get tags for multiple series in a single batch operation
    ///
    /// This is the core function for GROUP BY support. It efficiently fetches
    /// tag metadata for multiple series using Redis pipelines, enabling
    /// grouping of aggregation results by tag values.
    ///
    /// # Arguments
    ///
    /// * `series_ids` - List of series IDs to fetch tags for
    ///
    /// # Returns
    ///
    /// HashMap mapping series_id -> tags (HashMap<String, String>)
    ///
    /// # Performance
    ///
    /// Uses Redis pipelines with batching (100 per batch) to minimize
    /// round trips while avoiding overwhelming Redis with large requests.
    pub async fn get_series_tags_batch(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<HashMap<SeriesId, HashMap<String, String>>, IndexError> {
        if series_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<SeriesId, HashMap<String, String>> = HashMap::new();
        let mut cache_misses: Vec<SeriesId> = Vec::new();

        // First pass: check local cache
        {
            let cache = self.local_cache.read().await;
            for &series_id in series_ids {
                if let Some(meta) = cache.get_series_meta(series_id) {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    result.insert(series_id, meta.tags.clone());
                } else {
                    cache_misses.push(series_id);
                }
            }
        }

        // If all cache hits, return early
        if cache_misses.is_empty() {
            return Ok(result);
        }

        self.cache_misses
            .fetch_add(cache_misses.len() as u64, Ordering::Relaxed);

        // Batch fetch from Redis using pipelines
        const BATCH_SIZE: usize = 100;

        for batch in cache_misses.chunks(BATCH_SIZE) {
            let batch_ids: Vec<SeriesId> = batch.to_vec();

            let tags_results: Vec<Option<String>> = self
                .pool
                .execute(|mut conn| {
                    let batch = batch_ids.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for series_id in &batch {
                            let meta_key = Self::series_meta_key(*series_id);
                            pipe.hget(&meta_key, "tags");
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await
                .map_err(|e| {
                    IndexError::ConnectionError(format!("Pipeline fetch failed: {}", e))
                })?;

            // Process results and populate local cache
            let mut cache = self.local_cache.write().await;
            for (series_id, tags_opt) in batch_ids.iter().zip(tags_results) {
                let tags: HashMap<String, String> = tags_opt
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default();

                // Cache the metadata for future lookups (60 second TTL)
                let metadata = SeriesMetadata {
                    metric_name: String::new(), // Not available from this query
                    tags: tags.clone(),
                    created_at: 0, // Not available from this query
                    retention_days: None,
                };
                cache.set_series_meta(*series_id, metadata, 60_000);

                result.insert(*series_id, tags);
            }
        }

        debug!(
            "Fetched tags for {} series ({} cache hits, {} cache misses)",
            series_ids.len(),
            series_ids.len() - cache_misses.len(),
            cache_misses.len()
        );

        Ok(result)
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
        let mut cache_misses: Vec<(usize, String)> = Vec::new();

        // First pass: check local cache for metadata
        {
            let cache = self.local_cache.read().await;
            for (idx, series_id_str) in series_ids.iter().enumerate() {
                if let Ok(series_id) = series_id_str.parse::<SeriesId>() {
                    if let Some(meta) = cache.get_series_meta(series_id) {
                        // Cache hit - check pattern match
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        if meta.tags.values().any(|v| v.contains(pattern)) {
                            matching.push(series_id);
                        }
                    } else {
                        // Cache miss - need to fetch from Redis
                        cache_misses.push((idx, series_id_str.clone()));
                    }
                }
            }
        }

        // If all cache hits, return early
        if cache_misses.is_empty() {
            return Ok(matching);
        }

        self.cache_misses
            .fetch_add(cache_misses.len() as u64, Ordering::Relaxed);

        // Batch fetch metadata for cache misses using pipeline
        const BATCH_SIZE: usize = 100;

        for batch in cache_misses.chunks(BATCH_SIZE) {
            let batch_ids: Vec<String> = batch.iter().map(|(_, id)| id.clone()).collect();

            let metadata_results: Vec<Option<String>> = self
                .pool
                .execute(|mut conn| {
                    let batch = batch_ids.clone();
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
            for ((_, series_id_str), tags_opt) in batch.iter().zip(metadata_results) {
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
            compression: "Kuba".to_string(),
            status: "sealed".to_string(),
            created_at: current_time,
            min_value: None,
            max_value: None,
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

                    // ENH-003: Include statistics for cost estimation and zone map pruning
                    let mut chunk_ref = ChunkReference::new(
                        ChunkId::from_string_unchecked(chunk_id_str),
                        ChunkLocation {
                            engine_id: "local-disk-v1".to_string(),
                            path: metadata.path,
                            offset: None,
                            size: Some(metadata.size_bytes),
                        },
                        TimeRange {
                            start: metadata.start_time,
                            end: metadata.end_time,
                        },
                        status,
                    );

                    // Populate statistics from Redis metadata
                    chunk_ref.row_count = metadata.point_count as u32;
                    chunk_ref.size_bytes = metadata.size_bytes as u64;
                    chunk_ref.min_value = metadata.min_value;
                    chunk_ref.max_value = metadata.max_value;

                    references.push(chunk_ref);
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
            },

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
            },

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
            },
        }
    }

    /// Get tags for multiple series in a batch operation (GROUP BY support)
    ///
    /// Overrides the default trait implementation with an efficient
    /// Redis pipeline-based batch fetch.
    async fn get_series_tags_batch(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<HashMap<SeriesId, HashMap<String, String>>, IndexError> {
        // Delegate to the inherent implementation
        RedisTimeIndex::get_series_tags_batch(self, series_ids).await
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

        debug!("Deleted series {} ({} chunks)", series_id, deleted);
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
        debug!("Index rebuild requested (cache cleared)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Key Generation Tests
    // =========================================================================

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
    fn test_key_generation_various_series_ids() {
        // Test with various series_id values
        assert_eq!(RedisTimeIndex::series_index_key(0), "ts:series:0:index");
        assert_eq!(RedisTimeIndex::series_index_key(1), "ts:series:1:index");
        assert_eq!(
            RedisTimeIndex::series_index_key(u64::MAX as SeriesId),
            format!("ts:series:{}:index", u64::MAX)
        );
    }

    #[test]
    fn test_meta_key_generation_various_ids() {
        assert_eq!(RedisTimeIndex::series_meta_key(0), "ts:series:0:meta");
        assert_eq!(
            RedisTimeIndex::series_meta_key(999999),
            "ts:series:999999:meta"
        );
    }

    // =========================================================================
    // ChunkId Validation Tests
    // =========================================================================

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
    fn test_chunk_id_more_path_traversal() {
        // Additional path traversal attack vectors
        assert!(ChunkId::from_string("....//....//etc/passwd").is_err());
        assert!(ChunkId::from_string("..%2F..%2Fetc%2Fpasswd").is_err());
        assert!(ChunkId::from_string("..%5C..%5Cwindows%5Csystem32").is_err());
    }

    #[test]
    fn test_chunk_id_valid_uuids() {
        // Various valid UUID formats
        assert!(ChunkId::from_string("00000000-0000-0000-0000-000000000000").is_ok());
        assert!(ChunkId::from_string("ffffffff-ffff-ffff-ffff-ffffffffffff").is_ok());
        assert!(ChunkId::from_string("a1b2c3d4-e5f6-7890-abcd-ef1234567890").is_ok());
    }

    // =========================================================================
    // Key Constant Tests
    // =========================================================================

    #[test]
    fn test_key_constants() {
        assert_eq!(KEY_REGISTRY, "ts:registry");
        assert_eq!(KEY_SERIES_INDEX_PREFIX, "ts:series:");
        assert_eq!(KEY_SERIES_INDEX_SUFFIX, ":index");
        assert_eq!(KEY_SERIES_META_SUFFIX, ":meta");
        assert_eq!(KEY_CHUNKS_PREFIX, "ts:chunks:");
    }
}
