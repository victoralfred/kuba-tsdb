//! Database builder with pluggable engines
//!
//! This module provides the main TimeSeriesDB type that integrates all
//! components into a cohesive database system.

use super::traits::{
    ChunkStatus, Compressor, IndexConfig, SeriesMetadata, StorageConfig, StorageEngine, TimeIndex,
};
use crate::error::{Error, Result};
use crate::storage::active_chunk::{ActiveChunk, SealConfig};
use crate::types::{ChunkId, DataPoint, SeriesId, TagFilter, TimeRange};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Database configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Data directory for storage
    pub data_dir: PathBuf,
    /// Redis connection URL
    pub redis_url: Option<String>,
    /// Maximum chunk size in bytes
    pub max_chunk_size: usize,
    /// Retention period in days
    pub retention_days: Option<u32>,
    /// Custom options
    pub custom_options: HashMap<String, String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/tsdb"),
            redis_url: None,
            max_chunk_size: 1024 * 1024, // 1MB
            retention_days: None,
            custom_options: HashMap::new(),
        }
    }
}

impl DatabaseConfig {
    /// Convert to storage config
    pub fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            base_path: Some(self.data_dir.to_string_lossy().to_string()),
            max_chunk_size: self.max_chunk_size,
            compression_enabled: true,
            retention_days: self.retention_days,
            custom_options: self.custom_options.clone(),
        }
    }

    /// Convert to index config
    pub fn index_config(&self) -> IndexConfig {
        IndexConfig {
            connection_string: self.redis_url.clone(),
            cache_size_mb: 128,
            max_series: 1_000_000,
            custom_options: self.custom_options.clone(),
        }
    }
}

/// Builder for configuring the time-series database with custom engines
pub struct TimeSeriesDBBuilder {
    compressor: Option<Arc<dyn Compressor + Send + Sync>>,
    storage: Option<Arc<dyn StorageEngine + Send + Sync>>,
    index: Option<Arc<dyn TimeIndex + Send + Sync>>,
    config: DatabaseConfig,
    buffer_config: Option<BufferConfig>,
}

impl TimeSeriesDBBuilder {
    /// Create a new database builder
    pub fn new() -> Self {
        Self {
            compressor: None,
            storage: None,
            index: None,
            config: DatabaseConfig::default(),
            buffer_config: None,
        }
    }

    /// Set a custom compressor implementation
    pub fn with_compressor<C>(mut self, compressor: C) -> Self
    where
        C: Compressor + 'static,
    {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Set a custom storage engine implementation
    pub fn with_storage<S>(mut self, storage: S) -> Self
    where
        S: StorageEngine + 'static,
    {
        self.storage = Some(Arc::new(storage));
        self
    }

    /// Set a custom storage engine implementation from an existing Arc
    ///
    /// Use this when you need to retain a reference to the storage engine
    /// for direct operations (e.g., persisting series metadata).
    pub fn with_storage_arc(mut self, storage: Arc<dyn StorageEngine + Send + Sync>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set a custom time index implementation
    pub fn with_index<I>(mut self, index: I) -> Self
    where
        I: TimeIndex + 'static,
    {
        self.index = Some(Arc::new(index));
        self
    }

    /// Set database configuration
    pub fn with_config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    /// Set write buffer configuration
    ///
    /// Controls how writes are batched before being compressed and stored.
    /// Higher `max_points` values improve compression but increase memory usage.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let buffer_config = BufferConfig {
    ///     max_points: 2_000,       // Batch more points for better compression
    ///     max_duration_ms: 120_000, // 2 minutes max
    ///     max_size_bytes: 2 * 1024 * 1024, // 2MB
    ///     initial_capacity: 2_000,
    /// };
    /// let db = TimeSeriesDBBuilder::new()
    ///     .with_buffer_config(buffer_config)
    ///     // ... other configuration
    ///     .build()
    ///     .await?;
    /// ```
    pub fn with_buffer_config(mut self, buffer_config: BufferConfig) -> Self {
        self.buffer_config = Some(buffer_config);
        self
    }

    /// Build the database with configured engines
    pub async fn build(self) -> Result<TimeSeriesDB> {
        // For now, we'll require all engines to be provided
        // Later we'll add default implementations

        let compressor = self
            .compressor
            .ok_or_else(|| Error::Configuration("No compressor configured".to_string()))?;

        let storage = self
            .storage
            .ok_or_else(|| Error::Configuration("No storage engine configured".to_string()))?;

        let index = self
            .index
            .ok_or_else(|| Error::Configuration("No index configured".to_string()))?;

        // Initialize engines
        storage
            .initialize(self.config.storage_config())
            .await
            .map_err(Error::Storage)?;

        index
            .initialize(self.config.index_config())
            .await
            .map_err(Error::Index)?;

        Ok(TimeSeriesDB {
            compressor,
            storage,
            index,
            config: self.config,
            active_chunks: Arc::new(RwLock::new(HashMap::new())),
            buffer_config: self.buffer_config.unwrap_or_default(),
        })
    }
}

impl Default for TimeSeriesDBBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for write buffering
#[derive(Clone, Debug)]
pub struct BufferConfig {
    /// Maximum points per chunk before sealing
    pub max_points: usize,
    /// Maximum duration in ms before sealing
    pub max_duration_ms: i64,
    /// Maximum size in bytes before sealing
    pub max_size_bytes: usize,
    /// Initial capacity for active chunks
    pub initial_capacity: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_points: 1_000,           // 1K points per chunk (good for AHPAC)
            max_duration_ms: 60_000,     // 1 minute
            max_size_bytes: 1024 * 1024, // 1MB
            initial_capacity: 1_000,
        }
    }
}

impl BufferConfig {
    /// Convert to SealConfig
    fn to_seal_config(&self) -> SealConfig {
        SealConfig {
            max_points: self.max_points,
            max_duration_ms: self.max_duration_ms,
            max_size_bytes: self.max_size_bytes,
        }
    }
}

/// Main database instance with pluggable engines
pub struct TimeSeriesDB {
    compressor: Arc<dyn Compressor + Send + Sync>,
    storage: Arc<dyn StorageEngine + Send + Sync>,
    index: Arc<dyn TimeIndex + Send + Sync>,
    config: DatabaseConfig,
    /// Per-series active chunks for buffering writes
    active_chunks: Arc<RwLock<HashMap<SeriesId, Arc<ActiveChunk>>>>,
    /// Buffer configuration
    buffer_config: BufferConfig,
}

impl TimeSeriesDB {
    /// Get reference to the compressor
    pub fn compressor(&self) -> &Arc<dyn Compressor + Send + Sync> {
        &self.compressor
    }

    /// Get reference to the storage engine
    pub fn storage(&self) -> &Arc<dyn StorageEngine + Send + Sync> {
        &self.storage
    }

    /// Get reference to the time index
    pub fn index(&self) -> &Arc<dyn TimeIndex + Send + Sync> {
        &self.index
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Replace compressor at runtime
    pub fn set_compressor(&mut self, compressor: Arc<dyn Compressor + Send + Sync>) {
        self.compressor = compressor;
    }

    /// Replace storage engine at runtime (dangerous - ensure data migration)
    pub async fn set_storage(
        &mut self,
        storage: Arc<dyn StorageEngine + Send + Sync>,
    ) -> Result<()> {
        storage
            .initialize(self.config.storage_config())
            .await
            .map_err(Error::Storage)?;
        self.storage = storage;
        Ok(())
    }

    /// Replace index at runtime (requires rebuild)
    pub async fn set_index(&mut self, index: Arc<dyn TimeIndex + Send + Sync>) -> Result<()> {
        index
            .initialize(self.config.index_config())
            .await
            .map_err(Error::Index)?;
        index.rebuild().await.map_err(Error::Index)?;
        self.index = index;
        Ok(())
    }

    // =========================================================================
    // Data Operations
    // =========================================================================

    /// Write data points to the database with buffering
    ///
    /// Points are buffered in active chunks until a seal threshold is reached.
    /// This enables better compression ratios with AHPAC by batching points.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series identifier for these points
    /// * `points` - Data points to write (must be sorted by timestamp)
    ///
    /// # Returns
    ///
    /// The chunk ID (may be a placeholder if points are still buffered)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let points = vec![
    ///     DataPoint::new(1, 1000, 42.5),
    ///     DataPoint::new(1, 2000, 43.0),
    /// ];
    /// let chunk_id = db.write(1, points).await?;
    /// ```
    pub async fn write(&self, series_id: SeriesId, points: Vec<DataPoint>) -> Result<ChunkId> {
        if points.is_empty() {
            return Err(Error::General("Cannot write empty points".to_string()));
        }

        // Validate points are sorted
        for i in 1..points.len() {
            if points[i].timestamp <= points[i - 1].timestamp {
                return Err(Error::General(
                    "Points must be sorted by timestamp".to_string(),
                ));
            }
        }

        debug!(
            series_id = series_id,
            points = points.len(),
            "Writing points to buffer"
        );

        // Get or create active chunk for this series
        let active_chunk = {
            let chunks = self.active_chunks.read().await;
            chunks.get(&series_id).cloned()
        };

        let active_chunk = match active_chunk {
            Some(chunk) => chunk,
            None => {
                // Create new active chunk
                let seal_config = self.buffer_config.to_seal_config();
                let new_chunk = Arc::new(ActiveChunk::new(
                    series_id,
                    self.buffer_config.initial_capacity,
                    seal_config,
                ));
                let mut chunks = self.active_chunks.write().await;
                chunks.insert(series_id, Arc::clone(&new_chunk));
                new_chunk
            }
        };

        // Append points to active chunk
        for point in &points {
            if let Err(e) = active_chunk.append(*point) {
                warn!(series_id = series_id, error = %e, "Failed to append point");
                // If append fails (e.g., duplicate), continue with other points
            }
        }

        // Check if we should seal
        if active_chunk.should_seal() {
            return self.seal_active_chunk(series_id).await;
        }

        // Return a placeholder chunk ID for buffered writes
        // The actual chunk ID will be created when sealed
        Ok(ChunkId::new())
    }

    /// Seal the active chunk for a series and write to storage
    async fn seal_active_chunk(&self, series_id: SeriesId) -> Result<ChunkId> {
        // Remove and get the active chunk
        let active_chunk = {
            let mut chunks = self.active_chunks.write().await;
            chunks.remove(&series_id)
        };

        let active_chunk = match active_chunk {
            Some(chunk) => chunk,
            None => return Err(Error::General("No active chunk to seal".to_string())),
        };

        // Get all points from the active chunk
        let points = active_chunk
            .take_points()
            .map_err(|e| Error::General(format!("Failed to take points: {}", e)))?;

        if points.is_empty() {
            return Err(Error::General("Cannot seal empty chunk".to_string()));
        }

        info!(
            series_id = series_id,
            points = points.len(),
            "Sealing active chunk"
        );

        // Compress the data
        let compressed = self
            .compressor
            .compress(&points)
            .await
            .map_err(Error::Compression)?;

        // Generate chunk ID
        let chunk_id = ChunkId::new();

        // Write to storage
        let location = self
            .storage
            .write_chunk(series_id, chunk_id.clone(), &compressed)
            .await
            .map_err(Error::Storage)?;

        // Add to index
        let time_range = TimeRange::new_unchecked(
            compressed.metadata.start_timestamp,
            compressed.metadata.end_timestamp,
        );

        self.index
            .add_chunk(series_id, chunk_id.clone(), time_range, location)
            .await
            .map_err(Error::Index)?;

        debug!(
            series_id = series_id,
            chunk_id = %chunk_id,
            points = points.len(),
            compressed_size = compressed.compressed_size,
            "Chunk sealed and written successfully"
        );

        Ok(chunk_id)
    }

    /// Flush all active chunks to storage
    ///
    /// Call this during graceful shutdown to ensure no data is lost.
    pub async fn flush_all(&self) -> Result<Vec<ChunkId>> {
        let series_ids: Vec<SeriesId> = {
            let chunks = self.active_chunks.read().await;
            chunks.keys().cloned().collect()
        };

        let mut chunk_ids = Vec::new();
        for series_id in series_ids {
            match self.seal_active_chunk(series_id).await {
                Ok(chunk_id) => chunk_ids.push(chunk_id),
                Err(e) => warn!(series_id = series_id, error = %e, "Failed to flush series"),
            }
        }

        info!(
            chunks_flushed = chunk_ids.len(),
            "Flushed all active chunks"
        );
        Ok(chunk_ids)
    }

    /// Query data points from the database
    ///
    /// Retrieves data points for a series within a time range.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to query
    /// * `time_range` - Time range to query
    ///
    /// # Returns
    ///
    /// Vector of data points sorted by timestamp
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let range = TimeRange::new(0, 3600000)?;
    /// let points = db.query(1, range).await?;
    /// ```
    pub async fn query(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<DataPoint>> {
        debug!(
            series_id = series_id,
            start = time_range.start,
            end = time_range.end,
            "Querying database"
        );

        let mut all_points = Vec::new();

        // First, check active chunks for unbuffered data
        {
            let chunks = self.active_chunks.read().await;
            if let Some(active_chunk) = chunks.get(&series_id) {
                // Read points from active chunk without consuming them
                if let Ok(active_points) =
                    active_chunk.read_points_in_range(time_range.start, time_range.end)
                {
                    all_points.extend(active_points);
                }
            }
        }

        // Find matching chunks from index (sealed/persisted chunks)
        let chunk_refs = self
            .index
            .query_chunks(series_id, time_range)
            .await
            .map_err(Error::Index)?;

        // Read and decompress each sealed chunk
        for chunk_ref in chunk_refs {
            // Skip deleted/archived chunks
            if matches!(
                chunk_ref.status,
                ChunkStatus::Deleted | ChunkStatus::Archived
            ) {
                continue;
            }

            // Read chunk from storage
            let compressed = self
                .storage
                .read_chunk(&chunk_ref.location)
                .await
                .map_err(Error::Storage)?;

            // Decompress
            let points = self
                .compressor
                .decompress(&compressed)
                .await
                .map_err(Error::Compression)?;

            // Filter by time range and collect
            for point in points {
                if point.timestamp >= time_range.start && point.timestamp <= time_range.end {
                    all_points.push(point);
                }
            }
        }

        // Sort by timestamp (in case chunks overlap or active chunk data interleaves)
        all_points.sort_by_key(|p| p.timestamp);

        // Deduplicate points with same timestamp (active chunk might have duplicates with sealed)
        all_points.dedup_by_key(|p| p.timestamp);

        debug!(
            series_id = series_id,
            points = all_points.len(),
            "Query returned points"
        );

        Ok(all_points)
    }

    /// Query the N most recent data points for a series (ENH-001)
    ///
    /// This is an optimized query method that scans chunks in reverse
    /// chronological order (newest first) and stops early once enough
    /// points are collected. For large series, this is much more efficient
    /// than querying all data and taking the last N points.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to query
    /// * `count` - Maximum number of points to return
    ///
    /// # Returns
    ///
    /// A vector of the most recent `count` data points, sorted by timestamp
    /// (oldest to newest within the returned set).
    ///
    /// # Performance
    ///
    /// - **Best case:** O(1) when the most recent chunk contains >= `count` points
    /// - **Worst case:** O(n) when all chunks must be scanned (rare for LATEST queries)
    /// - **Typical:** O(k) where k is the number of chunks needed to find `count` points
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get the 10 most recent points
    /// let latest = db.query_latest(series_id, 10).await?;
    /// ```
    pub async fn query_latest(&self, series_id: SeriesId, count: usize) -> Result<Vec<DataPoint>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        debug!(
            series_id = series_id,
            count = count,
            "Querying latest points"
        );

        let mut collected_points: Vec<DataPoint> = Vec::with_capacity(count);

        // First, check active chunks for the most recent unbuffered data
        {
            let chunks = self.active_chunks.read().await;
            if let Some(active_chunk) = chunks.get(&series_id) {
                if let Ok(active_points) = active_chunk.read_points() {
                    // Active chunk points are already sorted by timestamp
                    collected_points.extend(active_points);
                }
            }
        }

        // Get all chunks for this series (no time range filter - we want all)
        let now = chrono::Utc::now().timestamp_millis();
        let full_range = TimeRange::new_unchecked(0, now);

        let mut chunk_refs = self
            .index
            .query_chunks(series_id, full_range)
            .await
            .map_err(Error::Index)?;

        // ENH-001: Sort chunks by end_timestamp descending (newest first)
        // This allows us to find the latest points with minimal chunk reads
        chunk_refs.sort_by(|a, b| b.time_range.end.cmp(&a.time_range.end));

        let mut chunks_read = 0;

        // Read chunks from newest to oldest, stopping when we have enough points
        for chunk_ref in chunk_refs {
            // Skip deleted/archived chunks
            if matches!(
                chunk_ref.status,
                ChunkStatus::Deleted | ChunkStatus::Archived
            ) {
                continue;
            }

            // Read and decompress the chunk
            let compressed = self
                .storage
                .read_chunk(&chunk_ref.location)
                .await
                .map_err(Error::Storage)?;

            let mut points = self
                .compressor
                .decompress(&compressed)
                .await
                .map_err(Error::Compression)?;

            chunks_read += 1;

            // Sort points within chunk by timestamp descending (newest first)
            points.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

            // Collect points until we have enough
            for point in points {
                collected_points.push(point);
                if collected_points.len() >= count {
                    break;
                }
            }

            // Early termination: stop reading chunks if we have enough points
            if collected_points.len() >= count {
                debug!(
                    series_id = series_id,
                    chunks_read = chunks_read,
                    "Early termination - found enough points"
                );
                break;
            }
        }

        // Reverse to get chronological order (oldest to newest)
        collected_points.reverse();

        // Take only the latest `count` points if we collected more
        // This can happen when the last chunk had more points than needed
        if collected_points.len() > count {
            // Skip older points, keeping only the most recent `count`
            let skip = collected_points.len() - count;
            collected_points = collected_points.into_iter().skip(skip).collect();
        }

        debug!(
            series_id = series_id,
            points = collected_points.len(),
            chunks_read = chunks_read,
            "Query latest returned points"
        );

        Ok(collected_points)
    }

    /// Register a new series with metadata
    ///
    /// # Arguments
    ///
    /// * `series_id` - Unique identifier for the series
    /// * `metric_name` - Name of the metric (e.g., "cpu.usage")
    /// * `tags` - Key-value tags for the series
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// db.register_series(
    ///     1,
    ///     "cpu.usage",
    ///     HashMap::from([("host".to_string(), "server1".to_string())]),
    /// ).await?;
    /// ```
    pub async fn register_series(
        &self,
        series_id: SeriesId,
        metric_name: &str,
        tags: HashMap<String, String>,
    ) -> Result<()> {
        let metadata = SeriesMetadata {
            metric_name: metric_name.to_string(),
            tags,
            created_at: chrono::Utc::now().timestamp_millis(),
            retention_days: self.config.retention_days,
        };

        self.index
            .register_series(series_id, metadata)
            .await
            .map_err(Error::Index)?;

        debug!(
            series_id = series_id,
            metric_name = metric_name,
            "Series registered"
        );

        Ok(())
    }

    /// Find series by metric name and tag filters
    ///
    /// # Arguments
    ///
    /// * `metric_name` - Metric name to search for
    /// * `tag_filter` - Tag filter to apply
    ///
    /// # Returns
    ///
    /// Vector of matching series IDs
    pub async fn find_series(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>> {
        self.index
            .find_series(metric_name, tag_filter)
            .await
            .map_err(Error::Index)
    }

    /// Get tags for multiple series in batch
    ///
    /// Efficiently fetches tag metadata for multiple series IDs.
    /// Used for GROUP BY support and displaying series labels in charts.
    ///
    /// # Arguments
    ///
    /// * `series_ids` - List of series IDs to fetch tags for
    ///
    /// # Returns
    ///
    /// HashMap mapping series_id -> tags (HashMap<String, String>)
    pub async fn get_series_tags_batch(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<HashMap<SeriesId, HashMap<String, String>>> {
        self.index
            .get_series_tags_batch(series_ids)
            .await
            .map_err(Error::Index)
    }

    /// Delete a series and all its data
    ///
    /// # Warning
    ///
    /// This permanently deletes all data for the series.
    pub async fn delete_series(&self, series_id: SeriesId) -> Result<()> {
        // Get all chunks for the series
        let chunks = self
            .storage
            .list_chunks(series_id, None)
            .await
            .map_err(Error::Storage)?;

        // Delete each chunk
        for chunk in chunks {
            if let Err(e) = self.storage.delete_chunk(&chunk.location).await {
                warn!(
                    series_id = series_id,
                    chunk_id = %chunk.chunk_id,
                    error = %e,
                    "Failed to delete chunk"
                );
            }
        }

        // Remove from index
        self.index
            .delete_series(series_id)
            .await
            .map_err(Error::Index)?;

        debug!(series_id = series_id, "Series deleted");

        Ok(())
    }

    /// Perform maintenance on the database
    ///
    /// This runs background maintenance tasks on storage.
    pub async fn maintenance(&self) -> Result<()> {
        let report = self.storage.maintenance().await.map_err(Error::Storage)?;

        debug!(
            chunks_deleted = report.chunks_deleted,
            bytes_freed = report.bytes_freed,
            "Maintenance completed"
        );

        Ok(())
    }

    /// Get database statistics
    ///
    /// Returns comprehensive statistics including storage, compression,
    /// and index cache metrics for monitoring and debugging.
    pub fn stats(&self) -> DatabaseStats {
        let storage_stats = self.storage.stats();
        let index_stats = self.index.stats();

        // Use storage-based compression ratio (from actual stored data)
        // This is more accurate than compressor stats which only track session operations
        let compression_ratio = storage_stats.compression_ratio();

        DatabaseStats {
            total_chunks: storage_stats.total_chunks,
            total_bytes: storage_stats.total_bytes,
            total_series: index_stats.total_series,
            write_ops: storage_stats.write_ops,
            read_ops: storage_stats.read_ops,
            compression_ratio,
            // Index cache statistics for cache-first lookup optimization
            index_cache_hits: index_stats.cache_hits,
            index_cache_misses: index_stats.cache_misses,
            index_queries_served: index_stats.queries_served,
        }
    }

    /// Rebuild the time index from storage
    ///
    /// This method scans the storage engine for all chunks and adds them to the
    /// time index. This is useful for restoring the index after a server restart
    /// when using an in-memory index.
    ///
    /// # Arguments
    ///
    /// * `series_ids` - List of series IDs to rebuild index for
    ///
    /// # Returns
    ///
    /// Number of chunks added to the index
    pub async fn rebuild_index_for_series(&self, series_ids: &[SeriesId]) -> Result<usize> {
        let mut total_chunks = 0;

        for &series_id in series_ids {
            // Get all chunks for this series from storage
            let chunks = self
                .storage
                .list_chunks(series_id, None)
                .await
                .map_err(Error::Storage)?;

            // Add each chunk to the index
            for chunk_meta in chunks {
                self.index
                    .add_chunk(
                        series_id,
                        chunk_meta.chunk_id,
                        chunk_meta.time_range,
                        chunk_meta.location,
                    )
                    .await
                    .map_err(Error::Index)?;
                total_chunks += 1;
            }
        }

        debug!(
            series_count = series_ids.len(),
            chunks_indexed = total_chunks,
            "Index rebuilt from storage"
        );

        Ok(total_chunks)
    }
}

/// Database statistics
#[derive(Debug, Clone, Default)]
pub struct DatabaseStats {
    /// Total number of chunks stored
    pub total_chunks: u64,
    /// Total bytes stored
    pub total_bytes: u64,
    /// Total number of series
    pub total_series: u64,
    /// Number of write operations
    pub write_ops: u64,
    /// Number of read operations
    pub read_ops: u64,
    /// Average compression ratio
    pub compression_ratio: f64,
    /// Index cache hits (for cache-first lookup optimization)
    pub index_cache_hits: u64,
    /// Index cache misses
    pub index_cache_misses: u64,
    /// Total index queries served
    pub index_queries_served: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.max_chunk_size, 1024 * 1024);
        assert!(config.redis_url.is_none());
    }

    #[test]
    fn test_builder_creation() {
        let builder = TimeSeriesDBBuilder::new();
        assert!(builder.compressor.is_none());
        assert!(builder.storage.is_none());
        assert!(builder.index.is_none());
    }
}
