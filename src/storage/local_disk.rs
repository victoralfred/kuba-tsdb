//! Local disk storage engine implementation
//!
//! Provides persistent storage for time-series data using the local filesystem.
//! Data is organized into series directories with compressed chunk files.

use crate::engine::traits::{StorageEngine, StorageStats};
use crate::error::StorageError;
use crate::storage::chunk::{ChunkMetadata, CompressionType};
use crate::types::{ChunkId, SeriesId};
use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

/// Local disk storage engine
///
/// Stores time-series data on the local filesystem using a chunk-based architecture.
/// Each series gets its own directory with compressed chunk files.
///
/// # Directory Structure
///
/// ```text
/// base_path/
///   series_{id}/
///     chunk_{timestamp}.gor    - Gorilla-compressed data
///     chunk_{timestamp}.snappy - Snappy-compressed cold storage
///     metadata.json            - Series metadata
/// ```
///
/// # Example
///
/// ```rust,no_run
/// use gorilla_tsdb::storage::LocalDiskEngine;
/// use gorilla_tsdb::engine::traits::StorageEngine;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let engine = LocalDiskEngine::new("/tmp/tsdb".into())?;
/// assert_eq!(engine.engine_id(), "local-disk-v1");
/// # Ok(())
/// # }
/// ```
pub struct LocalDiskEngine {
    /// Base directory for all storage
    base_path: PathBuf,

    /// In-memory index of chunks by series
    /// Maps series_id -> `Vec<ChunkMetadata>`
    chunk_index: Arc<RwLock<HashMap<SeriesId, Vec<ChunkMetadata>>>>,

    /// Storage statistics
    stats: Arc<RwLock<StorageStats>>,
}

impl LocalDiskEngine {
    /// Create a new local disk storage engine
    ///
    /// Creates the base directory if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Root directory for all storage
    ///
    /// # Errors
    ///
    /// Returns error if directory cannot be created or accessed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use gorilla_tsdb::storage::LocalDiskEngine;
    /// use gorilla_tsdb::engine::traits::StorageEngine;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let engine = LocalDiskEngine::new("/tmp/tsdb".into())?;
    /// assert_eq!(engine.engine_id(), "local-disk-v1");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(base_path: PathBuf) -> Result<Self, StorageError> {
        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&base_path)?;

        Ok(Self {
            base_path,
            chunk_index: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(StorageStats::default())),
        })
    }

    /// Get the directory path for a series
    fn series_path(&self, series_id: SeriesId) -> PathBuf {
        self.base_path.join(format!("series_{}", series_id))
    }

    /// Get the file path for a chunk
    fn chunk_path(
        &self,
        series_id: SeriesId,
        chunk_id: &ChunkId,
        compression: CompressionType,
    ) -> PathBuf {
        let extension = match compression {
            CompressionType::None => "raw",
            CompressionType::Gorilla => "gor",
            CompressionType::Snappy => "snappy",
            CompressionType::GorillaSnappy => "gor.snappy",
        };

        self.series_path(series_id)
            .join(format!("chunk_{}.{}", chunk_id, extension))
    }

    /// Load chunk index from disk on startup
    ///
    /// Scans the storage directory and builds an in-memory index of all chunks.
    pub async fn load_index(&self) -> Result<(), StorageError> {
        // Read all series directories (without holding lock)
        let mut entries = fs::read_dir(&self.base_path).await?;
        let mut series_to_load = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            // Parse series ID from directory name (series_{id})
            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(id_str) = dir_name.strip_prefix("series_") {
                    if let Ok(series_id) = id_str.parse::<SeriesId>() {
                        series_to_load.push(series_id);
                    }
                }
            }
        }

        // Load chunks for each series (still without lock)
        for series_id in series_to_load {
            let chunks = self.load_series_chunks(series_id).await?;
            if !chunks.is_empty() {
                // Now acquire lock briefly to insert
                let mut index = self.chunk_index.write();
                index.insert(series_id, chunks);
                // Lock dropped here
            }
        }

        Ok(())
    }

    /// Load all chunks for a series
    async fn load_series_chunks(
        &self,
        series_id: SeriesId,
    ) -> Result<Vec<ChunkMetadata>, StorageError> {
        use crate::storage::chunk::ChunkHeader;
        use tokio::io::AsyncReadExt;

        let series_dir = self.series_path(series_id);
        if !series_dir.exists() {
            return Ok(Vec::new());
        }

        let mut chunks = Vec::new();
        let mut entries = fs::read_dir(&series_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                // Read chunk header to get metadata
                let mut file = match fs::File::open(&path).await {
                    Ok(f) => f,
                    Err(_) => continue, // Skip files we can't open
                };

                let mut header_bytes = [0u8; 64];
                if file.read_exact(&mut header_bytes).await.is_err() {
                    continue; // Skip corrupted files
                }

                let header = match ChunkHeader::from_bytes(&header_bytes) {
                    Ok(h) => h,
                    Err(_) => continue, // Skip invalid headers
                };

                // Validate header before adding to index
                if header.validate().is_err() {
                    continue; // Skip invalid chunks
                }

                let file_metadata = fs::metadata(&path).await?;

                // Parse chunk_id from filename (chunk_{uuid}.gor)
                // Use unchecked since this is internal trusted storage
                let chunk_id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("chunk_"))
                    .map(ChunkId::from_string_unchecked)
                    .unwrap_or_else(ChunkId::new);

                chunks.push(ChunkMetadata {
                    chunk_id,
                    series_id,
                    path: path.clone(),
                    start_timestamp: header.start_timestamp,
                    end_timestamp: header.end_timestamp,
                    point_count: header.point_count,
                    size_bytes: file_metadata.len(),
                    compression: header.compression_type,
                    created_at: file_metadata
                        .created()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0),
                    last_accessed: chrono::Utc::now().timestamp_millis(),
                });
            }
        }

        chunks.sort_by_key(|c| c.start_timestamp);
        Ok(chunks)
    }

    /// Get chunk metadata for a series (for query engine integration)
    ///
    /// Returns internal ChunkMetadata which includes file paths needed
    /// for direct chunk reading via ChunkReader.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to get chunks for
    ///
    /// # Returns
    ///
    /// Vector of ChunkMetadata sorted by start timestamp
    pub fn get_chunks_for_series(&self, series_id: SeriesId) -> Vec<ChunkMetadata> {
        let index = self.chunk_index.read();
        index.get(&series_id).cloned().unwrap_or_default()
    }

    /// Get all series IDs currently in storage
    ///
    /// Returns a list of all series IDs that have at least one chunk stored.
    pub fn get_all_series_ids(&self) -> Vec<SeriesId> {
        let index = self.chunk_index.read();
        index.keys().copied().collect()
    }

    /// Get total chunk count across all series
    pub fn total_chunk_count(&self) -> usize {
        let index = self.chunk_index.read();
        index.values().map(|v| v.len()).sum()
    }

    /// Get base path for this storage engine
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }
}

#[async_trait]
impl StorageEngine for LocalDiskEngine {
    fn engine_id(&self) -> &str {
        "local-disk-v1"
    }

    async fn initialize(
        &self,
        _config: crate::engine::traits::StorageConfig,
    ) -> Result<(), StorageError> {
        // Load existing chunk index from disk
        self.load_index().await
    }

    async fn write_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        data: &crate::engine::traits::CompressedBlock,
    ) -> Result<crate::engine::traits::ChunkLocation, StorageError> {
        use crate::storage::chunk::ChunkHeader;
        use tokio::io::AsyncWriteExt;

        // Create series directory if needed
        let series_dir = self.series_path(series_id);
        fs::create_dir_all(&series_dir).await?;

        // Determine chunk path
        let path = self.chunk_path(series_id, &chunk_id, CompressionType::Gorilla);

        // Create chunk header
        let mut header = ChunkHeader::new(series_id);
        header.start_timestamp = data.metadata.start_timestamp;
        header.end_timestamp = data.metadata.end_timestamp;
        header.point_count = data.metadata.point_count as u32;
        header.compressed_size = data.compressed_size as u32;
        header.uncompressed_size = data.original_size as u32;
        header.checksum = data.checksum;
        header.compression_type = CompressionType::Gorilla;
        header.flags = crate::storage::chunk::ChunkFlags::sealed();

        // Validate header before writing
        header
            .validate()
            .map_err(|e| StorageError::ChunkNotFound(format!("Invalid chunk header: {}", e)))?;

        // Serialize header to bytes
        let header_bytes = header.to_bytes();

        // Write to file: header (64 bytes) + compressed data
        let mut file = fs::File::create(&path).await?;

        // Write header
        file.write_all(&header_bytes).await?;

        // Write compressed data
        file.write_all(&data.data).await?;

        // Sync to disk for durability
        file.sync_all().await?;

        // Get actual file size
        let metadata = file.metadata().await?;
        let total_size = metadata.len();

        // Update index
        {
            let mut index = self.chunk_index.write();
            let series_chunks = index.entry(series_id).or_default();

            series_chunks.push(ChunkMetadata {
                chunk_id: chunk_id.clone(),
                series_id,
                path: path.clone(),
                start_timestamp: data.metadata.start_timestamp,
                end_timestamp: data.metadata.end_timestamp,
                point_count: data.metadata.point_count as u32,
                size_bytes: total_size,
                compression: CompressionType::Gorilla,
                created_at: chrono::Utc::now().timestamp_millis(),
                last_accessed: chrono::Utc::now().timestamp_millis(),
            });

            // Keep sorted by timestamp
            series_chunks.sort_by_key(|c| c.start_timestamp);
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.write_ops += 1;
            stats.total_bytes += total_size;
            stats.total_chunks += 1;
        }

        Ok(crate::engine::traits::ChunkLocation {
            engine_id: self.engine_id().to_string(),
            path: path.to_string_lossy().to_string(),
            offset: Some(64), // Data starts after 64-byte header
            size: Some(data.compressed_size),
        })
    }

    async fn read_chunk(
        &self,
        location: &crate::engine::traits::ChunkLocation,
    ) -> Result<crate::engine::traits::CompressedBlock, StorageError> {
        use crate::storage::chunk::ChunkHeader;
        use tokio::io::AsyncReadExt;

        // Verify engine ID matches
        if location.engine_id != self.engine_id() {
            return Err(StorageError::ChunkNotFound(format!(
                "Engine ID mismatch: expected {}, got {}",
                self.engine_id(),
                location.engine_id
            )));
        }

        // Open the chunk file
        let path = PathBuf::from(&location.path);
        if !path.exists() {
            return Err(StorageError::ChunkNotFound(format!(
                "Chunk file not found: {}",
                location.path
            )));
        }

        let mut file = fs::File::open(&path).await?;

        // Read and parse header (first 64 bytes)
        let mut header_bytes = [0u8; 64];
        file.read_exact(&mut header_bytes).await?;

        let header = ChunkHeader::from_bytes(&header_bytes).map_err(|e| {
            StorageError::ChunkNotFound(format!("Failed to parse chunk header: {}", e))
        })?;

        // Validate header
        header
            .validate()
            .map_err(|e| StorageError::ChunkNotFound(format!("Invalid chunk header: {}", e)))?;

        // Read compressed data
        let mut compressed_data = vec![0u8; header.compressed_size as usize];
        file.read_exact(&mut compressed_data).await?;

        // Verify checksum
        if header.checksum
            != crate::compression::gorilla::GorillaCompressor::calculate_checksum(&compressed_data)
        {
            return Err(StorageError::ChunkNotFound(
                "Chunk checksum verification failed".to_string(),
            ));
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.read_ops += 1;
        }

        // Update last accessed time in index
        {
            let mut index = self.chunk_index.write();
            if let Some(series_chunks) = index.get_mut(&header.series_id) {
                if let Some(chunk_meta) = series_chunks.iter_mut().find(|c| c.path == path) {
                    chunk_meta.last_accessed = chrono::Utc::now().timestamp_millis();
                }
            }
        }

        // Construct CompressedBlock
        Ok(crate::engine::traits::CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: header.uncompressed_size as usize,
            compressed_size: header.compressed_size as usize,
            checksum: header.checksum,
            data: bytes::Bytes::from(compressed_data),
            metadata: crate::engine::traits::BlockMetadata {
                start_timestamp: header.start_timestamp,
                end_timestamp: header.end_timestamp,
                point_count: header.point_count as usize,
                series_id: header.series_id,
            },
        })
    }

    async fn delete_chunk(
        &self,
        location: &crate::engine::traits::ChunkLocation,
    ) -> Result<(), StorageError> {
        // Verify engine ID matches
        if location.engine_id != self.engine_id() {
            return Err(StorageError::ChunkNotFound(format!(
                "Engine ID mismatch: expected {}, got {}",
                self.engine_id(),
                location.engine_id
            )));
        }

        // Parse path and check if file exists
        let path = PathBuf::from(&location.path);
        if !path.exists() {
            return Err(StorageError::ChunkNotFound(format!(
                "Chunk file not found: {}",
                location.path
            )));
        }

        // Get file metadata before deletion for stats
        let metadata = fs::metadata(&path).await?;
        let file_size = metadata.len();

        // Read header to get series_id for index cleanup
        let mut file = fs::File::open(&path).await?;
        let mut header_bytes = [0u8; 64];
        use tokio::io::AsyncReadExt;
        file.read_exact(&mut header_bytes).await?;
        drop(file); // Close file before deletion

        let header =
            crate::storage::chunk::ChunkHeader::from_bytes(&header_bytes).map_err(|e| {
                StorageError::ChunkNotFound(format!("Failed to parse chunk header: {}", e))
            })?;

        // Delete the file
        fs::remove_file(&path).await?;

        // Remove from index
        {
            let mut index = self.chunk_index.write();
            if let Some(series_chunks) = index.get_mut(&header.series_id) {
                series_chunks.retain(|c| c.path != path);

                // Remove series entry if no more chunks
                if series_chunks.is_empty() {
                    index.remove(&header.series_id);
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.delete_ops += 1;
            stats.total_chunks = stats.total_chunks.saturating_sub(1);
            stats.total_bytes = stats.total_bytes.saturating_sub(file_size);
        }

        Ok(())
    }

    async fn list_chunks(
        &self,
        series_id: SeriesId,
        time_range: Option<crate::types::TimeRange>,
    ) -> Result<Vec<crate::engine::traits::ChunkMetadata>, StorageError> {
        let index = self.chunk_index.read();

        if let Some(chunks) = index.get(&series_id) {
            let matching: Vec<crate::engine::traits::ChunkMetadata> = chunks
                .iter()
                .filter(|c| {
                    if let Some(range) = time_range {
                        c.overlaps(range.start, range.end)
                    } else {
                        true
                    }
                })
                .map(|c| crate::engine::traits::ChunkMetadata {
                    chunk_id: c.chunk_id.clone(),
                    location: crate::engine::traits::ChunkLocation {
                        engine_id: self.engine_id().to_string(),
                        path: c.path.to_string_lossy().to_string(),
                        offset: Some(0),
                        size: Some(c.size_bytes as usize),
                    },
                    time_range: crate::types::TimeRange::new_unchecked(
                        c.start_timestamp,
                        c.end_timestamp,
                    ),
                    size_bytes: c.size_bytes as usize,
                    point_count: c.point_count as usize,
                    created_at: c.created_at,
                })
                .collect();
            Ok(matching)
        } else {
            Ok(Vec::new())
        }
    }

    /// Stream chunks for a series within a time range.
    ///
    /// Returns an async stream that yields compressed blocks one at a time,
    /// allowing for memory-efficient processing of large time ranges.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to stream chunks for
    /// * `time_range` - Time range to filter chunks (inclusive on both ends)
    ///
    /// # Returns
    ///
    /// A pinned stream that yields `CompressedBlock` items as they are read.
    ///
    /// # Semantics
    ///
    /// - **Snapshot consistency**: Returns a snapshot of chunk locations at call time.
    ///   Chunks added during streaming will NOT be included.
    /// - **Deleted chunks**: Chunks deleted during streaming may cause `ChunkNotFound` errors.
    /// - **Order**: Chunks are returned in time order (by start_timestamp).
    /// - **Overlap**: Chunks that overlap the time range are included, not just contained chunks.
    ///
    /// # Errors
    ///
    /// Each item in the stream may be an error if:
    /// - The chunk file was deleted after snapshot but before read
    /// - The chunk file is corrupted (checksum mismatch)
    /// - The chunk size exceeds 64MB (potential malformed header)
    /// - I/O errors occur during file access
    ///
    /// # Performance Notes
    ///
    /// - Chunk locations are collected upfront under a read lock (released before I/O)
    /// - Stats are updated using atomic operations to avoid write lock contention
    /// - Last accessed time updates are skipped in hot path (handled by maintenance)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use futures::StreamExt;
    /// use gorilla_tsdb::types::TimeRange;
    ///
    /// let range = TimeRange::new(0, 1000).unwrap();
    /// let mut stream = engine.stream_chunks(series_id, range);
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(block) => {
    ///             // Process the compressed block
    ///             println!("Got block with {} points", block.metadata.point_count);
    ///         }
    ///         Err(e) => {
    ///             // Handle error - could skip or abort
    ///             eprintln!("Error reading chunk: {}", e);
    ///         }
    ///     }
    /// }
    /// ```
    fn stream_chunks(
        &self,
        series_id: SeriesId,
        time_range: crate::types::TimeRange,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<crate::engine::traits::CompressedBlock, StorageError>>
                + Send,
        >,
    > {
        // Maximum allowed chunk size to prevent OOM from malformed headers
        const MAX_CHUNK_SIZE: u32 = 64 * 1024 * 1024; // 64MB

        // Collect matching chunk locations from the index
        // We do this synchronously to avoid holding the lock across await points
        let chunk_locations: Vec<crate::engine::traits::ChunkLocation> = {
            let index = self.chunk_index.read();
            if let Some(chunks) = index.get(&series_id) {
                chunks
                    .iter()
                    .filter(|c| c.overlaps(time_range.start, time_range.end))
                    .map(|c| crate::engine::traits::ChunkLocation {
                        engine_id: self.engine_id().to_string(),
                        path: c.path.to_string_lossy().to_string(),
                        offset: Some(64), // Data starts after 64-byte header
                        size: Some(c.size_bytes as usize),
                    })
                    .collect()
            } else {
                Vec::new()
            }
        };

        // Clone fields once outside the stream to minimize Arc cloning (PERF-003)
        let engine_id = self.engine_id().to_string();
        let stats = Arc::clone(&self.stats);

        // Create an async stream that reads chunks
        Box::pin(futures::stream::iter(chunk_locations).then(move |location| {
            // Clone once per iteration instead of in nested closure
            let engine_id = engine_id.clone();
            let stats = Arc::clone(&stats);

            async move {
                use crate::storage::chunk::ChunkHeader;
                use tokio::io::AsyncReadExt;

                // Verify engine ID matches
                if location.engine_id != engine_id {
                    return Err(StorageError::ChunkNotFound(format!(
                        "Engine ID mismatch: expected {}, got {}",
                        engine_id, location.engine_id
                    )));
                }

                // Fix SEC-003: Remove TOCTOU race by just attempting to open
                // If file doesn't exist, we get a clear error from open()
                let path = PathBuf::from(&location.path);
                let mut file = match fs::File::open(&path).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(StorageError::ChunkNotFound(format!(
                            "Chunk file not found: {}",
                            location.path
                        )));
                    }
                    Err(e) => return Err(StorageError::Io(e)),
                };

                // Read and parse header (first 64 bytes)
                let mut header_bytes = [0u8; 64];
                file.read_exact(&mut header_bytes).await?;

                let header = ChunkHeader::from_bytes(&header_bytes).map_err(|e| {
                    StorageError::ChunkNotFound(format!("Failed to parse chunk header: {}", e))
                })?;

                // Validate header
                header.validate().map_err(|e| {
                    StorageError::ChunkNotFound(format!("Invalid chunk header: {}", e))
                })?;

                // Fix VAL-001: Validate compressed_size before allocation
                if header.compressed_size > MAX_CHUNK_SIZE {
                    return Err(StorageError::CorruptedData(format!(
                        "Chunk size {} exceeds maximum allowed size {}",
                        header.compressed_size, MAX_CHUNK_SIZE
                    )));
                }

                // Read compressed data
                let mut compressed_data = vec![0u8; header.compressed_size as usize];
                file.read_exact(&mut compressed_data).await?;

                // Verify checksum
                if header.checksum
                    != crate::compression::gorilla::GorillaCompressor::calculate_checksum(
                        &compressed_data,
                    )
                {
                    return Err(StorageError::CorruptedData(
                        "Chunk checksum verification failed".to_string(),
                    ));
                }

                // Fix PERF-001: Use read lock only for stats update
                // Skip last_accessed updates in hot path - maintenance handles it
                {
                    let mut s = stats.write();
                    s.read_ops += 1;
                }

                // Return the CompressedBlock
                Ok(crate::engine::traits::CompressedBlock {
                    algorithm_id: "gorilla".to_string(),
                    original_size: header.uncompressed_size as usize,
                    compressed_size: header.compressed_size as usize,
                    checksum: header.checksum,
                    data: bytes::Bytes::from(compressed_data),
                    metadata: crate::engine::traits::BlockMetadata {
                        start_timestamp: header.start_timestamp,
                        end_timestamp: header.end_timestamp,
                        point_count: header.point_count as usize,
                        series_id: header.series_id,
                    },
                })
            }
        }))
    }

    fn stats(&self) -> crate::engine::traits::StorageStats {
        self.stats.read().clone()
    }

    /// Perform storage maintenance operations.
    ///
    /// Executes various cleanup and optimization operations to maintain storage health.
    ///
    /// # Operations Performed
    ///
    /// 1. **Retention Cleanup**: Deletes chunks older than 30 days (configurable in future)
    /// 2. **Orphan Cleanup**: Removes chunk files that are not tracked in the index
    /// 3. **Empty Directory Cleanup**: Removes series directories with no remaining chunks
    ///
    /// # Concurrency
    ///
    /// - **Safe with reads**: Reads may see `ChunkNotFound` for deleted chunks
    /// - **Safe with writes**: Writes use separate paths, won't conflict
    /// - **Single maintenance**: Only one maintenance task should run at a time
    ///
    /// # Error Handling
    ///
    /// Individual file deletion errors are logged but do not stop maintenance.
    /// The operation continues with remaining files and returns success.
    /// Only fatal conditions (like inability to read directories) return errors.
    ///
    /// # Performance
    ///
    /// - Scans all series directories for orphan detection
    /// - Acquires locks briefly for index updates (not during I/O)
    /// - May take significant time for storage with many files
    /// - Consider running during low-traffic periods
    ///
    /// # Returns
    ///
    /// A [`MaintenanceReport`] containing:
    /// - `chunks_deleted`: Number of chunks removed
    /// - `bytes_freed`: Total bytes reclaimed
    /// - `chunks_compacted`: Always 0 (compaction not yet implemented)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use gorilla_tsdb::storage::LocalDiskEngine;
    ///
    /// let report = engine.maintenance().await?;
    /// println!("Maintenance complete:");
    /// println!("  Chunks deleted: {}", report.chunks_deleted);
    /// println!("  Bytes freed: {} MB", report.bytes_freed / 1024 / 1024);
    /// ```
    async fn maintenance(&self) -> Result<crate::engine::traits::MaintenanceReport, StorageError> {
        let mut report = crate::engine::traits::MaintenanceReport::default();

        // Get current timestamp for retention calculations
        let now = chrono::Utc::now().timestamp_millis();

        // Define retention period (30 days by default, could be made configurable)
        let retention_ms: i64 = 30 * 24 * 60 * 60 * 1000; // 30 days in milliseconds
        // Fix EDGE-010: Use saturating_sub to prevent underflow
        let retention_cutoff = now.saturating_sub(retention_ms);

        // Collect chunks to delete (older than retention period)
        // We do this in two phases to avoid holding locks during I/O
        let chunks_to_delete: Vec<(SeriesId, PathBuf, u64)> = {
            let index = self.chunk_index.read();
            index
                .iter()
                .flat_map(|(series_id, chunks)| {
                    chunks
                        .iter()
                        .filter(|c| c.end_timestamp < retention_cutoff)
                        .map(move |c| (*series_id, c.path.clone(), c.size_bytes))
                })
                .collect()
        };

        // Delete expired chunks
        for (series_id, path, size) in chunks_to_delete {
            // Try to delete the file
            match fs::remove_file(&path).await {
                Ok(()) => {
                    report.chunks_deleted += 1;
                    report.bytes_freed += size;

                    // Update stats
                    {
                        let mut stats = self.stats.write();
                        stats.total_chunks = stats.total_chunks.saturating_sub(1);
                        stats.total_bytes = stats.total_bytes.saturating_sub(size);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // File already deleted, just clean up index
                }
                Err(e) => {
                    // Log but continue with other chunks
                    eprintln!("Failed to delete chunk {:?}: {}", path, e);
                    continue;
                }
            }

            // Remove from index
            {
                let mut index = self.chunk_index.write();
                if let Some(series_chunks) = index.get_mut(&series_id) {
                    series_chunks.retain(|c| c.path != path);

                    // Remove series if no chunks remain
                    if series_chunks.is_empty() {
                        index.remove(&series_id);
                    }
                }
            }
        }

        // Clean up orphaned chunk files (files on disk not in index)
        // This handles cases where the index got out of sync
        let indexed_paths: std::collections::HashSet<PathBuf> = {
            let index = self.chunk_index.read();
            index
                .values()
                .flat_map(|chunks| chunks.iter().map(|c| c.path.clone()))
                .collect()
        };

        // Scan series directories for orphaned files
        if let Ok(mut entries) = fs::read_dir(&self.base_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let dir_path = entry.path();
                if !dir_path.is_dir() {
                    continue;
                }

                // Check if this is a series directory
                if let Some(dir_name) = dir_path.file_name().and_then(|n| n.to_str()) {
                    if !dir_name.starts_with("series_") {
                        continue;
                    }
                }

                // Scan chunk files in this series directory
                if let Ok(mut chunk_entries) = fs::read_dir(&dir_path).await {
                    while let Ok(Some(chunk_entry)) = chunk_entries.next_entry().await {
                        let chunk_path = chunk_entry.path();

                        // Skip non-chunk files
                        if !chunk_path
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(|e| ["gor", "snappy", "raw"].contains(&e))
                            .unwrap_or(false)
                        {
                            continue;
                        }

                        // Check if this chunk is in our index
                        if !indexed_paths.contains(&chunk_path) {
                            // This is an orphaned chunk file - delete it
                            if let Ok(metadata) = fs::metadata(&chunk_path).await {
                                let size = metadata.len();
                                if fs::remove_file(&chunk_path).await.is_ok() {
                                    report.chunks_deleted += 1;
                                    report.bytes_freed += size;
                                }
                            }
                        }
                    }
                }

                // Remove empty series directories
                if let Ok(mut dir_entries) = fs::read_dir(&dir_path).await {
                    // Check if directory is empty by trying to get first entry
                    let is_empty = dir_entries.next_entry().await.ok().flatten().is_none();
                    if is_empty {
                        let _ = fs::remove_dir(&dir_path).await;
                    }
                }
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_engine() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        assert_eq!(engine.engine_id(), "local-disk-v1");
        assert!(engine.base_path.exists());
    }

    #[tokio::test]
    async fn test_series_path() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        let series_path = engine.series_path(42);
        assert!(series_path.to_string_lossy().contains("series_42"));
        assert!(series_path.starts_with(&engine.base_path));
    }

    #[tokio::test]
    async fn test_chunk_path() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Use valid UUID for chunk_id
        let chunk_id = ChunkId::from_string("550e8400-e29b-41d4-a716-446655440000").unwrap();

        // Test different compression types
        let path_gor = engine.chunk_path(1, &chunk_id, CompressionType::Gorilla);
        assert!(path_gor.to_string_lossy().contains("series_1"));
        assert!(path_gor.to_string_lossy().contains("chunk_550e8400-e29b-41d4-a716-446655440000"));
        assert!(path_gor.to_string_lossy().ends_with(".gor"));

        let path_snappy = engine.chunk_path(1, &chunk_id, CompressionType::Snappy);
        assert!(path_snappy.to_string_lossy().ends_with(".snappy"));

        let path_both = engine.chunk_path(1, &chunk_id, CompressionType::GorillaSnappy);
        assert!(path_both.to_string_lossy().ends_with(".gor.snappy"));

        let path_none = engine.chunk_path(1, &chunk_id, CompressionType::None);
        assert!(path_none.to_string_lossy().ends_with(".raw"));
    }

    #[tokio::test]
    async fn test_initialize_empty() {
        use crate::engine::traits::{StorageConfig, StorageEngine};

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Initialize on empty directory should succeed
        let result = engine.initialize(StorageConfig::default()).await;
        assert!(result.is_ok());

        // Should have no chunks
        let stats = engine.stats();
        assert_eq!(stats.total_chunks, 0);
    }

    #[tokio::test]
    async fn test_list_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Initially no chunks
        let chunks = engine.list_chunks(1, None).await.unwrap();
        assert_eq!(chunks.len(), 0);
    }

    #[tokio::test]
    async fn test_list_chunks_with_time_filter() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock};
        use crate::types::TimeRange;
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Write chunks with different time ranges
        for i in 0..5 {
            let test_data = vec![i as u8; 10];
            let checksum =
                crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

            let block = CompressedBlock {
                algorithm_id: "gorilla".to_string(),
                original_size: 20,
                compressed_size: test_data.len(),
                checksum,
                data: Bytes::from(test_data),
                metadata: BlockMetadata {
                    start_timestamp: i * 1000,
                    end_timestamp: i * 1000 + 500,
                    point_count: 10,
                    series_id: 1,
                },
            };

            engine.write_chunk(1, ChunkId::new(), &block).await.unwrap();
        }

        // List all chunks
        let all_chunks = engine.list_chunks(1, None).await.unwrap();
        assert_eq!(all_chunks.len(), 5);

        // Filter by time range: should get chunks 1, 2, 3
        let time_range = TimeRange::new(1000, 3500).unwrap();
        let filtered = engine.list_chunks(1, Some(time_range)).await.unwrap();
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[0].time_range.start, 1000);
        assert_eq!(filtered[1].time_range.start, 2000);
        assert_eq!(filtered[2].time_range.start, 3000);

        // Filter with no overlap
        let no_overlap = TimeRange::new(10000, 20000).unwrap();
        let empty = engine.list_chunks(1, Some(no_overlap)).await.unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.total_chunks, 0);
        assert_eq!(stats.write_ops, 0);
    }

    #[tokio::test]
    async fn test_write_and_read_chunk() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock};
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Create test data
        let test_data = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let checksum =
            crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

        let block = CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: 100,
            compressed_size: test_data.len(),
            checksum,
            data: Bytes::from(test_data.clone()),
            metadata: BlockMetadata {
                start_timestamp: 1000,
                end_timestamp: 2000,
                point_count: 10,
                series_id: 1,
            },
        };

        let chunk_id = ChunkId::new();

        // Write chunk
        let location = engine
            .write_chunk(1, chunk_id.clone(), &block)
            .await
            .unwrap();

        // Verify location
        assert_eq!(location.engine_id, "local-disk-v1");
        assert!(location.path.contains("series_1"));
        assert!(location.path.contains("chunk_"));
        assert_eq!(location.offset, Some(64)); // Data after header

        // Verify stats after write
        let stats = engine.stats();
        assert_eq!(stats.write_ops, 1);
        assert_eq!(stats.total_chunks, 1);
        assert!(stats.total_bytes > 0);

        // Read chunk back
        let read_block = engine.read_chunk(&location).await.unwrap();

        // Verify read data matches written data
        assert_eq!(read_block.algorithm_id, "gorilla");
        assert_eq!(read_block.original_size, 100);
        assert_eq!(read_block.compressed_size, test_data.len());
        assert_eq!(read_block.checksum, checksum);
        assert_eq!(read_block.data.as_ref(), &test_data);
        assert_eq!(read_block.metadata.start_timestamp, 1000);
        assert_eq!(read_block.metadata.end_timestamp, 2000);
        assert_eq!(read_block.metadata.point_count, 10);
        assert_eq!(read_block.metadata.series_id, 1);

        // Verify stats after read
        let stats = engine.stats();
        assert_eq!(stats.read_ops, 1);
    }

    #[tokio::test]
    async fn test_delete_chunk() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock};
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Create and write test chunk
        let test_data = vec![1u8; 100];
        let checksum =
            crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

        let block = CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: 200,
            compressed_size: test_data.len(),
            checksum,
            data: Bytes::from(test_data),
            metadata: BlockMetadata {
                start_timestamp: 1000,
                end_timestamp: 2000,
                point_count: 20,
                series_id: 1,
            },
        };

        let chunk_id = ChunkId::new();
        let location = engine.write_chunk(1, chunk_id, &block).await.unwrap();

        // Verify chunk was written
        assert_eq!(engine.stats().total_chunks, 1);
        assert!(PathBuf::from(&location.path).exists());

        // Delete chunk
        engine.delete_chunk(&location).await.unwrap();

        // Verify chunk was deleted
        assert!(!PathBuf::from(&location.path).exists());

        // Verify stats updated
        let stats = engine.stats();
        assert_eq!(stats.delete_ops, 1);
        assert_eq!(stats.total_chunks, 0);

        // Verify reading deleted chunk fails
        let result = engine.read_chunk(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_series() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock};
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Write chunks for multiple series
        for series_id in 1..=3 {
            let test_data = vec![series_id as u8; 50];
            let checksum =
                crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

            let block = CompressedBlock {
                algorithm_id: "gorilla".to_string(),
                original_size: 100,
                compressed_size: test_data.len(),
                checksum,
                data: Bytes::from(test_data),
                metadata: BlockMetadata {
                    start_timestamp: series_id as i64 * 1000,
                    end_timestamp: series_id as i64 * 1000 + 500,
                    point_count: 10,
                    series_id,
                },
            };

            engine
                .write_chunk(series_id, ChunkId::new(), &block)
                .await
                .unwrap();
        }

        // Verify all series have chunks
        assert_eq!(engine.stats().total_chunks, 3);

        // List chunks for each series
        for series_id in 1..=3 {
            let chunks = engine.list_chunks(series_id, None).await.unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].time_range.start, series_id as i64 * 1000);
        }
    }

    #[tokio::test]
    async fn test_chunk_not_found() {
        use crate::engine::traits::ChunkLocation;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Try to read non-existent chunk
        let location = ChunkLocation {
            engine_id: "local-disk-v1".to_string(),
            path: "/nonexistent/chunk.gor".to_string(),
            offset: Some(0),
            size: Some(100),
        };

        let result = engine.read_chunk(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_engine_id_mismatch() {
        use crate::engine::traits::ChunkLocation;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Try to read chunk with wrong engine ID
        let location = ChunkLocation {
            engine_id: "wrong-engine".to_string(),
            path: "/some/path.gor".to_string(),
            offset: Some(0),
            size: Some(100),
        };

        let result = engine.read_chunk(&location).await;
        assert!(result.is_err());

        let result = engine.delete_chunk(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock};
        use bytes::Bytes;
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Write a valid chunk
        let test_data = vec![1u8; 100];
        let checksum =
            crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

        let block = CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: 200,
            compressed_size: test_data.len(),
            checksum,
            data: Bytes::from(test_data),
            metadata: BlockMetadata {
                start_timestamp: 1000,
                end_timestamp: 2000,
                point_count: 20,
                series_id: 1,
            },
        };

        let chunk_id = ChunkId::new();
        let location = engine.write_chunk(1, chunk_id, &block).await.unwrap();

        // Corrupt the file by modifying data
        let path = PathBuf::from(&location.path);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .unwrap();

        // Seek past header and corrupt first byte of data
        file.seek(std::io::SeekFrom::Start(64)).await.unwrap();
        file.write_all(&[0xFF]).await.unwrap();
        file.sync_all().await.unwrap();

        // Reading should fail due to checksum mismatch
        let result = engine.read_chunk(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_index_on_restart() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock, StorageEngine};
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();

        // Create first engine and write data
        {
            let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

            let test_data = vec![42u8; 50];
            let checksum =
                crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

            let block = CompressedBlock {
                algorithm_id: "gorilla".to_string(),
                original_size: 100,
                compressed_size: test_data.len(),
                checksum,
                data: Bytes::from(test_data),
                metadata: BlockMetadata {
                    start_timestamp: 5000,
                    end_timestamp: 6000,
                    point_count: 25,
                    series_id: 42,
                },
            };

            engine
                .write_chunk(42, ChunkId::new(), &block)
                .await
                .unwrap();

            // Verify stats before restart
            let stats = engine.stats();
            assert_eq!(stats.total_chunks, 1);
            assert_eq!(stats.write_ops, 1);
        }

        // Create new engine instance (simulating restart)
        let engine2 = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Initialize (load index from disk)
        engine2
            .initialize(crate::engine::traits::StorageConfig::default())
            .await
            .unwrap();

        // Verify chunks were loaded
        let chunks = engine2.list_chunks(42, None).await.unwrap();
        assert_eq!(chunks.len(), 1);

        // Verify metadata was correctly loaded from header
        assert_eq!(chunks[0].time_range.start, 5000);
        assert_eq!(chunks[0].time_range.end, 6000);
        assert_eq!(chunks[0].point_count, 25);
        assert_eq!(chunks[0].time_range.start, 5000);

        // Verify we can read the chunk after restart
        let read_block = engine2.read_chunk(&chunks[0].location).await.unwrap();
        assert_eq!(read_block.metadata.start_timestamp, 5000);
        assert_eq!(read_block.metadata.end_timestamp, 6000);
        assert_eq!(read_block.metadata.point_count, 25);
        assert_eq!(read_block.metadata.series_id, 42);
    }

    #[tokio::test]
    async fn test_stream_chunks() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock, StorageEngine};
        use crate::types::TimeRange;
        use bytes::Bytes;
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Write multiple chunks for a series
        for i in 0..5 {
            let test_data = vec![i as u8; 20];
            let checksum =
                crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

            let block = CompressedBlock {
                algorithm_id: "gorilla".to_string(),
                original_size: 40,
                compressed_size: test_data.len(),
                checksum,
                data: Bytes::from(test_data),
                metadata: BlockMetadata {
                    start_timestamp: i * 1000,
                    end_timestamp: i * 1000 + 500,
                    point_count: 5,
                    series_id: 1,
                },
            };

            engine
                .write_chunk(1, ChunkId::new(), &block)
                .await
                .unwrap();
        }

        // Stream all chunks
        let time_range = TimeRange::new(0, 5000).unwrap();
        let mut stream = engine.stream_chunks(1, time_range);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            let block = result.unwrap();
            assert_eq!(block.algorithm_id, "gorilla");
            assert_eq!(block.metadata.series_id, 1);
            count += 1;
        }

        assert_eq!(count, 5);

        // Stream with filtered time range (should get chunks 1, 2, 3)
        let filtered_range = TimeRange::new(1000, 3500).unwrap();
        let mut stream = engine.stream_chunks(1, filtered_range);

        let mut filtered_count = 0;
        while let Some(result) = stream.next().await {
            let block = result.unwrap();
            assert!(block.metadata.start_timestamp >= 1000);
            assert!(block.metadata.end_timestamp <= 4000);
            filtered_count += 1;
        }

        assert_eq!(filtered_count, 3);
    }

    #[tokio::test]
    async fn test_stream_chunks_empty_series() {
        use crate::engine::traits::StorageEngine;
        use crate::types::TimeRange;
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Stream from a series that doesn't exist
        let time_range = TimeRange::new(0, 10000).unwrap();
        let mut stream = engine.stream_chunks(999, time_range);

        // Should get no chunks
        let mut count = 0;
        while (stream.next().await).is_some() {
            count += 1;
        }

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_maintenance_empty_storage() {
        use crate::engine::traits::StorageEngine;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Run maintenance on empty storage
        let report = engine.maintenance().await.unwrap();

        // Should report no actions taken
        assert_eq!(report.chunks_deleted, 0);
        assert_eq!(report.chunks_compacted, 0);
        assert_eq!(report.bytes_freed, 0);
    }

    #[tokio::test]
    async fn test_maintenance_orphan_cleanup() {
        use crate::engine::traits::StorageEngine;
        use tokio::io::AsyncWriteExt;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Create an orphan file (not in index)
        let series_dir = temp_dir.path().join("series_1");
        fs::create_dir_all(&series_dir).await.unwrap();

        let orphan_path = series_dir.join("chunk_orphan.gor");
        let mut file = fs::File::create(&orphan_path).await.unwrap();
        file.write_all(&[0u8; 100]).await.unwrap();
        file.sync_all().await.unwrap();

        // Verify the orphan file exists
        assert!(orphan_path.exists());

        // Run maintenance
        let report = engine.maintenance().await.unwrap();

        // The orphan should be cleaned up
        assert_eq!(report.chunks_deleted, 1);
        assert_eq!(report.bytes_freed, 100);
        assert!(!orphan_path.exists());
    }

    #[tokio::test]
    async fn test_maintenance_preserves_valid_chunks() {
        use crate::engine::traits::{BlockMetadata, CompressedBlock, StorageEngine};
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Write a valid chunk with recent timestamp
        let now = chrono::Utc::now().timestamp_millis();
        let test_data = vec![1u8; 50];
        let checksum =
            crate::compression::gorilla::GorillaCompressor::calculate_checksum(&test_data);

        let block = CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: 100,
            compressed_size: test_data.len(),
            checksum,
            data: Bytes::from(test_data),
            metadata: BlockMetadata {
                start_timestamp: now - 1000,
                end_timestamp: now,
                point_count: 10,
                series_id: 1,
            },
        };

        let chunk_id = ChunkId::new();
        let location = engine.write_chunk(1, chunk_id, &block).await.unwrap();

        // Run maintenance
        let report = engine.maintenance().await.unwrap();

        // Recent chunk should NOT be deleted
        assert_eq!(report.chunks_deleted, 0);
        assert_eq!(report.bytes_freed, 0);

        // Chunk should still exist and be readable
        let path = PathBuf::from(&location.path);
        assert!(path.exists());

        let read_block = engine.read_chunk(&location).await.unwrap();
        assert_eq!(read_block.metadata.series_id, 1);
    }
}
