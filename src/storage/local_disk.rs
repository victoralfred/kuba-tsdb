//! Local disk storage engine implementation
//!
//! Provides persistent storage for time-series data using the local filesystem.
//! Data is organized into series directories with compressed chunk files.

use crate::engine::traits::{StorageEngine, StorageStats};
use crate::error::StorageError;
use crate::storage::chunk::{ChunkMetadata, CompressionType};
use crate::types::{ChunkId, SeriesId};
use async_trait::async_trait;
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
/// ```rust
/// use gorilla_tsdb::storage::LocalDiskEngine;
/// use gorilla_tsdb::types::DataPoint;
/// use gorilla_tsdb::engine::traits::StorageEngine;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let engine = LocalDiskEngine::new("/data/tsdb".into())?;
///
/// let points = vec![DataPoint::new(1, 1000, 42.5)];
/// engine.write(1, &points).await?;
/// # Ok(())
/// # }
/// ```
pub struct LocalDiskEngine {
    /// Base directory for all storage
    base_path: PathBuf,

    /// In-memory index of chunks by series
    /// Maps series_id -> Vec<ChunkMetadata>
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
    /// ```rust
    /// use gorilla_tsdb::storage::LocalDiskEngine;
    ///
    /// let engine = LocalDiskEngine::new("/data/tsdb".into())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
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
    fn chunk_path(&self, series_id: SeriesId, chunk_id: &ChunkId, compression: CompressionType) -> PathBuf {
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
    async fn load_series_chunks(&self, series_id: SeriesId) -> Result<Vec<ChunkMetadata>, StorageError> {
        let series_dir = self.series_path(series_id);
        if !series_dir.exists() {
            return Ok(Vec::new());
        }

        let mut chunks = Vec::new();
        let mut entries = fs::read_dir(&series_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                // Load chunk metadata (simplified - would normally parse header)
                let metadata = fs::metadata(&path).await?;

                chunks.push(ChunkMetadata {
                    chunk_id: ChunkId::new(),
                    series_id,
                    path: path.clone(),
                    start_timestamp: 0,  // TODO: Load from chunk header
                    end_timestamp: 0,    // TODO: Load from chunk header
                    point_count: 0,       // TODO: Load from chunk header
                    size_bytes: metadata.len(),
                    compression: CompressionType::Gorilla,
                    created_at: 0,
                    last_accessed: 0,
                });
            }
        }

        chunks.sort_by_key(|c| c.start_timestamp);
        Ok(chunks)
    }
}

#[async_trait]
impl StorageEngine for LocalDiskEngine {
    fn engine_id(&self) -> &str {
        "local-disk-v1"
    }

    async fn initialize(&self, _config: crate::engine::traits::StorageConfig) -> Result<(), StorageError> {
        // Load existing chunk index from disk
        self.load_index().await
    }

    async fn write_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        data: &crate::engine::traits::CompressedBlock,
    ) -> Result<crate::engine::traits::ChunkLocation, StorageError> {
        // Create series directory if needed
        let series_dir = self.series_path(series_id);
        fs::create_dir_all(&series_dir).await?;

        // Determine chunk path
        let path = self.chunk_path(series_id, &chunk_id, CompressionType::Gorilla);

        // TODO: Write chunk data to file
        // For now, just return a placeholder location

        // Update index
        {
            let mut index = self.chunk_index.write();
            let series_chunks = index.entry(series_id).or_insert_with(Vec::new);

            series_chunks.push(ChunkMetadata {
                chunk_id: chunk_id.clone(),
                series_id,
                path: path.clone(),
                start_timestamp: data.metadata.start_timestamp,
                end_timestamp: data.metadata.end_timestamp,
                point_count: data.metadata.point_count as u32,
                size_bytes: data.compressed_size as u64,
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
            stats.total_bytes += data.compressed_size as u64;
            stats.total_chunks += 1;
        }

        Ok(crate::engine::traits::ChunkLocation {
            engine_id: self.engine_id().to_string(),
            path: path.to_string_lossy().to_string(),
            offset: Some(0),
            size: Some(data.compressed_size),
        })
    }

    async fn read_chunk(&self, location: &crate::engine::traits::ChunkLocation) -> Result<crate::engine::traits::CompressedBlock, StorageError> {
        // TODO: Implement chunk reading
        Err(StorageError::ChunkNotFound(format!("Chunk at {} not found", location.path)))
    }

    async fn delete_chunk(&self, location: &crate::engine::traits::ChunkLocation) -> Result<(), StorageError> {
        // TODO: Implement chunk deletion
        Err(StorageError::ChunkNotFound(format!("Chunk at {} not found", location.path)))
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
                    time_range: crate::types::TimeRange::new_unchecked(c.start_timestamp, c.end_timestamp),
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

    fn stream_chunks(
        &self,
        _series_id: SeriesId,
        _time_range: crate::types::TimeRange,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<crate::engine::traits::CompressedBlock, StorageError>> + Send>> {
        // TODO: Implement streaming
        Box::pin(futures::stream::empty())
    }

    fn stats(&self) -> crate::engine::traits::StorageStats {
        self.stats.read().clone()
    }

    async fn maintenance(&self) -> Result<crate::engine::traits::MaintenanceReport, StorageError> {
        // TODO: Implement maintenance (compaction, cleanup)
        Ok(crate::engine::traits::MaintenanceReport::default())
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
    async fn test_list_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        // Initially no chunks
        let chunks = engine.list_chunks(1, None).await.unwrap();
        assert_eq!(chunks.len(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.total_chunks, 0);
        assert_eq!(stats.write_ops, 0);
    }
}
