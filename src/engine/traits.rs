//! Core trait definitions for pluggable engines

use crate::error::{CompressionError, IndexError, StorageError};
use crate::types::{ChunkId, DataPoint, SeriesId, TagFilter, TimeRange};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;

// =============================================================================
// Compressor Trait
// =============================================================================

/// Core trait for compression engines
#[async_trait]
pub trait Compressor: Send + Sync + 'static {
    /// Unique identifier for this compression algorithm
    fn algorithm_id(&self) -> &str;

    /// Compress a batch of data points
    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock, CompressionError>;

    /// Decompress a block back to data points
    async fn decompress(&self, block: &CompressedBlock)
        -> Result<Vec<DataPoint>, CompressionError>;

    /// Estimate compression ratio for capacity planning
    fn estimate_ratio(&self, sample: &[DataPoint]) -> f64;

    /// Get compression statistics
    fn stats(&self) -> CompressionStats;
}

/// Compressed block with metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompressedBlock {
    /// Compression algorithm identifier
    pub algorithm_id: String,
    /// Original uncompressed size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// CRC64 checksum for data integrity
    pub checksum: u64,
    /// Compressed data
    pub data: Bytes,
    /// Block metadata
    pub metadata: BlockMetadata,
}

/// Block metadata
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BlockMetadata {
    /// First timestamp in the block
    pub start_timestamp: i64,
    /// Last timestamp in the block
    pub end_timestamp: i64,
    /// Number of data points in the block
    pub point_count: usize,
    /// Series identifier
    pub series_id: SeriesId,
}

/// Compression statistics
#[derive(Clone, Debug, Default)]
pub struct CompressionStats {
    /// Total number of compression operations
    pub total_compressed: u64,
    /// Total number of decompression operations
    pub total_decompressed: u64,
    /// Total compression time in milliseconds
    pub compression_time_ms: u64,
    /// Total decompression time in milliseconds
    pub decompression_time_ms: u64,
    /// Average compression ratio achieved
    pub average_ratio: f64,
}

// =============================================================================
// StorageEngine Trait
// =============================================================================

/// Core trait for storage backends
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Unique identifier for this storage backend
    fn engine_id(&self) -> &str;

    /// Initialize storage backend
    async fn initialize(&self, config: StorageConfig) -> Result<(), StorageError>;

    /// Write a chunk to storage
    async fn write_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        data: &CompressedBlock,
    ) -> Result<ChunkLocation, StorageError>;

    /// Read a chunk from storage
    async fn read_chunk(&self, location: &ChunkLocation) -> Result<CompressedBlock, StorageError>;

    /// Delete a chunk
    async fn delete_chunk(&self, location: &ChunkLocation) -> Result<(), StorageError>;

    /// List chunks for a series
    async fn list_chunks(
        &self,
        series_id: SeriesId,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<ChunkMetadata>, StorageError>;

    /// Stream chunks (for large queries)
    fn stream_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Pin<Box<dyn Stream<Item = Result<CompressedBlock, StorageError>> + Send>>;

    /// Get storage statistics
    fn stats(&self) -> StorageStats;

    /// Perform maintenance (compaction, cleanup, etc.)
    async fn maintenance(&self) -> Result<MaintenanceReport, StorageError>;
}

/// Storage configuration
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Base directory path for storage
    pub base_path: Option<String>,
    /// Maximum chunk size in bytes
    pub max_chunk_size: usize,
    /// Enable secondary compression (e.g., Snappy)
    pub compression_enabled: bool,
    /// Data retention period in days
    pub retention_days: Option<u32>,
    /// Custom storage-specific options
    pub custom_options: HashMap<String, String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base_path: Some("/var/lib/tsdb".to_string()),
            max_chunk_size: 1024 * 1024, // 1MB
            compression_enabled: true,
            retention_days: None,
            custom_options: HashMap::new(),
        }
    }
}

/// Chunk location identifier
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkLocation {
    /// Storage engine identifier
    pub engine_id: String,
    /// Path to the chunk file or object
    pub path: String,
    /// Optional offset within the file
    pub offset: Option<u64>,
    /// Optional size of the chunk
    pub size: Option<usize>,
}

/// Chunk metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Unique chunk identifier
    pub chunk_id: ChunkId,
    /// Location of the chunk
    pub location: ChunkLocation,
    /// Time range covered by this chunk
    pub time_range: TimeRange,
    /// Size of the chunk in bytes
    pub size_bytes: usize,
    /// Number of data points in the chunk
    pub point_count: usize,
    /// Chunk creation timestamp
    pub created_at: i64,
}

/// Storage statistics
#[derive(Clone, Debug, Default)]
pub struct StorageStats {
    /// Total number of chunks stored
    pub total_chunks: u64,
    /// Total bytes stored (compressed size on disk)
    pub total_bytes: u64,
    /// Total uncompressed bytes (original size before compression)
    /// Used to calculate compression ratio from stored data
    pub total_uncompressed_bytes: u64,
    /// Number of write operations
    pub write_ops: u64,
    /// Number of read operations
    pub read_ops: u64,
    /// Number of delete operations
    pub delete_ops: u64,
}

impl StorageStats {
    /// Calculate compression ratio from stored data
    ///
    /// Returns the ratio of compressed to uncompressed bytes.
    /// A ratio of 0.5 means data is compressed to 50% of original size.
    /// Returns 0.0 if no uncompressed data is tracked.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_uncompressed_bytes == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.total_uncompressed_bytes as f64
        }
    }
}

/// Maintenance report
#[derive(Clone, Debug, Default)]
pub struct MaintenanceReport {
    /// Number of chunks compacted
    pub chunks_compacted: usize,
    /// Number of chunks deleted
    pub chunks_deleted: usize,
    /// Bytes freed during maintenance
    pub bytes_freed: u64,
}

// =============================================================================
// TimeIndex Trait
// =============================================================================

/// Core trait for time-series indexing
#[async_trait]
pub trait TimeIndex: Send + Sync + 'static {
    /// Unique identifier for this index backend
    fn index_id(&self) -> &str;

    /// Initialize index backend
    async fn initialize(&self, config: IndexConfig) -> Result<(), IndexError>;

    /// Register a new series
    async fn register_series(
        &self,
        series_id: SeriesId,
        metadata: SeriesMetadata,
    ) -> Result<(), IndexError>;

    /// Add chunk reference to index
    async fn add_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        time_range: TimeRange,
        location: ChunkLocation,
    ) -> Result<(), IndexError>;

    /// Query chunks by time range
    async fn query_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError>;

    /// Find series by tags
    async fn find_series(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, IndexError>;

    /// Update chunk status
    async fn update_chunk_status(
        &self,
        chunk_id: ChunkId,
        status: ChunkStatus,
    ) -> Result<(), IndexError>;

    /// Delete series from index
    async fn delete_series(&self, series_id: SeriesId) -> Result<(), IndexError>;

    /// Get index statistics
    fn stats(&self) -> IndexStats;

    /// Rebuild index from storage (if storage engine is provided)
    async fn rebuild(&self) -> Result<(), IndexError>;
}

/// Index configuration
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// Connection string for the index backend (e.g., Redis URL)
    pub connection_string: Option<String>,
    /// Cache size in megabytes
    pub cache_size_mb: usize,
    /// Maximum number of series to track
    pub max_series: usize,
    /// Custom index-specific options
    pub custom_options: HashMap<String, String>,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            connection_string: None,
            cache_size_mb: 128,
            max_series: 1_000_000,
            custom_options: HashMap::new(),
        }
    }
}

/// Series metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesMetadata {
    /// Metric name (e.g., "cpu.usage")
    pub metric_name: String,
    /// Tag key-value pairs
    pub tags: HashMap<String, String>,
    /// Series creation timestamp
    pub created_at: i64,
    /// Retention period in days
    pub retention_days: Option<u32>,
}

/// Chunk reference in the index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkReference {
    /// Unique chunk identifier
    pub chunk_id: ChunkId,
    /// Location of the chunk
    pub location: ChunkLocation,
    /// Time range covered by this chunk
    pub time_range: TimeRange,
    /// Current status of the chunk
    pub status: ChunkStatus,
}

/// Chunk status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    /// Chunk is actively being written to
    Active,
    /// Chunk is sealed (no more writes)
    Sealed,
    /// Chunk has been compressed
    Compressed,
    /// Chunk has been archived to cold storage
    Archived,
    /// Chunk has been deleted
    Deleted,
}

/// Index statistics
#[derive(Clone, Debug, Default)]
pub struct IndexStats {
    /// Total number of series tracked
    pub total_series: u64,
    /// Total number of chunks indexed
    pub total_chunks: u64,
    /// Number of queries served
    pub queries_served: u64,
    /// Number of cache hits
    pub cache_hits: u64,
    /// Number of cache misses
    pub cache_misses: u64,
}
