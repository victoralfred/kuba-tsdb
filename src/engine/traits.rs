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

    /// Get tags for multiple series in a batch operation
    ///
    /// This method is essential for GROUP BY support. It efficiently fetches
    /// tag metadata for multiple series, enabling grouping of aggregation
    /// results by tag values.
    ///
    /// # Arguments
    ///
    /// * `series_ids` - List of series IDs to fetch tags for
    ///
    /// # Returns
    ///
    /// HashMap mapping series_id -> tags (HashMap<String, String>)
    ///
    /// # Default Implementation
    ///
    /// Returns empty tags for all series. Index backends should override
    /// this with an efficient batch implementation.
    async fn get_series_tags_batch(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<HashMap<SeriesId, HashMap<String, String>>, IndexError> {
        // Default implementation returns empty tags for each series
        // Redis implementation overrides with efficient batch fetch
        Ok(series_ids.iter().map(|&id| (id, HashMap::new())).collect())
    }

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
///
/// Contains metadata about a chunk for efficient query planning and pruning.
/// The statistics fields enable zone map pruning and accurate cost estimation.
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

    // ENH-003: Chunk statistics for cost estimation and zone map pruning
    /// Number of data points in this chunk (for accurate row count estimation)
    #[serde(default)]
    pub row_count: u32,

    /// Size of the chunk on disk in bytes (for I/O cost estimation)
    #[serde(default)]
    pub size_bytes: u64,

    /// Minimum value in the chunk (for zone map pruning)
    /// None if statistics haven't been computed
    #[serde(default)]
    pub min_value: Option<f64>,

    /// Maximum value in the chunk (for zone map pruning)
    /// None if statistics haven't been computed
    #[serde(default)]
    pub max_value: Option<f64>,
}

impl ChunkReference {
    /// Create a new chunk reference with basic information
    pub fn new(
        chunk_id: ChunkId,
        location: ChunkLocation,
        time_range: TimeRange,
        status: ChunkStatus,
    ) -> Self {
        Self {
            chunk_id,
            location,
            time_range,
            status,
            row_count: 0,
            size_bytes: 0,
            min_value: None,
            max_value: None,
        }
    }

    /// Create a chunk reference with full statistics
    #[allow(clippy::too_many_arguments)]
    pub fn with_statistics(
        chunk_id: ChunkId,
        location: ChunkLocation,
        time_range: TimeRange,
        status: ChunkStatus,
        row_count: u32,
        size_bytes: u64,
        min_value: f64,
        max_value: f64,
    ) -> Self {
        Self {
            chunk_id,
            location,
            time_range,
            status,
            row_count,
            size_bytes,
            min_value: Some(min_value),
            max_value: Some(max_value),
        }
    }

    /// Check if this chunk has computed statistics
    pub fn has_statistics(&self) -> bool {
        self.min_value.is_some() && self.max_value.is_some()
    }

    /// Check if this chunk can be pruned based on a value predicate
    ///
    /// Returns true if the chunk can definitely be skipped (no matching data).
    /// Returns false if the chunk might contain matching data.
    pub fn can_prune_by_value(&self, min_query: Option<f64>, max_query: Option<f64>) -> bool {
        // If we don't have statistics, we can't prune
        let Some(chunk_min) = self.min_value else {
            return false;
        };
        let Some(chunk_max) = self.max_value else {
            return false;
        };

        // Check if query range is completely outside chunk range
        if let Some(query_min) = min_query {
            if chunk_max < query_min {
                return true; // All chunk values are below query minimum
            }
        }
        if let Some(query_max) = max_query {
            if chunk_min > query_max {
                return true; // All chunk values are above query maximum
            }
        }

        false
    }
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // ENH-003: ChunkReference Statistics Tests
    // ========================================================================

    #[test]
    fn test_chunk_reference_new() {
        let chunk_ref = ChunkReference::new(
            ChunkId::new(),
            ChunkLocation {
                engine_id: "test".to_string(),
                path: "/test/chunk".to_string(),
                offset: None,
                size: Some(1024),
            },
            TimeRange {
                start: 1000,
                end: 2000,
            },
            ChunkStatus::Sealed,
        );

        assert_eq!(chunk_ref.status, ChunkStatus::Sealed);
        assert_eq!(chunk_ref.time_range.start, 1000);
        assert_eq!(chunk_ref.time_range.end, 2000);
        assert_eq!(chunk_ref.row_count, 0);
        assert_eq!(chunk_ref.size_bytes, 0);
        assert!(chunk_ref.min_value.is_none());
        assert!(chunk_ref.max_value.is_none());
    }

    #[test]
    fn test_chunk_reference_with_statistics() {
        let chunk_ref = ChunkReference::with_statistics(
            ChunkId::new(),
            ChunkLocation {
                engine_id: "test".to_string(),
                path: "/test/chunk".to_string(),
                offset: None,
                size: Some(1024),
            },
            TimeRange {
                start: 1000,
                end: 2000,
            },
            ChunkStatus::Sealed,
            10000,  // row_count
            102400, // size_bytes
            10.0,   // min_value
            100.0,  // max_value
        );

        assert_eq!(chunk_ref.row_count, 10000);
        assert_eq!(chunk_ref.size_bytes, 102400);
        assert_eq!(chunk_ref.min_value, Some(10.0));
        assert_eq!(chunk_ref.max_value, Some(100.0));
        assert!(chunk_ref.has_statistics());
    }

    #[test]
    fn test_chunk_reference_has_statistics() {
        let mut chunk_ref = ChunkReference::new(
            ChunkId::new(),
            ChunkLocation {
                engine_id: "test".to_string(),
                path: "/test/chunk".to_string(),
                offset: None,
                size: None,
            },
            TimeRange { start: 0, end: 100 },
            ChunkStatus::Sealed,
        );

        // Initially no statistics
        assert!(!chunk_ref.has_statistics());

        // Add partial statistics
        chunk_ref.min_value = Some(10.0);
        assert!(!chunk_ref.has_statistics());

        // Add complete statistics
        chunk_ref.max_value = Some(100.0);
        assert!(chunk_ref.has_statistics());
    }

    #[test]
    fn test_chunk_reference_can_prune_by_value() {
        let chunk_ref = ChunkReference::with_statistics(
            ChunkId::new(),
            ChunkLocation {
                engine_id: "test".to_string(),
                path: "/test/chunk".to_string(),
                offset: None,
                size: None,
            },
            TimeRange { start: 0, end: 100 },
            ChunkStatus::Sealed,
            1000,
            10240,
            50.0,  // min_value
            100.0, // max_value
        );

        // Query entirely above chunk values - can prune
        assert!(chunk_ref.can_prune_by_value(Some(200.0), None));

        // Query entirely below chunk values - can prune
        assert!(chunk_ref.can_prune_by_value(None, Some(25.0)));

        // Query overlaps chunk values - cannot prune
        assert!(!chunk_ref.can_prune_by_value(Some(75.0), None));
        assert!(!chunk_ref.can_prune_by_value(None, Some(75.0)));
        assert!(!chunk_ref.can_prune_by_value(Some(25.0), Some(75.0)));

        // No query bounds - cannot prune
        assert!(!chunk_ref.can_prune_by_value(None, None));
    }

    #[test]
    fn test_chunk_reference_prune_without_statistics() {
        let chunk_ref = ChunkReference::new(
            ChunkId::new(),
            ChunkLocation {
                engine_id: "test".to_string(),
                path: "/test/chunk".to_string(),
                offset: None,
                size: None,
            },
            TimeRange { start: 0, end: 100 },
            ChunkStatus::Sealed,
        );

        // Without statistics, cannot prune
        assert!(!chunk_ref.can_prune_by_value(Some(200.0), None));
        assert!(!chunk_ref.can_prune_by_value(None, Some(25.0)));
    }
}
