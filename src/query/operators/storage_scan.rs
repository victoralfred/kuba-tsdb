//! Storage-backed Scan Operator
//!
//! This operator integrates with the actual storage layer to read
//! time-series data from disk. It handles:
//! - Chunk discovery from LocalDiskEngine
//! - Zone map filtering for chunk pruning
//! - Parallel chunk decompression
//! - Batch formation from DataPoints
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::query::operators::StorageScanOperator;
//! use gorilla_tsdb::query::SeriesSelector;
//! use gorilla_tsdb::storage::LocalDiskEngine;
//! use gorilla_tsdb::types::TimeRange;
//! use std::sync::Arc;
//!
//! // Create storage engine (requires actual directory)
//! let engine = Arc::new(LocalDiskEngine::new("/data/tsdb".into()).unwrap());
//! // Create a series selector for series ID 1
//! let selector = SeriesSelector::by_id(1);
//! let scan = StorageScanOperator::new(engine, selector)
//!     .with_time_range(TimeRange::new(0, 3600000).unwrap())
//!     .with_batch_size(4096);
//! ```

use crate::query::ast::{Predicate, SeriesSelector};
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::{DataBatch, Operator};
use crate::storage::chunk::ChunkMetadata;
use crate::storage::{ChunkReader, LocalDiskEngine, QueryOptions};
use crate::types::{DataPoint, SeriesId, TimeRange};
use std::sync::Arc;

/// Storage-backed scan operator that reads from actual disk storage
///
/// This operator connects to LocalDiskEngine to discover chunks
/// and uses ChunkReader for decompression and reading.
pub struct StorageScanOperator {
    /// Storage engine reference for chunk discovery
    storage: Arc<LocalDiskEngine>,

    /// Chunk reader for decompression and async read operations
    reader: ChunkReader,

    /// Series selector for filtering
    selector: SeriesSelector,

    /// Time range filter (for zone map pruning)
    time_range: Option<TimeRange>,

    /// Optional predicate for value filtering
    predicate: Option<Predicate>,

    /// Batch size for output
    batch_size: usize,

    /// Chunks to scan (discovered and pruned)
    chunks: Vec<ChunkMetadata>,

    /// Current chunk index being processed
    current_chunk_idx: usize,

    /// Buffer of points from current chunk
    point_buffer: Vec<DataPoint>,

    /// Current position in point buffer
    buffer_position: usize,

    /// Whether initialization has been done
    initialized: bool,

    /// Whether scan is complete
    exhausted: bool,
}

impl StorageScanOperator {
    /// Create a new storage scan operator
    ///
    /// # Arguments
    ///
    /// * `storage` - Reference to LocalDiskEngine for chunk discovery
    /// * `selector` - Series selector to filter which series to scan
    pub fn new(storage: Arc<LocalDiskEngine>, selector: SeriesSelector) -> Self {
        Self {
            storage,
            reader: ChunkReader::new(),
            selector,
            time_range: None,
            predicate: None,
            batch_size: 4096,
            chunks: Vec::new(),
            current_chunk_idx: 0,
            point_buffer: Vec::new(),
            buffer_position: 0,
            initialized: false,
            exhausted: false,
        }
    }

    /// Set time range filter (enables zone map pruning)
    pub fn with_time_range(mut self, range: TimeRange) -> Self {
        self.time_range = Some(range);
        self
    }

    /// Set optional time range
    pub fn with_optional_time_range(mut self, range: Option<TimeRange>) -> Self {
        self.time_range = range;
        self
    }

    /// Set predicate filter for value filtering
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Initialize scan by discovering and pruning chunks
    ///
    /// This method:
    /// 1. Gets chunk metadata from storage engine
    /// 2. Filters chunks by series selector
    /// 3. Applies zone map filtering to prune chunks outside time range
    fn initialize(&mut self) -> Result<(), QueryError> {
        if self.initialized {
            return Ok(());
        }

        // Get series ID from selector
        let series_id = match self.selector.series_id {
            Some(id) => id,
            None => {
                // Without a specific series ID, we can't query storage efficiently
                // In production, this would query Redis for matching series
                self.initialized = true;
                self.exhausted = true;
                return Ok(());
            }
        };

        // Get chunks from storage index
        let all_chunks = self.storage.get_chunks_for_series(series_id);

        // Apply zone map filtering (time range pruning)
        self.chunks = all_chunks
            .into_iter()
            .filter(|chunk| self.chunk_overlaps_time_range(chunk))
            .collect();

        // Sort by start timestamp for ordered output
        self.chunks.sort_by_key(|c| c.start_timestamp);

        self.initialized = true;

        if self.chunks.is_empty() {
            self.exhausted = true;
        }

        Ok(())
    }

    /// Check if a chunk's time range overlaps with the query time range
    ///
    /// This is zone map filtering - we skip chunks that cannot contain
    /// any points in our time range.
    fn chunk_overlaps_time_range(&self, chunk: &ChunkMetadata) -> bool {
        match &self.time_range {
            Some(range) => {
                // Chunk overlaps if its range intersects with query range
                // chunk: [start_ts, end_ts]
                // query: [start, end]
                // overlaps if: chunk_end >= query_start AND chunk_start <= query_end
                chunk.end_timestamp >= range.start && chunk.start_timestamp <= range.end
            }
            None => true, // No time filter, include all chunks
        }
    }

    /// Load points from the current chunk (async version)
    ///
    /// Uses ChunkReader to decompress the chunk and load points into buffer.
    /// This async path is preferred when called from async context for
    /// better concurrency with other async operations.
    pub async fn load_current_chunk(&mut self) -> Result<bool, QueryError> {
        if self.current_chunk_idx >= self.chunks.len() {
            return Ok(false);
        }

        let chunk = &self.chunks[self.current_chunk_idx];

        // Build query options for chunk reader
        let options = QueryOptions {
            start_time: self.time_range.as_ref().map(|r| r.start),
            end_time: self.time_range.as_ref().map(|r| r.end),
            limit: None, // No limit at chunk level, we batch ourselves
            use_mmap: true,
        };

        // Read chunk using async API
        let points = self
            .reader
            .read_chunk(&chunk.path, options)
            .await
            .map_err(|e| QueryError::execution(format!("Failed to read chunk: {}", e)))?;

        // Apply predicate filtering if present
        self.point_buffer = if let Some(ref pred) = self.predicate {
            points
                .into_iter()
                .filter(|p| pred.evaluate(p.value))
                .collect()
        } else {
            points
        };

        self.buffer_position = 0;
        self.current_chunk_idx += 1;

        Ok(!self.point_buffer.is_empty())
    }

    /// Load current chunk synchronously
    ///
    /// Uses spawn_blocking to avoid blocking the async runtime threadpool.
    /// This is needed because Operator trait is sync, but chunk reading is async.
    /// spawn_blocking moves the blocking work to a dedicated thread pool (PERF-002).
    fn load_current_chunk_sync(&mut self) -> Result<bool, QueryError> {
        if self.current_chunk_idx >= self.chunks.len() {
            return Ok(false);
        }

        let chunk = &self.chunks[self.current_chunk_idx];

        // Build query options for chunk reader
        let options = QueryOptions {
            start_time: self.time_range.as_ref().map(|r| r.start),
            end_time: self.time_range.as_ref().map(|r| r.end),
            limit: None,
            use_mmap: true,
        };

        // Clone data needed for the blocking task
        let path = chunk.path.clone();

        // Use spawn_blocking to run async code on a dedicated blocking thread pool
        // This avoids blocking the main async runtime threads (PERF-002)
        let points = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                // spawn_blocking moves the I/O work to a dedicated blocking pool
                // The inner block_on runs the async chunk read
                let read_result = tokio::task::spawn_blocking(move || {
                    // Create a new runtime for the blocking thread to run async code
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create runtime for chunk read");

                    let reader = ChunkReader::new();
                    rt.block_on(reader.read_chunk(&path, options))
                })
                .await
                .map_err(|e| QueryError::execution(format!("Task join error: {}", e)))?;

                read_result
                    .map_err(|e| QueryError::execution(format!("Failed to read chunk: {}", e)))
            })
        })?;

        // Apply predicate filtering if present
        self.point_buffer = if let Some(ref pred) = self.predicate {
            points
                .into_iter()
                .filter(|p| pred.evaluate(p.value))
                .collect()
        } else {
            points
        };

        self.buffer_position = 0;
        self.current_chunk_idx += 1;

        Ok(!self.point_buffer.is_empty())
    }

    /// Form a batch from the point buffer
    fn form_batch(&mut self) -> DataBatch {
        let mut batch = DataBatch::with_capacity(self.batch_size);
        let mut count = 0;

        while self.buffer_position < self.point_buffer.len() && count < self.batch_size {
            let point = &self.point_buffer[self.buffer_position];
            batch.push_with_series(point.timestamp, point.value, point.series_id);
            self.buffer_position += 1;
            count += 1;
        }

        batch
    }
}

impl Operator for StorageScanOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // Check for cancellation/timeout
        if ctx.should_stop() {
            if ctx.is_timed_out() {
                return Err(QueryError::timeout("Storage scan operator timed out"));
            }
            return Ok(None);
        }

        // Initialize on first call
        if !self.initialized {
            self.initialize()?;
        }

        if self.exhausted {
            return Ok(None);
        }

        // Try to form a batch from current buffer
        loop {
            // If we have data in buffer, form a batch
            if self.buffer_position < self.point_buffer.len() {
                let batch = self.form_batch();

                if !batch.is_empty() {
                    // Track memory usage
                    let mem_size = batch.memory_size();
                    if !ctx.allocate_memory(mem_size) {
                        return Err(QueryError::resource_limit(
                            "Memory limit exceeded during storage scan",
                        ));
                    }
                    ctx.record_rows(batch.len());
                    return Ok(Some(batch));
                }
            }

            // Buffer exhausted, load next chunk
            let has_data = self.load_current_chunk_sync()?;

            if !has_data {
                // No more chunks with data
                if self.current_chunk_idx >= self.chunks.len() {
                    self.exhausted = true;
                    return Ok(None);
                }
                // Try next chunk
                continue;
            }
        }
    }

    fn reset(&mut self) {
        self.current_chunk_idx = 0;
        self.point_buffer.clear();
        self.buffer_position = 0;
        self.exhausted = false;
        // Keep initialized = true to avoid re-discovering chunks
    }

    fn name(&self) -> &'static str {
        "StorageScan"
    }

    fn estimated_cardinality(&self) -> usize {
        // Sum of point counts from all chunks
        self.chunks.iter().map(|c| c.point_count as usize).sum()
    }
}

// ============================================================================
// Helper trait for storage query operations
// ============================================================================

/// Extension trait for query-related storage operations
///
/// This trait provides query-specific functionality that can be implemented
/// by different storage backends.
pub trait StorageQueryExt {
    /// Get chunk metadata for a series
    fn query_chunks_for_series(&self, series_id: SeriesId) -> Vec<ChunkMetadata>;

    /// Get all series IDs in storage
    fn query_all_series(&self) -> Vec<SeriesId>;
}

impl StorageQueryExt for LocalDiskEngine {
    fn query_chunks_for_series(&self, series_id: SeriesId) -> Vec<ChunkMetadata> {
        // Use the native method we added to LocalDiskEngine
        self.get_chunks_for_series(series_id)
    }

    fn query_all_series(&self) -> Vec<SeriesId> {
        self.get_all_series_ids()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    // Helper to create test chunk metadata
    fn make_chunk(series_id: SeriesId, start: i64, end: i64, points: u32) -> ChunkMetadata {
        ChunkMetadata {
            chunk_id: crate::types::ChunkId::new(),
            series_id,
            path: PathBuf::from("/tmp/chunk.gor"),
            start_timestamp: start,
            end_timestamp: end,
            point_count: points,
            size_bytes: 1024,
            uncompressed_size: 0,
            compression: crate::storage::chunk::CompressionType::Gorilla,
            created_at: 0,
            last_accessed: 0,
        }
    }

    // ===== StorageScanOperator construction tests =====

    #[test]
    fn test_storage_scan_operator_new() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector);

        assert_eq!(scan.batch_size, 4096);
        assert!(scan.time_range.is_none());
        assert!(scan.predicate.is_none());
        assert!(!scan.initialized);
        assert!(!scan.exhausted);
    }

    #[test]
    fn test_storage_scan_operator_with_time_range() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector).with_time_range(TimeRange {
            start: 100,
            end: 200,
        });

        assert!(scan.time_range.is_some());
        assert_eq!(scan.time_range.unwrap().start, 100);
        assert_eq!(scan.time_range.unwrap().end, 200);
    }

    #[test]
    fn test_storage_scan_operator_with_optional_time_range_some() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan =
            StorageScanOperator::new(storage, selector).with_optional_time_range(Some(TimeRange {
                start: 50,
                end: 150,
            }));

        assert!(scan.time_range.is_some());
        assert_eq!(scan.time_range.unwrap().start, 50);
    }

    #[test]
    fn test_storage_scan_operator_with_optional_time_range_none() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector).with_optional_time_range(None);

        assert!(scan.time_range.is_none());
    }

    #[test]
    fn test_storage_scan_operator_with_predicate() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector)
            .with_predicate(Predicate::gt("value", 10.0));

        assert!(scan.predicate.is_some());
    }

    #[test]
    fn test_storage_scan_operator_with_batch_size() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector).with_batch_size(8192);

        assert_eq!(scan.batch_size, 8192);
    }

    #[test]
    fn test_storage_scan_operator_chaining() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector)
            .with_time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .with_predicate(Predicate::lt("value", 100.0))
            .with_batch_size(2048);

        assert!(scan.time_range.is_some());
        assert!(scan.predicate.is_some());
        assert_eq!(scan.batch_size, 2048);
    }

    // ===== Time range overlap tests =====

    #[test]
    fn test_chunk_time_overlap() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        // Query with time range
        let scan = StorageScanOperator::new(storage, selector).with_time_range(TimeRange {
            start: 100,
            end: 200,
        });

        // Test chunk that overlaps
        let overlapping_chunk = make_chunk(1, 50, 150, 100);
        assert!(scan.chunk_overlaps_time_range(&overlapping_chunk));

        // Test chunk that doesn't overlap (before range)
        let before_chunk = make_chunk(1, 0, 50, 50);
        assert!(!scan.chunk_overlaps_time_range(&before_chunk));

        // Test chunk that doesn't overlap (after range)
        let after_chunk = make_chunk(1, 300, 400, 100);
        assert!(!scan.chunk_overlaps_time_range(&after_chunk));
    }

    #[test]
    fn test_chunk_time_overlap_exact_boundary() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector).with_time_range(TimeRange {
            start: 100,
            end: 200,
        });

        // Chunk ending exactly at query start
        let edge_before = make_chunk(1, 50, 100, 50);
        assert!(scan.chunk_overlaps_time_range(&edge_before)); // includes boundary

        // Chunk starting exactly at query end
        let edge_after = make_chunk(1, 200, 250, 50);
        assert!(scan.chunk_overlaps_time_range(&edge_after)); // includes boundary
    }

    #[test]
    fn test_chunk_time_overlap_contained() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector).with_time_range(TimeRange {
            start: 100,
            end: 200,
        });

        // Chunk fully contained in query range
        let contained = make_chunk(1, 120, 180, 60);
        assert!(scan.chunk_overlaps_time_range(&contained));

        // Query fully contained in chunk range
        let containing = make_chunk(1, 50, 250, 200);
        assert!(scan.chunk_overlaps_time_range(&containing));
    }

    #[test]
    fn test_no_time_range_includes_all() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        // Query without time range should include all chunks
        let scan = StorageScanOperator::new(storage, selector);

        let any_chunk = make_chunk(1, 1000000, 2000000, 1000);
        assert!(scan.chunk_overlaps_time_range(&any_chunk));
    }

    // ===== Estimated cardinality tests =====

    #[test]
    fn test_estimated_cardinality() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector);

        scan.chunks = vec![make_chunk(1, 0, 100, 100), make_chunk(1, 100, 200, 200)];

        assert_eq!(scan.estimated_cardinality(), 300);
    }

    #[test]
    fn test_estimated_cardinality_empty() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector);
        assert_eq!(scan.estimated_cardinality(), 0);
    }

    #[test]
    fn test_estimated_cardinality_single_chunk() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector);
        scan.chunks = vec![make_chunk(1, 0, 100, 500)];

        assert_eq!(scan.estimated_cardinality(), 500);
    }

    // ===== Operator trait tests =====

    #[test]
    fn test_operator_name() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let scan = StorageScanOperator::new(storage, selector);
        assert_eq!(scan.name(), "StorageScan");
    }

    #[test]
    fn test_reset() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector);

        // Simulate some state
        scan.current_chunk_idx = 5;
        scan.buffer_position = 100;
        scan.exhausted = true;
        scan.point_buffer = vec![DataPoint::new(1, 1000, 42.0)];

        scan.reset();

        assert_eq!(scan.current_chunk_idx, 0);
        assert_eq!(scan.buffer_position, 0);
        assert!(!scan.exhausted);
        assert!(scan.point_buffer.is_empty());
    }

    // ===== Initialize tests =====

    #[test]
    fn test_initialize_without_series_id() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );

        // Selector without series ID
        let selector = SeriesSelector {
            series_id: None,
            measurement: Some("test".to_string()),
            tag_filters: vec![],
        };

        let mut scan = StorageScanOperator::new(storage, selector);

        // Initialize should mark as exhausted when no series ID
        let result = scan.initialize();
        assert!(result.is_ok());
        assert!(scan.initialized);
        assert!(scan.exhausted);
    }

    #[test]
    fn test_initialize_idempotent() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector);

        // First init
        scan.initialize().unwrap();
        assert!(scan.initialized);

        // Second init should be no-op
        let chunk_count = scan.chunks.len();
        scan.initialize().unwrap();
        assert_eq!(scan.chunks.len(), chunk_count);
    }

    // ===== Form batch tests =====

    #[test]
    fn test_form_batch_empty_buffer() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector);
        let batch = scan.form_batch();

        assert!(batch.is_empty());
    }

    #[test]
    fn test_form_batch_with_data() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        let selector = SeriesSelector::by_id(1);

        let mut scan = StorageScanOperator::new(storage, selector).with_batch_size(2);

        // Add some test data to buffer
        scan.point_buffer = vec![
            DataPoint::new(1, 1000, 1.0),
            DataPoint::new(1, 2000, 2.0),
            DataPoint::new(1, 3000, 3.0),
        ];

        // First batch should have 2 points (batch size)
        let batch1 = scan.form_batch();
        assert_eq!(batch1.len(), 2);

        // Second batch should have 1 point (remaining)
        let batch2 = scan.form_batch();
        assert_eq!(batch2.len(), 1);

        // Third batch should be empty
        let batch3 = scan.form_batch();
        assert!(batch3.is_empty());
    }

    // ===== Predicate filtering tests =====

    #[test]
    fn test_predicate_gt() {
        let pred = Predicate::gt("value", 50.0);
        assert!(pred.evaluate(60.0));
        assert!(!pred.evaluate(50.0));
        assert!(!pred.evaluate(40.0));
    }

    #[test]
    fn test_predicate_gte() {
        let pred = Predicate::gte("value", 50.0);
        assert!(pred.evaluate(60.0));
        assert!(pred.evaluate(50.0));
        assert!(!pred.evaluate(40.0));
    }

    #[test]
    fn test_predicate_lt() {
        let pred = Predicate::lt("value", 50.0);
        assert!(pred.evaluate(40.0));
        assert!(!pred.evaluate(50.0));
        assert!(!pred.evaluate(60.0));
    }

    #[test]
    fn test_predicate_lte() {
        let pred = Predicate::lte("value", 50.0);
        assert!(pred.evaluate(40.0));
        assert!(pred.evaluate(50.0));
        assert!(!pred.evaluate(60.0));
    }

    #[test]
    fn test_predicate_eq() {
        let pred = Predicate::eq("value", 42.0);
        assert!(pred.evaluate(42.0));
        assert!(!pred.evaluate(41.0));
        assert!(!pred.evaluate(43.0));
    }

    #[test]
    fn test_predicate_ne() {
        let pred = Predicate::ne("value", 42.0);
        assert!(!pred.evaluate(42.0));
        assert!(pred.evaluate(41.0));
        assert!(pred.evaluate(43.0));
    }

    // ===== StorageQueryExt tests =====

    #[test]
    fn test_storage_query_ext_query_chunks_for_series() {
        let dir = tempdir().unwrap();
        let storage =
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine");

        // Query for non-existent series should return empty
        let chunks = storage.query_chunks_for_series(999);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_storage_query_ext_query_all_series() {
        let dir = tempdir().unwrap();
        let storage =
            LocalDiskEngine::new(dir.path().to_path_buf()).expect("Failed to create engine");

        // Empty storage should return empty series list
        let series = storage.query_all_series();
        assert!(series.is_empty());
    }
}
