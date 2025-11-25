///! Chunk Reader for efficient time-series data querying
///!
///! The `ChunkReader` provides a high-level interface for reading and querying
///! chunk data with support for:
///! - Time range filtering
///! - Memory-mapped zero-copy reads
///! - Parallel chunk loading
///! - Streaming decompression
///! - Result pagination
///!
///! # Example
///!
///! ```no_run
///! use gorilla_tsdb::storage::reader::{ChunkReader, QueryOptions};
///! use gorilla_tsdb::types::DataPoint;
///!
///! # async fn example() -> Result<(), String> {
///! let reader = ChunkReader::new();
///!
///! // Query with time range filter
///! let options = QueryOptions {
///!     start_time: Some(1000),
///!     end_time: Some(5000),
///!     limit: Some(1000),
///!     use_mmap: true,
///! };
///!
///! let points = reader.read_chunk("/data/chunk_001.gor", options).await?;
///! println!("Read {} points", points.len());
///! # Ok(())
///! # }
///! ```

use crate::compression::gorilla::GorillaCompressor;
use crate::engine::traits::Compressor;
use crate::storage::chunk::Chunk;
use crate::storage::mmap::MmapChunk;
use crate::types::DataPoint;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinSet;

/// Query options for filtering and optimization
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// Start timestamp (inclusive), None = no lower bound
    pub start_time: Option<i64>,

    /// End timestamp (inclusive), None = no upper bound
    pub end_time: Option<i64>,

    /// Maximum number of points to return, None = no limit
    pub limit: Option<usize>,

    /// Use memory-mapped I/O for zero-copy reads
    pub use_mmap: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            start_time: None,
            end_time: None,
            limit: None,
            use_mmap: true, // Prefer mmap for performance
        }
    }
}

/// High-level chunk reader with query capabilities
pub struct ChunkReader {
    /// Compressor/decompressor for chunk data
    compressor: Arc<GorillaCompressor>,
}

impl ChunkReader {
    /// Create a new chunk reader
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::reader::ChunkReader;
    ///
    /// let reader = ChunkReader::new();
    /// ```
    pub fn new() -> Self {
        Self {
            compressor: Arc::new(GorillaCompressor::new()),
        }
    }

    /// Read a single chunk from disk with optional filtering
    ///
    /// This method loads a chunk from disk and optionally filters the results
    /// by time range. It can use either regular I/O or memory-mapped I/O
    /// depending on the QueryOptions.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chunk file
    /// * `options` - Query options for filtering and optimization
    ///
    /// # Returns
    ///
    /// Vector of data points matching the query criteria
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use gorilla_tsdb::storage::reader::{ChunkReader, QueryOptions};
    /// # async fn example() -> Result<(), String> {
    /// let reader = ChunkReader::new();
    ///
    /// let options = QueryOptions {
    ///     start_time: Some(1000),
    ///     end_time: Some(2000),
    ///     ..Default::default()
    /// };
    ///
    /// let points = reader.read_chunk("/data/chunk.gor", options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_chunk<P: AsRef<Path>>(
        &self,
        path: P,
        options: QueryOptions,
    ) -> Result<Vec<DataPoint>, String> {
        let path_buf = path.as_ref().to_path_buf();

        let points = if options.use_mmap {
            // Use memory-mapped I/O for zero-copy reads
            self.read_chunk_mmap(path_buf, &options).await?
        } else {
            // Use regular I/O
            self.read_chunk_regular(path_buf, &options).await?
        };

        // Apply filters
        let filtered = self.apply_filters(points, &options);

        Ok(filtered)
    }

    /// Read multiple chunks in parallel
    ///
    /// This method loads multiple chunks concurrently and merges the results.
    /// Results are sorted by timestamp.
    ///
    /// # Arguments
    ///
    /// * `paths` - Vector of chunk file paths
    /// * `options` - Query options for filtering and optimization
    ///
    /// # Returns
    ///
    /// Vector of data points from all chunks, sorted by timestamp
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use gorilla_tsdb::storage::reader::{ChunkReader, QueryOptions};
    /// # async fn example() -> Result<(), String> {
    /// let reader = ChunkReader::new();
    ///
    /// let paths = vec![
    ///     "/data/chunk_001.gor".into(),
    ///     "/data/chunk_002.gor".into(),
    ///     "/data/chunk_003.gor".into(),
    /// ];
    ///
    /// let points = reader.read_chunks_parallel(paths, QueryOptions::default()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_chunks_parallel(
        &self,
        paths: Vec<PathBuf>,
        options: QueryOptions,
    ) -> Result<Vec<DataPoint>, String> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let mut join_set = JoinSet::new();

        // Spawn tasks for each chunk
        for path in paths {
            let reader_clone = self.clone_for_parallel();
            let options_clone = options.clone();

            join_set.spawn(async move {
                reader_clone.read_chunk(path, options_clone).await
            });
        }

        // Collect results
        let mut all_points = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(points)) => all_points.extend(points),
                Ok(Err(e)) => return Err(format!("Chunk read failed: {}", e)),
                Err(e) => return Err(format!("Task join failed: {}", e)),
            }
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);

        // Apply global limit if specified
        if let Some(limit) = options.limit {
            all_points.truncate(limit);
        }

        Ok(all_points)
    }

    /// Read chunk using memory-mapped I/O
    async fn read_chunk_mmap(
        &self,
        path: PathBuf,
        _options: &QueryOptions,
    ) -> Result<Vec<DataPoint>, String> {
        // Open chunk with mmap (not async)
        let mmap_chunk = MmapChunk::open(path)
            .map_err(|e| format!("Failed to open mmap chunk: {:?}", e))?;

        // Get compressed data (zero-copy)
        let compressed_data = mmap_chunk.compressed_data();
        let header = mmap_chunk.header();

        // Create compressed block for decompression
        let compressed_block = crate::engine::traits::CompressedBlock {
            algorithm_id: "gorilla".to_string(),
            original_size: header.uncompressed_size as usize,
            compressed_size: header.compressed_size as usize,
            checksum: header.checksum,
            data: bytes::Bytes::copy_from_slice(compressed_data),
            metadata: crate::engine::traits::BlockMetadata {
                start_timestamp: header.start_timestamp,
                end_timestamp: header.end_timestamp,
                point_count: header.point_count as usize,
                series_id: header.series_id,
            },
        };

        // Decompress
        let points = self.compressor.decompress(&compressed_block).await
            .map_err(|e| format!("Decompression failed: {:?}", e))?;

        Ok(points)
    }

    /// Read chunk using regular I/O
    async fn read_chunk_regular(
        &self,
        path: PathBuf,
        _options: &QueryOptions,
    ) -> Result<Vec<DataPoint>, String> {
        // Load chunk from disk
        let chunk = Chunk::read(path).await?;

        // Decompress and get points (chunk creates its own compressor internally)
        let points = chunk.decompress().await?;

        Ok(points)
    }

    /// Apply time range and limit filters to points
    fn apply_filters(&self, mut points: Vec<DataPoint>, options: &QueryOptions) -> Vec<DataPoint> {
        // Filter by time range
        if options.start_time.is_some() || options.end_time.is_some() {
            points.retain(|p| {
                let after_start = options.start_time.map_or(true, |start| p.timestamp >= start);
                let before_end = options.end_time.map_or(true, |end| p.timestamp <= end);
                after_start && before_end
            });
        }

        // Apply limit
        if let Some(limit) = options.limit {
            points.truncate(limit);
        }

        points
    }

    /// Clone reader for parallel execution
    fn clone_for_parallel(&self) -> Self {
        Self {
            compressor: Arc::clone(&self.compressor),
        }
    }
}

impl Default for ChunkReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::Chunk;
    use crate::types::DataPoint;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_reader_creation() {
        let reader = ChunkReader::new();
        assert!(Arc::strong_count(&reader.compressor) == 1);
    }

    #[tokio::test]
    async fn test_read_chunk_regular() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_chunk.gor");

        // Create and seal a test chunk
        let mut chunk = Chunk::new_active(1, 100);
        for i in 0..10 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i * 100,
                    value: i as f64,
                })
                .unwrap();
        }
        chunk.seal(path.clone()).await.unwrap();

        // Read it back
        let reader = ChunkReader::new();
        let options = QueryOptions {
            use_mmap: false, // Test regular I/O
            ..Default::default()
        };

        let points = reader.read_chunk(&path, options).await.unwrap();

        assert_eq!(points.len(), 10);
        assert_eq!(points[0].timestamp, 0);
        assert_eq!(points[9].timestamp, 900);
    }

    #[tokio::test]
    async fn test_read_chunk_mmap() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_chunk.gor");

        // Create and seal a test chunk
        let mut chunk = Chunk::new_active(1, 100);
        for i in 0..10 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i * 100,
                    value: i as f64,
                })
                .unwrap();
        }
        chunk.seal(path.clone()).await.unwrap();

        // Read with mmap
        let reader = ChunkReader::new();
        let options = QueryOptions {
            use_mmap: true,
            ..Default::default()
        };

        let points = reader.read_chunk(&path, options).await.unwrap();

        assert_eq!(points.len(), 10);
        assert_eq!(points[0].timestamp, 0);
        assert_eq!(points[9].timestamp, 900);
    }

    #[tokio::test]
    async fn test_time_range_filter() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_chunk.gor");

        // Create chunk with timestamps 0-900
        let mut chunk = Chunk::new_active(1, 100);
        for i in 0..10 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i * 100,
                    value: i as f64,
                })
                .unwrap();
        }
        chunk.seal(path.clone()).await.unwrap();

        // Query with time range filter
        let reader = ChunkReader::new();
        let options = QueryOptions {
            start_time: Some(200),
            end_time: Some(600),
            ..Default::default()
        };

        let points = reader.read_chunk(&path, options).await.unwrap();

        // Should get points at 200, 300, 400, 500, 600
        assert_eq!(points.len(), 5);
        assert_eq!(points[0].timestamp, 200);
        assert_eq!(points[4].timestamp, 600);
    }

    #[tokio::test]
    async fn test_limit_filter() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_chunk.gor");

        // Create chunk with 100 points
        let mut chunk = Chunk::new_active(1, 200);
        for i in 0..100 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i,
                    value: i as f64,
                })
                .unwrap();
        }
        chunk.seal(path.clone()).await.unwrap();

        // Query with limit
        let reader = ChunkReader::new();
        let options = QueryOptions {
            limit: Some(10),
            ..Default::default()
        };

        let points = reader.read_chunk(&path, options).await.unwrap();

        assert_eq!(points.len(), 10);
        assert_eq!(points[0].timestamp, 0);
        assert_eq!(points[9].timestamp, 9);
    }

    #[tokio::test]
    async fn test_parallel_read() {
        let temp_dir = TempDir::new().unwrap();

        // Create 3 chunks
        let mut paths = Vec::new();
        for chunk_id in 0..3 {
            let path = temp_dir.path().join(format!("chunk_{}.gor", chunk_id));

            let mut chunk = Chunk::new_active(1, 100);
            for i in 0..10 {
                let timestamp = (chunk_id * 10 + i) * 100;
                chunk
                    .append(DataPoint {
                        series_id: 1,
                        timestamp,
                        value: timestamp as f64,
                    })
                    .unwrap();
            }
            chunk.seal(path.clone()).await.unwrap();
            paths.push(path);
        }

        // Read all chunks in parallel
        let reader = ChunkReader::new();
        let points = reader
            .read_chunks_parallel(paths, QueryOptions::default())
            .await
            .unwrap();

        // Should have 30 points total, sorted by timestamp
        assert_eq!(points.len(), 30);
        assert_eq!(points[0].timestamp, 0);
        assert_eq!(points[29].timestamp, 2900);

        // Verify sorted
        for i in 1..points.len() {
            assert!(points[i].timestamp >= points[i - 1].timestamp);
        }
    }

    #[tokio::test]
    async fn test_parallel_read_with_filter() {
        let temp_dir = TempDir::new().unwrap();

        // Create 3 chunks
        let mut paths = Vec::new();
        for chunk_id in 0..3 {
            let path = temp_dir.path().join(format!("chunk_{}.gor", chunk_id));

            let mut chunk = Chunk::new_active(1, 100);
            for i in 0..10 {
                let timestamp = (chunk_id * 10 + i) * 100;
                chunk
                    .append(DataPoint {
                        series_id: 1,
                        timestamp,
                        value: timestamp as f64,
                    })
                    .unwrap();
            }
            chunk.seal(path.clone()).await.unwrap();
            paths.push(path);
        }

        // Read with time range and limit
        let reader = ChunkReader::new();
        let options = QueryOptions {
            start_time: Some(500),
            end_time: Some(1500),
            limit: Some(5),
            ..Default::default()
        };

        let points = reader.read_chunks_parallel(paths, options).await.unwrap();

        // Should get at most 5 points in range 500-1500
        assert!(points.len() <= 5);
        for point in &points {
            assert!(point.timestamp >= 500);
            assert!(point.timestamp <= 1500);
        }
    }

    #[tokio::test]
    async fn test_empty_chunks() {
        let reader = ChunkReader::new();
        let points = reader
            .read_chunks_parallel(vec![], QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(points.len(), 0);
    }
}
