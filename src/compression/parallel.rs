//! Parallel Compression Module
//!
//! Provides multi-threaded compression for high-throughput ingestion scenarios.
//! When multiple series are being written simultaneously, this module enables
//! parallel compression of different series/chunks.
//!
//! # Architecture
//!
//! ```text
//! Incoming Data:
//! ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
//! │  Series A    │ │  Series B    │ │  Series C    │
//! │  1000 pts    │ │  1000 pts    │ │  1000 pts    │
//! └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
//!        │                │                │
//!        └────────────────┼────────────────┘
//!                         │
//!                  ┌──────▼───────┐
//!                  │ Thread Pool  │
//!                  │  (N workers) │
//!                  └──────────────┘
//!                         │
//!        ┌────────────────┼────────────────┐
//!        │                │                │
//!        ▼                ▼                ▼
//! ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
//! │ Compress A   │ │ Compress B   │ │ Compress C   │
//! │ (Thread 1)   │ │ (Thread 2)   │ │ (Thread 3)   │
//! └──────────────┘ └──────────────┘ └──────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::parallel::{ParallelCompressor, ParallelConfig};
//!
//! let config = ParallelConfig::default();
//! let compressor = ParallelCompressor::new(config);
//!
//! let batches = vec![
//!     (series_id_1, points_1),
//!     (series_id_2, points_2),
//! ];
//!
//! let results = compressor.compress_batch(batches).await;
//! ```

use crate::compression::AhpacCompressor;
use crate::types::{DataPoint, SeriesId};
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{debug, warn};

/// Configuration for parallel compression
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of concurrent compression tasks
    /// Default: number of CPU cores
    pub max_concurrent_tasks: usize,

    /// Minimum points per batch to enable parallel processing
    /// Smaller batches use single-threaded compression
    pub min_parallel_batch_size: usize,

    /// Whether to use AHPAC or Kuba compression
    pub use_ahpac: bool,

    /// Enable compression statistics collection
    pub collect_stats: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: num_cpus::get(),
            min_parallel_batch_size: 500,
            use_ahpac: true,
            collect_stats: true,
        }
    }
}

impl ParallelConfig {
    /// Create config optimized for high throughput (more threads)
    pub fn high_throughput() -> Self {
        Self {
            max_concurrent_tasks: num_cpus::get() * 2,
            min_parallel_batch_size: 100,
            use_ahpac: true,
            collect_stats: false,
        }
    }

    /// Create config optimized for low latency (fewer threads, less overhead)
    pub fn low_latency() -> Self {
        Self {
            max_concurrent_tasks: 4,
            min_parallel_batch_size: 1000,
            use_ahpac: true,
            collect_stats: false,
        }
    }
}

/// Result of a parallel compression operation
#[derive(Debug)]
pub struct ParallelCompressionResult {
    /// Series ID that was compressed
    pub series_id: SeriesId,
    /// Compressed data bytes
    pub compressed_data: Vec<u8>,
    /// Number of points compressed
    pub point_count: usize,
    /// Original uncompressed size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression time in microseconds
    pub compression_time_us: u64,
    /// Error if compression failed
    pub error: Option<String>,
}

impl ParallelCompressionResult {
    /// Calculate compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 0.0;
        }
        self.original_size as f64 / self.compressed_size as f64
    }

    /// Calculate bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        if self.point_count == 0 {
            return 0.0;
        }
        (self.compressed_size * 8) as f64 / self.point_count as f64
    }
}

/// Aggregate statistics for a batch of parallel compressions
#[derive(Debug, Default)]
pub struct ParallelBatchStats {
    /// Total series processed
    pub series_count: usize,
    /// Total points compressed
    pub total_points: usize,
    /// Total original bytes
    pub total_original_bytes: usize,
    /// Total compressed bytes
    pub total_compressed_bytes: usize,
    /// Total compression time across all threads (microseconds)
    pub total_compression_time_us: u64,
    /// Wall clock time for entire batch (microseconds)
    pub wall_time_us: u64,
    /// Number of failed compressions
    pub failed_count: usize,
    /// Parallelism efficiency (0.0 - 1.0)
    pub parallelism_efficiency: f64,
}

impl ParallelBatchStats {
    /// Calculate overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.total_compressed_bytes == 0 {
            return 0.0;
        }
        self.total_original_bytes as f64 / self.total_compressed_bytes as f64
    }

    /// Calculate average bits per sample
    pub fn avg_bits_per_sample(&self) -> f64 {
        if self.total_points == 0 {
            return 0.0;
        }
        (self.total_compressed_bytes * 8) as f64 / self.total_points as f64
    }

    /// Calculate throughput in MB/s
    pub fn throughput_mbps(&self) -> f64 {
        if self.wall_time_us == 0 {
            return 0.0;
        }
        (self.total_original_bytes as f64 / 1_000_000.0) / (self.wall_time_us as f64 / 1_000_000.0)
    }
}

/// Parallel compressor for multi-series batch processing
pub struct ParallelCompressor {
    config: ParallelConfig,
    compressor: Arc<AhpacCompressor>,
}

impl ParallelCompressor {
    /// Create a new parallel compressor with the given configuration
    pub fn new(config: ParallelConfig) -> Self {
        Self {
            config,
            compressor: Arc::new(AhpacCompressor::new()),
        }
    }

    /// Create with default configuration
    pub fn default_new() -> Self {
        Self::new(ParallelConfig::default())
    }

    /// Compress a batch of series data in parallel
    ///
    /// # Arguments
    ///
    /// * `batches` - Vector of (series_id, points) pairs to compress
    ///
    /// # Returns
    ///
    /// Vector of compression results for each series
    pub async fn compress_batch(
        &self,
        batches: Vec<(SeriesId, Vec<DataPoint>)>,
    ) -> Vec<ParallelCompressionResult> {
        if batches.is_empty() {
            return Vec::new();
        }

        let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();

        // Use single-threaded for small batches
        if batches.len() == 1 || total_points < self.config.min_parallel_batch_size {
            return self.compress_sequential(batches).await;
        }

        debug!(
            series_count = batches.len(),
            total_points = total_points,
            max_tasks = self.config.max_concurrent_tasks,
            "Starting parallel compression"
        );

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_concurrent_tasks,
        ));

        for (series_id, points) in batches {
            let compressor = Arc::clone(&self.compressor);
            let permit = Arc::clone(&semaphore);

            join_set.spawn(async move {
                // Acquire semaphore permit to limit concurrency
                let _permit = permit.acquire().await;

                let start = std::time::Instant::now();
                let point_count = points.len();
                let original_size = point_count * 16; // 8 bytes timestamp + 8 bytes value

                match Self::compress_single(&compressor, series_id, &points).await {
                    Ok(compressed_data) => ParallelCompressionResult {
                        series_id,
                        compressed_size: compressed_data.len(),
                        compressed_data,
                        point_count,
                        original_size,
                        compression_time_us: start.elapsed().as_micros() as u64,
                        error: None,
                    },
                    Err(e) => ParallelCompressionResult {
                        series_id,
                        compressed_data: Vec::new(),
                        compressed_size: 0,
                        point_count,
                        original_size,
                        compression_time_us: start.elapsed().as_micros() as u64,
                        error: Some(e),
                    },
                }
            });
        }

        // Collect all results
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(compression_result) => results.push(compression_result),
                Err(e) => {
                    warn!(error = %e, "Compression task panicked");
                },
            }
        }

        results
    }

    /// Compress batches sequentially (for small data or single series)
    async fn compress_sequential(
        &self,
        batches: Vec<(SeriesId, Vec<DataPoint>)>,
    ) -> Vec<ParallelCompressionResult> {
        let mut results = Vec::with_capacity(batches.len());

        for (series_id, points) in batches {
            let start = std::time::Instant::now();
            let point_count = points.len();
            let original_size = point_count * 16;

            let result = match Self::compress_single(&self.compressor, series_id, &points).await {
                Ok(compressed_data) => ParallelCompressionResult {
                    series_id,
                    compressed_size: compressed_data.len(),
                    compressed_data,
                    point_count,
                    original_size,
                    compression_time_us: start.elapsed().as_micros() as u64,
                    error: None,
                },
                Err(e) => ParallelCompressionResult {
                    series_id,
                    compressed_data: Vec::new(),
                    compressed_size: 0,
                    point_count,
                    original_size,
                    compression_time_us: start.elapsed().as_micros() as u64,
                    error: Some(e),
                },
            };
            results.push(result);
        }

        results
    }

    /// Compress a single series
    async fn compress_single(
        compressor: &AhpacCompressor,
        _series_id: SeriesId,
        points: &[DataPoint],
    ) -> Result<Vec<u8>, String> {
        use crate::engine::traits::Compressor;

        compressor
            .compress(points)
            .await
            .map(|block| block.data.to_vec())
            .map_err(|e| format!("Compression failed: {}", e))
    }

    /// Compress a batch and return aggregate statistics
    pub async fn compress_batch_with_stats(
        &self,
        batches: Vec<(SeriesId, Vec<DataPoint>)>,
    ) -> (Vec<ParallelCompressionResult>, ParallelBatchStats) {
        let wall_start = std::time::Instant::now();
        let series_count = batches.len();

        let results = self.compress_batch(batches).await;

        let wall_time_us = wall_start.elapsed().as_micros() as u64;

        let mut stats = ParallelBatchStats {
            series_count,
            wall_time_us,
            ..Default::default()
        };

        for result in &results {
            stats.total_points += result.point_count;
            stats.total_original_bytes += result.original_size;
            stats.total_compressed_bytes += result.compressed_size;
            stats.total_compression_time_us += result.compression_time_us;
            if result.error.is_some() {
                stats.failed_count += 1;
            }
        }

        // Calculate parallelism efficiency
        // Perfect parallelism: wall_time = total_time / num_threads
        if stats.total_compression_time_us > 0 && wall_time_us > 0 {
            let ideal_wall_time =
                stats.total_compression_time_us / self.config.max_concurrent_tasks as u64;
            stats.parallelism_efficiency = if wall_time_us > ideal_wall_time {
                ideal_wall_time as f64 / wall_time_us as f64
            } else {
                1.0
            };
        }

        (results, stats)
    }
}

impl Default for ParallelCompressor {
    fn default() -> Self {
        Self::default_new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize, base_ts: i64, series_id: SeriesId) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(series_id, base_ts + i as i64 * 1000, 100.0 + i as f64 * 0.1))
            .collect()
    }

    #[tokio::test]
    async fn test_parallel_compress_single_series() {
        let compressor = ParallelCompressor::default();
        let points = create_test_points(100, 1000000, 1);

        let results = compressor.compress_batch(vec![(1, points)]).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].error.is_none());
        assert!(results[0].compressed_size > 0);
    }

    #[tokio::test]
    async fn test_parallel_compress_multiple_series() {
        let compressor = ParallelCompressor::new(ParallelConfig {
            max_concurrent_tasks: 4,
            min_parallel_batch_size: 10,
            use_ahpac: true,
            collect_stats: true,
        });

        let batches: Vec<(SeriesId, Vec<DataPoint>)> = (1..=10)
            .map(|series_id| {
                let points = create_test_points(100, 1000000 + series_id as i64 * 1000, series_id);
                (series_id, points)
            })
            .collect();

        let (results, stats) = compressor.compress_batch_with_stats(batches).await;

        assert_eq!(results.len(), 10);
        assert_eq!(stats.series_count, 10);
        assert_eq!(stats.total_points, 1000);
        assert_eq!(stats.failed_count, 0);

        for result in &results {
            assert!(
                result.error.is_none(),
                "Series {} failed: {:?}",
                result.series_id,
                result.error
            );
            assert!(result.compressed_size > 0);
        }

        // Check compression ratio is reasonable
        assert!(stats.compression_ratio() > 1.0);

        println!(
            "Parallelism efficiency: {:.2}%",
            stats.parallelism_efficiency * 100.0
        );
        println!("Throughput: {:.2} MB/s", stats.throughput_mbps());
        println!("Compression ratio: {:.2}x", stats.compression_ratio());
    }

    #[tokio::test]
    async fn test_parallel_compress_empty_batch() {
        let compressor = ParallelCompressor::default();
        let results = compressor.compress_batch(vec![]).await;
        assert!(results.is_empty());
    }

    #[test]
    fn test_config_presets() {
        let high_throughput = ParallelConfig::high_throughput();
        let low_latency = ParallelConfig::low_latency();

        // High throughput should have more concurrent tasks
        assert!(high_throughput.max_concurrent_tasks >= low_latency.max_concurrent_tasks);

        // Low latency should have higher min batch size
        assert!(low_latency.min_parallel_batch_size >= high_throughput.min_parallel_batch_size);
    }

    #[test]
    fn test_compression_result_metrics() {
        let result = ParallelCompressionResult {
            series_id: 1,
            compressed_data: vec![0; 100],
            point_count: 1000,
            original_size: 16000,
            compressed_size: 100,
            compression_time_us: 1000,
            error: None,
        };

        assert!((result.compression_ratio() - 160.0).abs() < 0.01);
        assert!((result.bits_per_sample() - 0.8).abs() < 0.01);
    }
}
