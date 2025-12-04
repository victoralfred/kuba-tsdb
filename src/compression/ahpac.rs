//! AHPAC compression integration for the storage layer
//!
//! This module provides an `AhpacCompressor` that implements the engine's `Compressor` trait,
//! enabling AHPAC (Adaptive Hierarchical Predictive Arithmetic Compression) to be used
//! as a drop-in replacement for the default Kuba compressor.
//!
//! AHPAC automatically selects the best codec for each chunk based on data characteristics:
//! - **Kuba**: Best for general time-series with regular intervals
//! - **Chimp**: Improved XOR encoding for slowly changing values
//! - **ALP**: Optimal for decimal-scaled integer-like data (prices, percentages)
//! - **Delta+LZ4**: Best for smooth, highly autocorrelated data
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::ahpac::AhpacCompressor;
//! use kuba_tsdb::engine::traits::Compressor;
//!
//! let compressor = AhpacCompressor::new();
//! let compressed = compressor.compress(&points).await?;
//! let decompressed = compressor.decompress(&compressed).await?;
//! ```

use crate::ahpac::{AhpacCompressor as InnerCompressor, AhpacError, SelectionStrategy};
use crate::engine::traits::{BlockMetadata, CompressedBlock, CompressionStats, Compressor};
use crate::error::CompressionError;
use crate::types::DataPoint;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// AHPAC compressor implementing the storage engine's `Compressor` trait
///
/// This wrapper adapts the AHPAC compression module to work with the existing
/// storage layer infrastructure.
pub struct AhpacCompressor {
    /// Inner AHPAC compressor instance
    inner: InnerCompressor,
    /// Compression statistics
    stats: Arc<AhpacStats>,
}

/// Thread-safe statistics for AHPAC compression
struct AhpacStats {
    /// Total compression operations
    total_compressed: AtomicU64,
    /// Total decompression operations
    total_decompressed: AtomicU64,
    /// Total compression time in milliseconds
    compression_time_ms: AtomicU64,
    /// Total decompression time in milliseconds
    decompression_time_ms: AtomicU64,
    /// Running average compression ratio (protected by mutex for atomic update)
    average_ratio: Mutex<f64>,
    /// Count for averaging
    ratio_count: AtomicU64,
}

impl AhpacStats {
    fn new() -> Self {
        Self {
            total_compressed: AtomicU64::new(0),
            total_decompressed: AtomicU64::new(0),
            compression_time_ms: AtomicU64::new(0),
            decompression_time_ms: AtomicU64::new(0),
            average_ratio: Mutex::new(0.0),
            ratio_count: AtomicU64::new(0),
        }
    }

    /// Update running average compression ratio
    fn update_ratio(&self, new_ratio: f64) {
        let count = self.ratio_count.fetch_add(1, Ordering::Relaxed) + 1;
        let mut avg = self.average_ratio.lock();
        // Incremental average: new_avg = old_avg + (new_value - old_avg) / count
        *avg += (new_ratio - *avg) / count as f64;
    }
}

impl AhpacCompressor {
    /// Create a new AHPAC compressor with default settings
    ///
    /// Uses the `Verified` selection strategy which balances speed and compression ratio.
    pub fn new() -> Self {
        Self {
            inner: InnerCompressor::new(),
            stats: Arc::new(AhpacStats::new()),
        }
    }

    /// Create an AHPAC compressor with a specific selection strategy
    ///
    /// # Arguments
    /// * `strategy` - The codec selection strategy to use
    ///
    /// # Strategies
    /// * `Heuristic` - Fast rule-based selection (~1μs overhead)
    /// * `Exhaustive` - Try all codecs, pick smallest (~4x slower)
    /// * `Verified` - Heuristic with fallback verification (default, ~2x slower)
    pub fn with_strategy(strategy: SelectionStrategy) -> Self {
        Self {
            inner: InnerCompressor::new().with_strategy(strategy),
            stats: Arc::new(AhpacStats::new()),
        }
    }

    /// Create an AHPAC compressor optimized for speed
    ///
    /// Uses heuristic-only selection for minimal overhead.
    pub fn fast() -> Self {
        Self::with_strategy(SelectionStrategy::Heuristic)
    }

    /// Create an AHPAC compressor optimized for compression ratio
    ///
    /// Uses exhaustive search to find the smallest output.
    pub fn best_ratio() -> Self {
        Self::with_strategy(SelectionStrategy::Exhaustive)
    }
}

impl Default for AhpacCompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Compressor for AhpacCompressor {
    /// Returns the algorithm identifier for AHPAC
    fn algorithm_id(&self) -> &str {
        "ahpac"
    }

    /// Compress data points using AHPAC adaptive compression
    ///
    /// The algorithm profiles the data and selects the best codec automatically.
    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock, CompressionError> {
        if points.is_empty() {
            return Ok(CompressedBlock {
                algorithm_id: self.algorithm_id().to_string(),
                original_size: 0,
                compressed_size: 0,
                checksum: 0,
                data: Bytes::new(),
                metadata: BlockMetadata::default(),
            });
        }

        let start = Instant::now();

        // Compress using AHPAC
        let chunk = self.inner.compress(points).map_err(convert_ahpac_error)?;

        // Serialize the compressed chunk to bytes
        let data = chunk.to_bytes();
        let compressed_size = data.len();

        // Calculate original size (16 bytes per point: 8 for timestamp + 8 for value)
        let original_size = points.len() * 16;

        // Calculate checksum
        let checksum = crc64_checksum(&data);

        // Update statistics
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.stats.total_compressed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .compression_time_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);

        let ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            1.0
        };
        self.stats.update_ratio(ratio);

        // Build metadata
        let metadata = BlockMetadata {
            start_timestamp: points.first().map(|p| p.timestamp).unwrap_or(0),
            end_timestamp: points.last().map(|p| p.timestamp).unwrap_or(0),
            point_count: points.len(),
            series_id: points.first().map(|p| p.series_id).unwrap_or(0),
        };

        Ok(CompressedBlock {
            algorithm_id: self.algorithm_id().to_string(),
            original_size,
            compressed_size,
            checksum,
            data: Bytes::from(data),
            metadata,
        })
    }

    /// Decompress a block back to data points
    async fn decompress(
        &self,
        block: &CompressedBlock,
    ) -> Result<Vec<DataPoint>, CompressionError> {
        if block.data.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();

        // Verify checksum
        let calculated_checksum = crc64_checksum(&block.data);
        if calculated_checksum != block.checksum {
            return Err(CompressionError::CorruptedData(format!(
                "Checksum mismatch: expected {:#x}, got {:#x}",
                block.checksum, calculated_checksum
            )));
        }

        // Decompress using AHPAC
        let points = self
            .inner
            .decompress_bytes(&block.data)
            .map_err(convert_ahpac_error)?;

        // Update statistics
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.stats
            .total_decompressed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .decompression_time_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);

        Ok(points)
    }

    /// Estimate compression ratio for capacity planning
    ///
    /// Uses a sample of the data to estimate expected compression.
    fn estimate_ratio(&self, sample: &[DataPoint]) -> f64 {
        if sample.is_empty() {
            return 1.0;
        }

        // Try to compress a sample and calculate the ratio
        match self.inner.compress(sample) {
            Ok(chunk) => {
                let compressed_size = chunk.to_bytes().len();
                let original_size = sample.len() * 16;
                if compressed_size > 0 {
                    original_size as f64 / compressed_size as f64
                } else {
                    1.0
                }
            }
            Err(_) => 1.0, // Fallback to no compression estimate
        }
    }

    /// Get compression statistics
    fn stats(&self) -> CompressionStats {
        CompressionStats {
            total_compressed: self.stats.total_compressed.load(Ordering::Relaxed),
            total_decompressed: self.stats.total_decompressed.load(Ordering::Relaxed),
            compression_time_ms: self.stats.compression_time_ms.load(Ordering::Relaxed),
            decompression_time_ms: self.stats.decompression_time_ms.load(Ordering::Relaxed),
            average_ratio: *self.stats.average_ratio.lock(),
        }
    }
}

/// Convert AHPAC error to CompressionError
fn convert_ahpac_error(err: AhpacError) -> CompressionError {
    match err {
        AhpacError::EmptyInput => CompressionError::InvalidData("Empty input".to_string()),
        AhpacError::CodecError(msg) => CompressionError::CompressionFailed(msg),
        AhpacError::Codec(codec_err) => CompressionError::CompressionFailed(codec_err.to_string()),
        AhpacError::InvalidFormat(msg) => CompressionError::DecompressionFailed(msg),
        AhpacError::Serialization(msg) => CompressionError::CorruptedData(msg),
        AhpacError::CrcMismatch { expected, actual } => CompressionError::CorruptedData(format!(
            "CRC mismatch: expected {:#x}, got {:#x}",
            expected, actual
        )),
        AhpacError::UnsupportedCodec(id) => {
            CompressionError::UnsupportedAlgorithm(format!("Unsupported codec ID: {}", id))
        }
    }
}

/// Calculate CRC64 checksum for data integrity
///
/// Uses the same CRC-64-ECMA-182 polynomial as KubaCompressor for consistency
/// across all compression algorithms in the storage layer.
fn crc64_checksum(data: &[u8]) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182).checksum(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new(
                    1,
                    1000000 + i as i64 * 1000,
                    100.0 + (i as f64 * 0.1).sin() * 10.0,
                )
            })
            .collect()
    }

    fn create_integer_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, (100 + i) as f64))
            .collect()
    }

    fn create_constant_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, 42.0))
            .collect()
    }

    #[tokio::test]
    async fn test_compress_decompress_basic() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        assert!(compressed.compressed_size > 0);
        assert!(compressed.compressed_size < compressed.original_size);

        let decompressed = compressor.decompress(&compressed).await.unwrap();
        assert_eq!(decompressed.len(), points.len());

        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }
    }

    #[tokio::test]
    async fn test_compress_empty() {
        let compressor = AhpacCompressor::new();
        let compressed = compressor.compress(&[]).await.unwrap();
        assert_eq!(compressed.compressed_size, 0);
        assert!(compressed.data.is_empty());
    }

    #[tokio::test]
    async fn test_algorithm_id() {
        let compressor = AhpacCompressor::new();
        assert_eq!(compressor.algorithm_id(), "ahpac");
    }

    #[tokio::test]
    async fn test_compression_ratio() {
        let compressor = AhpacCompressor::new();
        let points = create_constant_points(100);

        let compressed = compressor.compress(&points).await.unwrap();

        // Constant data should compress very well
        let ratio = compressed.original_size as f64 / compressed.compressed_size as f64;
        assert!(ratio > 5.0, "Expected ratio > 5, got {}", ratio);
    }

    #[tokio::test]
    async fn test_integer_data_compression() {
        let compressor = AhpacCompressor::new();
        let points = create_integer_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert_eq!(orig.value, dec.value);
        }
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(50);

        // Initial stats
        let initial_stats = compressor.stats();
        assert_eq!(initial_stats.total_compressed, 0);

        // Compress
        let compressed = compressor.compress(&points).await.unwrap();
        let stats_after_compress = compressor.stats();
        assert_eq!(stats_after_compress.total_compressed, 1);
        assert!(stats_after_compress.average_ratio > 1.0);

        // Decompress
        let _ = compressor.decompress(&compressed).await.unwrap();
        let stats_after_decompress = compressor.stats();
        assert_eq!(stats_after_decompress.total_decompressed, 1);
    }

    #[tokio::test]
    async fn test_estimate_ratio() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(100);

        let ratio = compressor.estimate_ratio(&points);
        assert!(ratio > 1.0, "Expected ratio > 1, got {}", ratio);
    }

    #[tokio::test]
    async fn test_fast_compressor() {
        let compressor = AhpacCompressor::fast();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), points.len());
    }

    #[tokio::test]
    async fn test_best_ratio_compressor() {
        let compressor = AhpacCompressor::best_ratio();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), points.len());
    }

    #[tokio::test]
    async fn test_metadata() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();

        assert_eq!(compressed.metadata.point_count, 100);
        assert_eq!(compressed.metadata.start_timestamp, points[0].timestamp);
        assert_eq!(compressed.metadata.end_timestamp, points[99].timestamp);
        assert_eq!(compressed.metadata.series_id, 1);
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(100);

        let mut compressed = compressor.compress(&points).await.unwrap();

        // Corrupt the checksum
        compressed.checksum = compressed.checksum.wrapping_add(1);

        let result = compressor.decompress(&compressed).await;
        assert!(result.is_err());
    }
}
