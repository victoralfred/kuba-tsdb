//! Adaptive Hierarchical Predictive Arithmetic Compression (AHPAC)
//!
//! AHPAC is an adaptive compression system that selects the optimal codec
//! for each data chunk based on statistical profiling. It aims to achieve
//! better compression than any single fixed codec by matching the algorithm
//! to the data characteristics.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//! │  Profiler   │→ │  Selector   │→ │  Encoder    │→ Output
//! │             │  │             │  │             │
//! │ - variance  │  │ - heuristic │  │ - Kuba      │
//! │ - autocorr  │  │ - verified  │  │ - Chimp     │
//! │ - kurtosis  │  │ - exhaustive│  │ - ALP       │
//! │ - xor_ratio │  │ - neural    │  │ - Delta+LZ4 │
//! └─────────────┘  └─────────────┘  └─────────────┘
//!                        ↑ feedback
//!                  ┌─────┴─────┐
//!                  │  Neural   │ (online adaptive learning)
//!                  │ Predictor │
//!                  └───────────┘
//! ```
//!
//! # Selection Strategies
//!
//! - **Heuristic**: Fast rule-based selection using statistical profile (~0% overhead)
//! - **Verified**: Heuristic with fallback comparison (~5% overhead, default)
//! - **Exhaustive**: Try all codecs, pick smallest (~20% overhead, best ratio)
//! - **Neural**: Online adaptive learning from compression feedback (~1% overhead)
//!
//! The Neural strategy uses a lightweight neural network that learns from
//! actual compression results. It adapts to workload patterns over time
//! without manual tuning.
//!
//! # Usage
//!
//! ```rust,ignore
//! use kuba_tsdb::ahpac::{AhpacCompressor, SelectionStrategy};
//! use kuba_tsdb::types::DataPoint;
//!
//! // Default: Verified strategy (good balance)
//! let compressor = AhpacCompressor::new();
//!
//! // Or use Neural strategy for adaptive learning
//! let adaptive_compressor = AhpacCompressor::new()
//!     .with_strategy(SelectionStrategy::Neural);
//!
//! let points: Vec<DataPoint> = get_data();
//! let compressed = compressor.compress(&points)?;
//!
//! println!("Codec used: {:?}", compressed.codec);
//! println!("Bits per sample: {:.2}", compressed.bits_per_sample());
//!
//! // Check neural predictor statistics
//! let stats = adaptive_compressor.neural_predictor().stats();
//! println!("Samples learned: {}", stats.sample_count);
//! ```
//!
//! # Codecs
//!
//! The following codecs are available:
//!
//! - **Kuba**: XOR-based compression, good for slowly changing values
//! - **Chimp**: Improved XOR encoding with better leading zero handling
//! - **ALP**: Algebraic integer encoding for decimal-scaled floats
//! - **Delta+LZ4**: Delta encoding followed by LZ4, good for smooth data
//! - **Delta+Zstd**: Delta encoding followed by Zstd, best compression ratio

pub mod codecs;
pub mod frame;
pub mod neural_predictor;
pub mod profile;
pub mod selector;

// Re-exports for convenient access
pub use codecs::{Codec, CodecError, CodecId};
pub use frame::CompressedChunk;
pub use neural_predictor::{NeuralPredictor, NeuralPredictorConfig, NeuralPredictorStats};
pub use profile::{ChunkProfile, Monotonicity};
pub use selector::{CodecSelector, SelectionStrategy};

use crate::types::DataPoint;

/// Error types for AHPAC operations
#[derive(Debug, thiserror::Error)]
pub enum AhpacError {
    /// Input data is empty
    #[error("Cannot compress empty input")]
    EmptyInput,

    /// Codec error during compression or decompression
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),

    /// Codec-specific error message
    #[error("Codec error: {0}")]
    CodecError(String),

    /// Serialization or deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// CRC checksum mismatch
    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch {
        /// Expected CRC value
        expected: u32,
        /// Actual computed CRC value
        actual: u32,
    },

    /// Invalid data format
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Unsupported codec ID
    #[error("Unsupported codec ID: {0}")]
    UnsupportedCodec(u8),
}

/// Main AHPAC compressor interface
///
/// This is the primary entry point for AHPAC compression. It handles:
/// 1. Statistical profiling of input data
/// 2. Codec selection based on profile
/// 3. Compression with the selected codec
/// 4. Framing with metadata and checksums
pub struct AhpacCompressor {
    /// Codec selector instance
    selector: CodecSelector,
    /// Maximum samples to use for profiling (default: 256)
    profile_samples: usize,
    /// Selection strategy (default: Verified)
    strategy: SelectionStrategy,
}

impl Default for AhpacCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl AhpacCompressor {
    /// Create a new AHPAC compressor with default settings
    ///
    /// Default configuration:
    /// - Strategy: `Verified` (heuristic with fallback verification)
    /// - Profile samples: 256 points
    pub fn new() -> Self {
        Self {
            selector: CodecSelector::new(),
            profile_samples: 256,
            strategy: SelectionStrategy::Verified,
        }
    }

    /// Create a new AHPAC compressor with a shared neural predictor
    ///
    /// This allows multiple compressors to share learning from the same
    /// neural predictor, enabling consistent adaptive behavior across
    /// the entire application.
    ///
    /// # Arguments
    ///
    /// * `predictor` - Shared neural predictor for adaptive learning
    /// * `strategy` - Codec selection strategy to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use kuba_tsdb::ahpac::{AhpacCompressor, NeuralPredictor, SelectionStrategy};
    ///
    /// let predictor = Arc::new(NeuralPredictor::new());
    /// let compressor = AhpacCompressor::with_shared_predictor(
    ///     predictor,
    ///     SelectionStrategy::Neural,
    /// );
    /// ```
    pub fn with_shared_predictor(
        predictor: std::sync::Arc<NeuralPredictor>,
        strategy: SelectionStrategy,
    ) -> Self {
        Self {
            selector: CodecSelector::with_neural_predictor(predictor),
            profile_samples: 256,
            strategy,
        }
    }

    /// Set the codec selection strategy
    ///
    /// # Strategies
    ///
    /// - `Heuristic`: Fast rule-based selection (~0% overhead)
    /// - `Verified`: Heuristic with fallback comparison (~5% overhead)
    /// - `Exhaustive`: Try all codecs, pick best (~20% overhead)
    /// - `Neural`: Online adaptive learning (~1% overhead)
    pub fn with_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the number of samples used for profiling
    ///
    /// Larger values provide more accurate profiling but increase overhead.
    /// Default is 256, which balances accuracy and performance.
    pub fn with_profile_samples(mut self, samples: usize) -> Self {
        const MAX_PROFILE_SAMPLES: usize = 10_000;
        self.profile_samples = samples.max(16).min(MAX_PROFILE_SAMPLES);
        self
    }

    /// Compress a chunk of data points
    ///
    /// # Arguments
    ///
    /// * `points` - Slice of data points to compress
    ///
    /// # Returns
    ///
    /// A `CompressedChunk` containing the compressed data, codec ID, and metadata.
    ///
    /// # Errors
    ///
    /// Returns `AhpacError::EmptyInput` if the input is empty.
    pub fn compress(&self, points: &[DataPoint]) -> Result<CompressedChunk, AhpacError> {
        if points.is_empty() {
            return Err(AhpacError::EmptyInput);
        }

        // Step 1: Profile the data
        let profile = ChunkProfile::compute(points, self.profile_samples);

        // Step 2: Select codec and compress based on strategy
        let (codec_id, compressed_data) = match self.strategy {
            SelectionStrategy::Heuristic => {
                let id = self.selector.select_heuristic(&profile);
                let codec = self.selector.get_codec(id)?;
                let data = codec.compress(points)?;
                (id, data)
            },
            SelectionStrategy::Exhaustive => self.selector.select_exhaustive(points, &profile),
            SelectionStrategy::Verified => self.selector.select_verified(points, &profile),
            SelectionStrategy::Neural => self.selector.select_neural(points, &profile),
        };

        // Step 3: Create the compressed chunk with metadata
        // Validate point count fits in u32
        let point_count = points.len().try_into().map_err(|_| {
            AhpacError::InvalidFormat(format!(
                "Too many points: {} (exceeds u32::MAX)",
                points.len()
            ))
        })?;

        let chunk = CompressedChunk {
            codec: codec_id,
            point_count,
            start_timestamp: points.first().map(|p| p.timestamp).unwrap_or(0),
            end_timestamp: points.last().map(|p| p.timestamp).unwrap_or(0),
            data: compressed_data,
            profile: Some(profile),
        };

        Ok(chunk)
    }

    /// Decompress a chunk back to data points
    ///
    /// # Arguments
    ///
    /// * `chunk` - The compressed chunk to decompress
    ///
    /// # Returns
    ///
    /// A vector of `DataPoint` containing the decompressed data.
    pub fn decompress(&self, chunk: &CompressedChunk) -> Result<Vec<DataPoint>, AhpacError> {
        let codec = self.selector.get_codec(chunk.codec)?;
        let points = codec.decompress(&chunk.data, chunk.point_count as usize)?;
        Ok(points)
    }

    /// Decompress raw bytes back to data points
    ///
    /// This method first deserializes the bytes into a `CompressedChunk`,
    /// then decompresses it using the appropriate codec.
    ///
    /// # Arguments
    ///
    /// * `data` - Raw bytes from a serialized `CompressedChunk`
    ///
    /// # Returns
    ///
    /// A vector of `DataPoint` containing the decompressed data.
    pub fn decompress_bytes(&self, data: &[u8]) -> Result<Vec<DataPoint>, AhpacError> {
        let chunk = CompressedChunk::from_bytes(data)?;
        self.decompress(&chunk)
    }

    /// Get the current selection strategy
    pub fn strategy(&self) -> SelectionStrategy {
        self.strategy
    }

    /// Get the profile sample count
    pub fn profile_samples(&self) -> usize {
        self.profile_samples
    }

    /// Get access to the neural predictor for statistics or custom feedback
    ///
    /// Only available when using the Neural strategy, but can be used
    /// to inspect predictor state for any strategy.
    pub fn neural_predictor(&self) -> &NeuralPredictor {
        self.selector.neural_predictor()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new(
                    0,
                    1_000_000 + i as i64 * 1000,
                    100.0 + (i as f64 * 0.1).sin() * 10.0,
                )
            })
            .collect()
    }

    #[test]
    fn test_compressor_creation() {
        let compressor = AhpacCompressor::new();
        assert_eq!(compressor.strategy(), SelectionStrategy::Verified);
        assert_eq!(compressor.profile_samples(), 256);
    }

    #[test]
    fn test_compressor_with_strategy() {
        let compressor = AhpacCompressor::new().with_strategy(SelectionStrategy::Exhaustive);
        assert_eq!(compressor.strategy(), SelectionStrategy::Exhaustive);
    }

    #[test]
    fn test_compress_empty_input() {
        let compressor = AhpacCompressor::new();
        let result = compressor.compress(&[]);
        assert!(matches!(result, Err(AhpacError::EmptyInput)));
    }

    #[test]
    fn test_compress_and_decompress() {
        let compressor = AhpacCompressor::new();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).unwrap();
        assert!(compressed.point_count == 100);
        assert!(compressed.bits_per_sample() > 0.0);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), points.len());

        // Verify data integrity
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert!((original.value - decoded.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_profile_samples_minimum() {
        let compressor = AhpacCompressor::new().with_profile_samples(5);
        // Should be clamped to minimum of 16
        assert_eq!(compressor.profile_samples(), 16);
    }

    #[test]
    fn test_neural_strategy() {
        let compressor = AhpacCompressor::new().with_strategy(SelectionStrategy::Neural);
        let points = create_test_points(100);

        // Compress should work even during warm-up (falls back to heuristic)
        let compressed = compressor.compress(&points).unwrap();
        assert!(compressed.point_count == 100);
        assert!(compressed.bits_per_sample() > 0.0);

        // Verify decompression works
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed.len(), points.len());

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert!((original.value - decoded.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_neural_predictor_learning() {
        let compressor = AhpacCompressor::new().with_strategy(SelectionStrategy::Neural);

        // Compress multiple times to generate training data
        for _ in 0..50 {
            let points = create_test_points(100);
            let _ = compressor.compress(&points);
        }

        // Check that the predictor has recorded samples
        let stats = compressor.neural_predictor().stats();
        assert!(stats.sample_count >= 50);
    }

    #[test]
    fn test_neural_predictor_access() {
        let compressor = AhpacCompressor::new();
        let predictor = compressor.neural_predictor();

        // Should start with no samples
        let stats = predictor.stats();
        assert_eq!(stats.sample_count, 0);
    }
}
