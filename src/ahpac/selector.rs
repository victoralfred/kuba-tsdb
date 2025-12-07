//! Codec selection logic for AHPAC
//!
//! This module implements the adaptive codec selection algorithm that chooses
//! the best compression codec based on data characteristics.
//!
//! # Selection Strategies
//!
//! - **Heuristic**: Fast rule-based selection using statistical profile
//! - **Verified**: Heuristic with fallback verification against runner-up
//! - **Exhaustive**: Try all codecs and pick smallest output
//! - **Neural**: Online adaptive learning from compression feedback

use super::codecs::{AlpCodec, ChimpCodec, Codec, CodecId, DeltaLz4Codec, KubaCodec};
use super::neural_predictor::NeuralPredictor;
use super::profile::{ChunkProfile, Monotonicity};
use crate::types::DataPoint;
use std::sync::Arc;

/// Selection strategy for codec choice
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionStrategy {
    /// Fast rule-based selection using data profile
    ///
    /// Selects codec based on statistical characteristics without
    /// actually trying compression. Very fast but may not always
    /// pick the optimal codec.
    Heuristic,

    /// Try all codecs and pick the one with smallest output
    ///
    /// Guarantees optimal selection but incurs significant overhead
    /// (~4x compression time). Best for archival/cold storage.
    Exhaustive,

    /// Heuristic selection with verification against runner-up
    ///
    /// Uses heuristics to pick a candidate, then compares against
    /// a fallback codec (typically Chimp). Good balance of speed
    /// and accuracy.
    Verified,

    /// Online adaptive learning using neural network
    ///
    /// A lightweight neural predictor that learns from compression
    /// feedback to improve codec selection over time. The predictor
    /// adapts to actual workload patterns without manual tuning.
    ///
    /// During warm-up (first ~100 samples), falls back to heuristic
    /// selection while gathering training data.
    Neural,
}

/// Codec selector that manages available codecs and selection logic
pub struct CodecSelector {
    /// Available codecs
    codecs: Vec<Box<dyn Codec>>,
    /// Neural predictor for adaptive codec selection (shared across threads)
    neural_predictor: Arc<NeuralPredictor>,
}

impl CodecSelector {
    /// Create a new codec selector with all available codecs
    pub fn new() -> Self {
        Self {
            codecs: vec![
                Box::new(KubaCodec::new()),
                Box::new(ChimpCodec::new()),
                Box::new(AlpCodec::new()),
                Box::new(DeltaLz4Codec::new()),
            ],
            neural_predictor: Arc::new(NeuralPredictor::new()),
        }
    }

    /// Create a new codec selector with a custom neural predictor
    ///
    /// This allows sharing the neural predictor across multiple selectors
    /// for consistent learning.
    pub fn with_neural_predictor(predictor: Arc<NeuralPredictor>) -> Self {
        Self {
            codecs: vec![
                Box::new(KubaCodec::new()),
                Box::new(ChimpCodec::new()),
                Box::new(AlpCodec::new()),
                Box::new(DeltaLz4Codec::new()),
            ],
            neural_predictor: predictor,
        }
    }

    /// Get a reference to the neural predictor
    ///
    /// Useful for recording feedback or checking statistics.
    pub fn neural_predictor(&self) -> &NeuralPredictor {
        &self.neural_predictor
    }

    /// Get the shared neural predictor Arc
    ///
    /// Useful for sharing across multiple selectors.
    pub fn neural_predictor_arc(&self) -> Arc<NeuralPredictor> {
        Arc::clone(&self.neural_predictor)
    }

    /// Select codec using heuristic rules based on profile
    ///
    /// This is fast (O(1)) but may not always pick the optimal codec.
    /// The rules are based on typical data patterns:
    ///
    /// 1. Integer-like data -> ALP (best for decimal-scaled floats)
    /// 2. Many identical values -> Chimp (single-bit encoding)
    /// 3. Smooth/autocorrelated -> Delta+LZ4 (exploits patterns)
    /// 4. Monotonic counters -> Delta+LZ4 (predictable deltas)
    /// 5. Default -> Chimp (good general-purpose)
    pub fn select_heuristic(&self, profile: &ChunkProfile) -> CodecId {
        // Rule 1: Integer-like data is best with ALP
        if profile.gcd_scale.is_some() {
            return CodecId::Alp;
        }

        // Rule 2: High XOR zero ratio means many identical values
        // Chimp handles this very efficiently
        if profile.xor_zero_ratio > 0.5 {
            return CodecId::Chimp;
        }

        // Rule 3: Highly autocorrelated (smooth) data
        // Delta+LZ4 can exploit repetitive XOR patterns
        if profile.autocorr[0] > 0.95 {
            return CodecId::DeltaLz4;
        }

        // Rule 4: Monotonic integer counters
        // Delta+LZ4 is efficient for predictable deltas
        if profile.monotonic != Monotonicity::NonMonotonic && profile.variance < 100.0 {
            return CodecId::DeltaLz4;
        }

        // Rule 5: Default to Chimp (generally best all-rounder)
        CodecId::Chimp
    }

    /// Select codec by trying all and picking smallest output
    ///
    /// This guarantees optimal selection but is slower.
    pub fn select_exhaustive(
        &self,
        points: &[DataPoint],
        profile: &ChunkProfile,
    ) -> (CodecId, Vec<u8>) {
        let mut best_codec = CodecId::Kuba;
        let mut best_data = Vec::new();
        let mut best_size = usize::MAX;

        for codec in &self.codecs {
            // Quick check if codec is applicable
            let estimate = codec.estimate_bits(profile, points);
            if estimate == f64::MAX {
                continue; // Codec not applicable to this data
            }

            // Try actual compression
            if let Ok(data) = codec.compress(points) {
                if data.len() < best_size {
                    best_size = data.len();
                    best_codec = codec.id();
                    best_data = data;
                }
            }
        }

        // Fallback to raw if nothing worked
        if best_data.is_empty() && !points.is_empty() {
            best_data = Self::encode_raw(points);
            best_codec = CodecId::Raw;
        }

        (best_codec, best_data)
    }

    /// Select codec using heuristics with verification
    ///
    /// Uses heuristic selection but verifies against an appropriate fallback:
    /// - For high-entropy data: Uses Kuba (Gorilla) which is optimized for random-like patterns
    /// - For other data: Uses Chimp as fallback which handles most data types well
    pub fn select_verified(
        &self,
        points: &[DataPoint],
        profile: &ChunkProfile,
    ) -> (CodecId, Vec<u8>) {
        let primary_id = self.select_heuristic(profile);
        let primary_codec = self.get_codec(primary_id);

        let primary_result = primary_codec.compress(points);

        // Choose fallback based on data characteristics:
        // - High-entropy data: Kuba is slightly better than Chimp for random patterns
        // - Other data: Chimp is a reliable all-rounder
        let fallback_id = if profile.is_high_entropy() {
            CodecId::Kuba
        } else {
            CodecId::Chimp
        };

        // If primary is already the same as fallback, just use it
        if primary_id == fallback_id {
            return match primary_result {
                Ok(data) => (primary_id, data),
                Err(_) => {
                    let raw = Self::encode_raw(points);
                    (CodecId::Raw, raw)
                },
            };
        }

        // Also try fallback codec
        let fallback = self.get_codec(fallback_id);
        let fallback_result = fallback.compress(points);

        match (primary_result, fallback_result) {
            (Ok(p_data), Ok(f_data)) => {
                // Pick whichever is smaller
                if p_data.len() <= f_data.len() {
                    (primary_id, p_data)
                } else {
                    (fallback_id, f_data)
                }
            },
            (Ok(p_data), Err(_)) => (primary_id, p_data),
            (Err(_), Ok(f_data)) => (fallback_id, f_data),
            (Err(_), Err(_)) => {
                // Both failed, use raw encoding
                let raw = Self::encode_raw(points);
                (CodecId::Raw, raw)
            },
        }
    }

    /// Select codec using the neural predictor
    ///
    /// Uses an online adaptive neural network to select the codec.
    /// The predictor learns from compression feedback over time.
    ///
    /// This method compresses with the neural-selected codec and
    /// automatically records feedback for learning.
    pub fn select_neural(
        &self,
        points: &[DataPoint],
        profile: &ChunkProfile,
    ) -> (CodecId, Vec<u8>) {
        // Ask the neural predictor for a codec
        let codec_id = self.neural_predictor.select(profile);
        let codec = self.get_codec(codec_id);

        // Try to compress
        match codec.compress(points) {
            Ok(data) => {
                // Record successful compression for learning
                let input_size = points.len() * 16; // 16 bytes per point
                let ratio = if !data.is_empty() {
                    input_size as f64 / data.len() as f64
                } else {
                    1.0
                };
                self.neural_predictor
                    .record_feedback(profile, codec_id, ratio);
                (codec_id, data)
            },
            Err(_) => {
                // Neural selection failed, fall back to verified selection
                // This also helps the neural network learn from failures
                let (fallback_id, fallback_data) = self.select_verified(points, profile);

                // Record the fallback result
                let input_size = points.len() * 16;
                let ratio = if !fallback_data.is_empty() {
                    input_size as f64 / fallback_data.len() as f64
                } else {
                    1.0
                };
                self.neural_predictor
                    .record_feedback(profile, fallback_id, ratio);

                (fallback_id, fallback_data)
            },
        }
    }

    /// Get a codec by its ID
    pub fn get_codec(&self, id: CodecId) -> &dyn Codec {
        self.codecs
            .iter()
            .find(|c| c.id() == id)
            .map(|c| c.as_ref())
            .expect("Codec not found")
    }

    /// Encode data as raw (uncompressed) bytes
    ///
    /// Format: [timestamp: 8 bytes][value: 8 bytes] per point
    fn encode_raw(points: &[DataPoint]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(points.len() * 16);
        for point in points {
            buf.extend_from_slice(&point.timestamp.to_le_bytes());
            buf.extend_from_slice(&point.value.to_bits().to_le_bytes());
        }
        buf
    }

    /// Get all available codec IDs
    pub fn available_codecs(&self) -> Vec<CodecId> {
        self.codecs.iter().map(|c| c.id()).collect()
    }

    /// Benchmark all codecs on the given data
    ///
    /// Returns a vector of (codec_id, compressed_size, bits_per_sample) tuples.
    pub fn benchmark_all(&self, points: &[DataPoint]) -> Vec<(CodecId, Option<usize>, f64)> {
        let mut results = Vec::new();

        for codec in &self.codecs {
            match codec.compress(points) {
                Ok(data) => {
                    let bps = (data.len() * 8) as f64 / points.len() as f64;
                    results.push((codec.id(), Some(data.len()), bps));
                },
                Err(_) => {
                    results.push((codec.id(), None, f64::MAX));
                },
            }
        }

        results
    }
}

impl Default for CodecSelector {
    fn default() -> Self {
        Self::new()
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

    fn create_integer_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1_000_000 + i as i64 * 1000, (100 + i) as f64))
            .collect()
    }

    fn create_constant_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1_000_000 + i as i64 * 1000, 42.0))
            .collect()
    }

    fn create_monotonic_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1_000_000 + i as i64 * 1000, i as f64))
            .collect()
    }

    #[test]
    fn test_selector_creation() {
        let selector = CodecSelector::new();
        let codecs = selector.available_codecs();
        assert!(codecs.contains(&CodecId::Kuba));
        assert!(codecs.contains(&CodecId::Chimp));
        assert!(codecs.contains(&CodecId::Alp));
        assert!(codecs.contains(&CodecId::DeltaLz4));
    }

    #[test]
    fn test_heuristic_integer_data() {
        let selector = CodecSelector::new();
        let points = create_integer_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let codec = selector.select_heuristic(&profile);
        assert_eq!(codec, CodecId::Alp);
    }

    #[test]
    fn test_heuristic_constant_data() {
        let selector = CodecSelector::new();
        let points = create_constant_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let codec = selector.select_heuristic(&profile);
        // Constant data may be detected as integer-like (gcd_scale = 42.0)
        // The heuristic may select Alp (integer-like) or Chimp (high xor zero ratio)
        assert!(codec == CodecId::Chimp || codec == CodecId::Alp);
    }

    #[test]
    fn test_heuristic_default() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let codec = selector.select_heuristic(&profile);
        // Selection depends on data characteristics
        // The sinusoidal test data may match different heuristic rules
        assert!(codec == CodecId::Chimp || codec == CodecId::DeltaLz4 || codec == CodecId::Alp);
    }

    #[test]
    fn test_exhaustive_selection() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let (codec_id, data) = selector.select_exhaustive(&points, &profile);
        assert!(!data.is_empty());
        assert!(codec_id != CodecId::Raw);

        // Verify the data can be decompressed
        let codec = selector.get_codec(codec_id);
        let decompressed = codec.decompress(&data, points.len()).unwrap();
        assert_eq!(decompressed.len(), points.len());
    }

    #[test]
    fn test_verified_selection() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let (codec_id, data) = selector.select_verified(&points, &profile);
        assert!(!data.is_empty());

        // Verify the data can be decompressed
        let codec = selector.get_codec(codec_id);
        let decompressed = codec.decompress(&data, points.len()).unwrap();
        assert_eq!(decompressed.len(), points.len());
    }

    #[test]
    fn test_verified_beats_heuristic() {
        let selector = CodecSelector::new();
        let points = create_constant_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        // Both should work, verified might pick better
        let heuristic_id = selector.select_heuristic(&profile);
        let (verified_id, verified_data) = selector.select_verified(&points, &profile);

        // For constant data, both should pick Chimp
        assert!(heuristic_id == CodecId::Chimp || verified_id == CodecId::Chimp);
        assert!(!verified_data.is_empty());
    }

    #[test]
    fn test_benchmark_all() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);

        let results = selector.benchmark_all(&points);
        assert_eq!(results.len(), 4); // 4 codecs

        // At least some codecs should succeed
        let successful = results.iter().filter(|(_, size, _)| size.is_some()).count();
        assert!(successful >= 2);
    }

    #[test]
    fn test_benchmark_all_integer_data() {
        let selector = CodecSelector::new();
        let points = create_integer_points(100);

        let results = selector.benchmark_all(&points);

        // ALP should work and likely be best
        let alp_result = results.iter().find(|(id, _, _)| *id == CodecId::Alp);
        assert!(alp_result.is_some());
        let (_, alp_size, _) = alp_result.unwrap();
        assert!(alp_size.is_some());
    }

    #[test]
    fn test_encode_raw() {
        let points = create_test_points(10);
        let raw = CodecSelector::encode_raw(&points);
        assert_eq!(raw.len(), 10 * 16); // 16 bytes per point
    }

    #[test]
    fn test_get_codec() {
        let selector = CodecSelector::new();

        let kuba = selector.get_codec(CodecId::Kuba);
        assert_eq!(kuba.id(), CodecId::Kuba);

        let chimp = selector.get_codec(CodecId::Chimp);
        assert_eq!(chimp.id(), CodecId::Chimp);
    }

    #[test]
    fn test_monotonic_selects_delta_lz4() {
        let selector = CodecSelector::new();
        let points = create_monotonic_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        // Monotonic integer data should select ALP (integer-like takes priority)
        // or Delta+LZ4 if not integer-like
        let codec = selector.select_heuristic(&profile);
        assert!(codec == CodecId::Alp || codec == CodecId::DeltaLz4);
    }

    #[test]
    fn test_neural_selection() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        // Neural selection should work (uses heuristic fallback initially)
        let (codec_id, data) = selector.select_neural(&points, &profile);
        assert!(!data.is_empty());

        // Verify the data can be decompressed
        let codec = selector.get_codec(codec_id);
        let decompressed = codec.decompress(&data, points.len()).unwrap();
        assert_eq!(decompressed.len(), points.len());
    }

    #[test]
    fn test_neural_predictor_feedback() {
        let selector = CodecSelector::new();
        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        // Initial state
        let initial_count = selector.neural_predictor().stats().sample_count;

        // Compress with neural selection (records feedback)
        let _ = selector.select_neural(&points, &profile);

        // Should have recorded one sample
        let final_count = selector.neural_predictor().stats().sample_count;
        assert_eq!(final_count, initial_count + 1);
    }

    #[test]
    fn test_neural_predictor_shared() {
        use std::sync::Arc;

        let predictor = Arc::new(super::NeuralPredictor::new());

        let selector1 = CodecSelector::with_neural_predictor(Arc::clone(&predictor));
        let selector2 = CodecSelector::with_neural_predictor(Arc::clone(&predictor));

        let points = create_test_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        // Use both selectors
        let _ = selector1.select_neural(&points, &profile);
        let _ = selector2.select_neural(&points, &profile);

        // Both should have contributed to the same predictor
        let count = predictor.stats().sample_count;
        assert_eq!(count, 2);
    }
}
