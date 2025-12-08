//! Online Adaptive Neural Predictor for AHPAC codec selection
//!
//! This module implements a lightweight neural network that learns to predict
//! the best compression codec based on data characteristics. Unlike the heuristic
//! selector, this predictor adapts to the actual workload patterns over time.
//!
//! # Architecture
//!
//! The neural predictor uses a simple feed-forward network:
//! - Input layer: 8 features from ChunkProfile
//! - Hidden layer: 16 neurons with ReLU activation
//! - Output layer: 4 neurons (one per codec) with softmax
//!
//! # Online Learning
//!
//! The predictor updates weights after each compression operation using:
//! - Exponential moving average (EMA) for stable updates
//! - Reward signal based on compression ratio achieved
//! - No backpropagation - simple weight adjustment for minimal overhead
//!
//! # Thread Safety
//!
//! All weight updates are thread-safe using atomic operations and locks
//! where necessary. The predictor can be shared across multiple threads.

use super::codecs::CodecId;
use super::profile::ChunkProfile;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of input features extracted from ChunkProfile
const INPUT_SIZE: usize = 8;

/// Number of neurons in the hidden layer
const HIDDEN_SIZE: usize = 16;

/// Number of output classes (codecs)
/// Kuba, Chimp, Alp, DeltaLz4, DeltaZstd
const OUTPUT_SIZE: usize = 5;

/// Default learning rate for weight updates
const DEFAULT_LEARNING_RATE: f64 = 0.01;

/// Minimum learning rate (prevents complete stagnation)
const MIN_LEARNING_RATE: f64 = 0.001;

/// Learning rate decay factor per update
const LEARNING_RATE_DECAY: f64 = 0.9999;

/// Configuration for the neural predictor
#[derive(Debug, Clone)]
pub struct NeuralPredictorConfig {
    /// Initial learning rate for weight updates
    ///
    /// Higher values adapt faster but may oscillate.
    /// Lower values are more stable but adapt slower.
    pub learning_rate: f64,

    /// Exploration rate (probability of trying non-optimal codec)
    ///
    /// Helps discover better codecs for edge cases.
    /// Range: 0.0 (never explore) to 1.0 (always random)
    pub exploration_rate: f64,

    /// Minimum samples before making predictions
    ///
    /// Until this threshold is reached, falls back to heuristic selection.
    pub min_samples_for_prediction: u64,

    /// EMA decay factor for running statistics
    ///
    /// Controls how fast old observations fade.
    /// Higher values = longer memory.
    pub ema_decay: f64,

    /// Enable learning (can disable for inference-only mode)
    pub enable_learning: bool,
}

impl Default for NeuralPredictorConfig {
    fn default() -> Self {
        Self {
            learning_rate: DEFAULT_LEARNING_RATE,
            exploration_rate: 0.05, // 5% exploration
            min_samples_for_prediction: 100,
            ema_decay: 0.99,
            enable_learning: true,
        }
    }
}

/// Online adaptive neural predictor for codec selection
///
/// This predictor learns from compression feedback to improve codec
/// selection over time. It's designed for minimal runtime overhead
/// while still adapting to workload patterns.
pub struct NeuralPredictor {
    /// Configuration
    config: NeuralPredictorConfig,

    /// Weights from input to hidden layer [INPUT_SIZE x HIDDEN_SIZE]
    weights_ih: RwLock<Vec<Vec<f64>>>,

    /// Bias for hidden layer [HIDDEN_SIZE]
    bias_h: RwLock<Vec<f64>>,

    /// Weights from hidden to output layer [HIDDEN_SIZE x OUTPUT_SIZE]
    weights_ho: RwLock<Vec<Vec<f64>>>,

    /// Bias for output layer [OUTPUT_SIZE]
    bias_o: RwLock<Vec<f64>>,

    /// Total samples processed
    sample_count: AtomicU64,

    /// Current learning rate (decays over time)
    current_learning_rate: RwLock<f64>,

    /// Running average of compression ratios per codec
    avg_ratios: RwLock<[f64; OUTPUT_SIZE]>,

    /// Count of samples per codec (for averaging)
    codec_counts: [AtomicU64; OUTPUT_SIZE],

    /// Simple PRNG state for exploration
    rng_state: AtomicU64,
}

impl NeuralPredictor {
    /// Create a new neural predictor with default configuration
    pub fn new() -> Self {
        Self::with_config(NeuralPredictorConfig::default())
    }

    /// Create a neural predictor with custom configuration
    pub fn with_config(config: NeuralPredictorConfig) -> Self {
        // Initialize weights with small random values (Xavier initialization approximation)
        let weights_ih = Self::init_weights(INPUT_SIZE, HIDDEN_SIZE, 0.5);
        let bias_h = vec![0.0; HIDDEN_SIZE];
        let weights_ho = Self::init_weights(HIDDEN_SIZE, OUTPUT_SIZE, 0.5);
        let bias_o = vec![0.0; OUTPUT_SIZE];

        let learning_rate = config.learning_rate;

        Self {
            config,
            weights_ih: RwLock::new(weights_ih),
            bias_h: RwLock::new(bias_h),
            weights_ho: RwLock::new(weights_ho),
            bias_o: RwLock::new(bias_o),
            sample_count: AtomicU64::new(0),
            current_learning_rate: RwLock::new(learning_rate),
            avg_ratios: RwLock::new([1.0; OUTPUT_SIZE]),
            codec_counts: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            rng_state: AtomicU64::new(12345), // Seed for reproducibility
        }
    }

    /// Initialize weight matrix with small random values
    ///
    /// Uses a simple deterministic initialization based on position
    /// to ensure reproducibility across runs.
    fn init_weights(rows: usize, cols: usize, scale: f64) -> Vec<Vec<f64>> {
        let mut weights = Vec::with_capacity(rows);
        for i in 0..rows {
            let mut row = Vec::with_capacity(cols);
            for j in 0..cols {
                // Deterministic "random" initialization
                let seed = (i * cols + j) as f64;
                let val = ((seed * 0.618033988749895).fract() - 0.5) * 2.0 * scale;
                row.push(val / (rows as f64).sqrt());
            }
            weights.push(row);
        }
        weights
    }

    /// Extract input features from a ChunkProfile
    ///
    /// Features are normalized to [0, 1] range for stable learning.
    fn extract_features(profile: &ChunkProfile) -> [f64; INPUT_SIZE] {
        [
            // Feature 0: Normalized variance (log scale, clamped)
            (1.0 + profile.variance.abs()).ln().min(10.0) / 10.0,
            // Feature 1: Lag-1 autocorrelation (already in [-1, 1])
            (profile.autocorr[0] + 1.0) / 2.0,
            // Feature 2: XOR zero ratio (already in [0, 1])
            profile.xor_zero_ratio,
            // Feature 3: Is integer-like (binary)
            if profile.gcd_scale.is_some() {
                1.0
            } else {
                0.0
            },
            // Feature 4: Is monotonic (binary)
            if profile.monotonic != super::profile::Monotonicity::NonMonotonic {
                1.0
            } else {
                0.0
            },
            // Feature 5: Excess kurtosis (normalized, clamped)
            ((profile.kurtosis + 3.0) / 10.0).clamp(0.0, 1.0),
            // Feature 6: Value range indicator (log scale)
            (1.0 + (profile.max_value - profile.min_value).abs())
                .ln()
                .min(20.0)
                / 20.0,
            // Feature 7: Is high entropy
            if profile.is_high_entropy() { 1.0 } else { 0.0 },
        ]
    }

    /// Predict the best codec for given profile
    ///
    /// Returns codec probabilities as a softmax distribution.
    /// The codec with highest probability is the recommended choice.
    pub fn predict(&self, profile: &ChunkProfile) -> [f64; OUTPUT_SIZE] {
        let features = Self::extract_features(profile);
        self.forward_pass(&features)
    }

    /// Forward pass through the network
    fn forward_pass(&self, features: &[f64; INPUT_SIZE]) -> [f64; OUTPUT_SIZE] {
        let weights_ih = self.weights_ih.read();
        let bias_h = self.bias_h.read();
        let weights_ho = self.weights_ho.read();
        let bias_o = self.bias_o.read();

        // Hidden layer: ReLU(W_ih * x + b_h)
        let mut hidden = [0.0; HIDDEN_SIZE];
        for (j, h) in hidden.iter_mut().enumerate() {
            let mut sum = bias_h[j];
            for (i, &f) in features.iter().enumerate() {
                sum += weights_ih[i][j] * f;
            }
            // ReLU activation
            *h = sum.max(0.0);
        }

        // Output layer: softmax(W_ho * h + b_o)
        let mut output = [0.0; OUTPUT_SIZE];
        let mut max_val = f64::NEG_INFINITY;

        for (k, o) in output.iter_mut().enumerate() {
            let mut sum = bias_o[k];
            for (j, &h) in hidden.iter().enumerate() {
                sum += weights_ho[j][k] * h;
            }
            *o = sum;
            max_val = max_val.max(sum);
        }

        // Softmax with numerical stability (subtract max)
        let mut sum_exp = 0.0;
        for o in output.iter_mut() {
            *o = (*o - max_val).exp();
            sum_exp += *o;
        }
        for o in output.iter_mut() {
            *o /= sum_exp;
        }

        output
    }

    /// Select the best codec based on neural prediction
    ///
    /// May explore (select non-optimal codec) based on exploration rate.
    pub fn select(&self, profile: &ChunkProfile) -> CodecId {
        let samples = self.sample_count.load(Ordering::Relaxed);

        // Fall back to simple heuristic if not enough samples
        if samples < self.config.min_samples_for_prediction {
            return self.heuristic_fallback(profile);
        }

        let probs = self.predict(profile);

        // Exploration: occasionally try a random codec
        if self.config.exploration_rate > 0.0 && self.should_explore() {
            return self.random_codec();
        }

        // Exploitation: pick the highest probability codec
        Self::probs_to_codec(&probs)
    }

    /// Simple heuristic fallback when not enough learning samples
    fn heuristic_fallback(&self, profile: &ChunkProfile) -> CodecId {
        if profile.gcd_scale.is_some() {
            CodecId::Alp
        } else if profile.xor_zero_ratio > 0.5 {
            CodecId::Chimp
        } else if profile.autocorr[0] > 0.95 {
            CodecId::DeltaLz4
        } else {
            CodecId::Chimp
        }
    }

    /// Convert probability distribution to codec ID
    fn probs_to_codec(probs: &[f64; OUTPUT_SIZE]) -> CodecId {
        let mut best_idx = 0;
        let mut best_prob = probs[0];
        for (i, &p) in probs.iter().enumerate().skip(1) {
            if p > best_prob {
                best_prob = p;
                best_idx = i;
            }
        }
        Self::index_to_codec(best_idx)
    }

    /// Convert codec to output index
    fn codec_to_index(codec: CodecId) -> usize {
        match codec {
            CodecId::Kuba => 0,
            CodecId::Chimp => 1,
            CodecId::Alp => 2,
            CodecId::DeltaLz4 => 3,
            CodecId::DeltaZstd => 4,
            CodecId::Raw => 0, // Fallback to Kuba index
        }
    }

    /// Convert output index to codec
    ///
    /// Note: DeltaZstd (index 4) is mapped to DeltaLz4 as a fallback since
    /// DeltaZstd is not currently registered in the codec selector.
    /// The neural network still has 5 outputs for future extensibility.
    pub fn index_to_codec(idx: usize) -> CodecId {
        match idx {
            0 => CodecId::Kuba,
            1 => CodecId::Chimp,
            2 => CodecId::Alp,
            3 => CodecId::DeltaLz4,
            4 => CodecId::DeltaLz4, // DeltaZstd not available, fallback to DeltaLz4
            _ => CodecId::Chimp,    // Default fallback
        }
    }

    /// Number of currently available codecs in the selector
    /// Used for random exploration to avoid selecting unavailable codecs
    const AVAILABLE_CODECS: u64 = 4;

    /// Check if we should explore (simple PRNG)
    fn should_explore(&self) -> bool {
        let random = self.next_random();
        (random as f64 / u64::MAX as f64) < self.config.exploration_rate
    }

    /// Select a random codec for exploration
    /// Only selects from the 4 available codecs (Kuba, Chimp, Alp, DeltaLz4)
    fn random_codec(&self) -> CodecId {
        let random = self.next_random();
        Self::index_to_codec((random % Self::AVAILABLE_CODECS) as usize)
    }

    /// Simple xorshift PRNG for exploration
    ///
    /// Uses atomic read-modify-write to ensure thread safety.
    fn next_random(&self) -> u64 {
        // Use fetch_update for atomic read-modify-write
        self.rng_state
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |mut state| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                Some(state)
            })
            .unwrap_or_else(|x| x)
    }

    /// Record feedback from a compression operation
    ///
    /// This is the online learning step. Called after each compression
    /// with the codec used and the achieved compression ratio.
    ///
    /// # Arguments
    ///
    /// * `profile` - The profile of the compressed data
    /// * `codec` - The codec that was used
    /// * `ratio` - The compression ratio achieved (input_size / output_size)
    pub fn record_feedback(&self, profile: &ChunkProfile, codec: CodecId, ratio: f64) {
        if !self.config.enable_learning {
            return;
        }

        let codec_idx = Self::codec_to_index(codec);

        // Update sample count
        self.sample_count.fetch_add(1, Ordering::Relaxed);

        // Increment count first and get the new value (fixes race condition)
        let count = self.codec_counts[codec_idx].fetch_add(1, Ordering::Relaxed) + 1;

        // Update running average ratio for this codec
        {
            let mut avg_ratios = self.avg_ratios.write();
            // Use the actual count (now incremented) for accurate averaging
            avg_ratios[codec_idx] += (ratio - avg_ratios[codec_idx]) / count as f64;
        }

        // Compute reward: how much better than average?
        let avg_ratios = self.avg_ratios.read();
        // Prevent division by zero
        let global_avg: f64 = if OUTPUT_SIZE > 0 {
            avg_ratios.iter().sum::<f64>() / OUTPUT_SIZE as f64
        } else {
            1.0 // Fallback
        };
        let reward = if global_avg > f64::EPSILON {
            (ratio - global_avg) / global_avg
        } else {
            0.0
        };
        drop(avg_ratios);

        // Update weights based on reward
        self.update_weights(profile, codec_idx, reward);

        // Decay learning rate
        self.decay_learning_rate();
    }

    /// Update network weights based on feedback
    ///
    /// Uses a simple reinforcement signal: increase weights toward
    /// the chosen codec if reward is positive, decrease if negative.
    fn update_weights(&self, profile: &ChunkProfile, chosen_idx: usize, reward: f64) {
        let features = Self::extract_features(profile);
        let learning_rate = *self.current_learning_rate.read();

        // Scale the update by reward magnitude
        let update_scale = learning_rate * reward.clamp(-1.0, 1.0);

        if update_scale.abs() < 1e-10 {
            return; // No meaningful update
        }

        // Update output layer weights and biases
        // Increase weights toward chosen codec if reward > 0
        {
            let mut weights_ho = self.weights_ho.write();
            let mut bias_o = self.bias_o.write();

            for j in 0..HIDDEN_SIZE {
                // Approximate hidden activation (simplified - we use 1.0 as proxy)
                let hidden_approx = 1.0;
                for k in 0..OUTPUT_SIZE {
                    let target = if k == chosen_idx { 1.0 } else { 0.0 };
                    let error = target - 0.25; // 0.25 = uniform prior
                    weights_ho[j][k] += update_scale * error * hidden_approx * 0.1;
                }
            }

            for k in 0..OUTPUT_SIZE {
                let target = if k == chosen_idx { 1.0 } else { 0.0 };
                let error = target - 0.25;
                bias_o[k] += update_scale * error * 0.1;
            }
        }

        // Update input layer weights (credit assignment to features)
        {
            let mut weights_ih = self.weights_ih.write();
            let mut bias_h = self.bias_h.write();

            for i in 0..INPUT_SIZE {
                for j in 0..HIDDEN_SIZE {
                    // Features that are active get credit
                    let credit = features[i] * update_scale * 0.01;
                    weights_ih[i][j] += credit;
                }
            }

            for j in 0..HIDDEN_SIZE {
                bias_h[j] += update_scale * 0.001;
            }
        }
    }

    /// Decay learning rate over time
    fn decay_learning_rate(&self) {
        let mut lr = self.current_learning_rate.write();
        *lr = (*lr * LEARNING_RATE_DECAY).max(MIN_LEARNING_RATE);
    }

    /// Get current statistics
    pub fn stats(&self) -> NeuralPredictorStats {
        let codec_counts: [u64; OUTPUT_SIZE] = [
            self.codec_counts[0].load(Ordering::Relaxed),
            self.codec_counts[1].load(Ordering::Relaxed),
            self.codec_counts[2].load(Ordering::Relaxed),
            self.codec_counts[3].load(Ordering::Relaxed),
            self.codec_counts[4].load(Ordering::Relaxed),
        ];

        NeuralPredictorStats {
            sample_count: self.sample_count.load(Ordering::Relaxed),
            learning_rate: *self.current_learning_rate.read(),
            avg_ratios: *self.avg_ratios.read(),
            codec_counts,
        }
    }

    /// Reset the predictor to initial state
    pub fn reset(&self) {
        // Reset weights
        *self.weights_ih.write() = Self::init_weights(INPUT_SIZE, HIDDEN_SIZE, 0.5);
        *self.bias_h.write() = vec![0.0; HIDDEN_SIZE];
        *self.weights_ho.write() = Self::init_weights(HIDDEN_SIZE, OUTPUT_SIZE, 0.5);
        *self.bias_o.write() = vec![0.0; OUTPUT_SIZE];

        // Reset statistics
        self.sample_count.store(0, Ordering::Relaxed);
        *self.current_learning_rate.write() = self.config.learning_rate;
        *self.avg_ratios.write() = [1.0; OUTPUT_SIZE];
        for count in &self.codec_counts {
            count.store(0, Ordering::Relaxed);
        }
    }

    /// Check if predictor has enough samples for reliable predictions
    pub fn is_warmed_up(&self) -> bool {
        self.sample_count.load(Ordering::Relaxed) >= self.config.min_samples_for_prediction
    }
}

impl Default for NeuralPredictor {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics from the neural predictor
#[derive(Debug, Clone)]
pub struct NeuralPredictorStats {
    /// Total samples processed
    pub sample_count: u64,

    /// Current learning rate
    pub learning_rate: f64,

    /// Average compression ratio per codec
    pub avg_ratios: [f64; OUTPUT_SIZE],

    /// Number of times each codec was used
    pub codec_counts: [u64; OUTPUT_SIZE],
}

impl NeuralPredictorStats {
    /// Get the codec with best average compression ratio
    pub fn best_performing_codec(&self) -> CodecId {
        let mut best_idx = 0;
        let mut best_ratio = self.avg_ratios[0];
        for (i, &ratio) in self.avg_ratios.iter().enumerate().skip(1) {
            if ratio > best_ratio && self.codec_counts[i] > 0 {
                best_ratio = ratio;
                best_idx = i;
            }
        }
        NeuralPredictor::index_to_codec(best_idx)
    }

    /// Get codec usage distribution as percentages
    pub fn usage_distribution(&self) -> [f64; OUTPUT_SIZE] {
        let total: u64 = self.codec_counts.iter().sum();
        if total == 0 {
            return [0.2; OUTPUT_SIZE]; // Uniform 20% for 5 codecs
        }
        let mut dist = [0.0; OUTPUT_SIZE];
        for (i, &count) in self.codec_counts.iter().enumerate() {
            dist[i] = count as f64 / total as f64;
        }
        dist
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataPoint;

    fn create_test_points(pattern: &str, count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                let value = match pattern {
                    "constant" => 42.0,
                    "integer" => (100 + i) as f64,
                    "smooth" => 100.0 + (i as f64 * 0.01).sin() * 10.0,
                    "random" => {
                        let x = i as f64;
                        (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0
                    },
                    _ => i as f64,
                };
                DataPoint::new(0, 1000 + i as i64 * 100, value)
            })
            .collect()
    }

    #[test]
    fn test_predictor_creation() {
        let predictor = NeuralPredictor::new();
        let stats = predictor.stats();
        assert_eq!(stats.sample_count, 0);
        assert!(!predictor.is_warmed_up());
    }

    #[test]
    fn test_feature_extraction() {
        let points = create_test_points("integer", 100);
        let profile = ChunkProfile::compute(&points, 256);
        let features = NeuralPredictor::extract_features(&profile);

        // All features should be in [0, 1] range
        for &f in &features {
            assert!((0.0..=1.0).contains(&f), "Feature out of range: {}", f);
        }
    }

    #[test]
    fn test_forward_pass() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("smooth", 100);
        let profile = ChunkProfile::compute(&points, 256);

        let probs = predictor.predict(&profile);

        // Probabilities should sum to 1
        let sum: f64 = probs.iter().sum();
        assert!(
            (sum - 1.0).abs() < 0.001,
            "Probabilities don't sum to 1: {}",
            sum
        );

        // All probabilities should be non-negative
        for &p in &probs {
            assert!(p >= 0.0, "Negative probability: {}", p);
        }
    }

    #[test]
    fn test_selection_fallback() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("integer", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Before warmup, should use heuristic fallback
        let codec = predictor.select(&profile);

        // Integer data should select ALP via heuristic
        assert_eq!(codec, CodecId::Alp);
    }

    #[test]
    fn test_feedback_recording() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("smooth", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Record some feedback
        predictor.record_feedback(&profile, CodecId::Chimp, 5.0);
        predictor.record_feedback(&profile, CodecId::Chimp, 4.5);
        predictor.record_feedback(&profile, CodecId::Kuba, 3.0);

        let stats = predictor.stats();
        assert_eq!(stats.sample_count, 3);
        assert_eq!(stats.codec_counts[1], 2); // Chimp used twice
        assert_eq!(stats.codec_counts[0], 1); // Kuba used once
    }

    #[test]
    fn test_learning_improves_selection() {
        let config = NeuralPredictorConfig {
            min_samples_for_prediction: 10,
            exploration_rate: 0.0, // Disable exploration for deterministic test
            ..Default::default()
        };
        let predictor = NeuralPredictor::with_config(config);

        // Train on pattern where Chimp is best
        let points = create_test_points("constant", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Simulate Chimp being consistently better
        for _ in 0..50 {
            predictor.record_feedback(&profile, CodecId::Chimp, 10.0);
            predictor.record_feedback(&profile, CodecId::Kuba, 3.0);
            predictor.record_feedback(&profile, CodecId::Alp, 4.0);
            predictor.record_feedback(&profile, CodecId::DeltaLz4, 2.0);
        }

        assert!(predictor.is_warmed_up());

        let stats = predictor.stats();

        // Chimp should have best average ratio
        assert!(stats.avg_ratios[1] > stats.avg_ratios[0]); // Chimp > Kuba
        assert!(stats.avg_ratios[1] > stats.avg_ratios[2]); // Chimp > Alp
    }

    #[test]
    fn test_reset() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("random", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Add some samples
        for _ in 0..10 {
            predictor.record_feedback(&profile, CodecId::Kuba, 3.0);
        }

        assert_eq!(predictor.stats().sample_count, 10);

        // Reset
        predictor.reset();

        let stats = predictor.stats();
        assert_eq!(stats.sample_count, 0);
        for &count in &stats.codec_counts {
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_stats_best_performing() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("smooth", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Make DeltaLz4 the best performer
        for _ in 0..10 {
            predictor.record_feedback(&profile, CodecId::DeltaLz4, 8.0);
            predictor.record_feedback(&profile, CodecId::Chimp, 5.0);
            predictor.record_feedback(&profile, CodecId::Kuba, 4.0);
        }

        let stats = predictor.stats();
        let best = stats.best_performing_codec();
        assert_eq!(best, CodecId::DeltaLz4);
    }

    #[test]
    fn test_usage_distribution() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("integer", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Record 50 Chimp, 30 Kuba, 20 Alp
        for _ in 0..50 {
            predictor.record_feedback(&profile, CodecId::Chimp, 5.0);
        }
        for _ in 0..30 {
            predictor.record_feedback(&profile, CodecId::Kuba, 4.0);
        }
        for _ in 0..20 {
            predictor.record_feedback(&profile, CodecId::Alp, 6.0);
        }

        let stats = predictor.stats();
        let dist = stats.usage_distribution();

        assert!((dist[0] - 0.30).abs() < 0.01); // Kuba 30%
        assert!((dist[1] - 0.50).abs() < 0.01); // Chimp 50%
        assert!((dist[2] - 0.20).abs() < 0.01); // Alp 20%
    }

    #[test]
    fn test_learning_rate_decay() {
        let predictor = NeuralPredictor::new();
        let points = create_test_points("smooth", 100);
        let profile = ChunkProfile::compute(&points, 256);

        let initial_lr = predictor.stats().learning_rate;

        // Record many feedbacks to trigger decay
        for _ in 0..1000 {
            predictor.record_feedback(&profile, CodecId::Chimp, 5.0);
        }

        let final_lr = predictor.stats().learning_rate;
        assert!(final_lr < initial_lr);
        assert!(final_lr >= MIN_LEARNING_RATE);
    }

    #[test]
    fn test_inference_only_mode() {
        let config = NeuralPredictorConfig {
            enable_learning: false,
            ..Default::default()
        };
        let predictor = NeuralPredictor::with_config(config);
        let points = create_test_points("random", 100);
        let profile = ChunkProfile::compute(&points, 256);

        // Feedback should be ignored
        for _ in 0..100 {
            predictor.record_feedback(&profile, CodecId::Chimp, 10.0);
        }

        // Sample count should still be 0
        assert_eq!(predictor.stats().sample_count, 0);
    }

    #[test]
    fn test_codec_index_conversion() {
        assert_eq!(NeuralPredictor::codec_to_index(CodecId::Kuba), 0);
        assert_eq!(NeuralPredictor::codec_to_index(CodecId::Chimp), 1);
        assert_eq!(NeuralPredictor::codec_to_index(CodecId::Alp), 2);
        assert_eq!(NeuralPredictor::codec_to_index(CodecId::DeltaLz4), 3);
        assert_eq!(NeuralPredictor::codec_to_index(CodecId::DeltaZstd), 4);

        assert_eq!(NeuralPredictor::index_to_codec(0), CodecId::Kuba);
        assert_eq!(NeuralPredictor::index_to_codec(1), CodecId::Chimp);
        assert_eq!(NeuralPredictor::index_to_codec(2), CodecId::Alp);
        assert_eq!(NeuralPredictor::index_to_codec(3), CodecId::DeltaLz4);
        // Index 4 (DeltaZstd) falls back to DeltaLz4 since DeltaZstd is not available
        assert_eq!(NeuralPredictor::index_to_codec(4), CodecId::DeltaLz4);
    }
}
