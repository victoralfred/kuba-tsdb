//! Compression Algorithm Selection
//!
//! This module provides intelligent selection of compression algorithms based on
//! data characteristics, performance history, and latency requirements.
//!
//! # Overview
//!
//! The algorithm selector analyzes chunk characteristics (entropy, variance,
//! timestamp regularity) and chooses the most appropriate compression algorithm:
//!
//! - **AHPAC**: Best for regular time-series with good compression ratios
//! - **Snappy**: Fast compression for high-entropy or latency-sensitive data
//! - **LZ4**: Balanced speed and compression for mixed workloads
//! - **Kuba**: Specialized XOR-based compression for numeric time-series
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::algorithm_selector::{AlgorithmSelector, SelectionConfig};
//! use kuba_tsdb::types::DataPoint;
//!
//! let selector = AlgorithmSelector::new(SelectionConfig::default());
//!
//! let points: Vec<DataPoint> = vec![/* ... */];
//! let characteristics = ChunkCharacteristics::analyze(&points);
//! let algorithm = selector.select(&characteristics);
//! ```

use crate::storage::chunk::CompressionType;
use crate::types::DataPoint;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for algorithm selection
#[derive(Debug, Clone)]
pub struct SelectionConfig {
    /// Enable automatic algorithm selection
    pub enabled: bool,

    /// Default algorithm when selection is disabled
    pub default_algorithm: CompressionType,

    /// Entropy threshold above which to use fast compression (Snappy/LZ4)
    ///
    /// Higher entropy data compresses poorly with specialized algorithms.
    /// Range: 0.0 (no entropy) to 1.0 (maximum entropy)
    pub high_entropy_threshold: f64,

    /// Variance threshold for value data
    ///
    /// High variance indicates irregular data that benefits from general-purpose compression.
    pub high_variance_threshold: f64,

    /// Timestamp regularity threshold
    ///
    /// Regular timestamps (close to 1.0) benefit from delta encoding.
    /// Range: 0.0 (irregular) to 1.0 (perfectly regular)
    pub regular_timestamp_threshold: f64,

    /// Minimum point count to use AHPAC
    ///
    /// AHPAC has higher overhead, so smaller chunks may use simpler algorithms.
    pub min_points_for_ahpac: usize,

    /// Maximum point count before switching to streaming compression
    pub max_points_for_simple: usize,

    /// Latency budget in milliseconds
    ///
    /// If set, selector prefers faster algorithms within this budget.
    pub latency_budget_ms: Option<u64>,

    /// Enable learning from compression results
    pub enable_learning: bool,

    /// Number of samples to keep for learning
    pub learning_sample_size: usize,

    /// Weight for historical performance in selection
    ///
    /// Range: 0.0 (ignore history) to 1.0 (rely heavily on history)
    pub history_weight: f64,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_algorithm: CompressionType::Ahpac,
            high_entropy_threshold: 0.7,
            high_variance_threshold: 1000.0,
            regular_timestamp_threshold: 0.9,
            min_points_for_ahpac: 100,
            max_points_for_simple: 100_000,
            latency_budget_ms: None,
            enable_learning: true,
            learning_sample_size: 100,
            history_weight: 0.3,
        }
    }
}

impl SelectionConfig {
    /// Create configuration optimized for compression ratio
    pub fn optimize_ratio() -> Self {
        Self {
            default_algorithm: CompressionType::Ahpac,
            high_entropy_threshold: 0.8, // Higher tolerance for entropy
            min_points_for_ahpac: 50,    // Use AHPAC more aggressively
            history_weight: 0.5,         // Rely more on history
            ..Default::default()
        }
    }

    /// Create configuration optimized for speed
    pub fn optimize_speed() -> Self {
        Self {
            default_algorithm: CompressionType::Snappy,
            high_entropy_threshold: 0.5, // Quick switch to fast algorithms
            min_points_for_ahpac: 500,   // Use AHPAC less often
            latency_budget_ms: Some(50), // Strict latency budget
            history_weight: 0.2,         // Less reliance on history
            ..Default::default()
        }
    }

    /// Create configuration for balanced performance
    pub fn balanced() -> Self {
        Self::default()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.high_entropy_threshold < 0.0 || self.high_entropy_threshold > 1.0 {
            return Err("high_entropy_threshold must be between 0.0 and 1.0".to_string());
        }

        if self.regular_timestamp_threshold < 0.0 || self.regular_timestamp_threshold > 1.0 {
            return Err("regular_timestamp_threshold must be between 0.0 and 1.0".to_string());
        }

        if self.history_weight < 0.0 || self.history_weight > 1.0 {
            return Err("history_weight must be between 0.0 and 1.0".to_string());
        }

        if self.min_points_for_ahpac == 0 {
            return Err("min_points_for_ahpac must be > 0".to_string());
        }

        if self.learning_sample_size == 0 {
            return Err("learning_sample_size must be > 0".to_string());
        }

        Ok(())
    }
}

// ============================================================================
// Chunk Characteristics
// ============================================================================

/// Analyzed characteristics of a chunk's data
///
/// Used by the selector to determine the best compression algorithm.
#[derive(Debug, Clone)]
pub struct ChunkCharacteristics {
    /// Number of data points
    pub point_count: usize,

    /// Estimated entropy of values (0.0 = low, 1.0 = high)
    pub entropy: f64,

    /// Variance of values
    pub value_variance: f64,

    /// Mean of values
    pub value_mean: f64,

    /// Range of values (max - min)
    pub value_range: f64,

    /// Regularity of timestamps (0.0 = irregular, 1.0 = perfectly regular)
    pub timestamp_regularity: f64,

    /// Average time between points in milliseconds
    pub avg_timestamp_delta_ms: f64,

    /// Standard deviation of timestamp deltas
    pub timestamp_delta_stddev: f64,

    /// Estimated size in bytes (uncompressed)
    pub estimated_size_bytes: usize,

    /// Whether data appears to be integer-like
    pub is_integer_like: bool,

    /// Fraction of values that are zero
    pub zero_fraction: f64,

    /// Fraction of values that are repeated
    pub repeat_fraction: f64,
}

impl Default for ChunkCharacteristics {
    fn default() -> Self {
        Self {
            point_count: 0,
            entropy: 0.5,
            value_variance: 0.0,
            value_mean: 0.0,
            value_range: 0.0,
            timestamp_regularity: 1.0,
            avg_timestamp_delta_ms: 0.0,
            timestamp_delta_stddev: 0.0,
            estimated_size_bytes: 0,
            is_integer_like: false,
            zero_fraction: 0.0,
            repeat_fraction: 0.0,
        }
    }
}

impl ChunkCharacteristics {
    /// Analyze data points to determine characteristics
    pub fn analyze(points: &[DataPoint]) -> Self {
        if points.is_empty() {
            return Self::default();
        }

        let point_count = points.len();

        // Calculate value statistics
        let values: Vec<f64> = points.iter().map(|p| p.value).collect();
        let (value_mean, value_variance) = Self::calculate_mean_variance(&values);
        let value_range = Self::calculate_range(&values);

        // Calculate timestamp statistics
        let timestamps: Vec<i64> = points.iter().map(|p| p.timestamp).collect();
        let (timestamp_regularity, avg_delta, delta_stddev) =
            Self::calculate_timestamp_stats(&timestamps);

        // Calculate entropy estimate
        let entropy = Self::estimate_entropy(&values);

        // Check for integer-like values
        let is_integer_like = Self::check_integer_like(&values);

        // Calculate zero and repeat fractions
        let zero_fraction = Self::calculate_zero_fraction(&values);
        let repeat_fraction = Self::calculate_repeat_fraction(&values);

        // Estimate size (16 bytes per point: 8 for timestamp, 8 for value)
        let estimated_size_bytes = point_count * 16;

        Self {
            point_count,
            entropy,
            value_variance,
            value_mean,
            value_range,
            timestamp_regularity,
            avg_timestamp_delta_ms: avg_delta,
            timestamp_delta_stddev: delta_stddev,
            estimated_size_bytes,
            is_integer_like,
            zero_fraction,
            repeat_fraction,
        }
    }

    /// Calculate mean and variance of values
    fn calculate_mean_variance(values: &[f64]) -> (f64, f64) {
        if values.is_empty() {
            return (0.0, 0.0);
        }

        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;

        (mean, variance)
    }

    /// Calculate range (max - min)
    fn calculate_range(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        max - min
    }

    /// Calculate timestamp regularity and statistics
    fn calculate_timestamp_stats(timestamps: &[i64]) -> (f64, f64, f64) {
        if timestamps.len() < 2 {
            return (1.0, 0.0, 0.0);
        }

        // Calculate deltas
        let deltas: Vec<i64> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();

        // Calculate average delta
        let avg_delta = deltas.iter().sum::<i64>() as f64 / deltas.len() as f64;

        // Calculate standard deviation
        let variance = deltas
            .iter()
            .map(|&d| (d as f64 - avg_delta).powi(2))
            .sum::<f64>()
            / deltas.len() as f64;
        let stddev = variance.sqrt();

        // Calculate regularity as 1 - (stddev / avg)
        // Higher regularity means more consistent spacing
        let regularity = if avg_delta.abs() > 0.0 {
            (1.0 - (stddev / avg_delta.abs())).clamp(0.0, 1.0)
        } else {
            1.0
        };

        (regularity, avg_delta, stddev)
    }

    /// Estimate entropy using value distribution
    ///
    /// Uses a simple histogram-based approach to estimate entropy.
    fn estimate_entropy(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        // Create histogram with 256 buckets
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;

        if range == 0.0 {
            return 0.0; // All same value = zero entropy
        }

        const BUCKETS: usize = 256;
        let mut histogram = vec![0usize; BUCKETS];

        for &v in values {
            let bucket = ((v - min) / range * (BUCKETS - 1) as f64) as usize;
            let bucket = bucket.min(BUCKETS - 1);
            histogram[bucket] += 1;
        }

        // Calculate entropy from histogram
        let n = values.len() as f64;
        let mut entropy = 0.0;

        for &count in &histogram {
            if count > 0 {
                let p = count as f64 / n;
                entropy -= p * p.log2();
            }
        }

        // Normalize to 0-1 range (max entropy for 256 buckets is 8 bits)
        (entropy / 8.0).clamp(0.0, 1.0)
    }

    /// Check if values appear to be integers
    fn check_integer_like(values: &[f64]) -> bool {
        if values.is_empty() {
            return false;
        }

        let integer_count = values.iter().filter(|&&v| v.fract() == 0.0).count();

        integer_count as f64 / values.len() as f64 > 0.95
    }

    /// Calculate fraction of zero values
    fn calculate_zero_fraction(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        let zero_count = values.iter().filter(|&&v| v == 0.0).count();
        zero_count as f64 / values.len() as f64
    }

    /// Calculate fraction of repeated consecutive values
    fn calculate_repeat_fraction(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        let repeat_count = values
            .windows(2)
            .filter(|w| (w[0] - w[1]).abs() < f64::EPSILON)
            .count();

        repeat_count as f64 / (values.len() - 1) as f64
    }

    /// Get a score indicating how well data suits AHPAC compression
    ///
    /// Higher score = better fit for AHPAC
    pub fn ahpac_suitability_score(&self) -> f64 {
        let mut score = 0.5; // Base score

        // Regular timestamps are good for AHPAC
        score += self.timestamp_regularity * 0.2;

        // Low entropy is good for AHPAC
        score += (1.0 - self.entropy) * 0.2;

        // Repeated values are good
        score += self.repeat_fraction * 0.1;

        // Need enough points for AHPAC to be worthwhile
        if self.point_count >= 100 {
            score += 0.1;
        }

        score.clamp(0.0, 1.0)
    }

    /// Get a score indicating how well data suits Snappy compression
    pub fn snappy_suitability_score(&self) -> f64 {
        let mut score: f64 = 0.5;

        // High entropy benefits from fast compression
        score += self.entropy * 0.2;

        // High variance data doesn't compress well with specialized algorithms
        if self.value_variance > 1000.0 {
            score += 0.2;
        }

        // Small chunks benefit from low overhead
        if self.point_count < 100 {
            score += 0.1;
        }

        score.clamp(0.0, 1.0)
    }

    /// Get a score indicating how well data suits LZ4 compression
    pub fn lz4_suitability_score(&self) -> f64 {
        let mut score: f64 = 0.5;

        // LZ4 is a good middle ground
        // Moderate entropy is fine
        if self.entropy > 0.3 && self.entropy < 0.7 {
            score += 0.2;
        }

        // Large chunks benefit from LZ4's efficiency
        if self.point_count > 1000 {
            score += 0.1;
        }

        // Integer-like data compresses well
        if self.is_integer_like {
            score += 0.1;
        }

        score.clamp(0.0, 1.0)
    }
}

// ============================================================================
// Algorithm Statistics
// ============================================================================

/// Statistics for a single compression algorithm
#[derive(Debug, Default)]
pub struct AlgorithmStats {
    /// Total compressions performed
    pub total_compressions: AtomicU64,

    /// Total compression time in nanoseconds
    pub total_time_ns: AtomicU64,

    /// Sum of compression ratios (for averaging)
    ratio_sum: AtomicU64,

    /// Minimum compression ratio (stored as ratio * 1000)
    min_ratio: AtomicU64,

    /// Maximum compression ratio (stored as ratio * 1000)
    max_ratio: AtomicU64,

    /// Total bytes before compression
    pub total_input_bytes: AtomicU64,

    /// Total bytes after compression
    pub total_output_bytes: AtomicU64,
}

impl AlgorithmStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            min_ratio: AtomicU64::new(u64::MAX),
            max_ratio: AtomicU64::new(0),
            ..Default::default()
        }
    }

    /// Record a compression result
    pub fn record(&self, input_bytes: usize, output_bytes: usize, duration: Duration) {
        self.total_compressions.fetch_add(1, Ordering::Relaxed);
        self.total_time_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.total_input_bytes
            .fetch_add(input_bytes as u64, Ordering::Relaxed);
        self.total_output_bytes
            .fetch_add(output_bytes as u64, Ordering::Relaxed);

        // Calculate and store ratio
        let ratio = if output_bytes > 0 {
            (input_bytes as f64 / output_bytes as f64 * 1000.0) as u64
        } else {
            1000
        };

        self.ratio_sum.fetch_add(ratio, Ordering::Relaxed);

        // Update min/max atomically (may have races but acceptable for stats)
        let _ = self.min_ratio.fetch_min(ratio, Ordering::Relaxed);
        let _ = self.max_ratio.fetch_max(ratio, Ordering::Relaxed);
    }

    /// Get average compression ratio
    pub fn avg_ratio(&self) -> f64 {
        let total = self.total_compressions.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        self.ratio_sum.load(Ordering::Relaxed) as f64 / total as f64 / 1000.0
    }

    /// Get average compression time
    pub fn avg_time(&self) -> Duration {
        let total = self.total_compressions.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.total_time_ns.load(Ordering::Relaxed) / total)
    }

    /// Get throughput in bytes per second
    pub fn throughput_bytes_per_sec(&self) -> f64 {
        let time_ns = self.total_time_ns.load(Ordering::Relaxed);
        if time_ns == 0 {
            return 0.0;
        }
        let bytes = self.total_input_bytes.load(Ordering::Relaxed) as f64;
        let seconds = time_ns as f64 / 1_000_000_000.0;
        bytes / seconds
    }

    /// Get a snapshot of statistics
    pub fn snapshot(&self) -> AlgorithmStatsSnapshot {
        AlgorithmStatsSnapshot {
            total_compressions: self.total_compressions.load(Ordering::Relaxed),
            avg_ratio: self.avg_ratio(),
            avg_time: self.avg_time(),
            throughput_bytes_per_sec: self.throughput_bytes_per_sec(),
            total_input_bytes: self.total_input_bytes.load(Ordering::Relaxed),
            total_output_bytes: self.total_output_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of algorithm statistics
#[derive(Debug, Clone)]
pub struct AlgorithmStatsSnapshot {
    /// Total compressions
    pub total_compressions: u64,

    /// Average compression ratio
    pub avg_ratio: f64,

    /// Average compression time
    pub avg_time: Duration,

    /// Throughput in bytes per second
    pub throughput_bytes_per_sec: f64,

    /// Total input bytes
    pub total_input_bytes: u64,

    /// Total output bytes
    pub total_output_bytes: u64,
}

// ============================================================================
// Algorithm Selector
// ============================================================================

/// Selects the best compression algorithm based on data characteristics
pub struct AlgorithmSelector {
    /// Configuration
    config: SelectionConfig,

    /// Statistics per algorithm
    stats: HashMap<CompressionType, AlgorithmStats>,

    /// Historical performance samples for learning
    samples: RwLock<Vec<CompressionSample>>,
}

/// A sample of compression performance
#[derive(Debug, Clone)]
struct CompressionSample {
    /// Algorithm used
    algorithm: CompressionType,

    /// Characteristics of the data
    characteristics: ChunkCharacteristics,

    /// Achieved compression ratio
    ratio: f64,

    /// Compression duration (kept for potential latency-based selection)
    #[allow(dead_code)]
    duration: Duration,

    /// When this sample was recorded
    #[allow(dead_code)]
    recorded_at: Instant,
}

impl AlgorithmSelector {
    /// Create a new algorithm selector
    pub fn new(config: SelectionConfig) -> Self {
        let mut stats = HashMap::new();
        stats.insert(CompressionType::Ahpac, AlgorithmStats::new());
        stats.insert(CompressionType::Snappy, AlgorithmStats::new());
        stats.insert(CompressionType::Kuba, AlgorithmStats::new());
        stats.insert(CompressionType::None, AlgorithmStats::new());

        Self {
            config,
            stats,
            samples: RwLock::new(Vec::new()),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &SelectionConfig {
        &self.config
    }

    /// Select the best algorithm for the given characteristics
    pub fn select(&self, characteristics: &ChunkCharacteristics) -> CompressionType {
        if !self.config.enabled {
            return self.config.default_algorithm;
        }

        // Check latency constraint first
        if let Some(budget_ms) = self.config.latency_budget_ms {
            return self.select_for_latency(budget_ms);
        }

        // For small chunks, use simple compression
        if characteristics.point_count < self.config.min_points_for_ahpac {
            return self.select_for_small_chunk(characteristics);
        }

        // For high entropy data, use general-purpose compression
        if characteristics.entropy > self.config.high_entropy_threshold {
            return CompressionType::Snappy;
        }

        // For very regular time-series, AHPAC is optimal
        if characteristics.timestamp_regularity > self.config.regular_timestamp_threshold
            && characteristics.entropy < 0.5
        {
            return CompressionType::Ahpac;
        }

        // Use suitability scores with historical learning
        self.select_by_score(characteristics)
    }

    /// Select algorithm for small chunks
    fn select_for_small_chunk(&self, characteristics: &ChunkCharacteristics) -> CompressionType {
        // Very small chunks: no compression is faster
        if characteristics.point_count < 10 {
            return CompressionType::None;
        }

        // Small chunks with low entropy: Kuba is efficient
        if characteristics.entropy < 0.3 {
            return CompressionType::Kuba;
        }

        // Otherwise use Snappy for speed
        CompressionType::Snappy
    }

    /// Select the fastest algorithm that meets the latency budget
    pub fn select_for_latency(&self, max_latency_ms: u64) -> CompressionType {
        // Order by typical speed (fastest first)
        let algorithms = [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Kuba,
            CompressionType::Ahpac,
        ];

        for algo in algorithms {
            if let Some(stats) = self.stats.get(&algo) {
                let avg_time = stats.avg_time();
                if stats.total_compressions.load(Ordering::Relaxed) == 0
                    || avg_time.as_millis() <= max_latency_ms as u128
                {
                    // If we don't have data yet, assume it's fast
                    // Or if it meets the budget, use it
                    return algo;
                }
            }
        }

        // Fallback to no compression if nothing else fits
        CompressionType::None
    }

    /// Select by comparing suitability scores and historical performance
    fn select_by_score(&self, characteristics: &ChunkCharacteristics) -> CompressionType {
        let ahpac_score = characteristics.ahpac_suitability_score()
            * (1.0 - self.config.history_weight)
            + self.historical_score(CompressionType::Ahpac, characteristics)
                * self.config.history_weight;

        let snappy_score = characteristics.snappy_suitability_score()
            * (1.0 - self.config.history_weight)
            + self.historical_score(CompressionType::Snappy, characteristics)
                * self.config.history_weight;

        let lz4_score = characteristics.lz4_suitability_score()
            * (1.0 - self.config.history_weight)
            + self.historical_score(CompressionType::Kuba, characteristics)
                * self.config.history_weight;

        // Select highest scoring algorithm
        if ahpac_score >= snappy_score && ahpac_score >= lz4_score {
            CompressionType::Ahpac
        } else if snappy_score >= lz4_score {
            CompressionType::Snappy
        } else {
            CompressionType::Kuba
        }
    }

    /// Get historical performance score for an algorithm
    fn historical_score(
        &self,
        algorithm: CompressionType,
        characteristics: &ChunkCharacteristics,
    ) -> f64 {
        if !self.config.enable_learning {
            return 0.5;
        }

        let samples = self.samples.read().unwrap();

        // Find similar samples
        let similar: Vec<&CompressionSample> = samples
            .iter()
            .filter(|s| {
                s.algorithm == algorithm
                    && Self::characteristics_similar(&s.characteristics, characteristics)
            })
            .collect();

        if similar.is_empty() {
            return 0.5; // Neutral score if no data
        }

        // Calculate average ratio for similar samples
        let avg_ratio: f64 = similar.iter().map(|s| s.ratio).sum::<f64>() / similar.len() as f64;

        // Convert ratio to score (higher ratio = better = higher score)
        // Typical ratios are 1-10, normalize to 0-1
        (avg_ratio / 10.0).clamp(0.0, 1.0)
    }

    /// Check if two characteristics are similar
    fn characteristics_similar(a: &ChunkCharacteristics, b: &ChunkCharacteristics) -> bool {
        // Similar point count (within 50%)
        let count_ratio = a.point_count as f64 / b.point_count.max(1) as f64;
        if !(0.5..=2.0).contains(&count_ratio) {
            return false;
        }

        // Similar entropy (within 0.2)
        if (a.entropy - b.entropy).abs() > 0.2 {
            return false;
        }

        // Similar timestamp regularity (within 0.2)
        if (a.timestamp_regularity - b.timestamp_regularity).abs() > 0.2 {
            return false;
        }

        true
    }

    /// Record a compression result for learning
    ///
    /// This is the full version that includes chunk characteristics for
    /// adaptive learning. Use `record_stats` if you only need to update
    /// statistics without learning samples.
    pub fn record_result(
        &self,
        algorithm: CompressionType,
        characteristics: &ChunkCharacteristics,
        input_bytes: usize,
        output_bytes: usize,
        duration: Duration,
    ) {
        // Update statistics
        if let Some(stats) = self.stats.get(&algorithm) {
            stats.record(input_bytes, output_bytes, duration);
        }

        // Add sample for learning
        if self.config.enable_learning {
            let ratio = if output_bytes > 0 {
                input_bytes as f64 / output_bytes as f64
            } else {
                1.0
            };

            let sample = CompressionSample {
                algorithm,
                characteristics: characteristics.clone(),
                ratio,
                duration,
                recorded_at: Instant::now(),
            };

            let mut samples = self.samples.write().unwrap();
            samples.push(sample);

            // Trim to max size
            if samples.len() > self.config.learning_sample_size {
                samples.remove(0);
            }
        }
    }

    /// Record compression statistics without learning
    ///
    /// Use this method when you need to update algorithm statistics
    /// but don't have or don't need to store chunk characteristics.
    /// This is useful for post-hoc recording in async contexts.
    pub fn record_stats(
        &self,
        algorithm: CompressionType,
        input_bytes: usize,
        output_bytes: usize,
        duration: Duration,
    ) {
        if let Some(stats) = self.stats.get(&algorithm) {
            stats.record(input_bytes, output_bytes, duration);
        }
    }

    /// Get statistics for an algorithm
    pub fn get_stats(&self, algorithm: CompressionType) -> Option<AlgorithmStatsSnapshot> {
        self.stats.get(&algorithm).map(|s| s.snapshot())
    }

    /// Get statistics for all algorithms
    pub fn all_stats(&self) -> HashMap<CompressionType, AlgorithmStatsSnapshot> {
        self.stats
            .iter()
            .map(|(algo, stats)| (*algo, stats.snapshot()))
            .collect()
    }

    /// Clear all statistics and samples
    pub fn reset(&self) {
        for stats in self.stats.values() {
            stats.total_compressions.store(0, Ordering::Relaxed);
            stats.total_time_ns.store(0, Ordering::Relaxed);
            stats.ratio_sum.store(0, Ordering::Relaxed);
            stats.min_ratio.store(u64::MAX, Ordering::Relaxed);
            stats.max_ratio.store(0, Ordering::Relaxed);
            stats.total_input_bytes.store(0, Ordering::Relaxed);
            stats.total_output_bytes.store(0, Ordering::Relaxed);
        }

        self.samples.write().unwrap().clear();
    }
}

impl Default for AlgorithmSelector {
    fn default() -> Self {
        Self::new(SelectionConfig::default())
    }
}

// ============================================================================
// Compression Benchmark
// ============================================================================

/// Benchmarks compression algorithms on sample data
pub struct CompressionBenchmark {
    /// Results per algorithm
    results: HashMap<CompressionType, BenchmarkResult>,
}

/// Result of benchmarking an algorithm
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Compression ratio achieved
    pub ratio: f64,

    /// Compression time
    pub compress_time: Duration,

    /// Decompression time
    pub decompress_time: Duration,

    /// Throughput in MB/s
    pub throughput_mb_s: f64,
}

impl CompressionBenchmark {
    /// Run benchmark on sample data
    pub fn run(points: &[DataPoint]) -> Self {
        let mut results = HashMap::new();

        // Benchmark each algorithm
        results.insert(CompressionType::None, Self::benchmark_none(points));
        results.insert(CompressionType::Snappy, Self::benchmark_snappy(points));

        Self { results }
    }

    /// Benchmark no compression (baseline)
    fn benchmark_none(_points: &[DataPoint]) -> BenchmarkResult {
        // No compression: ratio is 1.0, no time spent compressing
        BenchmarkResult {
            ratio: 1.0,
            compress_time: Duration::ZERO,
            decompress_time: Duration::ZERO,
            throughput_mb_s: f64::INFINITY,
        }
    }

    /// Benchmark Snappy compression
    fn benchmark_snappy(points: &[DataPoint]) -> BenchmarkResult {
        // Serialize points to bytes
        let input: Vec<u8> = points
            .iter()
            .flat_map(|p| {
                let mut bytes = Vec::with_capacity(16);
                bytes.extend_from_slice(&p.timestamp.to_le_bytes());
                bytes.extend_from_slice(&p.value.to_le_bytes());
                bytes
            })
            .collect();

        if input.is_empty() {
            return BenchmarkResult {
                ratio: 1.0,
                compress_time: Duration::ZERO,
                decompress_time: Duration::ZERO,
                throughput_mb_s: 0.0,
            };
        }

        // Compress
        let start = Instant::now();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&input)
            .unwrap_or_default();
        let compress_time = start.elapsed();

        // Decompress
        let start = Instant::now();
        let _decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap_or_default();
        let decompress_time = start.elapsed();

        let ratio = if compressed.is_empty() {
            1.0
        } else {
            input.len() as f64 / compressed.len() as f64
        };

        let throughput = if compress_time.as_secs_f64() > 0.0 {
            input.len() as f64 / 1024.0 / 1024.0 / compress_time.as_secs_f64()
        } else {
            f64::INFINITY
        };

        BenchmarkResult {
            ratio,
            compress_time,
            decompress_time,
            throughput_mb_s: throughput,
        }
    }

    /// Get benchmark result for an algorithm
    pub fn get(&self, algorithm: CompressionType) -> Option<&BenchmarkResult> {
        self.results.get(&algorithm)
    }

    /// Get the algorithm with best compression ratio
    pub fn best_ratio(&self) -> Option<CompressionType> {
        self.results
            .iter()
            .max_by(|a, b| a.1.ratio.partial_cmp(&b.1.ratio).unwrap())
            .map(|(algo, _)| *algo)
    }

    /// Get the algorithm with best throughput
    pub fn best_throughput(&self) -> Option<CompressionType> {
        self.results
            .iter()
            .max_by(|a, b| {
                a.1.throughput_mb_s
                    .partial_cmp(&b.1.throughput_mb_s)
                    .unwrap()
            })
            .map(|(algo, _)| *algo)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates regular time-series data with low entropy
    /// - Regular timestamps (every 1 second)
    /// - Values oscillate between a small set of values for low entropy
    fn create_regular_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                // Use only 5 different values for low entropy
                let value = (i % 5) as f64 * 10.0;
                DataPoint::new(1, 1000 + i as i64 * 1000, value)
            })
            .collect()
    }

    /// Creates irregular data with varying timestamps and values
    fn create_irregular_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                // Irregular timestamps with varying gaps
                let ts = 1000 + (i as i64 * 1000) + (i as i64 % 3) * 500;
                // Continuous sin wave = high entropy when bucketed
                let value = (i as f64 * 17.3).sin() * 1000.0;
                DataPoint::new(1, ts, value)
            })
            .collect()
    }

    /// Creates high entropy data - pseudo-random values
    fn create_high_entropy_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                // Pseudo-random value that creates many distinct values
                let value = ((i * 12345 + 67890) % 100000) as f64;
                DataPoint::new(1, 1000 + i as i64 * 1000, value)
            })
            .collect()
    }

    // Config tests
    #[test]
    fn test_config_default() {
        let config = SelectionConfig::default();
        assert!(config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_presets() {
        let ratio = SelectionConfig::optimize_ratio();
        let speed = SelectionConfig::optimize_speed();
        let balanced = SelectionConfig::balanced();

        assert!(ratio.validate().is_ok());
        assert!(speed.validate().is_ok());
        assert!(balanced.validate().is_ok());

        assert!(speed.latency_budget_ms.is_some());
    }

    #[test]
    fn test_config_validation_entropy_threshold() {
        let config = SelectionConfig {
            high_entropy_threshold: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = SelectionConfig {
            high_entropy_threshold: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_history_weight() {
        let config = SelectionConfig {
            history_weight: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // Characteristics tests
    #[test]
    fn test_characteristics_empty() {
        let chars = ChunkCharacteristics::analyze(&[]);
        assert_eq!(chars.point_count, 0);
        assert_eq!(chars.entropy, 0.5);
    }

    #[test]
    fn test_characteristics_regular_data() {
        let points = create_regular_points(100);
        let chars = ChunkCharacteristics::analyze(&points);

        assert_eq!(chars.point_count, 100);
        assert!(chars.timestamp_regularity > 0.9);
        assert!(chars.entropy < 0.5);
    }

    #[test]
    fn test_characteristics_irregular_data() {
        let points = create_irregular_points(100);
        let chars = ChunkCharacteristics::analyze(&points);

        assert_eq!(chars.point_count, 100);
        assert!(chars.timestamp_regularity < 0.9);
    }

    #[test]
    fn test_characteristics_high_entropy() {
        let points = create_high_entropy_points(100);
        let chars = ChunkCharacteristics::analyze(&points);

        assert!(chars.entropy > 0.3);
    }

    #[test]
    fn test_characteristics_integer_like() {
        let points: Vec<DataPoint> = (0..100)
            .map(|i| DataPoint::new(1, 1000 + i as i64 * 1000, i as f64))
            .collect();
        let chars = ChunkCharacteristics::analyze(&points);

        assert!(chars.is_integer_like);
    }

    #[test]
    fn test_characteristics_zero_fraction() {
        let points: Vec<DataPoint> = (0..100)
            .map(|i| DataPoint::new(1, 1000 + i as i64 * 1000, if i < 50 { 0.0 } else { 1.0 }))
            .collect();
        let chars = ChunkCharacteristics::analyze(&points);

        assert!((chars.zero_fraction - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_characteristics_repeat_fraction() {
        let points: Vec<DataPoint> = (0..100)
            .map(|i| DataPoint::new(1, 1000 + i as i64 * 1000, 42.0))
            .collect();
        let chars = ChunkCharacteristics::analyze(&points);

        assert!(chars.repeat_fraction > 0.9);
    }

    #[test]
    fn test_suitability_scores() {
        let regular_chars = ChunkCharacteristics::analyze(&create_regular_points(100));
        let irregular_chars = ChunkCharacteristics::analyze(&create_high_entropy_points(100));

        // Regular data should suit AHPAC better
        assert!(
            regular_chars.ahpac_suitability_score() > irregular_chars.ahpac_suitability_score()
        );

        // High entropy data should suit Snappy better
        assert!(
            irregular_chars.snappy_suitability_score() > regular_chars.snappy_suitability_score()
        );
    }

    // Selector tests
    #[test]
    fn test_selector_creation() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        assert!(selector.config().enabled);
    }

    #[test]
    fn test_selector_disabled() {
        let config = SelectionConfig {
            enabled: false,
            default_algorithm: CompressionType::Snappy,
            ..Default::default()
        };
        let selector = AlgorithmSelector::new(config);
        let chars = ChunkCharacteristics::analyze(&create_regular_points(100));

        assert_eq!(selector.select(&chars), CompressionType::Snappy);
    }

    #[test]
    fn test_selector_regular_data() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let chars = ChunkCharacteristics::analyze(&create_regular_points(200));

        let algo = selector.select(&chars);
        assert_eq!(algo, CompressionType::Ahpac);
    }

    #[test]
    fn test_selector_high_entropy() {
        let selector = AlgorithmSelector::new(SelectionConfig {
            high_entropy_threshold: 0.3,
            ..Default::default()
        });
        let chars = ChunkCharacteristics::analyze(&create_high_entropy_points(200));

        let algo = selector.select(&chars);
        assert_eq!(algo, CompressionType::Snappy);
    }

    #[test]
    fn test_selector_small_chunk() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let chars = ChunkCharacteristics::analyze(&create_regular_points(50));

        let algo = selector.select(&chars);
        // Small chunks should not use AHPAC
        assert_ne!(algo, CompressionType::Ahpac);
    }

    #[test]
    fn test_selector_very_small_chunk() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let chars = ChunkCharacteristics::analyze(&create_regular_points(5));

        let algo = selector.select(&chars);
        // Very small chunks should use no compression
        assert_eq!(algo, CompressionType::None);
    }

    #[test]
    fn test_selector_latency_constraint() {
        let selector = AlgorithmSelector::new(SelectionConfig {
            latency_budget_ms: Some(1),
            ..Default::default()
        });
        let chars = ChunkCharacteristics::analyze(&create_regular_points(100));

        let algo = selector.select(&chars);
        // Should select fast algorithm due to tight latency
        assert!(algo == CompressionType::None || algo == CompressionType::Snappy);
    }

    // Stats tests
    #[test]
    fn test_algorithm_stats() {
        let stats = AlgorithmStats::new();

        stats.record(1000, 500, Duration::from_millis(10));
        stats.record(2000, 800, Duration::from_millis(15));

        assert_eq!(stats.total_compressions.load(Ordering::Relaxed), 2);
        assert!(stats.avg_ratio() > 1.0);
        assert!(stats.avg_time() > Duration::ZERO);
    }

    #[test]
    fn test_record_result() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let chars = ChunkCharacteristics::analyze(&create_regular_points(100));

        selector.record_result(
            CompressionType::Ahpac,
            &chars,
            1000,
            500,
            Duration::from_millis(10),
        );

        let stats = selector.get_stats(CompressionType::Ahpac).unwrap();
        assert_eq!(stats.total_compressions, 1);
        assert!(stats.avg_ratio > 1.0);
    }

    #[test]
    fn test_all_stats() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let all = selector.all_stats();

        assert!(all.contains_key(&CompressionType::Ahpac));
        assert!(all.contains_key(&CompressionType::Snappy));
    }

    #[test]
    fn test_reset() {
        let selector = AlgorithmSelector::new(SelectionConfig::default());
        let chars = ChunkCharacteristics::analyze(&create_regular_points(100));

        selector.record_result(
            CompressionType::Ahpac,
            &chars,
            1000,
            500,
            Duration::from_millis(10),
        );

        selector.reset();

        let stats = selector.get_stats(CompressionType::Ahpac).unwrap();
        assert_eq!(stats.total_compressions, 0);
    }

    // Benchmark tests
    #[test]
    fn test_benchmark_run() {
        let points = create_regular_points(100);
        let benchmark = CompressionBenchmark::run(&points);

        let none_result = benchmark.get(CompressionType::None);
        assert!(none_result.is_some());
        assert_eq!(none_result.unwrap().ratio, 1.0);

        let snappy_result = benchmark.get(CompressionType::Snappy);
        assert!(snappy_result.is_some());
    }

    #[test]
    fn test_benchmark_best() {
        let points = create_regular_points(100);
        let benchmark = CompressionBenchmark::run(&points);

        assert!(benchmark.best_ratio().is_some());
        assert!(benchmark.best_throughput().is_some());
    }

    #[test]
    fn test_benchmark_empty() {
        let benchmark = CompressionBenchmark::run(&[]);

        let result = benchmark.get(CompressionType::Snappy).unwrap();
        assert_eq!(result.ratio, 1.0);
    }
}
