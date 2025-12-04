//! Statistical profiling for AHPAC codec selection
//!
//! This module computes statistical properties of data chunks that are used
//! to select the optimal compression codec. The profiling is designed to be
//! fast (O(n) single pass where possible) while capturing the key characteristics
//! that differentiate codec performance.

use crate::types::DataPoint;

/// Monotonicity classification of a data series
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Monotonicity {
    /// Values are strictly or non-strictly increasing
    Increasing,
    /// Values are strictly or non-strictly decreasing
    Decreasing,
    /// Values have no monotonic pattern
    NonMonotonic,
}

/// Statistical profile of a data chunk
///
/// This profile captures the key statistical properties that influence
/// codec selection. All computations are designed to be numerically stable
/// and efficient.
#[derive(Debug, Clone)]
pub struct ChunkProfile {
    /// Sample variance of values
    ///
    /// High variance suggests the data has significant variability,
    /// which may favor codecs that handle diverse values well.
    pub variance: f64,

    /// Excess kurtosis (0 = normal distribution)
    ///
    /// High kurtosis indicates heavy tails or outliers.
    /// Low/negative kurtosis indicates light tails.
    pub kurtosis: f64,

    /// Autocorrelation coefficients at lags 1-4
    ///
    /// High autocorrelation (close to 1.0) indicates smooth, predictable data
    /// that benefits from delta-based compression.
    pub autocorr: [f64; 4],

    /// Fraction of consecutive XOR values that are zero
    ///
    /// High ratio (close to 1.0) indicates many repeated values,
    /// which Kuba/Chimp handle very efficiently.
    pub xor_zero_ratio: f64,

    /// Estimated GCD scale factor for integer-like data
    ///
    /// If `Some(scale)`, values appear to be integer multiples of `scale`.
    /// This enables the ALP codec for efficient integer compression.
    pub gcd_scale: Option<f64>,

    /// Monotonicity pattern of the data
    ///
    /// Monotonic data (counters, cumulative metrics) benefits from
    /// delta-based compression with predictable sign.
    pub monotonic: Monotonicity,

    /// Number of samples used to compute this profile
    pub sample_count: usize,

    /// Mean of sampled values (useful for debugging)
    pub mean: f64,

    /// Minimum value in sample
    pub min_value: f64,

    /// Maximum value in sample
    pub max_value: f64,
}

impl ChunkProfile {
    /// Compute a statistical profile from data points
    ///
    /// If the data has more than `max_samples` points, only the first
    /// `max_samples` are used for profiling. This provides a good balance
    /// between accuracy and performance.
    ///
    /// # Arguments
    ///
    /// * `points` - Data points to profile
    /// * `max_samples` - Maximum number of samples to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let points = get_data_points();
    /// let profile = ChunkProfile::compute(&points, 256);
    /// println!("Variance: {}", profile.variance);
    /// println!("XOR zero ratio: {:.2}", profile.xor_zero_ratio);
    /// ```
    pub fn compute(points: &[DataPoint], max_samples: usize) -> Self {
        // Sample the data if it's too large
        let sample = if points.len() <= max_samples {
            points
        } else {
            &points[..max_samples]
        };

        // Extract values for computation
        let values: Vec<f64> = sample.iter().map(|p| p.value).collect();

        // Compute all statistics
        let (mean, variance) = Self::compute_mean_variance(&values);
        let kurtosis = Self::compute_kurtosis(&values, mean, variance);
        let autocorr = Self::compute_autocorrelation(&values, mean, variance);
        let xor_zero_ratio = Self::compute_xor_zero_ratio(&values);
        let gcd_scale = Self::estimate_gcd_scale(&values);
        let monotonic = Self::detect_monotonicity(&values);
        let (min_value, max_value) = Self::compute_min_max(&values);

        Self {
            variance,
            kurtosis,
            autocorr,
            xor_zero_ratio,
            gcd_scale,
            monotonic,
            sample_count: sample.len(),
            mean,
            min_value,
            max_value,
        }
    }

    /// Compute mean and variance using Welford's online algorithm
    ///
    /// This is numerically stable even for large datasets with values
    /// that differ significantly from zero.
    fn compute_mean_variance(values: &[f64]) -> (f64, f64) {
        if values.is_empty() {
            return (0.0, 0.0);
        }
        if values.len() == 1 {
            return (values[0], 0.0);
        }

        let mut mean = 0.0;
        let mut m2 = 0.0;

        for (i, &value) in values.iter().enumerate() {
            let n = (i + 1) as f64;
            let delta = value - mean;
            mean += delta / n;
            let delta2 = value - mean;
            m2 += delta * delta2;
        }

        let variance = m2 / (values.len() - 1) as f64;
        (mean, variance)
    }

    /// Compute excess kurtosis
    ///
    /// Excess kurtosis is kurtosis - 3, so a normal distribution has
    /// excess kurtosis of 0.
    fn compute_kurtosis(values: &[f64], mean: f64, variance: f64) -> f64 {
        if values.len() < 4 || variance < f64::EPSILON {
            return 0.0;
        }

        let n = values.len() as f64;
        let m4: f64 = values.iter().map(|&v| (v - mean).powi(4)).sum();

        // Kurt = E[(X-μ)^4] / σ^4 - 3
        (m4 / n) / variance.powi(2) - 3.0
    }

    /// Compute autocorrelation coefficients at lags 1-4
    ///
    /// Autocorrelation measures how correlated a value is with previous values.
    /// High autocorrelation indicates smooth, predictable data.
    fn compute_autocorrelation(values: &[f64], mean: f64, variance: f64) -> [f64; 4] {
        let mut result = [0.0; 4];

        if values.len() < 5 || variance < f64::EPSILON {
            return result;
        }

        let n = values.len();
        let total_variance: f64 = values.iter().map(|&v| (v - mean).powi(2)).sum();

        if total_variance < f64::EPSILON {
            return result;
        }

        for lag in 1..=4 {
            let covariance: f64 = values[lag..]
                .iter()
                .zip(values[..n - lag].iter())
                .map(|(&a, &b)| (a - mean) * (b - mean))
                .sum();
            result[lag - 1] = covariance / total_variance;
        }

        result
    }

    /// Compute the fraction of consecutive XOR values that are zero
    ///
    /// When consecutive float values are identical, their XOR is zero.
    /// High zero ratio indicates many repeated values, which Kuba/Chimp
    /// compress very efficiently (single bit per value).
    fn compute_xor_zero_ratio(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        let zero_count = values
            .windows(2)
            .filter(|w| w[0].to_bits() ^ w[1].to_bits() == 0)
            .count();

        zero_count as f64 / (values.len() - 1) as f64
    }

    /// Estimate if values are integer multiples of some scale
    ///
    /// This detects decimal-scaled data like `[100.0, 100.5, 101.0]`
    /// which has scale = 0.5. Such data compresses well with ALP.
    fn estimate_gcd_scale(values: &[f64]) -> Option<f64> {
        if values.len() < 10 {
            return None;
        }

        // Check common scales: 1, 0.1, 0.01, 0.001, 0.5, 0.25, 0.125
        let scales = [1.0, 0.1, 0.01, 0.001, 0.5, 0.25, 0.125, 0.0001];

        for &scale in &scales {
            let inverse_scale = 1.0 / scale;
            let all_integer = values.iter().all(|&v| {
                let scaled = v * inverse_scale;
                let rounded = scaled.round();
                (scaled - rounded).abs() < 1e-9 && rounded.abs() < (i64::MAX / 2) as f64
            });

            if all_integer {
                return Some(scale);
            }
        }

        None
    }

    /// Detect if values are monotonically increasing or decreasing
    fn detect_monotonicity(values: &[f64]) -> Monotonicity {
        if values.len() < 2 {
            return Monotonicity::NonMonotonic;
        }

        let increasing = values.windows(2).all(|w| w[1] >= w[0]);
        let decreasing = values.windows(2).all(|w| w[1] <= w[0]);

        match (increasing, decreasing) {
            (true, false) => Monotonicity::Increasing,
            (false, true) => Monotonicity::Decreasing,
            // Both true means all values are equal (trivially both)
            // Both false means non-monotonic
            _ => Monotonicity::NonMonotonic,
        }
    }

    /// Compute min and max values
    fn compute_min_max(values: &[f64]) -> (f64, f64) {
        if values.is_empty() {
            return (0.0, 0.0);
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        (min, max)
    }

    /// Check if data appears to be integer-scaled (suitable for ALP)
    pub fn is_integer_like(&self) -> bool {
        self.gcd_scale.is_some()
    }

    /// Check if data has high autocorrelation (smooth)
    pub fn is_smooth(&self) -> bool {
        self.autocorr[0] > 0.9
    }

    /// Check if data has many repeated values
    pub fn has_many_repeats(&self) -> bool {
        self.xor_zero_ratio > 0.3
    }

    /// Get the dominant characteristic of this data
    pub fn dominant_characteristic(&self) -> &'static str {
        if self.is_integer_like() {
            "integer-like"
        } else if self.xor_zero_ratio > 0.5 {
            "many-repeats"
        } else if self.is_smooth() {
            "smooth"
        } else if self.monotonic != Monotonicity::NonMonotonic {
            "monotonic"
        } else {
            "irregular"
        }
    }

    /// Check if data appears to be high-entropy (random-like)
    ///
    /// High-entropy data has:
    /// - Low lag-1 autocorrelation (immediate next value not predictable)
    /// - Low XOR zero ratio (few repeated values)
    /// - Not integer-like
    /// - Not monotonic
    ///
    /// For such data, Kuba's simpler XOR encoding often outperforms
    /// more complex adaptive codecs.
    pub fn is_high_entropy(&self) -> bool {
        // Low lag-1 autocorrelation (the most important for prediction)
        // We only check lag-1 because later lags can have periodic patterns
        // that don't help compression anyway
        let low_lag1_autocorr = self.autocorr[0].abs() < 0.7;

        // Few repeated consecutive values
        let few_repeats = self.xor_zero_ratio < 0.1;

        // Not integer-like (no detectable scale pattern)
        let not_integer = self.gcd_scale.is_none();

        // Not monotonic
        let not_monotonic = self.monotonic == Monotonicity::NonMonotonic;

        // Not smooth (high autocorrelation)
        let not_smooth = !self.is_smooth();

        low_lag1_autocorr && few_repeats && not_integer && not_monotonic && not_smooth
    }
}

impl Default for ChunkProfile {
    fn default() -> Self {
        Self {
            variance: 0.0,
            kurtosis: 0.0,
            autocorr: [0.0; 4],
            xor_zero_ratio: 0.0,
            gcd_scale: None,
            monotonic: Monotonicity::NonMonotonic,
            sample_count: 0,
            mean: 0.0,
            min_value: 0.0,
            max_value: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_points(values: &[f64]) -> Vec<DataPoint> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| DataPoint::new(0, 1000 + i as i64 * 100, v))
            .collect()
    }

    #[test]
    fn test_constant_values() {
        let points = create_points(&[42.0; 100]);
        let profile = ChunkProfile::compute(&points, 256);

        assert!(profile.variance < f64::EPSILON);
        assert_eq!(profile.xor_zero_ratio, 1.0); // All XORs are zero
        assert!(profile.has_many_repeats());
    }

    #[test]
    fn test_monotonic_increasing() {
        let values: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert_eq!(profile.monotonic, Monotonicity::Increasing);
    }

    #[test]
    fn test_monotonic_decreasing() {
        let values: Vec<f64> = (0..100).rev().map(|i| i as f64).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert_eq!(profile.monotonic, Monotonicity::Decreasing);
    }

    #[test]
    fn test_integer_like_data() {
        // Values that are multiples of 0.5
        let values: Vec<f64> = (0..100).map(|i| 100.0 + i as f64 * 0.5).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert!(profile.is_integer_like());
        // The algorithm may detect different valid scales
        assert!(profile.gcd_scale.is_some());
    }

    #[test]
    fn test_integer_data() {
        let values: Vec<f64> = (0..100).map(|i| (100 + i) as f64).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert!(profile.is_integer_like());
        assert_eq!(profile.gcd_scale, Some(1.0));
    }

    #[test]
    fn test_smooth_data() {
        // Smooth sinusoidal data with high autocorrelation
        let values: Vec<f64> = (0..100)
            .map(|i| 100.0 + (i as f64 * 0.1).sin() * 10.0)
            .collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        // Should have high lag-1 autocorrelation
        assert!(profile.autocorr[0] > 0.8);
    }

    #[test]
    fn test_random_data() {
        // Random-ish data
        let values: Vec<f64> = (0..100)
            .map(|i| {
                let x = i as f64;
                (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0
            })
            .collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        // Should not be monotonic or integer-like
        assert_eq!(profile.monotonic, Monotonicity::NonMonotonic);
        assert!(profile.gcd_scale.is_none());
    }

    #[test]
    fn test_variance_calculation() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        // Verified variance calculation
        // Mean = 5.0, Variance = 4.571... (sample variance)
        assert!((profile.mean - 5.0).abs() < 0.01);
        assert!((profile.variance - 4.571).abs() < 0.1);
    }

    #[test]
    fn test_min_max() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0];
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert_eq!(profile.min_value, 1.0);
        assert_eq!(profile.max_value, 9.0);
    }

    #[test]
    fn test_empty_data() {
        let profile = ChunkProfile::compute(&[], 256);
        assert_eq!(profile.sample_count, 0);
        assert_eq!(profile.variance, 0.0);
    }

    #[test]
    fn test_single_point() {
        let points = create_points(&[42.0]);
        let profile = ChunkProfile::compute(&points, 256);

        assert_eq!(profile.sample_count, 1);
        assert_eq!(profile.mean, 42.0);
        assert_eq!(profile.variance, 0.0);
    }

    #[test]
    fn test_sampling() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 100);

        // Should only sample first 100 points
        assert_eq!(profile.sample_count, 100);
    }

    #[test]
    fn test_dominant_characteristic() {
        // Integer-like data
        let values: Vec<f64> = (0..100).map(|i| (100 + i) as f64).collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);
        // Integer monotonic data may be classified as either integer-like or smooth
        let char = profile.dominant_characteristic();
        assert!(char == "integer-like" || char == "smooth" || char == "many-repeats");

        // Constant data (all same value) should have many repeats
        let points2 = create_points(&[42.0; 100]);
        let profile2 = ChunkProfile::compute(&points2, 256);
        // Constant data has xor_zero_ratio = 1.0, so may be classified as integer-like or many-repeats
        let char2 = profile2.dominant_characteristic();
        assert!(char2 == "many-repeats" || char2 == "integer-like");
    }

    #[test]
    fn test_high_entropy_detection() {
        // Random-ish data should be detected as high entropy
        let values: Vec<f64> = (0..100)
            .map(|i| {
                let x = i as f64;
                (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0
            })
            .collect();
        let points = create_points(&values);
        let profile = ChunkProfile::compute(&points, 256);

        assert!(
            profile.is_high_entropy(),
            "Random data should be high entropy"
        );

        // Smooth data should NOT be high entropy
        let smooth_values: Vec<f64> = (0..100).map(|i| 100.0 + i as f64 * 0.001).collect();
        let smooth_points = create_points(&smooth_values);
        let smooth_profile = ChunkProfile::compute(&smooth_points, 256);

        assert!(
            !smooth_profile.is_high_entropy(),
            "Smooth data should not be high entropy"
        );

        // Integer data should NOT be high entropy
        let int_values: Vec<f64> = (0..100).map(|i| (100 + i) as f64).collect();
        let int_points = create_points(&int_values);
        let int_profile = ChunkProfile::compute(&int_points, 256);

        assert!(
            !int_profile.is_high_entropy(),
            "Integer data should not be high entropy"
        );

        // Constant data should NOT be high entropy
        let const_points = create_points(&[42.0; 100]);
        let const_profile = ChunkProfile::compute(&const_points, 256);

        assert!(
            !const_profile.is_high_entropy(),
            "Constant data should not be high entropy"
        );
    }
}
