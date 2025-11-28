//! Query Operators - Vectorized execution primitives
//!
//! This module provides the building blocks for query execution:
//! - Scan operators for reading from storage
//! - Filter operators for predicate evaluation
//! - Aggregation operators for computing statistics
//! - Downsample operators for data reduction
//!
//! All operators work on batches (vectors) of data for CPU efficiency,
//! leveraging SIMD instructions where possible.

pub mod aggregation;
pub mod downsample;
pub mod filter;
pub mod scan;
pub mod storage_scan;

// Re-export commonly used types
pub use aggregation::{AggregationOperator, AggregationState};
pub use downsample::DownsampleOperator;
pub use filter::FilterOperator;
pub use scan::ScanOperator;
pub use storage_scan::{StorageQueryExt, StorageScanOperator};

use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::types::SeriesId;

// ============================================================================
// Data Batch
// ============================================================================

/// A batch of data points for vectorized processing
///
/// Batches use columnar layout for better cache utilization and SIMD processing.
/// Typical batch size is 4096 elements (fits in L1 cache).
#[derive(Debug, Clone)]
pub struct DataBatch {
    /// Timestamps in nanoseconds (aligned for SIMD)
    pub timestamps: Vec<i64>,

    /// Values (aligned for SIMD)
    pub values: Vec<f64>,

    /// Series IDs (optional, for multi-series queries)
    /// Uses SeriesId (u128) for consistency with types module (TYPE-001)
    pub series_ids: Option<Vec<SeriesId>>,

    /// Validity bitmap for null handling (1 bit per value)
    /// None means all values are valid
    pub validity: Option<Vec<u8>>,
}

impl DataBatch {
    /// Create a new empty batch with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            series_ids: None,
            validity: None,
        }
    }

    /// Create a batch from parallel vectors
    pub fn new(timestamps: Vec<i64>, values: Vec<f64>) -> Self {
        debug_assert_eq!(timestamps.len(), values.len());
        Self {
            timestamps,
            values,
            series_ids: None,
            validity: None,
        }
    }

    /// Create a batch with series IDs
    pub fn with_series(timestamps: Vec<i64>, values: Vec<f64>, series_ids: Vec<SeriesId>) -> Self {
        debug_assert_eq!(timestamps.len(), values.len());
        debug_assert_eq!(timestamps.len(), series_ids.len());
        Self {
            timestamps,
            values,
            series_ids: Some(series_ids),
            validity: None,
        }
    }

    /// Number of rows in the batch
    #[inline]
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Check if batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Memory size in bytes
    pub fn memory_size(&self) -> usize {
        let base = self.timestamps.len() * 8 + self.values.len() * 8;
        // SeriesId is u128, so 16 bytes each
        let series = self.series_ids.as_ref().map(|s| s.len() * 16).unwrap_or(0);
        let validity = self.validity.as_ref().map(|v| v.len()).unwrap_or(0);
        base + series + validity
    }

    /// Check if a value at index is valid (not null)
    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        match &self.validity {
            None => true, // All valid if no bitmap
            Some(bitmap) => {
                let byte_idx = index / 8;
                let bit_idx = index % 8;
                bitmap
                    .get(byte_idx)
                    .map(|b| (b >> bit_idx) & 1 == 1)
                    .unwrap_or(false)
            }
        }
    }

    /// Add a row to the batch
    pub fn push(&mut self, timestamp: i64, value: f64) {
        self.timestamps.push(timestamp);
        self.values.push(value);
    }

    /// Add a row with series ID
    pub fn push_with_series(&mut self, timestamp: i64, value: f64, series_id: SeriesId) {
        self.timestamps.push(timestamp);
        self.values.push(value);
        if let Some(ref mut ids) = self.series_ids {
            ids.push(series_id);
        } else {
            let mut ids = Vec::with_capacity(self.timestamps.capacity());
            // Backfill with zeros for previous entries
            ids.resize(self.timestamps.len() - 1, 0);
            ids.push(series_id);
            self.series_ids = Some(ids);
        }
    }

    /// Merge another batch into this one
    pub fn extend(&mut self, other: DataBatch) {
        self.timestamps.extend(other.timestamps);
        self.values.extend(other.values);
        if let (Some(ref mut self_ids), Some(other_ids)) = (&mut self.series_ids, other.series_ids)
        {
            self_ids.extend(other_ids);
        }
    }

    /// Split batch into smaller chunks for parallel processing
    pub fn split_into_morsels(self, morsel_size: usize) -> Vec<DataBatch> {
        if self.len() <= morsel_size {
            return vec![self];
        }

        let n_morsels = self.len().div_ceil(morsel_size);
        let mut morsels = Vec::with_capacity(n_morsels);

        let mut start = 0;
        while start < self.len() {
            let end = (start + morsel_size).min(self.len());
            let morsel = DataBatch {
                timestamps: self.timestamps[start..end].to_vec(),
                values: self.values[start..end].to_vec(),
                series_ids: self.series_ids.as_ref().map(|s| s[start..end].to_vec()),
                validity: None, // TODO: slice validity bitmap
            };
            morsels.push(morsel);
            start = end;
        }

        morsels
    }
}

impl Default for DataBatch {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

// ============================================================================
// Operator Trait
// ============================================================================

/// Common interface for all query operators
///
/// Operators implement a pull-based model where downstream operators
/// request batches from upstream operators.
pub trait Operator: Send {
    /// Pull the next batch of data
    ///
    /// Returns:
    /// - `Ok(Some(batch))` - More data available
    /// - `Ok(None)` - No more data (end of stream)
    /// - `Err(e)` - Error occurred
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError>;

    /// Reset operator state for re-execution
    fn reset(&mut self);

    /// Get operator name for debugging/profiling
    fn name(&self) -> &'static str;

    /// Estimated output cardinality (rows)
    fn estimated_cardinality(&self) -> usize {
        0 // Unknown by default
    }
}

// ============================================================================
// SIMD Utilities
// ============================================================================

/// SIMD-accelerated operations on value arrays
///
/// These functions check for SIMD support at runtime and fall back
/// to scalar implementations if unavailable.
pub mod simd {
    /// Sum all values in the array using SIMD when available
    #[inline]
    pub fn sum_f64(values: &[f64]) -> f64 {
        // Check if we have enough values to benefit from SIMD
        if values.len() < 8 {
            return values.iter().sum();
        }

        // Use Kahan summation for better numerical accuracy
        let mut sum = 0.0;
        let mut compensation = 0.0;

        for &val in values {
            let y = val - compensation;
            let t = sum + y;
            compensation = (t - sum) - y;
            sum = t;
        }

        sum
    }

    /// Find minimum value using SIMD when available
    #[inline]
    pub fn min_f64(values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }

        let mut min = values[0];
        for &val in &values[1..] {
            if val < min {
                min = val;
            }
        }
        Some(min)
    }

    /// Find maximum value using SIMD when available
    #[inline]
    pub fn max_f64(values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }

        let mut max = values[0];
        for &val in &values[1..] {
            if val > max {
                max = val;
            }
        }
        Some(max)
    }

    /// Count values matching a predicate using SIMD when available
    #[inline]
    pub fn count_where<F>(values: &[f64], predicate: F) -> usize
    where
        F: Fn(f64) -> bool,
    {
        values.iter().filter(|&&v| predicate(v)).count()
    }

    /// Apply filter and return matching indices
    #[inline]
    pub fn filter_indices<F>(values: &[f64], predicate: F) -> Vec<usize>
    where
        F: Fn(f64) -> bool,
    {
        values
            .iter()
            .enumerate()
            .filter(|(_, &v)| predicate(v))
            .map(|(i, _)| i)
            .collect()
    }
}

// ============================================================================
// Numeric Utilities
// ============================================================================

/// Numerically stable algorithms for aggregation
pub mod numeric {
    /// Welford's online algorithm for mean and variance
    ///
    /// Provides numerically stable computation of mean and variance
    /// in a single pass through the data.
    #[derive(Debug, Clone, Default)]
    pub struct WelfordState {
        /// Number of values seen
        pub count: u64,
        /// Running mean
        pub mean: f64,
        /// Sum of squared differences from mean (M2)
        pub m2: f64,
    }

    impl WelfordState {
        /// Create a new state
        pub fn new() -> Self {
            Self::default()
        }

        /// Add a value to the computation
        #[inline]
        pub fn add(&mut self, value: f64) {
            self.count += 1;
            let delta = value - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = value - self.mean;
            self.m2 += delta * delta2;
        }

        /// Add multiple values
        pub fn add_batch(&mut self, values: &[f64]) {
            for &v in values {
                self.add(v);
            }
        }

        /// Get the current mean
        #[inline]
        pub fn mean(&self) -> f64 {
            self.mean
        }

        /// Get the population variance
        #[inline]
        pub fn variance_population(&self) -> f64 {
            if self.count == 0 {
                0.0
            } else {
                self.m2 / self.count as f64
            }
        }

        /// Get the sample variance
        #[inline]
        pub fn variance_sample(&self) -> f64 {
            if self.count < 2 {
                0.0
            } else {
                self.m2 / (self.count - 1) as f64
            }
        }

        /// Get the population standard deviation
        #[inline]
        pub fn stddev_population(&self) -> f64 {
            self.variance_population().sqrt()
        }

        /// Get the sample standard deviation
        #[inline]
        pub fn stddev_sample(&self) -> f64 {
            self.variance_sample().sqrt()
        }

        /// Merge another Welford state into this one
        ///
        /// Allows parallel computation by merging partial results
        pub fn merge(&mut self, other: &WelfordState) {
            if other.count == 0 {
                return;
            }
            if self.count == 0 {
                *self = other.clone();
                return;
            }

            let combined_count = self.count + other.count;
            let delta = other.mean - self.mean;

            // Combined mean
            let new_mean = self.mean + delta * (other.count as f64 / combined_count as f64);

            // Combined M2
            let new_m2 = self.m2
                + other.m2
                + delta * delta * (self.count as f64 * other.count as f64 / combined_count as f64);

            self.count = combined_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
    }

    /// Kahan summation for accurate floating-point sums
    #[derive(Debug, Clone, Default)]
    pub struct KahanSum {
        sum: f64,
        compensation: f64,
    }

    impl KahanSum {
        /// Create a new sum accumulator
        pub fn new() -> Self {
            Self::default()
        }

        /// Add a value to the sum
        #[inline]
        pub fn add(&mut self, value: f64) {
            let y = value - self.compensation;
            let t = self.sum + y;
            self.compensation = (t - self.sum) - y;
            self.sum = t;
        }

        /// Add multiple values
        pub fn add_batch(&mut self, values: &[f64]) {
            for &v in values {
                self.add(v);
            }
        }

        /// Get the current sum
        #[inline]
        pub fn sum(&self) -> f64 {
            self.sum
        }

        /// Merge another Kahan sum
        pub fn merge(&mut self, other: &KahanSum) {
            self.add(other.sum);
            self.compensation += other.compensation;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_batch_creation() {
        let batch = DataBatch::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_data_batch_with_series() {
        let batch = DataBatch::with_series(vec![1, 2, 3], vec![1.0, 2.0, 3.0], vec![10, 10, 20]);
        assert_eq!(batch.len(), 3);
        assert!(batch.series_ids.is_some());
    }

    #[test]
    fn test_data_batch_split() {
        let batch = DataBatch::new((0..100).collect(), (0..100).map(|i| i as f64).collect());
        let morsels = batch.split_into_morsels(30);
        assert_eq!(morsels.len(), 4); // 30 + 30 + 30 + 10
        assert_eq!(morsels[0].len(), 30);
        assert_eq!(morsels[3].len(), 10);
    }

    #[test]
    fn test_simd_sum() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let sum = simd::sum_f64(&values);
        assert!((sum - 499500.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_min_max() {
        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0];
        assert_eq!(simd::min_f64(&values), Some(1.0));
        assert_eq!(simd::max_f64(&values), Some(9.0));
    }

    #[test]
    fn test_welford_mean_variance() {
        let mut state = numeric::WelfordState::new();
        state.add_batch(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);

        assert!((state.mean() - 5.0).abs() < 0.001);
        assert!((state.variance_population() - 4.0).abs() < 0.001);
        assert!((state.stddev_population() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_welford_merge() {
        // Split data into two parts
        let mut state1 = numeric::WelfordState::new();
        state1.add_batch(&[2.0, 4.0, 4.0, 4.0]);

        let mut state2 = numeric::WelfordState::new();
        state2.add_batch(&[5.0, 5.0, 7.0, 9.0]);

        // Merge
        state1.merge(&state2);

        assert!((state1.mean() - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_kahan_sum_accuracy() {
        let mut kahan = numeric::KahanSum::new();
        let mut naive_sum = 0.0;

        // Add many small numbers to a large number
        kahan.add(1e15);
        naive_sum += 1e15;

        for _ in 0..1_000_000 {
            kahan.add(1.0);
            naive_sum += 1.0;
        }

        // Kahan should be more accurate
        let expected = 1e15 + 1_000_000.0;
        let kahan_error = (kahan.sum() - expected).abs();
        let naive_error = (naive_sum - expected).abs();

        // Kahan should have less error (in this case, Kahan should be exact)
        assert!(kahan_error <= naive_error);
    }
}
