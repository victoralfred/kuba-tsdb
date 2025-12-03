//! Downsample Operator - Data reduction for visualization
//!
//! Provides algorithms to reduce large time-series datasets to a manageable
//! size for visualization while preserving important visual features:
//!
//! - **LTTB (Largest Triangle Three Buckets)**: Preserves visual shape by
//!   selecting points that form the largest triangles with neighbors
//! - **M4**: Preserves min/max extremes in each bucket (good for spotting anomalies)
//! - **Simple**: Basic bucket-based average (fastest, least accurate)
//!
//! # When to Use Each Algorithm
//!
//! - **LTTB**: General purpose visualization, smooth time-series
//! - **M4**: When you need to preserve peaks/valleys (error rates, latency spikes)
//! - **Simple**: When speed is critical and approximate shape is acceptable

use crate::query::ast::DownsampleMethod;
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::{DataBatch, Operator};
use crate::types::SeriesId;

/// Downsample operator that reduces data points for visualization
pub struct DownsampleOperator {
    /// Input operator
    input: Box<dyn Operator>,

    /// Downsampling method
    method: DownsampleMethod,

    /// Target number of output points
    target_points: usize,

    /// Collected data (need all data for downsampling)
    collected: Vec<(i64, f64)>,

    /// Series ID (if applicable)
    /// Uses SeriesId (u128) for consistency with types module (TYPE-001)
    series_id: Option<SeriesId>,

    /// Whether we've consumed all input
    input_exhausted: bool,

    /// Whether we've emitted results
    results_emitted: bool,
}

impl DownsampleOperator {
    /// Create a new downsample operator
    pub fn new(input: Box<dyn Operator>, method: DownsampleMethod, target_points: usize) -> Self {
        Self {
            input,
            method,
            target_points: target_points.max(3), // Need at least 3 points for LTTB
            collected: Vec::new(),
            series_id: None,
            input_exhausted: false,
            results_emitted: false,
        }
    }

    /// Downsample collected data
    fn downsample(&self) -> Vec<(i64, f64)> {
        if self.collected.len() <= self.target_points {
            return self.collected.clone();
        }

        match self.method {
            DownsampleMethod::Lttb => self.lttb(),
            DownsampleMethod::M4 => self.m4(),
            DownsampleMethod::Average => self.simple_average(),
            DownsampleMethod::MinMax => self.m4(), // M4 preserves min/max
            DownsampleMethod::First => self.first_per_bucket(),
            DownsampleMethod::Last => self.last_per_bucket(),
        }
    }

    /// First value per bucket downsampling
    fn first_per_bucket(&self) -> Vec<(i64, f64)> {
        let n = self.collected.len();
        if n <= self.target_points {
            return self.collected.clone();
        }

        let bucket_size = n / self.target_points;
        let mut result = Vec::with_capacity(self.target_points);

        for bucket in 0..self.target_points {
            let start = bucket * bucket_size;
            if start < n {
                result.push(self.collected[start]);
            }
        }

        result
    }

    /// Last value per bucket downsampling
    fn last_per_bucket(&self) -> Vec<(i64, f64)> {
        let n = self.collected.len();
        if n <= self.target_points {
            return self.collected.clone();
        }

        let bucket_size = n / self.target_points;
        let mut result = Vec::with_capacity(self.target_points);

        for bucket in 0..self.target_points {
            let end = if bucket == self.target_points - 1 {
                n - 1
            } else {
                (bucket + 1) * bucket_size - 1
            };
            if end < n {
                result.push(self.collected[end]);
            }
        }

        result
    }

    /// LTTB (Largest Triangle Three Buckets) algorithm
    ///
    /// Selects points that form the largest triangles with their neighbors,
    /// preserving the visual shape of the data.
    fn lttb(&self) -> Vec<(i64, f64)> {
        let n = self.collected.len();
        let target = self.target_points;

        // Handle edge cases (EDGE-002)
        if n == 0 {
            return Vec::new();
        }

        if n <= 2 || n <= target {
            return self.collected.clone();
        }

        // Need at least 3 points for LTTB algorithm and target > 2
        if target <= 2 {
            // Return first and last points only
            return vec![self.collected[0], self.collected[n - 1]];
        }

        let mut result = Vec::with_capacity(target);

        // Always include first point
        result.push(self.collected[0]);

        // Bucket size (excluding first and last points)
        let bucket_size = (n - 2) as f64 / (target - 2) as f64;

        let mut a = 0; // Previous selected point index

        for i in 0..(target - 2) {
            // Calculate bucket boundaries
            let bucket_start = ((i as f64 * bucket_size) + 1.0).floor() as usize;
            let bucket_end = (((i + 1) as f64 * bucket_size) + 1.0).floor() as usize;
            let bucket_end = bucket_end.min(n - 1);

            // Calculate average point in next bucket (for triangle area)
            let next_bucket_start = bucket_end;
            let next_bucket_end = (((i + 2) as f64 * bucket_size) + 1.0).floor() as usize;
            let next_bucket_end = next_bucket_end.min(n);

            let (avg_x, avg_y) = if next_bucket_end > next_bucket_start {
                let count = (next_bucket_end - next_bucket_start) as f64;
                let sum_x: f64 = self.collected[next_bucket_start..next_bucket_end]
                    .iter()
                    .map(|(t, _)| *t as f64)
                    .sum();
                let sum_y: f64 = self.collected[next_bucket_start..next_bucket_end]
                    .iter()
                    .map(|(_, v)| *v)
                    .sum();
                (sum_x / count, sum_y / count)
            } else {
                let last = self.collected[n - 1];
                (last.0 as f64, last.1)
            };

            // Find point in current bucket that forms largest triangle
            let (a_x, a_y) = (self.collected[a].0 as f64, self.collected[a].1);
            let mut max_area = -1.0;
            let mut max_idx = bucket_start;

            for j in bucket_start..bucket_end {
                let (p_x, p_y) = (self.collected[j].0 as f64, self.collected[j].1);

                // Triangle area formula (simplified, we only need relative comparison)
                let area = ((a_x - avg_x) * (p_y - a_y) - (a_x - p_x) * (avg_y - a_y)).abs();

                if area > max_area {
                    max_area = area;
                    max_idx = j;
                }
            }

            result.push(self.collected[max_idx]);
            a = max_idx;
        }

        // Always include last point
        result.push(self.collected[n - 1]);

        result
    }

    /// M4 algorithm - preserves min/max extremes
    ///
    /// For each bucket, outputs 4 points: first, min, max, last
    /// This preserves peaks and valleys that might be missed by averaging.
    fn m4(&self) -> Vec<(i64, f64)> {
        let n = self.collected.len();
        let num_buckets = (self.target_points / 4).max(1);

        if n <= self.target_points {
            return self.collected.clone();
        }

        let bucket_size = n / num_buckets;
        let mut result = Vec::with_capacity(num_buckets * 4);

        for bucket in 0..num_buckets {
            let start = bucket * bucket_size;
            let end = if bucket == num_buckets - 1 {
                n
            } else {
                (bucket + 1) * bucket_size
            };

            if start >= end {
                continue;
            }

            let bucket_data = &self.collected[start..end];

            // Find first, min, max, last
            let first = bucket_data[0];
            let last = bucket_data[bucket_data.len() - 1];

            let mut min = first;
            let mut max = first;
            let mut min_idx = 0;
            let mut max_idx = 0;

            for (i, &(ts, val)) in bucket_data.iter().enumerate() {
                if val < min.1 {
                    min = (ts, val);
                    min_idx = i;
                }
                if val > max.1 {
                    max = (ts, val);
                    max_idx = i;
                }
            }

            // Add points in timestamp order
            let mut points = vec![
                (0, first),
                (min_idx, min),
                (max_idx, max),
                (bucket_data.len() - 1, last),
            ];
            points.sort_by_key(|(idx, _)| *idx);

            // Deduplicate (if min/max are same as first/last)
            let mut prev_ts = i64::MIN;
            for (_, (ts, val)) in points {
                if ts != prev_ts {
                    result.push((ts, val));
                    prev_ts = ts;
                }
            }
        }

        result
    }

    /// Simple bucket average downsampling
    ///
    /// Fast but may miss important peaks/valleys.
    fn simple_average(&self) -> Vec<(i64, f64)> {
        let n = self.collected.len();

        if n <= self.target_points {
            return self.collected.clone();
        }

        let bucket_size = n / self.target_points;
        let mut result = Vec::with_capacity(self.target_points);

        for bucket in 0..self.target_points {
            let start = bucket * bucket_size;
            let end = if bucket == self.target_points - 1 {
                n
            } else {
                (bucket + 1) * bucket_size
            };

            if start >= end {
                continue;
            }

            let bucket_data = &self.collected[start..end];
            let count = bucket_data.len() as f64;

            // Average timestamp and value
            let avg_ts =
                bucket_data.iter().map(|(t, _)| *t).sum::<i64>() / bucket_data.len() as i64;
            let avg_val = bucket_data.iter().map(|(_, v)| *v).sum::<f64>() / count;

            result.push((avg_ts, avg_val));
        }

        result
    }
}

impl Operator for DownsampleOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // If we've emitted results, we're done
        if self.results_emitted {
            return Ok(None);
        }

        // Collect all input first
        if !self.input_exhausted {
            while let Some(batch) = self.input.next_batch(ctx)? {
                if ctx.should_stop() {
                    if ctx.is_timed_out() {
                        return Err(QueryError::timeout("Downsample operator timed out"));
                    }
                    return Ok(None);
                }

                // Memory limit check BEFORE allocating (SEC-002)
                let additional_mem = batch.len() * 16; // 8 bytes timestamp + 8 bytes value
                if !ctx.allocate_memory(additional_mem) {
                    return Err(QueryError::resource_limit(
                        "Memory limit exceeded during downsampling",
                    ));
                }

                // Now safe to collect data points
                self.collected.reserve(batch.len());
                for i in 0..batch.len() {
                    self.collected.push((batch.timestamps[i], batch.values[i]));
                }

                // Track series ID
                if self.series_id.is_none() {
                    if let Some(ref sids) = batch.series_ids {
                        if !sids.is_empty() {
                            self.series_id = Some(sids[0]);
                        }
                    }
                }
            }
            self.input_exhausted = true;
        }

        // Apply downsampling
        let downsampled = self.downsample();
        self.results_emitted = true;

        if downsampled.is_empty() {
            return Ok(None);
        }

        // Build output batch
        let mut result = DataBatch::with_capacity(downsampled.len());
        for (ts, val) in downsampled {
            if let Some(sid) = self.series_id {
                result.push_with_series(ts, val, sid);
            } else {
                result.push(ts, val);
            }
        }

        ctx.record_rows(result.len());
        Ok(Some(result))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.collected.clear();
        self.series_id = None;
        self.input_exhausted = false;
        self.results_emitted = false;
    }

    fn name(&self) -> &'static str {
        "Downsample"
    }

    fn estimated_cardinality(&self) -> usize {
        self.target_points
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::SeriesSelector;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::scan::ScanOperator;

    fn all_series() -> SeriesSelector {
        SeriesSelector::by_measurement("test").unwrap()
    }

    fn create_test_data(n: usize) -> Vec<(i64, f64, SeriesId)> {
        (0..n)
            .map(|i| {
                let ts = i as i64 * 1000;
                // Create a sine wave for interesting visual data
                let val = (i as f64 * 0.1).sin() * 100.0 + 100.0;
                (ts, val, 1)
            })
            .collect()
    }

    #[test]
    fn test_lttb_downsample() {
        let data = create_test_data(1000);
        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(1000);

        let mut downsample = DownsampleOperator::new(Box::new(scan), DownsampleMethod::Lttb, 100);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 100);

        // Verify first and last points are preserved
        assert_eq!(result.timestamps[0], 0);
        assert_eq!(result.timestamps[99], 999000);
    }

    #[test]
    fn test_m4_downsample() {
        let data = create_test_data(1000);
        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(1000);

        let mut downsample = DownsampleOperator::new(Box::new(scan), DownsampleMethod::M4, 100);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();
        // M4 produces approximately target_points (may vary slightly)
        assert!(result.len() <= 100 && result.len() >= 50);
    }

    #[test]
    fn test_average_downsample() {
        let data = create_test_data(1000);
        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(1000);

        let mut downsample =
            DownsampleOperator::new(Box::new(scan), DownsampleMethod::Average, 100);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_downsample_small_data() {
        // Data smaller than target - should pass through unchanged
        let data = create_test_data(50);
        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(100);

        let mut downsample = DownsampleOperator::new(Box::new(scan), DownsampleMethod::Lttb, 100);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn test_lttb_preserves_shape() {
        // Create data with clear peaks
        let mut data: Vec<(i64, f64, SeriesId)> = Vec::new();
        for i in 0..100 {
            let val = if i == 25 || i == 75 {
                1000.0 // Clear peaks
            } else {
                100.0
            };
            data.push((i as i64 * 1000, val, 1));
        }

        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(100);

        let mut downsample = DownsampleOperator::new(Box::new(scan), DownsampleMethod::Lttb, 20);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();

        // LTTB should preserve the peaks (they form large triangles)
        assert!(result.values.contains(&1000.0));
    }

    #[test]
    fn test_m4_preserves_extremes() {
        // Create data with clear min/max
        let mut data: Vec<(i64, f64, SeriesId)> = Vec::new();
        for i in 0..100 {
            let val = if i == 10 {
                0.0 // Min
            } else if i == 90 {
                200.0 // Max
            } else {
                100.0
            };
            data.push((i as i64 * 1000, val, 1));
        }

        let scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_batch_size(100);

        let mut downsample = DownsampleOperator::new(Box::new(scan), DownsampleMethod::M4, 20);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = downsample.next_batch(&mut ctx).unwrap().unwrap();

        // M4 should preserve min and max
        let has_min = result.values.contains(&0.0);
        let has_max = result.values.contains(&200.0);
        assert!(has_min, "M4 should preserve minimum");
        assert!(has_max, "M4 should preserve maximum");
    }
}
