//! Aggregation Operator - Statistical computations
//!
//! Provides vectorized aggregation functions for time-series data:
//! - Basic: count, sum, min, max, avg
//! - Statistical: stddev, variance, percentiles (via t-digest)
//! - Time-series specific: rate, increase, delta
//! - Windowed aggregations (tumbling, sliding)
//!
//! Uses numerically stable algorithms:
//! - Welford's algorithm for mean/variance
//! - Kahan summation for accurate sums
//! - T-digest for streaming percentiles

use crate::query::ast::{AggregationFunction, WindowSpec, WindowType};
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::numeric::{KahanSum, WelfordState};
use crate::query::operators::{simd, DataBatch, Operator};
use crate::types::SeriesId;
use std::collections::HashMap;
use tdigest::TDigest;

// ============================================================================
// Aggregation State
// ============================================================================

/// State for incremental aggregation computation
#[derive(Debug, Clone)]
pub enum AggregationState {
    /// Count of values
    Count(u64),

    /// Sum using Kahan summation
    Sum(KahanSum),

    /// Minimum value seen
    Min(Option<f64>),

    /// Maximum value seen
    Max(Option<f64>),

    /// Mean and variance using Welford's algorithm
    Stats(WelfordState),

    /// First value and timestamp
    First(Option<(i64, f64)>),

    /// Last value and timestamp
    Last(Option<(i64, f64)>),

    /// For rate calculation: first timestamp, first value, last timestamp, last value
    Rate {
        /// First timestamp and value seen
        first: Option<(i64, f64)>,
        /// Last timestamp and value seen
        last: Option<(i64, f64)>,
    },

    /// Distinct values (using HashSet for now, HLL for large cardinalities later)
    Distinct(std::collections::HashSet<u64>),

    /// Percentile computation state using t-digest for streaming estimation
    ///
    /// T-digest provides:
    /// - O(1) insertion with constant memory
    /// - Accurate percentile estimates (especially at tails)
    /// - Mergeable digests for parallel aggregation
    PercentileTDigest {
        /// Target percentile (0-100)
        target: u8,
        /// T-digest for streaming percentile estimation
        digest: TDigest,
    },

    /// Fallback percentile using collected values (for small datasets)
    /// Used when exact percentile is needed or dataset is small
    PercentileExact {
        /// Target percentile (0-100)
        target: u8,
        /// Collected values for exact percentile calculation
        values: Vec<f64>,
    },
}

impl AggregationState {
    /// Create initial state for an aggregation function
    pub fn new(function: &AggregationFunction) -> Self {
        match function {
            AggregationFunction::Count => AggregationState::Count(0),
            AggregationFunction::Sum => AggregationState::Sum(KahanSum::new()),
            AggregationFunction::Min => AggregationState::Min(None),
            AggregationFunction::Max => AggregationState::Max(None),
            AggregationFunction::Avg => AggregationState::Stats(WelfordState::new()),
            AggregationFunction::StdDev => AggregationState::Stats(WelfordState::new()),
            AggregationFunction::Variance => AggregationState::Stats(WelfordState::new()),
            AggregationFunction::Median => AggregationState::PercentileTDigest {
                target: 50,
                digest: TDigest::new_with_size(100), // 100 centroids for good accuracy
            },
            AggregationFunction::Percentile(p) => AggregationState::PercentileTDigest {
                target: *p,
                digest: TDigest::new_with_size(100),
            },
            AggregationFunction::First => AggregationState::First(None),
            AggregationFunction::Last => AggregationState::Last(None),
            AggregationFunction::Rate => AggregationState::Rate {
                first: None,
                last: None,
            },
            AggregationFunction::Increase => AggregationState::Rate {
                first: None,
                last: None,
            },
            AggregationFunction::Delta => AggregationState::Rate {
                first: None,
                last: None,
            },
            AggregationFunction::CountDistinct => {
                AggregationState::Distinct(std::collections::HashSet::new())
            },
        }
    }

    /// Update state with a batch of values
    pub fn update_batch(&mut self, timestamps: &[i64], values: &[f64]) {
        match self {
            AggregationState::Count(count) => {
                *count += values.len() as u64;
            },
            AggregationState::Sum(kahan) => {
                kahan.add_batch(values);
            },
            AggregationState::Min(min) => {
                if let Some(batch_min) = simd::min_f64(values) {
                    *min = Some(min.map(|m| m.min(batch_min)).unwrap_or(batch_min));
                }
            },
            AggregationState::Max(max) => {
                if let Some(batch_max) = simd::max_f64(values) {
                    *max = Some(max.map(|m| m.max(batch_max)).unwrap_or(batch_max));
                }
            },
            AggregationState::Stats(welford) => {
                welford.add_batch(values);
            },
            AggregationState::First(first) => {
                if first.is_none() && !values.is_empty() {
                    *first = Some((timestamps[0], values[0]));
                }
            },
            AggregationState::Last(last) => {
                if !values.is_empty() {
                    let idx = values.len() - 1;
                    *last = Some((timestamps[idx], values[idx]));
                }
            },
            AggregationState::Rate { first, last } => {
                if first.is_none() && !values.is_empty() {
                    *first = Some((timestamps[0], values[0]));
                }
                if !values.is_empty() {
                    let idx = values.len() - 1;
                    *last = Some((timestamps[idx], values[idx]));
                }
            },
            AggregationState::Distinct(set) => {
                // Hash values for distinct counting
                for &v in values {
                    set.insert(v.to_bits());
                }
            },
            AggregationState::PercentileTDigest { digest, .. } => {
                // Use t-digest for streaming percentile estimation
                // Filter NaN values before adding to digest
                let valid_values: Vec<f64> =
                    values.iter().filter(|v| !v.is_nan()).copied().collect();
                if !valid_values.is_empty() {
                    // Merge new values into the digest
                    *digest = digest.merge_unsorted(valid_values);
                }
            },
            AggregationState::PercentileExact { values: vals, .. } => {
                // Collect all values (will be sorted when finalizing)
                // Filter NaN values
                vals.extend(values.iter().filter(|v| !v.is_nan()).copied());
            },
        }
    }

    /// Finalize and return the aggregated value
    pub fn finalize(&self, function: &AggregationFunction) -> f64 {
        match (self, function) {
            (AggregationState::Count(count), _) => *count as f64,
            (AggregationState::Sum(kahan), _) => kahan.sum(),
            (AggregationState::Min(min), _) => min.unwrap_or(f64::NAN),
            (AggregationState::Max(max), _) => max.unwrap_or(f64::NAN),
            (AggregationState::Stats(welford), AggregationFunction::Avg) => welford.mean(),
            (AggregationState::Stats(welford), AggregationFunction::StdDev) => {
                welford.stddev_sample()
            },
            (AggregationState::Stats(welford), AggregationFunction::Variance) => {
                welford.variance_sample()
            },
            (AggregationState::First(first), _) => first.map(|(_, v)| v).unwrap_or(f64::NAN),
            (AggregationState::Last(last), _) => last.map(|(_, v)| v).unwrap_or(f64::NAN),
            (AggregationState::Rate { first, last }, AggregationFunction::Rate) => {
                // Rate = (last_value - first_value) / time_delta_in_seconds
                match (first, last) {
                    (Some((t1, v1)), Some((t2, v2))) if *t2 > *t1 => {
                        // Use saturating_sub to prevent overflow (SEC-005)
                        let time_delta_nanos = t2.saturating_sub(*t1);
                        if time_delta_nanos == 0 {
                            return f64::NAN;
                        }
                        let time_delta_secs = time_delta_nanos as f64 / 1_000_000_000.0;
                        (v2 - v1) / time_delta_secs
                    },
                    _ => f64::NAN,
                }
            },
            (AggregationState::Rate { first, last }, AggregationFunction::Increase) => {
                // Increase = last_value - first_value (for counters)
                match (first, last) {
                    (Some((_, v1)), Some((_, v2))) => v2 - v1,
                    _ => f64::NAN,
                }
            },
            (AggregationState::Rate { first, last }, AggregationFunction::Delta) => {
                // Delta = last_value - first_value (for gauges, can be negative)
                match (first, last) {
                    (Some((_, v1)), Some((_, v2))) => v2 - v1,
                    _ => f64::NAN,
                }
            },
            (AggregationState::Distinct(set), _) => set.len() as f64,
            (AggregationState::PercentileTDigest { target, digest }, _) => {
                // Use t-digest for streaming percentile estimation
                // Returns estimate at the given percentile (0-100 scale converted to 0-1)
                if digest.is_empty() {
                    return f64::NAN;
                }
                digest.estimate_quantile(*target as f64 / 100.0)
            },
            (AggregationState::PercentileExact { target, values }, _) => {
                // Exact percentile calculation for small datasets
                // Filter out NaN values before calculation (EDGE-004)
                let mut valid_values: Vec<f64> =
                    values.iter().filter(|v| !v.is_nan()).copied().collect();

                // Handle empty input (EDGE-001)
                if valid_values.is_empty() {
                    return f64::NAN;
                }

                // Handle single value case
                if valid_values.len() == 1 {
                    return valid_values[0];
                }

                // Use O(n) selection algorithm instead of O(n log n) sort (PERF-003)
                let idx =
                    ((*target as f64 / 100.0) * (valid_values.len() - 1) as f64).round() as usize;
                let idx = idx.min(valid_values.len().saturating_sub(1));

                // select_nth_unstable partially sorts and returns the element at idx
                let (_, percentile_value, _) = valid_values.select_nth_unstable_by(idx, |a, b| {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                });
                *percentile_value
            },
            _ => f64::NAN,
        }
    }

    /// Merge another state into this one (for parallel aggregation)
    pub fn merge(&mut self, other: &AggregationState) {
        match (self, other) {
            (AggregationState::Count(a), AggregationState::Count(b)) => {
                *a += b;
            },
            (AggregationState::Sum(a), AggregationState::Sum(b)) => {
                a.merge(b);
            },
            (AggregationState::Min(a), AggregationState::Min(b)) => {
                *a = match (*a, *b) {
                    (Some(va), Some(vb)) => Some(va.min(vb)),
                    (Some(v), None) | (None, Some(v)) => Some(v),
                    (None, None) => None,
                };
            },
            (AggregationState::Max(a), AggregationState::Max(b)) => {
                *a = match (*a, *b) {
                    (Some(va), Some(vb)) => Some(va.max(vb)),
                    (Some(v), None) | (None, Some(v)) => Some(v),
                    (None, None) => None,
                };
            },
            (AggregationState::Stats(a), AggregationState::Stats(b)) => {
                a.merge(b);
            },
            (AggregationState::First(a), AggregationState::First(b)) => {
                // Keep the earlier timestamp
                *a = match (*a, *b) {
                    (Some((t1, v1)), Some((t2, v2))) => {
                        if t1 <= t2 {
                            Some((t1, v1))
                        } else {
                            Some((t2, v2))
                        }
                    },
                    (Some(v), None) | (None, Some(v)) => Some(v),
                    (None, None) => None,
                };
            },
            (AggregationState::Last(a), AggregationState::Last(b)) => {
                // Keep the later timestamp
                *a = match (*a, *b) {
                    (Some((t1, v1)), Some((t2, v2))) => {
                        if t1 >= t2 {
                            Some((t1, v1))
                        } else {
                            Some((t2, v2))
                        }
                    },
                    (Some(v), None) | (None, Some(v)) => Some(v),
                    (None, None) => None,
                };
            },
            (AggregationState::Distinct(a), AggregationState::Distinct(b)) => {
                a.extend(b.iter());
            },
            (
                AggregationState::PercentileTDigest { digest: a, .. },
                AggregationState::PercentileTDigest { digest: b, .. },
            ) => {
                // T-digest supports efficient merging for parallel aggregation
                *a = TDigest::merge_digests(vec![a.clone(), b.clone()]);
            },
            (
                AggregationState::PercentileExact { values: a, .. },
                AggregationState::PercentileExact { values: b, .. },
            ) => {
                // For exact percentile, concatenate the values
                a.extend_from_slice(b);
            },
            _ => {}, // Incompatible states - no-op
        }
    }
}

// ============================================================================
// Aggregation Operator
// ============================================================================

/// Aggregation operator that computes statistics over input batches
pub struct AggregationOperator {
    /// Input operator
    input: Box<dyn Operator>,

    /// Aggregation function to compute
    function: AggregationFunction,

    /// Window specification (None for global aggregation)
    window: Option<WindowSpec>,

    /// Group by series ID
    group_by_series: bool,

    /// States for each aggregation (keyed by series_id if grouping)
    /// Uses SeriesId (u128) for consistency with types module (TYPE-001)
    states: HashMap<SeriesId, AggregationState>,

    /// Reusable buffer for grouping data by series (PERF-006)
    /// This avoids allocating a new HashMap for each batch
    series_data_buffer: HashMap<SeriesId, (Vec<i64>, Vec<f64>)>,

    /// Whether we've consumed all input
    input_exhausted: bool,

    /// Whether we've emitted results
    results_emitted: bool,
}

impl AggregationOperator {
    /// Create a new aggregation operator for global aggregation
    pub fn new(input: Box<dyn Operator>, function: AggregationFunction) -> Self {
        Self {
            input,
            function,
            window: None,
            group_by_series: false,
            states: HashMap::new(),
            series_data_buffer: HashMap::new(),
            input_exhausted: false,
            results_emitted: false,
        }
    }

    /// Enable grouping by series
    pub fn with_group_by_series(mut self) -> Self {
        self.group_by_series = true;
        self
    }

    /// Set window specification
    pub fn with_window(mut self, window: WindowSpec) -> Self {
        self.window = Some(window);
        self
    }

    /// Get or create state for a series
    ///
    /// Used for multi-series aggregation where each series maintains
    /// its own aggregation state (e.g., separate averages per host).
    fn get_state(&mut self, series_id: SeriesId) -> &mut AggregationState {
        let function = self.function;
        self.states
            .entry(series_id)
            .or_insert_with(|| AggregationState::new(&function))
    }

    /// Process a batch of input data
    fn process_batch(&mut self, batch: &DataBatch) {
        if self.group_by_series {
            // Group by series ID
            if let Some(ref series_ids) = batch.series_ids {
                // Reuse the series_data_buffer instead of allocating new (PERF-006)
                // Clear vectors but keep HashMap allocation
                for (_, (ts, vals)) in self.series_data_buffer.iter_mut() {
                    ts.clear();
                    vals.clear();
                }

                // Group data by series using the reusable buffer
                for (i, &sid) in series_ids.iter().enumerate() {
                    let entry = self
                        .series_data_buffer
                        .entry(sid)
                        .or_insert_with(|| (Vec::new(), Vec::new()));
                    entry.0.push(batch.timestamps[i]);
                    entry.1.push(batch.values[i]);
                }

                // Collect series IDs to process (avoids borrow conflict)
                let series_to_update: Vec<SeriesId> =
                    self.series_data_buffer.keys().copied().collect();

                // Update states for each series using get_state helper
                for sid in series_to_update {
                    if let Some((timestamps, values)) = self.series_data_buffer.get(&sid) {
                        if !timestamps.is_empty() {
                            // Clone data to avoid borrow conflict
                            let ts = timestamps.clone();
                            let vals = values.clone();
                            self.get_state(sid).update_batch(&ts, &vals);
                        }
                    }
                }
            }
        } else {
            // Global aggregation - all data goes to series 0
            self.get_state(0)
                .update_batch(&batch.timestamps, &batch.values);
        }
    }

    /// Finalize aggregations and produce output batch
    fn finalize(&self) -> DataBatch {
        let mut result = DataBatch::with_capacity(self.states.len());

        for (&series_id, state) in &self.states {
            let value = state.finalize(&self.function);
            // Use 0 as timestamp for aggregated results
            result.push_with_series(0, value, series_id);
        }

        result
    }
}

impl Operator for AggregationOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // If we've already emitted results, we're done
        if self.results_emitted {
            return Ok(None);
        }

        // Consume all input first (for global aggregation)
        if !self.input_exhausted {
            while let Some(batch) = self.input.next_batch(ctx)? {
                if ctx.should_stop() {
                    if ctx.is_timed_out() {
                        return Err(QueryError::timeout("Aggregation operator timed out"));
                    }
                    return Ok(None);
                }

                // Memory limit check for percentile aggregation (SEC-001)
                // Note: With t-digest, memory is constant (~8KB per centroid).
                // This check is kept for PercentileExact variant which stores all values.
                // T-digest uses ~100 centroids by default = ~800 bytes total.
                if matches!(
                    self.function,
                    AggregationFunction::Percentile(_) | AggregationFunction::Median
                ) {
                    // Track batch processing memory (temporary allocations)
                    let additional_mem = batch.len() * 8; // 8 bytes per f64 value
                    if !ctx.allocate_memory(additional_mem) {
                        return Err(QueryError::resource_limit(
                            "Memory limit exceeded during percentile aggregation",
                        ));
                    }
                    // Release immediately since t-digest doesn't keep all values
                    ctx.release_memory(additional_mem);
                }

                self.process_batch(&batch);
            }
            self.input_exhausted = true;
        }

        // Emit final results
        self.results_emitted = true;
        let result = self.finalize();

        if result.is_empty() {
            Ok(None)
        } else {
            ctx.record_rows(result.len());
            Ok(Some(result))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.states.clear();
        self.input_exhausted = false;
        self.results_emitted = false;
    }

    fn name(&self) -> &'static str {
        "Aggregation"
    }

    fn estimated_cardinality(&self) -> usize {
        if self.group_by_series {
            // Estimate number of unique series
            100
        } else {
            // Global aggregation produces one row
            1
        }
    }
}

// ============================================================================
// Window Aggregation
// ============================================================================

/// Windowed aggregation state for tumbling/sliding windows
pub struct WindowedAggregation {
    /// Window specification
    window: WindowSpec,

    /// Current window boundaries
    window_start: i64,
    window_end: i64,

    /// States for current window
    states: Vec<AggregationState>,

    /// Aggregation functions
    functions: Vec<AggregationFunction>,
}

impl WindowedAggregation {
    /// Create a new windowed aggregation
    pub fn new(window: WindowSpec, functions: Vec<AggregationFunction>) -> Self {
        let states = functions.iter().map(AggregationState::new).collect();

        Self {
            window,
            window_start: 0,
            window_end: 0,
            states,
            functions,
        }
    }

    /// Initialize window boundaries based on first timestamp
    pub fn init_window(&mut self, timestamp: i64) {
        // Safely convert duration to nanoseconds (EDGE-011)
        // Duration::as_nanos() returns u128, which can overflow i64 for durations > 292 years
        let window_nanos = self.window.duration.as_nanos();
        let window_nanos = if window_nanos > i64::MAX as u128 {
            i64::MAX // Cap at max i64 for very long durations
        } else {
            window_nanos as i64
        };

        if window_nanos > 0 {
            // Align to window boundary
            self.window_start = (timestamp / window_nanos) * window_nanos;
            self.window_end = self.window_start.saturating_add(window_nanos);
        }
    }

    /// Check if timestamp falls in current window
    pub fn in_current_window(&self, timestamp: i64) -> bool {
        timestamp >= self.window_start && timestamp < self.window_end
    }

    /// Advance to next window
    pub fn advance_window(&mut self) {
        // Helper to safely convert u128 nanos to i64 (EDGE-011)
        let safe_nanos = |nanos: u128| -> i64 {
            if nanos > i64::MAX as u128 {
                i64::MAX
            } else {
                nanos as i64
            }
        };

        let advance_nanos = match &self.window.window_type {
            WindowType::Tumbling => safe_nanos(self.window.duration.as_nanos()),
            WindowType::Sliding { slide } => safe_nanos(slide.as_nanos()),
            WindowType::Session { .. } => safe_nanos(self.window.duration.as_nanos()),
        };

        self.window_start = self.window_start.saturating_add(advance_nanos);
        self.window_end = self
            .window_start
            .saturating_add(safe_nanos(self.window.duration.as_nanos()));

        // Reset states for new window
        self.states = self.functions.iter().map(AggregationState::new).collect();
    }

    /// Update states with a value
    pub fn update(&mut self, timestamp: i64, value: f64) {
        for state in &mut self.states {
            state.update_batch(&[timestamp], &[value]);
        }
    }

    /// Finalize current window and return results
    pub fn finalize_window(&self) -> Vec<f64> {
        self.states
            .iter()
            .zip(&self.functions)
            .map(|(state, func)| state.finalize(func))
            .collect()
    }

    /// Get current window start timestamp
    pub fn current_window_start(&self) -> i64 {
        self.window_start
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::scan::ScanOperator;
    use crate::query::SeriesSelector;

    fn create_test_scan() -> ScanOperator {
        let data: Vec<(i64, f64, SeriesId)> = (0..100)
            .map(|i| (i as i64 * 1_000_000_000, i as f64, 1)) // 1-second intervals
            .collect();

        ScanOperator::new(SeriesSelector::by_measurement("test").unwrap(), None)
            .with_test_data(data)
            .with_batch_size(100)
    }

    #[test]
    fn test_count_aggregation() {
        let scan = create_test_scan();
        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Count);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.values[0], 100.0);
    }

    #[test]
    fn test_sum_aggregation() {
        let scan = create_test_scan();
        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Sum);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 1);
        // Sum of 0..99 = 4950
        assert!((result.values[0] - 4950.0).abs() < 0.001);
    }

    #[test]
    fn test_avg_aggregation() {
        let scan = create_test_scan();
        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Avg);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 1);
        // Avg of 0..99 = 49.5
        assert!((result.values[0] - 49.5).abs() < 0.001);
    }

    #[test]
    fn test_min_max_aggregation() {
        let scan = create_test_scan();
        let mut agg_min = AggregationOperator::new(Box::new(scan), AggregationFunction::Min);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg_min.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.values[0], 0.0);
    }

    #[test]
    fn test_percentile_aggregation() {
        let scan = create_test_scan();
        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Percentile(50));

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        // P50 of 0..99 should be around 49-50
        assert!(result.values[0] >= 49.0 && result.values[0] <= 50.0);
    }

    #[test]
    fn test_rate_aggregation() {
        let data: Vec<(i64, f64, SeriesId)> = vec![
            (0, 0.0, 1),
            (1_000_000_000, 10.0, 1), // 10 increase over 1 second
            (2_000_000_000, 30.0, 1), // 20 more increase over 1 second
        ];

        let scan = ScanOperator::new(SeriesSelector::by_measurement("test").unwrap(), None)
            .with_test_data(data)
            .with_batch_size(100);

        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Rate);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        // Rate = (30 - 0) / 2 seconds = 15 per second
        assert!((result.values[0] - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_group_by_series() {
        // Data from two series
        let data: Vec<(i64, f64, SeriesId)> =
            vec![(0, 10.0, 1), (0, 20.0, 2), (1, 15.0, 1), (1, 25.0, 2)];

        let scan = ScanOperator::new(SeriesSelector::by_measurement("test").unwrap(), None)
            .with_test_data(data)
            .with_batch_size(100);

        let mut agg = AggregationOperator::new(Box::new(scan), AggregationFunction::Avg)
            .with_group_by_series();

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 2); // Two series

        // Check that both averages are correct
        let series_ids = result.series_ids.as_ref().unwrap();
        for (i, &sid) in series_ids.iter().enumerate().take(2) {
            if sid == 1 {
                // Series 1: avg of 10, 15 = 12.5
                assert!((result.values[i] - 12.5).abs() < 0.001);
            } else {
                // Series 2: avg of 20, 25 = 22.5
                assert!((result.values[i] - 22.5).abs() < 0.001);
            }
        }
    }

    #[test]
    fn test_state_merge() {
        // Test merging aggregation states (for parallel execution)
        let mut state1 = AggregationState::new(&AggregationFunction::Avg);
        let mut state2 = AggregationState::new(&AggregationFunction::Avg);

        state1.update_batch(&[0, 1], &[10.0, 20.0]);
        state2.update_batch(&[2, 3], &[30.0, 40.0]);

        state1.merge(&state2);

        let avg = state1.finalize(&AggregationFunction::Avg);
        assert!((avg - 25.0).abs() < 0.001); // (10+20+30+40)/4
    }
}
