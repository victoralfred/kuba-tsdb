//! Space-Time Aggregation Engine
//!
//! This module provides sophisticated aggregation across multiple time-series
//! from different hosts/sources with overlapping tags. It performs two-phase
//! aggregation:
//!
//! 1. **Spatial Aggregation**: Merge data from multiple series based on tag matchers
//! 2. **Temporal Aggregation**: Aggregate over time windows (downsampling)
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────┐
//! │                    Space-Time Aggregator                          │
//! ├────────────────────────────────────────────────────────────────────┤
//! │  Input: Multiple series with overlapping timestamps               │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
//! │  │ Series 1 │  │ Series 2 │  │ Series 3 │  │ Series N │          │
//! │  │ host=s1  │  │ host=s2  │  │ host=s3  │  │ host=sN  │          │
//! │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘          │
//! │       │              │              │              │              │
//! │       └──────────────┴──────────────┴──────────────┘              │
//! │                              │                                     │
//! │                    ┌─────────▼─────────┐                          │
//! │                    │ Spatial Aggregate │                          │
//! │                    │ (across hosts)    │                          │
//! │                    └─────────┬─────────┘                          │
//! │                              │                                     │
//! │                    ┌─────────▼─────────┐                          │
//! │                    │ Temporal Aggregate│                          │
//! │                    │ (time windows)    │                          │
//! │                    └─────────┬─────────┘                          │
//! │                              │                                     │
//! │                    ┌─────────▼─────────┐                          │
//! │                    │ Unified Series    │                          │
//! │                    └───────────────────┘                          │
//! └────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::aggregation::{
//!     SpaceTimeAggregator, AggregateFunction, AggregateQuery, InMemoryDataSource
//! };
//! use gorilla_tsdb::aggregation::index::TagMatcher;
//! use gorilla_tsdb::types::{DataPoint, TimeRange};
//! use std::time::Duration;
//!
//! // Create in-memory data source with sample data
//! let mut data_source = InMemoryDataSource::new();
//! data_source.add_series(1, vec![
//!     DataPoint::new(1, 1000, 10.0),
//!     DataPoint::new(1, 2000, 20.0),
//!     DataPoint::new(1, 3000, 30.0),
//! ]);
//!
//! let aggregator = SpaceTimeAggregator::new(data_source);
//!
//! // Query: avg over the time range
//! let query = AggregateQuery {
//!     matcher: TagMatcher::new(),
//!     time_range: TimeRange::new(0, 4000).unwrap(),
//!     window: Some(Duration::from_millis(4000)),
//!     function: AggregateFunction::Avg,
//!     step: None,
//!     offset: None,
//!     limit: None,
//! };
//!
//! let result = aggregator.aggregate(&[1], &query).unwrap();
//! assert!(!result.points.is_empty());
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::types::{DataPoint, SeriesId, TimeRange};

use super::data_model::{AggregatedPoint, AggregationMetadata, UnifiedTimeSeries};
use super::index::TagMatcher;

// ============================================================================
// Timestamp Validation Constants (SEC-009)
// ============================================================================

/// Minimum valid timestamp: January 1, 2000 00:00:00 UTC (in milliseconds)
/// This prevents accidental use of timestamps that are clearly invalid.
pub const MIN_VALID_TIMESTAMP_MS: i64 = 946_684_800_000;

/// Maximum valid timestamp: January 1, 2100 00:00:00 UTC (in milliseconds)
/// This prevents timestamps far in the future that could cause issues.
pub const MAX_VALID_TIMESTAMP_MS: i64 = 4_102_444_800_000;

/// SEC-009: Validate that a timestamp is within reasonable bounds
///
/// Returns true if the timestamp is between year 2000 and 2100.
/// This helps catch obviously invalid timestamps that could cause issues.
#[must_use]
pub fn is_valid_timestamp(timestamp_ms: i64) -> bool {
    (MIN_VALID_TIMESTAMP_MS..=MAX_VALID_TIMESTAMP_MS).contains(&timestamp_ms)
}

// ============================================================================
// Internal Data Structures
// ============================================================================

/// Timestamps for a set of aligned series data
///
/// Stores all unique timestamps across multiple series for alignment.
#[derive(Debug, Clone)]
struct SpaceTimeTimestamps {
    /// All unique timestamps, sorted
    pub timestamps: Vec<i64>,
}

impl SpaceTimeTimestamps {
    /// Create from a sorted, deduplicated list of timestamps
    fn new(timestamps: Vec<i64>) -> Self {
        Self { timestamps }
    }

    /// Get start time
    fn start_time(&self) -> i64 {
        self.timestamps.first().copied().unwrap_or(0)
    }

    /// Get end time
    fn end_time(&self) -> i64 {
        self.timestamps.last().copied().unwrap_or(0)
    }

    /// Get midpoint timestamp
    fn midpoint(&self) -> i64 {
        if self.timestamps.is_empty() {
            return 0;
        }
        let start = self.start_time();
        let end = self.end_time();
        start + (end - start) / 2
    }
}

/// Aligned data for a single series
///
/// Values aligned to a common timestamp grid. `None` means no data at that timestamp.
#[derive(Debug, Clone)]
struct SpaceTimeAlignedData {
    /// Values at each timestamp index (None if no data)
    pub values: Vec<Option<f64>>,
}

impl SpaceTimeAlignedData {
    /// Create aligned data from data points against a timestamp grid
    fn from_points(points: &[DataPoint], timestamps: &SpaceTimeTimestamps) -> Self {
        // Build a map from timestamp to value for fast lookup
        let point_map: HashMap<i64, f64> = points.iter().map(|p| (p.timestamp, p.value)).collect();

        // Create values vector aligned to timestamp grid
        let values: Vec<Option<f64>> = timestamps
            .timestamps
            .iter()
            .map(|&ts| point_map.get(&ts).copied())
            .collect();

        Self { values }
    }
}

/// Data from multiple series aligned on a common timestamp grid
#[derive(Debug)]
struct SpaceTimeMultiSeriesData {
    /// Common timestamp grid
    pub timestamps: SpaceTimeTimestamps,

    /// Aligned data per series
    pub series: HashMap<SeriesId, SpaceTimeAlignedData>,
}

impl SpaceTimeMultiSeriesData {
    /// Create from aligned timestamps and series data
    fn new(
        timestamps: SpaceTimeTimestamps,
        series: HashMap<SeriesId, SpaceTimeAlignedData>,
    ) -> Self {
        Self { timestamps, series }
    }
}

// ============================================================================
// Aggregate Functions
// ============================================================================

/// Aggregation function to apply across series and time
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    /// Sum of all values
    Sum,

    /// Average (mean) of all values
    Avg,

    /// Minimum value
    Min,

    /// Maximum value
    Max,

    /// Count of values
    Count,

    /// First value (by timestamp)
    First,

    /// Last value (by timestamp)
    Last,

    /// Rate of change per second (requires at least 2 points)
    Rate,

    /// Increase (monotonic counter difference)
    Increase,

    /// Histogram quantile (requires histogram data)
    Quantile(u8), // Percentile 0-100

    /// Standard deviation
    StdDev,

    /// Variance
    Variance,
}

impl AggregateFunction {
    /// Check if this function requires sorted input
    pub fn requires_sorted(&self) -> bool {
        matches!(
            self,
            AggregateFunction::First
                | AggregateFunction::Last
                | AggregateFunction::Rate
                | AggregateFunction::Increase
        )
    }

    /// Check if this function is streaming (can be computed incrementally)
    pub fn is_streaming(&self) -> bool {
        matches!(
            self,
            AggregateFunction::Sum
                | AggregateFunction::Min
                | AggregateFunction::Max
                | AggregateFunction::Count
        )
    }
}

// ============================================================================
// Aggregate State
// ============================================================================

/// State for incremental aggregation
#[derive(Debug, Clone)]
pub struct AggregateState {
    /// Sum of values (for sum, avg)
    sum: f64,

    /// Count of values
    count: u64,

    /// Minimum value
    min: f64,

    /// Maximum value
    max: f64,

    /// First value and timestamp
    first: Option<(i64, f64)>,

    /// Last value and timestamp
    last: Option<(i64, f64)>,

    /// Sum of squared differences from mean (for variance/stddev)
    m2: f64,

    /// Running mean (for Welford's algorithm)
    mean: f64,

    /// All values (for quantile - uses more memory)
    values: Option<Vec<f64>>,
}

impl AggregateState {
    /// Create new empty state
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
            m2: 0.0,
            mean: 0.0,
            values: None,
        }
    }

    /// Create state that tracks all values (for quantiles)
    pub fn with_values() -> Self {
        Self {
            values: Some(Vec::new()),
            ..Self::new()
        }
    }

    /// Add a value with timestamp
    pub fn add(&mut self, timestamp: i64, value: f64) {
        // Update sum and count
        self.sum += value;
        self.count += 1;

        // Update min/max
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }

        // Update first/last
        if self.first.is_none() || timestamp < self.first.unwrap().0 {
            self.first = Some((timestamp, value));
        }
        if self.last.is_none() || timestamp > self.last.unwrap().0 {
            self.last = Some((timestamp, value));
        }

        // Welford's online algorithm for variance
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;

        // Store value if tracking all
        if let Some(ref mut values) = self.values {
            values.push(value);
        }
    }

    /// Merge another state into this one
    pub fn merge(&mut self, other: &AggregateState) {
        if other.count == 0 {
            return;
        }

        // For first/last, compare timestamps
        if let Some((ts, val)) = other.first {
            if self.first.is_none() || ts < self.first.unwrap().0 {
                self.first = Some((ts, val));
            }
        }
        if let Some((ts, val)) = other.last {
            if self.last.is_none() || ts > self.last.unwrap().0 {
                self.last = Some((ts, val));
            }
        }

        // Update min/max
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Merge sums and counts
        let combined_count = self.count + other.count;
        let combined_sum = self.sum + other.sum;

        // Parallel algorithm for merging variance (Chan et al.)
        let delta = other.mean - self.mean;
        let combined_mean = (self.sum + other.sum) / combined_count as f64;
        let combined_m2 = self.m2
            + other.m2
            + delta * delta * (self.count * other.count) as f64 / combined_count as f64;

        self.sum = combined_sum;
        self.count = combined_count;
        self.mean = combined_mean;
        self.m2 = combined_m2;

        // Merge values if tracking
        if let (Some(ref mut self_vals), Some(ref other_vals)) = (&mut self.values, &other.values) {
            self_vals.extend(other_vals);
        }
    }

    /// Finalize and get result for given function
    pub fn finalize(&self, function: AggregateFunction) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        Some(match function {
            AggregateFunction::Sum => self.sum,
            AggregateFunction::Avg => self.sum / self.count as f64,
            AggregateFunction::Min => self.min,
            AggregateFunction::Max => self.max,
            AggregateFunction::Count => self.count as f64,
            AggregateFunction::First => self.first.map(|(_, v)| v)?,
            AggregateFunction::Last => self.last.map(|(_, v)| v)?,
            AggregateFunction::Rate => {
                // Rate = (last - first) / time_delta_in_seconds
                if let (Some((first_ts, first_val)), Some((last_ts, last_val))) =
                    (self.first, self.last)
                {
                    let time_delta = (last_ts - first_ts) as f64 / 1000.0; // ms to s
                    if time_delta > 0.0 {
                        (last_val - first_val) / time_delta
                    } else {
                        0.0
                    }
                } else {
                    return None;
                }
            }
            AggregateFunction::Increase => {
                // Increase = last - first (for counters)
                if let (Some((_, first_val)), Some((_, last_val))) = (self.first, self.last) {
                    // Handle counter reset
                    if last_val >= first_val {
                        last_val - first_val
                    } else {
                        last_val // Counter reset, return current value
                    }
                } else {
                    return None;
                }
            }
            AggregateFunction::StdDev => {
                if self.count < 2 {
                    return Some(0.0);
                }
                (self.m2 / (self.count - 1) as f64).sqrt()
            }
            AggregateFunction::Variance => {
                if self.count < 2 {
                    return Some(0.0);
                }
                self.m2 / (self.count - 1) as f64
            }
            AggregateFunction::Quantile(percentile) => {
                let values = self.values.as_ref()?;
                if values.is_empty() {
                    return None;
                }

                let mut sorted: Vec<f64> = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                let idx = (percentile as f64 / 100.0 * (sorted.len() - 1) as f64) as usize;
                sorted.get(idx).copied()?
            }
        })
    }
}

impl Default for AggregateState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Aggregate Query
// ============================================================================

/// Query specification for space-time aggregation
#[derive(Debug, Clone)]
pub struct AggregateQuery {
    /// Tag matcher for selecting series
    pub matcher: TagMatcher,

    /// Time range to query
    pub time_range: TimeRange,

    /// Aggregation function
    pub function: AggregateFunction,

    /// Time window for temporal aggregation (e.g., 5 minutes)
    /// If None, aggregates all data into a single point
    pub window: Option<Duration>,

    /// Step interval for output (if different from window)
    /// Defaults to window size if not specified
    pub step: Option<Duration>,

    /// Offset for window alignment
    /// Useful for aligning to specific boundaries (e.g., hourly)
    pub offset: Option<Duration>,

    /// Maximum number of output points
    pub limit: Option<usize>,
}

impl AggregateQuery {
    /// Create a new query with required fields
    pub fn new(matcher: TagMatcher, time_range: TimeRange, function: AggregateFunction) -> Self {
        Self {
            matcher,
            time_range,
            function,
            window: None,
            step: None,
            offset: None,
            limit: None,
        }
    }

    /// Set the time window
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = Some(window);
        self
    }

    /// Set the step interval
    pub fn with_step(mut self, step: Duration) -> Self {
        self.step = Some(step);
        self
    }

    /// Set the offset
    pub fn with_offset(mut self, offset: Duration) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set the limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

// ============================================================================
// Window Iterator
// ============================================================================

/// Iterator over time windows
pub struct WindowIterator {
    /// Current window start
    current: i64,

    /// Window end (exclusive)
    end: i64,

    /// Window size in milliseconds
    window_ms: i64,

    /// Step size in milliseconds
    step_ms: i64,
}

/// Error type for window iterator creation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowIteratorError {
    /// Start timestamp out of valid range
    InvalidStartTimestamp(i64),
    /// End timestamp out of valid range
    InvalidEndTimestamp(i64),
    /// Start timestamp after end timestamp
    StartAfterEnd {
        /// The start timestamp provided
        start: i64,
        /// The end timestamp provided
        end: i64,
    },
    /// Window size is zero or negative
    InvalidWindowSize,
}

impl std::fmt::Display for WindowIteratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowIteratorError::InvalidStartTimestamp(ts) => {
                write!(
                    f,
                    "Start timestamp {} is outside valid range (2000-2100)",
                    ts
                )
            }
            WindowIteratorError::InvalidEndTimestamp(ts) => {
                write!(f, "End timestamp {} is outside valid range (2000-2100)", ts)
            }
            WindowIteratorError::StartAfterEnd { start, end } => {
                write!(
                    f,
                    "Start timestamp {} is after end timestamp {}",
                    start, end
                )
            }
            WindowIteratorError::InvalidWindowSize => {
                write!(f, "Window size must be positive")
            }
        }
    }
}

impl std::error::Error for WindowIteratorError {}

impl WindowIterator {
    /// Create a new window iterator
    ///
    /// Note: This constructor does not validate timestamps.
    /// Use `try_new` for validated construction.
    pub fn new(start: i64, end: i64, window: Duration, step: Option<Duration>) -> Self {
        let window_ms = window.as_millis() as i64;
        let step_ms = step.map(|s| s.as_millis() as i64).unwrap_or(window_ms);

        // Align start to window boundary
        let aligned_start = if window_ms > 0 {
            (start / window_ms) * window_ms
        } else {
            start
        };

        Self {
            current: aligned_start,
            end,
            window_ms,
            step_ms,
        }
    }

    /// Create a new window iterator with timestamp validation
    ///
    /// SEC-009: Validates timestamps are within reasonable bounds (2000-2100).
    pub fn try_new(
        start: i64,
        end: i64,
        window: Duration,
        step: Option<Duration>,
    ) -> std::result::Result<Self, WindowIteratorError> {
        // SEC-009: Validate timestamp ranges
        if !is_valid_timestamp(start) {
            return Err(WindowIteratorError::InvalidStartTimestamp(start));
        }
        if !is_valid_timestamp(end) {
            return Err(WindowIteratorError::InvalidEndTimestamp(end));
        }
        if start > end {
            return Err(WindowIteratorError::StartAfterEnd { start, end });
        }
        if window.is_zero() {
            return Err(WindowIteratorError::InvalidWindowSize);
        }

        Ok(Self::new(start, end, window, step))
    }
}

impl Iterator for WindowIterator {
    type Item = (i64, i64); // (window_start, window_end)

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.end {
            return None;
        }

        let window_start = self.current;
        let window_end = (window_start + self.window_ms).min(self.end);

        self.current += self.step_ms;

        Some((window_start, window_end))
    }
}

// ============================================================================
// Aggregator Statistics
// ============================================================================

/// Statistics for aggregation operations
#[derive(Debug, Default)]
pub struct AggregatorStats {
    /// Total queries executed
    pub queries: AtomicU64,

    /// Total series processed
    pub series_processed: AtomicU64,

    /// Total data points processed
    pub points_processed: AtomicU64,

    /// Total windows computed
    pub windows_computed: AtomicU64,

    /// Queries that returned empty results
    pub empty_results: AtomicU64,
}

impl AggregatorStats {
    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> AggregatorStatsSnapshot {
        AggregatorStatsSnapshot {
            queries: self.queries.load(Ordering::Relaxed),
            series_processed: self.series_processed.load(Ordering::Relaxed),
            points_processed: self.points_processed.load(Ordering::Relaxed),
            windows_computed: self.windows_computed.load(Ordering::Relaxed),
            empty_results: self.empty_results.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of aggregator statistics
#[derive(Debug, Clone)]
pub struct AggregatorStatsSnapshot {
    /// Total queries executed
    pub queries: u64,
    /// Total series processed
    pub series_processed: u64,
    /// Total data points processed
    pub points_processed: u64,
    /// Total windows computed
    pub windows_computed: u64,
    /// Empty result count
    pub empty_results: u64,
}

// ============================================================================
// Space-Time Aggregator
// ============================================================================

/// Data source trait for retrieving time-series data
///
/// This trait abstracts the storage layer so the aggregator can work
/// with different backends (in-memory, disk, remote).
pub trait DataSource: Send + Sync {
    /// Fetch data points for a series within a time range
    fn fetch_series(&self, series_id: SeriesId, range: &TimeRange) -> Result<Vec<DataPoint>>;

    /// Fetch data points for multiple series (batch operation)
    fn fetch_many(
        &self,
        series_ids: &[SeriesId],
        range: &TimeRange,
    ) -> Result<HashMap<SeriesId, Vec<DataPoint>>>;
}

/// Space-Time Aggregator
///
/// Performs two-phase aggregation:
/// 1. Spatial: Combines data from multiple series matching tag criteria
/// 2. Temporal: Aggregates data into time windows
pub struct SpaceTimeAggregator<S: DataSource> {
    /// Data source for fetching series data
    source: S,

    /// Statistics
    stats: AggregatorStats,

    /// Maximum series to process per query
    max_series_per_query: usize,

    /// Maximum points to process per series
    max_points_per_series: usize,
}

impl<S: DataSource> SpaceTimeAggregator<S> {
    /// Create a new aggregator with the given data source
    pub fn new(source: S) -> Self {
        Self {
            source,
            stats: AggregatorStats::default(),
            max_series_per_query: 10_000,
            max_points_per_series: 1_000_000,
        }
    }

    /// Set maximum series per query
    pub fn with_max_series(mut self, max: usize) -> Self {
        self.max_series_per_query = max;
        self
    }

    /// Set maximum points per series
    pub fn with_max_points(mut self, max: usize) -> Self {
        self.max_points_per_series = max;
        self
    }

    /// Execute an aggregate query with pre-resolved series IDs
    ///
    /// This is the main entry point when series have already been resolved
    /// by the TagResolver.
    pub fn aggregate(
        &self,
        series_ids: &[SeriesId],
        query: &AggregateQuery,
    ) -> Result<UnifiedTimeSeries> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);

        if series_ids.is_empty() {
            self.stats.empty_results.fetch_add(1, Ordering::Relaxed);
            return Ok(UnifiedTimeSeries::empty());
        }

        // Check series limit
        if series_ids.len() > self.max_series_per_query {
            return Err(Error::Configuration(format!(
                "Too many series: {} > {}",
                series_ids.len(),
                self.max_series_per_query
            )));
        }

        // Fetch data from all series
        let data = self.source.fetch_many(series_ids, &query.time_range)?;

        self.stats
            .series_processed
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        // Convert to MultiSeriesData
        let multi = self.build_multi_series_data(data)?;

        // Perform aggregation
        let result = if let Some(window) = query.window {
            self.aggregate_windowed(&multi, query.function, window, query.step)?
        } else {
            self.aggregate_instant(&multi, query.function)?
        };

        // Apply limit if specified
        let result = if let Some(limit) = query.limit {
            result.truncate(limit)
        } else {
            result
        };

        Ok(result)
    }

    /// Build SpaceTimeMultiSeriesData from fetched data
    fn build_multi_series_data(
        &self,
        data: HashMap<SeriesId, Vec<DataPoint>>,
    ) -> Result<SpaceTimeMultiSeriesData> {
        // Collect all timestamps across all series
        let mut all_timestamps: Vec<i64> = data
            .values()
            .flat_map(|points| points.iter().map(|p| p.timestamp))
            .collect();

        all_timestamps.sort_unstable();
        all_timestamps.dedup();

        let total_points: usize = data.values().map(|p| p.len()).sum();
        self.stats
            .points_processed
            .fetch_add(total_points as u64, Ordering::Relaxed);

        // Create timestamp data
        let timestamp_data = SpaceTimeTimestamps::new(all_timestamps);

        // Convert series data to aligned format
        let mut series_data = HashMap::new();
        for (series_id, points) in data {
            let aligned = SpaceTimeAlignedData::from_points(&points, &timestamp_data);
            series_data.insert(series_id, aligned);
        }

        Ok(SpaceTimeMultiSeriesData::new(timestamp_data, series_data))
    }

    /// Perform instant (non-windowed) aggregation
    fn aggregate_instant(
        &self,
        data: &SpaceTimeMultiSeriesData,
        function: AggregateFunction,
    ) -> Result<UnifiedTimeSeries> {
        let needs_values = matches!(function, AggregateFunction::Quantile(_));

        // Aggregate all values into single state
        let mut state = if needs_values {
            AggregateState::with_values()
        } else {
            AggregateState::new()
        };

        for aligned in data.series.values() {
            for i in 0..aligned.values.len() {
                if let Some(value) = aligned.values[i] {
                    let timestamp = data.timestamps.timestamps[i];
                    state.add(timestamp, value);
                }
            }
        }

        // Finalize
        let value = state.finalize(function);

        if let Some(v) = value {
            // Use the midpoint of the time range as the timestamp
            let timestamp = data.timestamps.midpoint();
            let point = AggregatedPoint::new(timestamp, v, state.count);

            Ok(UnifiedTimeSeries::from_points(
                vec![point],
                AggregationMetadata::new(data.timestamps.start_time(), data.timestamps.end_time()),
            ))
        } else {
            Ok(UnifiedTimeSeries::empty())
        }
    }

    /// Perform windowed aggregation
    fn aggregate_windowed(
        &self,
        data: &SpaceTimeMultiSeriesData,
        function: AggregateFunction,
        window: Duration,
        step: Option<Duration>,
    ) -> Result<UnifiedTimeSeries> {
        let needs_values = matches!(function, AggregateFunction::Quantile(_));

        // Build index from timestamp to position for efficient lookup
        let ts_index: BTreeMap<i64, usize> = data
            .timestamps
            .timestamps
            .iter()
            .enumerate()
            .map(|(i, &ts)| (ts, i))
            .collect();

        // Iterate over windows
        // Use end_time + 1 to include the last timestamp in the final window
        let windows = WindowIterator::new(
            data.timestamps.start_time(),
            data.timestamps.end_time() + 1,
            window,
            step,
        );

        let mut points = Vec::new();

        for (window_start, window_end) in windows {
            let mut state = if needs_values {
                AggregateState::with_values()
            } else {
                AggregateState::new()
            };

            // Find indices within this window using BTreeMap range
            let range_indices: Vec<usize> = ts_index
                .range(window_start..window_end)
                .map(|(_, &idx)| idx)
                .collect();

            // Aggregate values at these indices from all series
            for aligned in data.series.values() {
                for &idx in &range_indices {
                    if let Some(value) = aligned.values.get(idx).copied().flatten() {
                        let timestamp = data.timestamps.timestamps[idx];
                        state.add(timestamp, value);
                    }
                }
            }

            // Finalize this window
            if let Some(value) = state.finalize(function) {
                let timestamp = window_start + (window_end - window_start) / 2; // Midpoint
                let point = AggregatedPoint::new(timestamp, value, state.count);
                points.push(point);

                self.stats.windows_computed.fetch_add(1, Ordering::Relaxed);
            }
        }

        if points.is_empty() {
            Ok(UnifiedTimeSeries::empty())
        } else {
            Ok(UnifiedTimeSeries::from_points(
                points,
                AggregationMetadata::new(data.timestamps.start_time(), data.timestamps.end_time()),
            ))
        }
    }

    /// Get statistics
    pub fn stats(&self) -> AggregatorStatsSnapshot {
        self.stats.snapshot()
    }
}

// ============================================================================
// In-Memory Data Source (for testing)
// ============================================================================

/// Simple in-memory data source for testing
pub struct InMemoryDataSource {
    /// Series data: series_id -> data points
    data: HashMap<SeriesId, Vec<DataPoint>>,
}

impl InMemoryDataSource {
    /// Create a new empty data source
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Add data for a series
    pub fn add_series(&mut self, series_id: SeriesId, points: Vec<DataPoint>) {
        self.data.insert(series_id, points);
    }
}

impl Default for InMemoryDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSource for InMemoryDataSource {
    fn fetch_series(&self, series_id: SeriesId, range: &TimeRange) -> Result<Vec<DataPoint>> {
        let points = self
            .data
            .get(&series_id)
            .map(|pts| {
                pts.iter()
                    .filter(|p| range.contains(p.timestamp))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(points)
    }

    fn fetch_many(
        &self,
        series_ids: &[SeriesId],
        range: &TimeRange,
    ) -> Result<HashMap<SeriesId, Vec<DataPoint>>> {
        let mut result = HashMap::new();

        for &series_id in series_ids {
            let points = self.fetch_series(series_id, range)?;
            if !points.is_empty() {
                result.insert(series_id, points);
            }
        }

        Ok(result)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(
        series_id: SeriesId,
        start_ts: i64,
        count: usize,
        value_offset: f64,
    ) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new(
                    series_id,
                    start_ts + (i as i64) * 1000, // 1 second intervals
                    (i as f64) + value_offset,
                )
            })
            .collect()
    }

    #[test]
    fn test_aggregate_state_basic() {
        let mut state = AggregateState::new();

        state.add(1000, 10.0);
        state.add(2000, 20.0);
        state.add(3000, 30.0);

        assert_eq!(state.finalize(AggregateFunction::Sum), Some(60.0));
        assert_eq!(state.finalize(AggregateFunction::Avg), Some(20.0));
        assert_eq!(state.finalize(AggregateFunction::Min), Some(10.0));
        assert_eq!(state.finalize(AggregateFunction::Max), Some(30.0));
        assert_eq!(state.finalize(AggregateFunction::Count), Some(3.0));
    }

    #[test]
    fn test_aggregate_state_first_last() {
        let mut state = AggregateState::new();

        // Add out of order
        state.add(2000, 20.0);
        state.add(1000, 10.0);
        state.add(3000, 30.0);

        assert_eq!(state.finalize(AggregateFunction::First), Some(10.0));
        assert_eq!(state.finalize(AggregateFunction::Last), Some(30.0));
    }

    #[test]
    fn test_aggregate_state_merge() {
        let mut state1 = AggregateState::new();
        state1.add(1000, 10.0);
        state1.add(2000, 20.0);

        let mut state2 = AggregateState::new();
        state2.add(3000, 30.0);
        state2.add(4000, 40.0);

        state1.merge(&state2);

        assert_eq!(state1.finalize(AggregateFunction::Sum), Some(100.0));
        assert_eq!(state1.finalize(AggregateFunction::Count), Some(4.0));
        assert_eq!(state1.finalize(AggregateFunction::First), Some(10.0));
        assert_eq!(state1.finalize(AggregateFunction::Last), Some(40.0));
    }

    #[test]
    fn test_aggregate_state_quantile() {
        let mut state = AggregateState::with_values();

        for i in 1..=100 {
            state.add(i * 1000, i as f64);
        }

        // P50 should be around 50
        let p50 = state.finalize(AggregateFunction::Quantile(50)).unwrap();
        assert!((p50 - 50.0).abs() < 2.0);

        // P95 should be around 95
        let p95 = state.finalize(AggregateFunction::Quantile(95)).unwrap();
        assert!((p95 - 95.0).abs() < 2.0);
    }

    #[test]
    fn test_aggregate_state_stddev() {
        let mut state = AggregateState::new();

        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Sample StdDev ≈ 2.14 (sqrt(32/7))
        for &v in &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            state.add(1000, v);
        }

        let stddev = state.finalize(AggregateFunction::StdDev).unwrap();
        // Sample stddev = sqrt(32/7) ≈ 2.138
        assert!((stddev - 2.138).abs() < 0.1);
    }

    #[test]
    fn test_aggregate_state_rate() {
        let mut state = AggregateState::new();

        // Counter increases by 100 over 10 seconds
        state.add(0, 0.0);
        state.add(10_000, 100.0); // 10 seconds later

        // Rate = 100 / 10 = 10 per second
        let rate = state.finalize(AggregateFunction::Rate).unwrap();
        assert_eq!(rate, 10.0);
    }

    #[test]
    fn test_window_iterator() {
        let windows: Vec<_> =
            WindowIterator::new(0, 10_000, Duration::from_secs(2), None).collect();

        assert_eq!(windows.len(), 5);
        assert_eq!(windows[0], (0, 2000));
        assert_eq!(windows[1], (2000, 4000));
        assert_eq!(windows[4], (8000, 10000));
    }

    #[test]
    fn test_window_iterator_with_step() {
        // Window of 4s, step of 2s (overlapping windows)
        let windows: Vec<_> = WindowIterator::new(
            0,
            10_000,
            Duration::from_secs(4),
            Some(Duration::from_secs(2)),
        )
        .collect();

        assert!(windows.len() >= 4);
        assert_eq!(windows[0], (0, 4000));
        assert_eq!(windows[1], (2000, 6000));
    }

    #[test]
    fn test_space_time_aggregator_instant() {
        let mut source = InMemoryDataSource::new();

        // Two series with overlapping data
        source.add_series(1, create_test_points(1, 0, 5, 0.0)); // 0, 1, 2, 3, 4
        source.add_series(2, create_test_points(2, 0, 5, 10.0)); // 10, 11, 12, 13, 14

        let aggregator = SpaceTimeAggregator::new(source);

        let query = AggregateQuery::new(
            TagMatcher::new(),
            TimeRange::new(0, 5000).unwrap(),
            AggregateFunction::Sum,
        );

        let result = aggregator.aggregate(&[1, 2], &query).unwrap();

        // Sum of all values: (0+1+2+3+4) + (10+11+12+13+14) = 10 + 60 = 70
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.points[0].value, 70.0);
    }

    #[test]
    fn test_space_time_aggregator_windowed() {
        let mut source = InMemoryDataSource::new();

        // Series with 10 points over 10 seconds (timestamps 0, 1000, 2000, ... 9000)
        // Values are 0.0, 1.0, 2.0, ... 9.0
        source.add_series(1, create_test_points(1, 0, 10, 0.0));

        let aggregator = SpaceTimeAggregator::new(source);

        let query = AggregateQuery::new(
            TagMatcher::new(),
            TimeRange::new(0, 10_000).unwrap(),
            AggregateFunction::Sum,
        )
        .with_window(Duration::from_secs(2));

        let result = aggregator.aggregate(&[1], &query).unwrap();

        // Windows: [0,2000), [2000,4000), [4000,6000), [6000,8000), [8000,10000)
        // Values at timestamps:
        // - 0ms, 1000ms (values 0,1) -> window 0 gets 0+1=1
        // - 2000ms, 3000ms (values 2,3) -> window 1 gets 2+3=5
        // - 4000ms, 5000ms (values 4,5) -> window 2 gets 4+5=9
        // - 6000ms, 7000ms (values 6,7) -> window 3 gets 6+7=13
        // - 8000ms, 9000ms (values 8,9) -> window 4 gets 8+9=17
        // But note: if query end is 10000 and window is [8000,10000), 9000 < 10000
        assert_eq!(result.points.len(), 5);
        assert_eq!(result.points[0].value, 1.0);
        // The actual sum depends on window boundaries
        // Window 4: [8000, 10000) includes ts 8000 and 9000, so values 8 and 9
        assert_eq!(result.points[4].value, 17.0);
    }

    #[test]
    fn test_space_time_aggregator_avg() {
        let mut source = InMemoryDataSource::new();

        // Two series
        source.add_series(
            1,
            vec![DataPoint::new(1, 1000, 10.0), DataPoint::new(1, 2000, 20.0)],
        );
        source.add_series(
            2,
            vec![DataPoint::new(2, 1000, 30.0), DataPoint::new(2, 2000, 40.0)],
        );

        let aggregator = SpaceTimeAggregator::new(source);

        let query = AggregateQuery::new(
            TagMatcher::new(),
            TimeRange::new(0, 3000).unwrap(),
            AggregateFunction::Avg,
        );

        let result = aggregator.aggregate(&[1, 2], &query).unwrap();

        // Average of 10, 20, 30, 40 = 25
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.points[0].value, 25.0);
    }

    #[test]
    fn test_space_time_aggregator_empty() {
        let source = InMemoryDataSource::new();
        let aggregator = SpaceTimeAggregator::new(source);

        let query = AggregateQuery::new(
            TagMatcher::new(),
            TimeRange::new(0, 1000).unwrap(),
            AggregateFunction::Sum,
        );

        let result = aggregator.aggregate(&[], &query).unwrap();

        assert!(result.points.is_empty());
    }

    #[test]
    fn test_space_time_aggregator_stats() {
        let mut source = InMemoryDataSource::new();
        source.add_series(1, create_test_points(1, 0, 100, 0.0));
        source.add_series(2, create_test_points(2, 0, 100, 0.0));

        let aggregator = SpaceTimeAggregator::new(source);

        let query = AggregateQuery::new(
            TagMatcher::new(),
            TimeRange::new(0, 100_000).unwrap(),
            AggregateFunction::Sum,
        )
        .with_window(Duration::from_secs(10));

        aggregator.aggregate(&[1, 2], &query).unwrap();

        let stats = aggregator.stats();
        assert_eq!(stats.queries, 1);
        assert_eq!(stats.series_processed, 2);
        assert!(stats.points_processed >= 200);
    }
}
