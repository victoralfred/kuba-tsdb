//! Aggregation Functions Library
//!
//! This module provides extended aggregation functions beyond the basic ones
//! in `space_time.rs`. It includes:
//!
//! - Delta/derivative functions (irate, idelta)
//! - Moving aggregations (moving_avg, moving_stddev)
//! - Histogram functions (histogram_quantile)
//! - Label manipulation (absent, absent_over_time)
//! - Comparison functions (topk, bottomk)
//! - Time-based functions (timestamp, year, month, day_of_week)
//!
//! # PromQL Compatibility
//!
//! These functions are designed to be compatible with PromQL semantics
//! where applicable, making it easier for users familiar with Prometheus
//! to adopt this system.
//!
//! # Example
//!
//! ```rust
//! use kuba_tsdb::aggregation::{topk, moving_avg};
//!
//! // Create sample series data as (SeriesId, value) tuples
//! let series_data: Vec<(u128, f64)> = vec![
//!     (1, 100.0),
//!     (2, 50.0),
//!     (3, 75.0),
//! ];
//!
//! // Get top 2 series by value
//! let top_series = topk(2, series_data);
//! assert_eq!(top_series.len(), 2);
//! assert_eq!(top_series[0].series_id, 1); // Highest value
//!
//! // Calculate 3-point moving average
//! let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
//! let smoothed = moving_avg(&values, 3).unwrap();
//! assert_eq!(smoothed.len(), 5);
//! ```

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::types::SeriesId;

/// Maximum allowed window size for moving functions
/// VAL-001: Prevents excessive memory allocation
pub const MAX_WINDOW_SIZE: usize = 1_000_000;

// ============================================================================
// TopK / BottomK
// ============================================================================

/// A single series value for ranking
#[derive(Debug, Clone)]
pub struct RankedSeries {
    /// Series identifier
    pub series_id: SeriesId,
    /// Representative value for ranking (e.g., last value, avg, max)
    pub value: f64,
}

impl PartialEq for RankedSeries {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for RankedSeries {}

impl PartialOrd for RankedSeries {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedSeries {
    fn cmp(&self, other: &Self) -> Ordering {
        // Use partial_cmp and default to Equal for NaN comparisons
        self.value
            .partial_cmp(&other.value)
            .unwrap_or(Ordering::Equal)
    }
}

/// Get the top K series by value
///
/// Returns series sorted in descending order by their representative value.
///
/// # Arguments
/// * `k` - Number of top series to return
/// * `series` - Iterator over (series_id, value) pairs
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::aggregation::topk;
///
/// let series: Vec<(u128, f64)> = vec![(1, 10.0), (2, 50.0), (3, 30.0)];
/// let top_2 = topk(2, series);
/// assert_eq!(top_2.len(), 2);
/// assert_eq!(top_2[0].series_id, 2); // Highest value (50.0)
/// assert_eq!(top_2[1].series_id, 3); // Second highest (30.0)
/// ```
pub fn topk<I>(k: usize, series: I) -> Vec<RankedSeries>
where
    I: IntoIterator<Item = (SeriesId, f64)>,
{
    if k == 0 {
        return Vec::new();
    }

    // Use a min-heap to maintain top K
    // We keep the smallest K values, then at the end we have the top K
    let mut heap: BinaryHeap<std::cmp::Reverse<RankedSeries>> = BinaryHeap::with_capacity(k);

    for (series_id, value) in series {
        if value.is_nan() {
            continue; // Skip NaN values
        }

        let ranked = RankedSeries { series_id, value };

        if heap.len() < k {
            heap.push(std::cmp::Reverse(ranked));
        } else if let Some(std::cmp::Reverse(ref min)) = heap.peek() {
            if ranked.value > min.value {
                heap.pop();
                heap.push(std::cmp::Reverse(ranked));
            }
        }
    }

    // Convert to vec and sort descending
    let mut result: Vec<RankedSeries> = heap.into_iter().map(|r| r.0).collect();
    result.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap_or(Ordering::Equal));
    result
}

/// Get the bottom K series by value
///
/// Returns series sorted in ascending order by their representative value.
pub fn bottomk<I>(k: usize, series: I) -> Vec<RankedSeries>
where
    I: IntoIterator<Item = (SeriesId, f64)>,
{
    if k == 0 {
        return Vec::new();
    }

    // Use a max-heap to maintain bottom K
    let mut heap: BinaryHeap<RankedSeries> = BinaryHeap::with_capacity(k);

    for (series_id, value) in series {
        if value.is_nan() {
            continue;
        }

        let ranked = RankedSeries { series_id, value };

        if heap.len() < k {
            heap.push(ranked);
        } else if let Some(max) = heap.peek() {
            if ranked.value < max.value {
                heap.pop();
                heap.push(ranked);
            }
        }
    }

    // Convert to vec and sort ascending
    let mut result: Vec<RankedSeries> = heap.into_vec();
    result.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal));
    result
}

// ============================================================================
// Moving/Rolling Aggregations
// ============================================================================

/// Calculate a moving average over a sliding window
///
/// Returns a vector of the same length as input, where each position
/// contains the average of the `window_size` values ending at that position.
/// The first `window_size - 1` positions contain partial window averages.
///
/// # Arguments
/// * `values` - Input values
/// * `window_size` - Size of the sliding window (max: MAX_WINDOW_SIZE)
///
/// # Complexity
/// Time: O(n), Space: O(n)
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::aggregation::moving_avg;
///
/// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
/// let ma = moving_avg(&values, 3).unwrap();
/// assert_eq!(ma.len(), 5);
/// assert_eq!(ma[0], 1.0);  // Partial window: avg of [1.0]
/// assert_eq!(ma[1], 1.5);  // Partial window: avg of [1.0, 2.0]
/// assert_eq!(ma[2], 2.0);  // Full window: avg of [1.0, 2.0, 3.0]
/// ```
/// Calculate a moving average over a sliding window
///
/// # Returns
/// - `Ok(Vec<f64>)` - Moving averages for each position
/// - `Err(String)` - If sum overflow occurs (BUG-003 FIX: no longer silently corrupts)
///
/// # Errors
/// Returns an error if the sum exceeds the maximum allowed value (1e100),
/// preventing silent data corruption from overflow.
pub fn moving_avg(values: &[f64], window_size: usize) -> Result<Vec<f64>, String> {
    // VAL-001: Validate window size
    if values.is_empty() || window_size == 0 || window_size > MAX_WINDOW_SIZE {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(values.len());
    let mut sum: f64 = 0.0;

    for (i, &value) in values.iter().enumerate() {
        // BUG-003 FIX: Check for sum overflow and return error instead of silently corrupting
        // f64 can represent values up to ~1.7e308, but we check for reasonable limits
        const MAX_SUM: f64 = 1e100; // Reasonable upper bound

        // SEC: Check if adding value would cause overflow
        // Use checked arithmetic to detect potential overflow
        let new_sum = sum + value;
        if new_sum.abs() > MAX_SUM || new_sum.is_infinite() || new_sum.is_nan() {
            return Err(format!(
                "Moving average sum overflow at index {}: sum={}, value={}, max_allowed={}",
                i, sum, value, MAX_SUM
            ));
        }
        sum = new_sum;

        // Remove the element that falls out of the window
        if i >= window_size {
            sum -= values[i - window_size];
            // SEC: Prevent division by zero (window_size is validated > 0)
            result.push(sum / window_size as f64);
        } else {
            // Partial window at the start
            // SEC: i + 1 is always > 0 here, so division is safe
            result.push(sum / (i + 1) as f64);
        }
    }

    Ok(result)
}

/// Calculate a moving sum over a sliding window
///
/// # Complexity
/// Time: O(n), Space: O(n)
/// Calculate a moving sum over a sliding window
///
/// # Returns
/// - `Ok(Vec<f64>)` - Moving sums for each position
/// - `Err(String)` - If sum overflow occurs (BUG-003 FIX: no longer silently corrupts)
///
/// # Errors
/// Returns an error if the sum exceeds the maximum allowed value (1e100),
/// preventing silent data corruption from overflow.
pub fn moving_sum(values: &[f64], window_size: usize) -> Result<Vec<f64>, String> {
    // VAL-001: Validate window size
    if values.is_empty() || window_size == 0 || window_size > MAX_WINDOW_SIZE {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(values.len());
    let mut sum: f64 = 0.0;

    for (i, &value) in values.iter().enumerate() {
        // BUG-003 FIX: Check for sum overflow and return error instead of silently corrupting
        const MAX_SUM: f64 = 1e100; // Reasonable upper bound

        // SEC: Check if adding value would cause overflow
        let new_sum = sum + value;
        if new_sum.abs() > MAX_SUM || new_sum.is_infinite() || new_sum.is_nan() {
            return Err(format!(
                "Moving sum overflow at index {}: sum={}, value={}, max_allowed={}",
                i, sum, value, MAX_SUM
            ));
        }
        sum = new_sum;

        if i >= window_size {
            sum -= values[i - window_size];
        }
        result.push(sum);
    }

    Ok(result)
}

/// Calculate moving minimum over a sliding window
///
/// PERF-001: Uses monotonic deque for O(n) total complexity instead of O(n*w)
///
/// # Complexity
/// Time: O(n), Space: O(n)
#[must_use]
pub fn moving_min(values: &[f64], window_size: usize) -> Vec<f64> {
    // VAL-001: Validate window size
    if values.is_empty() || window_size == 0 || window_size > MAX_WINDOW_SIZE {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(values.len());
    // Deque stores (index, value) pairs where values are monotonically increasing
    let mut deque: std::collections::VecDeque<(usize, f64)> = std::collections::VecDeque::new();

    for (i, &value) in values.iter().enumerate() {
        // Remove elements outside the window from front
        while let Some(&(idx, _)) = deque.front() {
            if i >= window_size && idx <= i - window_size {
                deque.pop_front();
            } else {
                break;
            }
        }

        // Remove elements larger than current from back (they can never be minimum)
        while let Some(&(_, v)) = deque.back() {
            if v >= value {
                deque.pop_back();
            } else {
                break;
            }
        }

        deque.push_back((i, value));

        // Front of deque is always the minimum
        // Safety: deque is never empty here since we just pushed to it
        if let Some(&(_, min_val)) = deque.front() {
            result.push(min_val);
        }
    }

    result
}

/// Calculate moving maximum over a sliding window
///
/// PERF-001: Uses monotonic deque for O(n) total complexity instead of O(n*w)
///
/// # Complexity
/// Time: O(n), Space: O(n)
#[must_use]
pub fn moving_max(values: &[f64], window_size: usize) -> Vec<f64> {
    // VAL-001: Validate window size
    if values.is_empty() || window_size == 0 || window_size > MAX_WINDOW_SIZE {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(values.len());
    // Deque stores (index, value) pairs where values are monotonically decreasing
    let mut deque: std::collections::VecDeque<(usize, f64)> = std::collections::VecDeque::new();

    for (i, &value) in values.iter().enumerate() {
        // Remove elements outside the window from front
        while let Some(&(idx, _)) = deque.front() {
            if i >= window_size && idx <= i - window_size {
                deque.pop_front();
            } else {
                break;
            }
        }

        // Remove elements smaller than current from back (they can never be maximum)
        while let Some(&(_, v)) = deque.back() {
            if v <= value {
                deque.pop_back();
            } else {
                break;
            }
        }

        deque.push_back((i, value));

        // Front of deque is always the maximum
        // Safety: deque is never empty here since we just pushed to it
        if let Some(&(_, max_val)) = deque.front() {
            result.push(max_val);
        }
    }

    result
}

/// Calculate exponential moving average (EMA)
///
/// The smoothing factor alpha determines how quickly the EMA responds to changes:
/// - alpha = 0.1: Slow response, smooth
/// - alpha = 0.5: Medium response
/// - alpha = 0.9: Fast response, noisy
///
/// # Arguments
/// * `values` - Input values
/// * `alpha` - Smoothing factor (0 < alpha <= 1)
pub fn exponential_moving_avg(values: &[f64], alpha: f64) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }

    let alpha = alpha.clamp(0.0, 1.0);
    let mut result = Vec::with_capacity(values.len());
    let mut ema = values[0];

    for &value in values {
        ema = alpha * value + (1.0 - alpha) * ema;
        result.push(ema);
    }

    result
}

// ============================================================================
// Delta / Rate Functions
// ============================================================================

/// Calculate point-to-point delta (difference between consecutive values)
///
/// Returns a vector of length `n - 1` where each element is `values\[i+1\] - values\[i\]`.
pub fn delta(values: &[f64]) -> Vec<f64> {
    if values.len() < 2 {
        return Vec::new();
    }

    values.windows(2).map(|w| w[1] - w[0]).collect()
}

/// Calculate instantaneous rate of change per second
///
/// Similar to PromQL's `irate()`, this uses only the last two points.
/// Useful for volatile, fast-moving counters.
///
/// # Arguments
/// * `timestamps` - Timestamps in milliseconds
/// * `values` - Counter values
pub fn irate(timestamps: &[i64], values: &[f64]) -> Option<f64> {
    if timestamps.len() < 2 || values.len() < 2 {
        return None;
    }

    let last_idx = values.len() - 1;
    let second_last_idx = last_idx - 1;

    let time_delta = (timestamps[last_idx] - timestamps[second_last_idx]) as f64 / 1000.0;
    if time_delta <= 0.0 {
        return None;
    }

    let value_delta = values[last_idx] - values[second_last_idx];

    // Handle counter reset
    if value_delta < 0.0 {
        Some(values[last_idx] / time_delta)
    } else {
        Some(value_delta / time_delta)
    }
}

/// Calculate instant delta (difference between last two points)
///
/// Similar to PromQL's `idelta()`.
pub fn idelta(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }

    Some(values[values.len() - 1] - values[values.len() - 2])
}

/// Calculate rate of change with counter reset handling
///
/// Similar to PromQL's `rate()` but for a vector of values.
/// Returns per-second rate assuming timestamps are in milliseconds.
pub fn rate_with_resets(timestamps: &[i64], values: &[f64]) -> Option<f64> {
    if timestamps.len() < 2 || values.len() < 2 {
        return None;
    }

    let first_idx = 0;
    let last_idx = values.len() - 1;

    let time_delta = (timestamps[last_idx] - timestamps[first_idx]) as f64 / 1000.0;
    if time_delta <= 0.0 {
        return None;
    }

    // Calculate total increase, handling counter resets
    let mut total_increase = 0.0;
    let mut prev_value = values[0];

    for &value in &values[1..] {
        if value < prev_value {
            // Counter reset detected, add the current value
            total_increase += value;
        } else {
            total_increase += value - prev_value;
        }
        prev_value = value;
    }

    Some(total_increase / time_delta)
}

// ============================================================================
// Histogram Functions
// ============================================================================

/// Bucket for histogram data
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Upper bound of the bucket (le value)
    pub upper_bound: f64,
    /// Cumulative count up to this bucket
    pub count: f64,
}

/// Calculate histogram quantile from bucket data
///
/// Implements linear interpolation within buckets, similar to PromQL's
/// `histogram_quantile()`.
///
/// # Arguments
/// * `quantile` - Desired quantile (0.0 to 1.0)
/// * `buckets` - Histogram buckets sorted by upper_bound
///
/// # Returns
/// The estimated value at the given quantile, or None if invalid input.
pub fn histogram_quantile(quantile: f64, buckets: &[HistogramBucket]) -> Option<f64> {
    if buckets.is_empty() || !(0.0..=1.0).contains(&quantile) {
        return None;
    }

    // Total count should be the last bucket's count (if it has +Inf upper bound)
    let total = buckets.last()?.count;
    if total == 0.0 {
        return None;
    }

    // Target count for this quantile
    let target = quantile * total;

    // Find the bucket containing the target
    let mut prev_upper = 0.0;
    let mut prev_count = 0.0;

    for bucket in buckets {
        if bucket.count >= target {
            // Linear interpolation within this bucket
            let bucket_fraction = if bucket.count > prev_count {
                (target - prev_count) / (bucket.count - prev_count)
            } else {
                0.0
            };

            // Handle +Inf upper bound
            if bucket.upper_bound.is_infinite() {
                return Some(prev_upper);
            }

            return Some(prev_upper + (bucket.upper_bound - prev_upper) * bucket_fraction);
        }

        prev_upper = bucket.upper_bound;
        prev_count = bucket.count;
    }

    // Quantile is beyond all buckets (shouldn't happen if last bucket is +Inf)
    Some(buckets.last()?.upper_bound)
}

// ============================================================================
// Comparison and Filtering Functions
// ============================================================================

/// Filter values that are above a threshold
pub fn filter_above(values: &[(i64, f64)], threshold: f64) -> Vec<(i64, f64)> {
    values
        .iter()
        .filter(|(_, v)| *v > threshold)
        .copied()
        .collect()
}

/// Filter values that are below a threshold
pub fn filter_below(values: &[(i64, f64)], threshold: f64) -> Vec<(i64, f64)> {
    values
        .iter()
        .filter(|(_, v)| *v < threshold)
        .copied()
        .collect()
}

/// Clamp values to a range
pub fn clamp_values(values: &[f64], min: f64, max: f64) -> Vec<f64> {
    values.iter().map(|v| v.clamp(min, max)).collect()
}

/// Clamp minimum only
pub fn clamp_min(values: &[f64], min: f64) -> Vec<f64> {
    values.iter().map(|v| v.max(min)).collect()
}

/// Clamp maximum only
pub fn clamp_max(values: &[f64], max: f64) -> Vec<f64> {
    values.iter().map(|v| v.min(max)).collect()
}

// ============================================================================
// Time Functions
// ============================================================================

/// Extract hour from a Unix timestamp in milliseconds (0-23)
///
/// EDGE-008: Returns None for negative (pre-epoch) timestamps
#[must_use]
pub fn hour(timestamp_ms: i64) -> Option<u32> {
    if timestamp_ms < 0 {
        return None;
    }
    let secs = timestamp_ms / 1000;
    Some(((secs % 86400) / 3600) as u32)
}

/// Extract minute from a Unix timestamp in milliseconds (0-59)
///
/// EDGE-008: Returns None for negative (pre-epoch) timestamps
#[must_use]
pub fn minute(timestamp_ms: i64) -> Option<u32> {
    if timestamp_ms < 0 {
        return None;
    }
    let secs = timestamp_ms / 1000;
    Some(((secs % 3600) / 60) as u32)
}

/// Extract day of week from a Unix timestamp in milliseconds (0=Sunday, 6=Saturday)
///
/// EDGE-008: Returns None for negative (pre-epoch) timestamps
#[must_use]
pub fn day_of_week(timestamp_ms: i64) -> Option<u32> {
    if timestamp_ms < 0 {
        return None;
    }
    let days_since_epoch = timestamp_ms / 86_400_000;
    // January 1, 1970 was a Thursday (day 4)
    Some(((days_since_epoch + 4) % 7) as u32)
}

/// Extract day of month from a Unix timestamp in milliseconds (1-31)
///
/// Note: This is an approximation that doesn't account for all calendar rules.
/// For precise date handling, use a proper datetime library.
///
/// EDGE-008: Returns None for negative (pre-epoch) timestamps
#[must_use]
pub fn day_of_month(timestamp_ms: i64) -> Option<u32> {
    if timestamp_ms < 0 {
        return None;
    }
    // Simplified: just extract the day part
    let secs = timestamp_ms / 1000;
    let days_in_year = secs / 86400 % 365;

    // Very rough approximation (doesn't handle leap years, varying month lengths)
    let day = (days_in_year % 30) + 1;
    Some(day as u32)
}

// ============================================================================
// Absent / Present Functions
// ============================================================================

/// Check if a time range has any data
///
/// Returns 1.0 if no data is present (absent), 0.0 if data exists.
pub fn absent(values: &[f64]) -> f64 {
    if values.is_empty() {
        1.0
    } else {
        0.0
    }
}

/// Check if a series has data over a time range
///
/// Returns true if any valid (non-NaN) values exist.
pub fn present(values: &[f64]) -> bool {
    values.iter().any(|v| !v.is_nan())
}

// ============================================================================
// Group By Functions
// ============================================================================

/// Group series data by a tag value
///
/// Returns a map from tag value to list of series IDs.
pub fn group_by_tag<'a>(
    series_tags: impl Iterator<Item = (SeriesId, &'a HashMap<String, String>)>,
    group_key: &str,
) -> HashMap<String, Vec<SeriesId>> {
    let mut groups: HashMap<String, Vec<SeriesId>> = HashMap::new();

    for (series_id, tags) in series_tags {
        if let Some(value) = tags.get(group_key) {
            groups.entry(value.clone()).or_default().push(series_id);
        } else {
            // Series without the tag go into an empty-string group
            groups.entry(String::new()).or_default().push(series_id);
        }
    }

    groups
}

// ============================================================================
// Label Functions
// ============================================================================

/// SEC-008: Validate that a label name follows Prometheus conventions
///
/// Valid label names:
/// - Must not be empty
/// - Must start with a letter or underscore
/// - Can contain letters, digits, and underscores
#[must_use]
pub fn is_valid_label_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {},
        _ => return false,
    }
    // Remaining characters must be alphanumeric or underscore
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Error type for label operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelError {
    /// Invalid regex pattern
    RegexError(String),
    /// Invalid label name format
    InvalidLabelName(String),
}

impl std::fmt::Display for LabelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LabelError::RegexError(msg) => write!(f, "Regex error: {}", msg),
            LabelError::InvalidLabelName(name) => {
                write!(f, "Invalid label name '{}': must start with letter or underscore and contain only alphanumeric or underscore", name)
            },
        }
    }
}

impl std::error::Error for LabelError {}

impl From<regex::Error> for LabelError {
    fn from(e: regex::Error) -> Self {
        LabelError::RegexError(e.to_string())
    }
}

/// Replace label values using regex
///
/// Similar to PromQL's `label_replace()`.
/// SEC-008: Validates label names before use.
pub fn label_replace(
    labels: &mut HashMap<String, String>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex_pattern: &str,
) -> Result<(), LabelError> {
    // SEC-008: Validate destination label name
    if !is_valid_label_name(dst_label) {
        return Err(LabelError::InvalidLabelName(dst_label.to_string()));
    }
    // Note: src_label doesn't need validation - if it doesn't exist, nothing happens

    let regex = regex::Regex::new(regex_pattern)?;

    if let Some(src_value) = labels.get(src_label) {
        if let Some(captures) = regex.captures(src_value) {
            // Build replacement string with capture groups
            let mut new_value = replacement.to_string();

            // Replace $1, $2, etc. with captured groups
            for i in 1..=captures.len() {
                if let Some(m) = captures.get(i) {
                    new_value = new_value.replace(&format!("${}", i), m.as_str());
                }
            }

            labels.insert(dst_label.to_string(), new_value);
        }
    }

    Ok(())
}

/// Join label values from multiple labels
pub fn label_join(
    labels: &mut HashMap<String, String>,
    dst_label: &str,
    separator: &str,
    src_labels: &[&str],
) {
    let values: Vec<&str> = src_labels
        .iter()
        .filter_map(|l| labels.get(*l).map(|s| s.as_str()))
        .collect();

    labels.insert(dst_label.to_string(), values.join(separator));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topk() {
        let series = vec![(1, 10.0), (2, 50.0), (3, 30.0), (4, 20.0), (5, 40.0)];
        let top = topk(3, series);

        assert_eq!(top.len(), 3);
        assert_eq!(top[0].series_id, 2);
        assert_eq!(top[0].value, 50.0);
        assert_eq!(top[1].series_id, 5);
        assert_eq!(top[2].series_id, 3);
    }

    #[test]
    fn test_bottomk() {
        let series = vec![(1, 10.0), (2, 50.0), (3, 30.0), (4, 20.0), (5, 40.0)];
        let bottom = bottomk(3, series);

        assert_eq!(bottom.len(), 3);
        assert_eq!(bottom[0].series_id, 1);
        assert_eq!(bottom[0].value, 10.0);
        assert_eq!(bottom[1].series_id, 4);
        assert_eq!(bottom[2].series_id, 3);
    }

    #[test]
    fn test_moving_avg() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let ma = moving_avg(&values, 3).unwrap();

        assert_eq!(ma.len(), 5);
        assert!((ma[0] - 1.0).abs() < 0.001);
        assert!((ma[1] - 1.5).abs() < 0.001);
        assert!((ma[2] - 2.0).abs() < 0.001);
        assert!((ma[3] - 3.0).abs() < 0.001);
        assert!((ma[4] - 4.0).abs() < 0.001);
    }

    #[test]
    fn test_moving_sum() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let ms = moving_sum(&values, 3).unwrap();

        assert_eq!(ms.len(), 5);
        assert_eq!(ms[0], 1.0);
        assert_eq!(ms[1], 3.0);
        assert_eq!(ms[2], 6.0);
        assert_eq!(ms[3], 9.0);
        assert_eq!(ms[4], 12.0);
    }

    #[test]
    fn test_exponential_moving_avg() {
        let values = vec![10.0, 10.0, 10.0, 20.0, 20.0, 20.0];
        let ema = exponential_moving_avg(&values, 0.5);

        assert_eq!(ema.len(), 6);
        // First value should be unchanged
        assert_eq!(ema[0], 10.0);
        // EMA should gradually move toward new values
        assert!(ema[3] > 10.0);
        assert!(ema[5] > ema[4]);
    }

    #[test]
    fn test_delta() {
        let values = vec![10.0, 12.0, 15.0, 14.0, 18.0];
        let d = delta(&values);

        assert_eq!(d.len(), 4);
        assert_eq!(d[0], 2.0);
        assert_eq!(d[1], 3.0);
        assert_eq!(d[2], -1.0);
        assert_eq!(d[3], 4.0);
    }

    #[test]
    fn test_irate() {
        let timestamps = vec![0, 1000, 2000, 3000, 4000];
        let values = vec![0.0, 10.0, 20.0, 30.0, 40.0];

        let rate = irate(&timestamps, &values).unwrap();
        assert_eq!(rate, 10.0); // 10 per second
    }

    #[test]
    fn test_irate_with_reset() {
        let timestamps = vec![0, 1000, 2000, 3000, 4000];
        let values = vec![0.0, 10.0, 20.0, 5.0, 15.0]; // Reset at position 3

        let rate = irate(&timestamps, &values).unwrap();
        assert_eq!(rate, 10.0); // 10 per second (15 - 5)
    }

    #[test]
    fn test_histogram_quantile() {
        let buckets = vec![
            HistogramBucket {
                upper_bound: 0.1,
                count: 10.0,
            },
            HistogramBucket {
                upper_bound: 0.5,
                count: 50.0,
            },
            HistogramBucket {
                upper_bound: 1.0,
                count: 80.0,
            },
            HistogramBucket {
                upper_bound: f64::INFINITY,
                count: 100.0,
            },
        ];

        let p50 = histogram_quantile(0.5, &buckets).unwrap();
        assert!(p50 > 0.1 && p50 <= 0.5);

        let p80 = histogram_quantile(0.8, &buckets).unwrap();
        assert!(p80 > 0.5 && p80 <= 1.0);
    }

    #[test]
    fn test_filter_above() {
        let values = vec![(1000, 5.0), (2000, 15.0), (3000, 8.0), (4000, 20.0)];
        let filtered = filter_above(&values, 10.0);

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].1, 15.0);
        assert_eq!(filtered[1].1, 20.0);
    }

    #[test]
    fn test_clamp() {
        let values = vec![0.5, 1.5, 2.5, 3.5, 4.5];
        let clamped = clamp_values(&values, 1.0, 4.0);

        assert_eq!(clamped[0], 1.0);
        assert_eq!(clamped[1], 1.5);
        assert_eq!(clamped[3], 3.5);
        assert_eq!(clamped[4], 4.0);
    }

    #[test]
    fn test_hour() {
        // 2024-01-01 12:30:00 UTC = 1704112200000 ms
        let ts = 1_704_112_200_000;
        assert_eq!(hour(ts), Some(12));

        // EDGE-008: Negative timestamps return None
        assert_eq!(hour(-1000), None);
    }

    #[test]
    fn test_absent() {
        assert_eq!(absent(&[]), 1.0);
        assert_eq!(absent(&[1.0, 2.0]), 0.0);
    }

    #[test]
    fn test_group_by_tag() {
        let mut tags1 = HashMap::new();
        tags1.insert("region".to_string(), "us-east".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("region".to_string(), "us-west".to_string());

        let mut tags3 = HashMap::new();
        tags3.insert("region".to_string(), "us-east".to_string());

        let series = vec![(1, &tags1), (2, &tags2), (3, &tags3)];

        let groups = group_by_tag(series.into_iter(), "region");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get("us-east").unwrap().len(), 2);
        assert_eq!(groups.get("us-west").unwrap().len(), 1);
    }

    #[test]
    fn test_label_replace() {
        let mut labels = HashMap::new();
        labels.insert("instance".to_string(), "server-01:9090".to_string());

        label_replace(&mut labels, "host", "$1", "instance", r"(.+):.*").unwrap();

        assert_eq!(labels.get("host").unwrap(), "server-01");
    }

    #[test]
    fn test_label_join() {
        let mut labels = HashMap::new();
        labels.insert("region".to_string(), "us".to_string());
        labels.insert("zone".to_string(), "east".to_string());
        labels.insert("cluster".to_string(), "prod".to_string());

        label_join(&mut labels, "location", "-", &["region", "zone", "cluster"]);

        assert_eq!(labels.get("location").unwrap(), "us-east-prod");
    }

    #[test]
    fn test_moving_min() {
        let values = vec![5.0, 3.0, 8.0, 1.0, 4.0, 2.0];
        let result = moving_min(&values, 3);

        // Window 1: [5] -> min=5
        // Window 2: [5,3] -> min=3
        // Window 3: [5,3,8] -> min=3
        // Window 4: [3,8,1] -> min=1
        // Window 5: [8,1,4] -> min=1
        // Window 6: [1,4,2] -> min=1
        assert_eq!(result, vec![5.0, 3.0, 3.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn test_moving_max() {
        let values = vec![1.0, 3.0, 2.0, 5.0, 4.0, 2.0];
        let result = moving_max(&values, 3);

        // Window 1: [1] -> max=1
        // Window 2: [1,3] -> max=3
        // Window 3: [1,3,2] -> max=3
        // Window 4: [3,2,5] -> max=5
        // Window 5: [2,5,4] -> max=5
        // Window 6: [5,4,2] -> max=5
        assert_eq!(result, vec![1.0, 3.0, 3.0, 5.0, 5.0, 5.0]);
    }

    #[test]
    fn test_moving_min_max_edge_cases() {
        // Empty input
        assert!(moving_min(&[], 3).is_empty());
        assert!(moving_max(&[], 3).is_empty());

        // Window size 0
        assert!(moving_min(&[1.0, 2.0], 0).is_empty());
        assert!(moving_max(&[1.0, 2.0], 0).is_empty());

        // Window size 1
        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0];
        assert_eq!(moving_min(&values, 1), values);
        assert_eq!(moving_max(&values, 1), values);

        // Window larger than input
        let values = vec![3.0, 1.0, 4.0];
        let min_result = moving_min(&values, 5);
        assert_eq!(min_result, vec![3.0, 1.0, 1.0]);
        let max_result = moving_max(&values, 5);
        assert_eq!(max_result, vec![3.0, 3.0, 4.0]);
    }
}
