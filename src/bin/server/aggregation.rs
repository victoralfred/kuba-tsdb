//! Aggregation Functions and Time Bucketing
//!
//! This module provides time-based aggregation with auto-interval calculation
//! for optimal visualization of time-series data.

use super::types::{TimeAggregationInfo, TimeBucket};
use kuba_tsdb::query::AggregationFunction as QueryAggFunction;
use kuba_tsdb::types::{DataPoint, TimeRange};
use tdigest::TDigest;

// =============================================================================
// Constants
// =============================================================================

/// Standard intervals for time bucketing (in milliseconds)
/// These are the "nice" intervals that make sense for visualization
pub const STANDARD_INTERVALS_MS: &[(i64, &str)] = &[
    (1_000, "1s"),
    (10_000, "10s"),
    (30_000, "30s"),
    (60_000, "1m"),
    (300_000, "5m"),
    (900_000, "15m"),
    (1_800_000, "30m"),
    (3_600_000, "1h"),
    (21_600_000, "6h"),
    (43_200_000, "12h"),
    (86_400_000, "1d"),
];

/// Default target number of data points for auto-interval calculation
pub const DEFAULT_TARGET_POINTS: usize = 200;

/// Maximum recommended data points before warning
pub const MAX_RECOMMENDED_POINTS: usize = 300;

/// Minimum recommended data points before warning
pub const MIN_RECOMMENDED_POINTS: usize = 10;

// =============================================================================
// Auto-Interval Calculation
// =============================================================================

/// Calculate the optimal time bucket interval based on the query timeframe
///
/// This function automatically chooses an interval that results in approximately
/// `target_points` data points, then rounds to the nearest standard interval.
///
/// # Arguments
/// * `time_range_ms` - The total time range in milliseconds
/// * `target_points` - Target number of data points (default: 200)
///
/// # Returns
/// * Tuple of (interval_ms, interval_string)
pub fn calculate_auto_interval(time_range_ms: i64, target_points: usize) -> (i64, String) {
    if time_range_ms <= 0 || target_points == 0 {
        return (60_000, "1m".to_string()); // Default to 1 minute
    }

    // Calculate ideal interval
    let ideal_interval_ms = time_range_ms / target_points as i64;

    // Find the nearest standard interval
    let mut best_interval = STANDARD_INTERVALS_MS[0];
    let mut min_diff = i64::MAX;

    for &(interval_ms, label) in STANDARD_INTERVALS_MS {
        let diff = (interval_ms - ideal_interval_ms).abs();
        if diff < min_diff {
            min_diff = diff;
            best_interval = (interval_ms, label);
        }
    }

    (best_interval.0, best_interval.1.to_string())
}

/// Validate a user-specified interval and return warnings if needed
///
/// # Arguments
/// * `interval_ms` - The user-specified interval in milliseconds
/// * `time_range_ms` - The total time range in milliseconds
///
/// # Returns
/// * Optional warning message if the interval may cause issues
pub fn validate_interval(interval_ms: i64, time_range_ms: i64) -> Option<String> {
    if interval_ms <= 0 || time_range_ms <= 0 {
        return None;
    }

    let estimated_points = time_range_ms / interval_ms;

    if estimated_points > MAX_RECOMMENDED_POINTS as i64 {
        Some(format!(
            "The selected interval will produce approximately {} data points, which may impact performance. Consider using 'auto' or a larger interval.",
            estimated_points
        ))
    } else if estimated_points < MIN_RECOMMENDED_POINTS as i64 {
        Some(format!(
            "The selected interval will produce only approximately {} data points, which may not provide enough granularity. Consider using 'auto' or a smaller interval.",
            estimated_points
        ))
    } else {
        None
    }
}

/// Parse a duration string like "5m", "1h", "30s" into milliseconds
///
/// # Supported formats
/// * "Ns" - N seconds
/// * "Nm" - N minutes
/// * "Nh" - N hours
/// * "Nd" - N days
#[allow(dead_code)]
pub fn parse_interval_to_ms(interval: &str) -> Option<i64> {
    let interval = interval.trim().to_lowercase();
    if interval == "auto" {
        return None; // Signal to use auto calculation
    }

    let len = interval.len();
    if len < 2 {
        return None;
    }

    let (num_str, unit) = interval.split_at(len - 1);
    let num: i64 = num_str.parse().ok()?;

    match unit {
        "s" => Some(num * 1_000),
        "m" => Some(num * 60_000),
        "h" => Some(num * 3_600_000),
        "d" => Some(num * 86_400_000),
        _ => None,
    }
}

/// Format milliseconds as a human-readable interval string
pub fn format_interval_ms(ms: i64) -> String {
    if ms >= 86_400_000 && ms % 86_400_000 == 0 {
        format!("{}d", ms / 86_400_000)
    } else if ms >= 3_600_000 && ms % 3_600_000 == 0 {
        format!("{}h", ms / 3_600_000)
    } else if ms >= 60_000 && ms % 60_000 == 0 {
        format!("{}m", ms / 60_000)
    } else {
        format!("{}s", ms / 1_000)
    }
}

// =============================================================================
// Time Bucketing
// =============================================================================

/// Aggregate data points into time buckets
///
/// Groups data points by time windows and applies the specified aggregation
/// function to each bucket. Returns a vector of TimeBucket with one entry
/// per non-empty bucket.
///
/// # Arguments
/// * `points` - Sorted data points to aggregate
/// * `interval_ms` - Bucket size in milliseconds
/// * `time_range` - The full query time range for bucket alignment
/// * `func` - Aggregation function to apply per bucket
///
/// # Returns
/// Vector of `TimeBucket` structs with aggregated values per time window
pub fn aggregate_into_buckets(
    points: &[DataPoint],
    interval_ms: i64,
    time_range: TimeRange,
    func: &QueryAggFunction,
) -> Vec<TimeBucket> {
    use std::collections::BTreeMap;

    if points.is_empty() || interval_ms <= 0 {
        return vec![];
    }

    // Group points by bucket start time
    let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();

    // Align bucket start to the beginning of the time range
    let range_start = time_range.start;

    for point in points {
        // Calculate which bucket this point belongs to
        let bucket_index = (point.timestamp - range_start) / interval_ms;
        let bucket_start = range_start + (bucket_index * interval_ms);

        buckets.entry(bucket_start).or_default().push(point.value);
    }

    // Apply aggregation function to each bucket
    buckets
        .into_iter()
        .map(|(timestamp, values)| {
            let value = aggregate_bucket_values(&values, func);
            TimeBucket { timestamp, value }
        })
        .collect()
}

/// Apply aggregation function to a bucket of values
///
/// Helper function that computes the aggregation for a single time bucket.
pub fn aggregate_bucket_values(values: &[f64], func: &QueryAggFunction) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    match func {
        QueryAggFunction::Count => values.len() as f64,
        QueryAggFunction::Sum => {
            // Kahan summation for numerical stability
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for &v in values {
                let y = v - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            sum
        },
        QueryAggFunction::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
        QueryAggFunction::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        QueryAggFunction::Avg => {
            // Welford's algorithm for numerical stability
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
            }
            mean
        },
        QueryAggFunction::First => values.first().cloned().unwrap_or(f64::NAN),
        QueryAggFunction::Last => values.last().cloned().unwrap_or(f64::NAN),
        QueryAggFunction::StdDev => {
            if values.len() < 2 {
                return 0.0;
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
                let delta2 = v - mean;
                m2 += delta * delta2;
            }
            (m2 / (count - 1) as f64).sqrt()
        },
        QueryAggFunction::Variance => {
            if values.len() < 2 {
                return 0.0;
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
                let delta2 = v - mean;
                m2 += delta * delta2;
            }
            m2 / (count - 1) as f64
        },
        QueryAggFunction::Median => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = sorted.len() / 2;
            if sorted.len() % 2 == 0 {
                (sorted[mid - 1] + sorted[mid]) / 2.0
            } else {
                sorted[mid]
            }
        },
        // For rate/increase, use counter-aware calculation with reset detection
        QueryAggFunction::Rate | QueryAggFunction::Increase => {
            compute_bucket_increase_with_resets(values)
        },
        // Delta is simple difference (not counter-aware, allows negative)
        QueryAggFunction::Delta => {
            if values.len() < 2 {
                0.0
            } else {
                values.last().unwrap() - values.first().unwrap()
            }
        },
        QueryAggFunction::Percentile(p) => {
            // Use t-digest for streaming percentile estimation
            let digest = TDigest::new_with_size(100);
            let digest = digest.merge_unsorted(values.to_vec());
            digest.estimate_quantile(*p as f64 / 100.0)
        },
        QueryAggFunction::CountDistinct => {
            use std::collections::HashSet;
            let distinct: HashSet<u64> = values.iter().map(|v| v.to_bits()).collect();
            distinct.len() as f64
        },
    }
}

// =============================================================================
// Scalar Aggregation (for REST API)
// =============================================================================

/// Compute aggregation using function name string (REST API compatibility)
///
/// # Arguments
/// * `function` - Function name as string (count, sum, min, max, avg, etc.)
/// * `points` - Data points to aggregate
///
/// # Returns
/// * Optional aggregated value (None if invalid function or empty points)
pub fn compute_aggregation(function: &str, points: &[DataPoint]) -> Option<f64> {
    if points.is_empty() {
        return None;
    }

    match function.to_lowercase().as_str() {
        "count" => Some(points.len() as f64),
        "sum" => {
            // Kahan summation for numerical stability
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for p in points {
                let y = p.value - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            Some(sum)
        },
        "min" => points
            .iter()
            .map(|p| p.value)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        "max" => points
            .iter()
            .map(|p| p.value)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        "avg" | "mean" => {
            // Welford's online algorithm for numerical stability
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
            }
            Some(mean)
        },
        "first" => points.first().map(|p| p.value),
        "last" => points.last().map(|p| p.value),
        "stddev" | "std" => {
            if points.len() < 2 {
                return Some(0.0);
            }
            // Welford's online algorithm
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            Some((m2 / (count - 1) as f64).sqrt())
        },
        "variance" | "var" => {
            if points.len() < 2 {
                return Some(0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            Some(m2 / (count - 1) as f64)
        },
        "median" | "p50" => {
            let mut values: Vec<f64> = points.iter().map(|p| p.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = values.len() / 2;
            if values.len() % 2 == 0 {
                Some((values[mid - 1] + values[mid]) / 2.0)
            } else {
                Some(values[mid])
            }
        },
        s if s.starts_with("p") || s.starts_with("percentile") => {
            // Parse percentile value: p95, p99, percentile95, percentile(95), etc.
            let num_str = s
                .trim_start_matches("percentile")
                .trim_start_matches('p')
                .trim_start_matches('(')
                .trim_end_matches(')');
            if let Ok(pct) = num_str.parse::<f64>() {
                let values: Vec<f64> = points.iter().map(|p| p.value).collect();
                let digest = TDigest::new_with_size(100);
                let digest = digest.merge_unsorted(values);
                Some(digest.estimate_quantile(pct / 100.0))
            } else {
                None
            }
        },
        s if s.starts_with("quantile") => {
            // Parse quantile value: quantile(0.95), quantile0.95
            let num_str = s
                .trim_start_matches("quantile")
                .trim_start_matches('(')
                .trim_end_matches(')');
            if let Ok(q) = num_str.parse::<f64>() {
                let values: Vec<f64> = points.iter().map(|p| p.value).collect();
                let digest = TDigest::new_with_size(100);
                let digest = digest.merge_unsorted(values);
                Some(digest.estimate_quantile(q))
            } else {
                None
            }
        },
        _ => None,
    }
}

/// Compute aggregation from AST function enum
///
/// # Arguments
/// * `func` - QueryAggFunction enum
/// * `points` - Data points to aggregate
///
/// # Returns
/// * Tuple of (function_name, aggregated_value)
pub fn compute_aggregation_from_ast(
    func: &QueryAggFunction,
    points: &[DataPoint],
) -> (String, f64) {
    let func_name = format!("{:?}", func).to_lowercase();

    if points.is_empty() {
        return (func_name, f64::NAN);
    }

    let value = match func {
        QueryAggFunction::Count => points.len() as f64,
        QueryAggFunction::Sum => {
            // Kahan summation
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for p in points {
                let y = p.value - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            sum
        },
        QueryAggFunction::Min => points.iter().map(|p| p.value).fold(f64::INFINITY, f64::min),
        QueryAggFunction::Max => points
            .iter()
            .map(|p| p.value)
            .fold(f64::NEG_INFINITY, f64::max),
        QueryAggFunction::Avg => {
            // Welford's algorithm
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
            }
            mean
        },
        QueryAggFunction::First => points.first().map(|p| p.value).unwrap_or(f64::NAN),
        QueryAggFunction::Last => points.last().map(|p| p.value).unwrap_or(f64::NAN),
        QueryAggFunction::StdDev => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            (m2 / (count - 1) as f64).sqrt()
        },
        QueryAggFunction::Variance => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            m2 / (count - 1) as f64
        },
        QueryAggFunction::Median => {
            let mut values: Vec<f64> = points.iter().map(|p| p.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = values.len() / 2;
            if values.len() % 2 == 0 {
                (values[mid - 1] + values[mid]) / 2.0
            } else {
                values[mid]
            }
        },
        // Time-based calculations with counter reset detection
        QueryAggFunction::Rate => {
            return compute_rate_with_resets(points, func_name);
        },
        QueryAggFunction::Increase => {
            return compute_increase_with_resets(points, func_name);
        },
        QueryAggFunction::Delta => {
            // Delta is simple difference (not counter-aware, allows negative)
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let first = points.first().unwrap();
            let last = points.last().unwrap();
            last.value - first.value
        },
        QueryAggFunction::Percentile(p) => {
            // Use t-digest for efficient streaming percentile estimation
            let values: Vec<f64> = points.iter().map(|p| p.value).collect();
            let digest = TDigest::new_with_size(100);
            let digest = digest.merge_unsorted(values);
            digest.estimate_quantile(*p as f64 / 100.0)
        },
        QueryAggFunction::CountDistinct => {
            // Approximate distinct count using a simple hash set
            use std::collections::HashSet;
            let distinct: HashSet<u64> = points.iter().map(|p| p.value.to_bits()).collect();
            distinct.len() as f64
        },
    };

    (func_name, value)
}

// =============================================================================
// Counter Reset Detection Functions
// =============================================================================

/// Compute rate with counter reset detection
///
/// Handles Prometheus-style counter semantics:
/// - Counters only increase (monotonic)
/// - When value decreases, a reset occurred (process restart)
/// - After reset, we add current value to total increase
///
/// # Algorithm
/// 1. Walk through all points in order
/// 2. If value < previous, counter reset detected
/// 3. On reset: add current value (counter started from 0)
/// 4. Normal: add (current - previous)
/// 5. Divide total increase by time span for rate
///
/// # Returns
/// Tuple of (function_name, per-second rate of increase)
fn compute_rate_with_resets(points: &[DataPoint], func_name: String) -> (String, f64) {
    if points.len() < 2 {
        return (func_name, 0.0);
    }

    // Sort by timestamp to ensure correct ordering
    let mut sorted_points = points.to_vec();
    sorted_points.sort_by_key(|p| p.timestamp);

    let first_ts = sorted_points.first().unwrap().timestamp;
    let last_ts = sorted_points.last().unwrap().timestamp;

    // Time delta in seconds
    let time_delta_secs = (last_ts - first_ts) as f64 / 1000.0;
    if time_delta_secs <= 0.0 {
        return (func_name, 0.0);
    }

    // Calculate total increase with reset handling
    let mut total_increase = 0.0;
    let mut prev_value = sorted_points[0].value;
    let mut reset_count = 0;

    for point in sorted_points.iter().skip(1) {
        let current_value = point.value;

        if current_value < prev_value {
            // Counter reset detected
            // The counter restarted from 0, so current_value is the increase since reset
            total_increase += current_value;
            reset_count += 1;

            tracing::debug!(
                prev = prev_value,
                current = current_value,
                "Counter reset detected in rate calculation"
            );
        } else {
            // Normal increase
            total_increase += current_value - prev_value;
        }

        prev_value = current_value;
    }

    if reset_count > 0 {
        tracing::debug!(
            reset_count = reset_count,
            total_increase = total_increase,
            time_delta_secs = time_delta_secs,
            "Rate calculated with counter resets"
        );
    }

    (func_name, total_increase / time_delta_secs)
}

/// Compute counter increase with reset detection
///
/// Similar to rate but returns absolute increase, not per-second rate.
/// Handles counter resets the same way.
///
/// # Returns
/// Tuple of (function_name, total increase accounting for resets)
fn compute_increase_with_resets(points: &[DataPoint], func_name: String) -> (String, f64) {
    if points.len() < 2 {
        return (func_name, 0.0);
    }

    // Sort by timestamp to ensure correct ordering
    let mut sorted_points = points.to_vec();
    sorted_points.sort_by_key(|p| p.timestamp);

    // Calculate total increase with reset handling
    let mut total_increase = 0.0;
    let mut prev_value = sorted_points[0].value;

    for point in sorted_points.iter().skip(1) {
        let current_value = point.value;

        if current_value < prev_value {
            // Counter reset - add current value (increase since reset from 0)
            total_increase += current_value;

            tracing::debug!(
                prev = prev_value,
                current = current_value,
                "Counter reset detected in increase calculation"
            );
        } else {
            total_increase += current_value - prev_value;
        }

        prev_value = current_value;
    }

    // Ensure non-negative (increase can't be negative)
    (func_name, total_increase.max(0.0))
}

/// Compute increase within a bucket with reset detection
///
/// Handles counter resets within a single time bucket.
/// Used by aggregate_bucket_values for Rate and Increase functions.
fn compute_bucket_increase_with_resets(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }

    let mut total_increase = 0.0;
    let mut prev_value = values[0];

    for &value in &values[1..] {
        if value < prev_value {
            // Counter reset within bucket - add current value
            total_increase += value;
        } else {
            total_increase += value - prev_value;
        }
        prev_value = value;
    }

    total_increase.max(0.0)
}

/// Create TimeAggregationInfo for response
pub fn create_time_aggregation_info(
    mode: String,
    interval_str: String,
    interval_ms: i64,
    bucket_count: usize,
) -> TimeAggregationInfo {
    TimeAggregationInfo {
        mode,
        interval: interval_str,
        interval_ms,
        bucket_count,
        target_points: Some(DEFAULT_TARGET_POINTS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test data points
    fn make_points(values: &[(i64, f64)]) -> Vec<DataPoint> {
        values
            .iter()
            .map(|(ts, val)| DataPoint {
                series_id: 1, // Test series ID
                timestamp: *ts,
                value: *val,
            })
            .collect()
    }

    #[test]
    fn test_calculate_auto_interval() {
        // 1 hour range should give ~1m buckets for 200 points
        let (interval_ms, label) = calculate_auto_interval(3_600_000, 200);
        assert!((10_000..=60_000).contains(&interval_ms));
        assert!(!label.is_empty());

        // 1 day range should give larger buckets
        let (interval_ms, _) = calculate_auto_interval(86_400_000, 200);
        assert!(interval_ms >= 300_000); // At least 5 minutes

        // Edge case: zero/negative time range
        let (interval_ms, _) = calculate_auto_interval(0, 200);
        assert_eq!(interval_ms, 60_000); // Default 1m

        let (interval_ms, _) = calculate_auto_interval(-100, 200);
        assert_eq!(interval_ms, 60_000); // Default 1m

        // Edge case: zero target points
        let (interval_ms, _) = calculate_auto_interval(3_600_000, 0);
        assert_eq!(interval_ms, 60_000); // Default 1m
    }

    #[test]
    fn test_validate_interval() {
        // Too few points - 5m interval for 10 mins = 2 points
        let warning = validate_interval(300_000, 600_000);
        assert!(warning.is_some(), "Expected warning for too few points (2)");

        // Too many points - 1s interval for 1 hour = 3600 points
        let warning = validate_interval(1_000, 3_600_000);
        assert!(
            warning.is_some(),
            "Expected warning for too many points (3600)"
        );

        // Good range - 1m interval for 200 mins = 200 points
        let warning = validate_interval(60_000, 12_000_000);
        assert!(warning.is_none(), "Expected no warning for 200 points");

        // Edge cases: zero or negative
        assert!(validate_interval(0, 100).is_none());
        assert!(validate_interval(100, 0).is_none());
        assert!(validate_interval(-100, 100).is_none());
    }

    #[test]
    fn test_format_interval_ms() {
        assert_eq!(format_interval_ms(1_000), "1s");
        assert_eq!(format_interval_ms(60_000), "1m");
        assert_eq!(format_interval_ms(3_600_000), "1h");
        assert_eq!(format_interval_ms(86_400_000), "1d");

        // Non-aligned values fall back to seconds
        assert_eq!(format_interval_ms(90_000), "90s"); // 1.5 minutes
    }

    #[test]
    fn test_parse_interval_to_ms() {
        assert_eq!(parse_interval_to_ms("5s"), Some(5_000));
        assert_eq!(parse_interval_to_ms("5m"), Some(300_000));
        assert_eq!(parse_interval_to_ms("2h"), Some(7_200_000));
        assert_eq!(parse_interval_to_ms("1d"), Some(86_400_000));

        // "auto" returns None (signal for auto-calculation)
        assert_eq!(parse_interval_to_ms("auto"), None);

        // Invalid inputs
        assert_eq!(parse_interval_to_ms("5"), None); // No unit
        assert_eq!(parse_interval_to_ms("m"), None); // No number
        assert_eq!(parse_interval_to_ms(""), None);
        assert_eq!(parse_interval_to_ms("5x"), None); // Invalid unit
    }

    #[test]
    fn test_aggregate_into_buckets_empty() {
        let points: Vec<DataPoint> = vec![];
        let time_range = TimeRange {
            start: 0,
            end: 100_000,
        };
        let buckets = aggregate_into_buckets(&points, 10_000, time_range, &QueryAggFunction::Avg);
        assert!(buckets.is_empty());
    }

    #[test]
    fn test_aggregate_into_buckets_basic() {
        let points = make_points(&[(0, 10.0), (5_000, 20.0), (10_000, 30.0), (15_000, 40.0)]);
        let time_range = TimeRange {
            start: 0,
            end: 20_000,
        };

        // 10-second buckets
        let buckets = aggregate_into_buckets(&points, 10_000, time_range, &QueryAggFunction::Avg);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].timestamp, 0);
        assert!((buckets[0].value - 15.0).abs() < 0.001); // avg(10, 20) = 15
        assert_eq!(buckets[1].timestamp, 10_000);
        assert!((buckets[1].value - 35.0).abs() < 0.001); // avg(30, 40) = 35
    }

    #[test]
    fn test_aggregate_bucket_values_count() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::Count);
        assert_eq!(result, 5.0);
    }

    #[test]
    fn test_aggregate_bucket_values_sum() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::Sum);
        assert!((result - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_bucket_values_min_max() {
        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0];
        assert_eq!(
            aggregate_bucket_values(&values, &QueryAggFunction::Min),
            1.0
        );
        assert_eq!(
            aggregate_bucket_values(&values, &QueryAggFunction::Max),
            9.0
        );
    }

    #[test]
    fn test_aggregate_bucket_values_avg() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::Avg);
        assert!((result - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_bucket_values_first_last() {
        let values = vec![10.0, 20.0, 30.0];
        assert_eq!(
            aggregate_bucket_values(&values, &QueryAggFunction::First),
            10.0
        );
        assert_eq!(
            aggregate_bucket_values(&values, &QueryAggFunction::Last),
            30.0
        );
    }

    #[test]
    fn test_aggregate_bucket_values_stddev() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::StdDev);
        // Standard deviation should be approximately 2.138
        assert!((result - 2.138).abs() < 0.01);

        // Single value should return 0
        let single = vec![5.0];
        assert_eq!(
            aggregate_bucket_values(&single, &QueryAggFunction::StdDev),
            0.0
        );
    }

    #[test]
    fn test_aggregate_bucket_values_variance() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::Variance);
        // Variance should be approximately 4.571
        assert!((result - 4.571).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_bucket_values_median() {
        // Odd number of elements
        let odd = vec![1.0, 3.0, 5.0, 7.0, 9.0];
        assert_eq!(
            aggregate_bucket_values(&odd, &QueryAggFunction::Median),
            5.0
        );

        // Even number of elements
        let even = vec![1.0, 3.0, 5.0, 7.0];
        assert_eq!(
            aggregate_bucket_values(&even, &QueryAggFunction::Median),
            4.0
        );
    }

    #[test]
    fn test_aggregate_bucket_values_percentile() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();

        // p50 should be around 50
        let p50 = aggregate_bucket_values(&values, &QueryAggFunction::Percentile(50));
        assert!((p50 - 50.0).abs() < 2.0);

        // p95 should be around 95
        let p95 = aggregate_bucket_values(&values, &QueryAggFunction::Percentile(95));
        assert!((p95 - 95.0).abs() < 2.0);

        // p99 should be around 99
        let p99 = aggregate_bucket_values(&values, &QueryAggFunction::Percentile(99));
        assert!((p99 - 99.0).abs() < 2.0);
    }

    #[test]
    fn test_aggregate_bucket_values_count_distinct() {
        let values = vec![1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0];
        let result = aggregate_bucket_values(&values, &QueryAggFunction::CountDistinct);
        assert_eq!(result, 4.0); // 4 distinct values
    }

    #[test]
    fn test_aggregate_bucket_values_rate_functions() {
        let values = vec![100.0, 150.0, 200.0];

        // Increase: last - first (positive only)
        let increase = aggregate_bucket_values(&values, &QueryAggFunction::Increase);
        assert_eq!(increase, 100.0);

        // Delta: last - first
        let delta = aggregate_bucket_values(&values, &QueryAggFunction::Delta);
        assert_eq!(delta, 100.0);

        // Rate: also last - first
        let rate = aggregate_bucket_values(&values, &QueryAggFunction::Rate);
        assert_eq!(rate, 100.0);

        // With only one value, rate functions return 0
        let single = vec![100.0];
        assert_eq!(
            aggregate_bucket_values(&single, &QueryAggFunction::Rate),
            0.0
        );
    }

    #[test]
    fn test_aggregate_bucket_values_empty() {
        let empty: Vec<f64> = vec![];
        assert!(aggregate_bucket_values(&empty, &QueryAggFunction::Avg).is_nan());
    }

    #[test]
    fn test_compute_aggregation_string_api() {
        let points = make_points(&[(0, 1.0), (1000, 2.0), (2000, 3.0), (3000, 4.0), (4000, 5.0)]);

        assert_eq!(compute_aggregation("count", &points), Some(5.0));
        assert_eq!(compute_aggregation("sum", &points), Some(15.0));
        assert_eq!(compute_aggregation("min", &points), Some(1.0));
        assert_eq!(compute_aggregation("max", &points), Some(5.0));
        assert_eq!(compute_aggregation("avg", &points), Some(3.0));
        assert_eq!(compute_aggregation("mean", &points), Some(3.0)); // alias
        assert_eq!(compute_aggregation("first", &points), Some(1.0));
        assert_eq!(compute_aggregation("last", &points), Some(5.0));

        // Unknown function returns None
        assert_eq!(compute_aggregation("unknown", &points), None);

        // Empty points returns None
        let empty: Vec<DataPoint> = vec![];
        assert_eq!(compute_aggregation("count", &empty), None);
    }

    #[test]
    fn test_compute_aggregation_percentile_string() {
        let points = make_points(&(1..=100).map(|x| (x * 1000, x as f64)).collect::<Vec<_>>());

        // p50
        let p50 = compute_aggregation("p50", &points).unwrap();
        assert!((p50 - 50.0).abs() < 2.0);

        // p95
        let p95 = compute_aggregation("p95", &points).unwrap();
        assert!((p95 - 95.0).abs() < 2.0);

        // percentile(99)
        let p99 = compute_aggregation("percentile(99)", &points).unwrap();
        assert!((p99 - 99.0).abs() < 2.0);
    }

    #[test]
    fn test_compute_aggregation_quantile_string() {
        let points = make_points(&(1..=100).map(|x| (x * 1000, x as f64)).collect::<Vec<_>>());

        // quantile(0.5)
        let q50 = compute_aggregation("quantile(0.5)", &points).unwrap();
        assert!((q50 - 50.0).abs() < 2.0);

        // quantile(0.95)
        let q95 = compute_aggregation("quantile(0.95)", &points).unwrap();
        assert!((q95 - 95.0).abs() < 2.0);
    }

    #[test]
    fn test_compute_aggregation_stddev_variance() {
        let points = make_points(&[
            (0, 2.0),
            (1, 4.0),
            (2, 4.0),
            (3, 4.0),
            (4, 5.0),
            (5, 5.0),
            (6, 7.0),
            (7, 9.0),
        ]);

        let stddev = compute_aggregation("stddev", &points).unwrap();
        assert!((stddev - 2.138).abs() < 0.01);

        let var = compute_aggregation("variance", &points).unwrap();
        assert!((var - 4.571).abs() < 0.01);

        // Single point
        let single = make_points(&[(0, 5.0)]);
        assert_eq!(compute_aggregation("stddev", &single), Some(0.0));
    }

    #[test]
    fn test_compute_aggregation_from_ast() {
        let points = make_points(&[(0, 10.0), (1000, 20.0), (2000, 30.0)]);

        let (name, value) = compute_aggregation_from_ast(&QueryAggFunction::Sum, &points);
        assert!(name.contains("sum"));
        assert!((value - 60.0).abs() < 0.001);

        let (_, value) = compute_aggregation_from_ast(&QueryAggFunction::Avg, &points);
        assert!((value - 20.0).abs() < 0.001);

        // Percentile from AST
        let (_, value) = compute_aggregation_from_ast(&QueryAggFunction::Percentile(50), &points);
        assert!((value - 20.0).abs() < 5.0);

        // Empty points
        let empty: Vec<DataPoint> = vec![];
        let (_, value) = compute_aggregation_from_ast(&QueryAggFunction::Avg, &empty);
        assert!(value.is_nan());
    }

    #[test]
    fn test_create_time_aggregation_info() {
        let info = create_time_aggregation_info("auto".to_string(), "5m".to_string(), 300_000, 24);
        assert_eq!(info.mode, "auto");
        assert_eq!(info.interval, "5m");
        assert_eq!(info.interval_ms, 300_000);
        assert_eq!(info.bucket_count, 24);
        assert_eq!(info.target_points, Some(DEFAULT_TARGET_POINTS));
    }

    // =========================================================================
    // Counter Reset Detection Tests
    // =========================================================================

    #[test]
    fn test_rate_with_counter_reset() {
        // Simulate counter that resets mid-way
        // Counter: 100 -> 150 (+50) -> RESET to 10 -> 60 (+50)
        let points = make_points(&[
            (1000, 100.0), // Counter at 100
            (2000, 150.0), // Counter at 150 (+50 increase)
            (3000, 10.0),  // RESET! Counter restarted at 10
            (4000, 60.0),  // Counter at 60 (+50 since reset)
        ]);

        let (_, rate) = compute_rate_with_resets(&points, "rate".to_string());

        // Total increase: 50 (100->150) + 10 (reset, counter at 10) + 50 (10->60) = 110
        // Time span: 3 seconds
        // Rate: 110 / 3 = 36.67 per second
        assert!(
            (rate - 36.67).abs() < 0.1,
            "Expected rate ~36.67, got {}",
            rate
        );
    }

    #[test]
    fn test_rate_no_reset() {
        // Normal counter with no resets
        let points = make_points(&[(1000, 100.0), (2000, 200.0), (3000, 300.0)]);

        let (_, rate) = compute_rate_with_resets(&points, "rate".to_string());

        // Increase: 200 over 2 seconds = 100/s
        assert!(
            (rate - 100.0).abs() < 0.01,
            "Expected rate 100.0, got {}",
            rate
        );
    }

    #[test]
    fn test_rate_single_point() {
        let points = make_points(&[(1000, 100.0)]);
        let (_, rate) = compute_rate_with_resets(&points, "rate".to_string());
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_rate_empty_points() {
        let points: Vec<DataPoint> = vec![];
        let (_, rate) = compute_rate_with_resets(&points, "rate".to_string());
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_increase_with_counter_reset() {
        // Simulate counter that resets
        let points = make_points(&[
            (1000, 50.0),  // Start at 50
            (2000, 100.0), // +50
            (3000, 20.0),  // RESET to 20
            (4000, 70.0),  // +50
        ]);

        let (_, increase) = compute_increase_with_resets(&points, "increase".to_string());

        // Total increase: 50 (50->100) + 20 (reset) + 50 (20->70) = 120
        assert!(
            (increase - 120.0).abs() < 0.01,
            "Expected increase 120.0, got {}",
            increase
        );
    }

    #[test]
    fn test_increase_with_multiple_resets() {
        // Multiple resets
        let points = make_points(&[
            (1000, 50.0), // Start at 50
            (2000, 5.0),  // Reset to 5
            (3000, 10.0), // +5
            (4000, 3.0),  // Reset to 3
            (5000, 23.0), // +20
        ]);

        let (_, increase) = compute_increase_with_resets(&points, "increase".to_string());

        // Total: 5 (after first reset) + 5 (5->10) + 3 (after second reset) + 20 (3->23) = 33
        assert!(
            (increase - 33.0).abs() < 0.01,
            "Expected increase 33.0, got {}",
            increase
        );
    }

    #[test]
    fn test_increase_no_reset() {
        let points = make_points(&[(1000, 10.0), (2000, 30.0), (3000, 50.0)]);

        let (_, increase) = compute_increase_with_resets(&points, "increase".to_string());

        // Simple increase: 50 - 10 = 40
        assert!(
            (increase - 40.0).abs() < 0.01,
            "Expected increase 40.0, got {}",
            increase
        );
    }

    #[test]
    fn test_bucket_increase_with_reset() {
        // Values within a bucket with reset
        let values = vec![100.0, 150.0, 10.0, 60.0];

        let increase = compute_bucket_increase_with_resets(&values);

        // 50 (100->150) + 10 (reset) + 50 (10->60) = 110
        assert!(
            (increase - 110.0).abs() < 0.01,
            "Expected bucket increase 110.0, got {}",
            increase
        );
    }

    #[test]
    fn test_bucket_increase_no_reset() {
        let values = vec![10.0, 20.0, 30.0];

        let increase = compute_bucket_increase_with_resets(&values);

        // 10 + 10 = 20
        assert!(
            (increase - 20.0).abs() < 0.01,
            "Expected bucket increase 20.0, got {}",
            increase
        );
    }

    #[test]
    fn test_bucket_increase_single_value() {
        let values = vec![100.0];
        let increase = compute_bucket_increase_with_resets(&values);
        assert_eq!(increase, 0.0);
    }

    #[test]
    fn test_bucket_increase_empty() {
        let values: Vec<f64> = vec![];
        let increase = compute_bucket_increase_with_resets(&values);
        assert_eq!(increase, 0.0);
    }

    #[test]
    fn test_delta_ignores_resets() {
        // Delta should NOT handle resets - it's just last - first
        let points = make_points(&[
            (1000, 100.0),
            (2000, 150.0),
            (3000, 10.0), // This is a reset but delta doesn't care
        ]);

        let (_, delta) = compute_aggregation_from_ast(&QueryAggFunction::Delta, &points);

        // Delta is simply: 10 - 100 = -90
        assert!(
            (delta - (-90.0)).abs() < 0.01,
            "Expected delta -90.0, got {}",
            delta
        );
    }

    #[test]
    fn test_aggregate_bucket_with_reset() {
        // Test aggregate_bucket_values uses reset detection for Rate/Increase
        let values = vec![100.0, 150.0, 10.0, 60.0];

        let rate_result = aggregate_bucket_values(&values, &QueryAggFunction::Rate);
        let increase_result = aggregate_bucket_values(&values, &QueryAggFunction::Increase);

        // Both should detect the reset and compute increase as 110
        assert!(
            (rate_result - 110.0).abs() < 0.01,
            "Expected rate bucket 110.0, got {}",
            rate_result
        );
        assert!(
            (increase_result - 110.0).abs() < 0.01,
            "Expected increase bucket 110.0, got {}",
            increase_result
        );

        // Delta should be simple difference: 60 - 100 = -40
        let delta_result = aggregate_bucket_values(&values, &QueryAggFunction::Delta);
        assert!(
            (delta_result - (-40.0)).abs() < 0.01,
            "Expected delta bucket -40.0, got {}",
            delta_result
        );
    }
}
