//! Per-series write buffer implementation
//!
//! Manages buffering of data points for a single time series with
//! out-of-order point handling and efficient sorting.

use crate::types::{DataPoint, SeriesId};

use std::time::{Duration, Instant};

use super::config::{MAX_VALID_TIMESTAMP, MIN_VALID_TIMESTAMP};

/// Per-series write buffer
///
/// Optimized for time-series data which is typically mostly-ordered.
/// Uses a hybrid approach:
/// - Fast path: Vec append for in-order timestamps (O(1))
/// - Slow path: Insertion for out-of-order data, with deferred sorting
///
/// This provides significant performance improvement over BTreeMap
/// for the common case of monotonically increasing timestamps.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::ingestion::SeriesBuffer;
///
/// let mut buffer = SeriesBuffer::new(1);
///
/// // Add points (in order is faster)
/// buffer.add(1000, 42.0);
/// buffer.add(1001, 43.0);
///
/// // Out-of-order is handled automatically
/// buffer.add(999, 41.0);
///
/// // Drain returns sorted points
/// let points = buffer.drain();
/// assert_eq!(points[0].timestamp, 999);
/// ```
pub struct SeriesBuffer {
    /// Series identifier
    series_id: SeriesId,
    /// Points stored as (timestamp, value) pairs
    /// May be temporarily unsorted if out-of-order points were added
    points: Vec<(i64, f64)>,
    /// Whether the points vector is known to be sorted
    is_sorted: bool,
    /// Last timestamp added (for fast-path detection)
    last_timestamp: Option<i64>,
    /// When this buffer was created
    created_at: Instant,
    /// When this buffer was last modified
    last_modified: Instant,
    /// Total points ever added (including overwrites)
    total_added: u64,
    /// Points that overwrote existing timestamps
    pub(crate) overwrites: u64,
    /// Count of out-of-order insertions (for monitoring)
    out_of_order_count: u64,
}

impl SeriesBuffer {
    /// Create a new series buffer
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series identifier this buffer belongs to
    pub fn new(series_id: SeriesId) -> Self {
        let now = Instant::now();
        Self {
            series_id,
            points: Vec::new(),
            is_sorted: true, // Empty vec is trivially sorted
            last_timestamp: None,
            created_at: now,
            last_modified: now,
            total_added: 0,
            overwrites: 0,
            out_of_order_count: 0,
        }
    }

    /// Create a new series buffer with pre-allocated capacity
    ///
    /// Use when you know approximately how many points will be buffered.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series identifier
    /// * `capacity` - Initial capacity for the points vector
    pub fn with_capacity(series_id: SeriesId, capacity: usize) -> Self {
        let now = Instant::now();
        Self {
            series_id,
            points: Vec::with_capacity(capacity),
            is_sorted: true,
            last_timestamp: None,
            created_at: now,
            last_modified: now,
            total_added: 0,
            overwrites: 0,
            out_of_order_count: 0,
        }
    }

    /// Add a point to the buffer with validation
    ///
    /// Validates timestamp range and value finiteness before adding.
    /// If a point with the same timestamp already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds (must be in valid range)
    /// * `value` - The data point value (must be finite, not NaN or Infinity)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if this was an overwrite of an existing timestamp
    /// * `Ok(false)` if this was a new timestamp
    /// * `Err` if timestamp or value is invalid
    pub fn add_validated(
        &mut self,
        timestamp: i64,
        value: f64,
    ) -> Result<bool, crate::error::IngestionError> {
        // Validate timestamp range using RangeInclusive for cleaner code
        if !(MIN_VALID_TIMESTAMP..=MAX_VALID_TIMESTAMP).contains(&timestamp) {
            return Err(crate::error::IngestionError::ValidationError(format!(
                "Timestamp {} out of valid range [{}, {}]",
                timestamp, MIN_VALID_TIMESTAMP, MAX_VALID_TIMESTAMP
            )));
        }

        // Validate value is finite (not NaN or Infinity)
        if !value.is_finite() {
            return Err(crate::error::IngestionError::ValidationError(
                "Value must be finite (not NaN or Infinity)".to_string(),
            ));
        }

        Ok(self.add_unchecked(timestamp, value))
    }

    /// Add a point without validation (for internal use or pre-validated data)
    ///
    /// Uses a fast path for in-order timestamps (O(1) append) and
    /// falls back to a slower path for out-of-order data.
    ///
    /// Returns true if this was an overwrite of an existing timestamp.
    ///
    /// Note: last_modified is updated lazily (every 100 points or on first point)
    /// to reduce Instant::now() overhead in the hot path.
    #[inline]
    pub fn add_unchecked(&mut self, timestamp: i64, value: f64) -> bool {
        self.total_added += 1;

        // Update last_modified lazily to reduce Instant::now() calls
        // Only update every 100 points or on first point to reduce syscall overhead
        if self.total_added == 1 || self.total_added % 100 == 0 {
            self.last_modified = Instant::now();
        }

        // Fast path: timestamp is greater than last (in-order, most common case)
        if let Some(last_ts) = self.last_timestamp {
            if timestamp > last_ts {
                // Simple append - O(1)
                self.points.push((timestamp, value));
                self.last_timestamp = Some(timestamp);
                return false;
            } else if timestamp == last_ts {
                // Overwrite the last point (common case for duplicate timestamps)
                if let Some(last) = self.points.last_mut() {
                    last.1 = value;
                    self.overwrites += 1;
                    return true;
                }
            }
        } else {
            // First point - just append
            self.points.push((timestamp, value));
            self.last_timestamp = Some(timestamp);
            return false;
        }

        // Slow path: out-of-order insertion
        self.out_of_order_count += 1;

        // PERF-005: Use binary search when data is sorted for O(log n) lookup
        // Check was_sorted before marking as unsorted
        let was_sorted = self.is_sorted;
        self.is_sorted = false;

        // Check if this timestamp already exists (for overwrite tracking)
        let existing_idx = if self.points.len() > 1 {
            if was_sorted {
                // Binary search for O(log n) lookup when data was sorted
                self.points
                    .binary_search_by_key(&timestamp, |(ts, _)| *ts)
                    .ok()
            } else {
                // Linear scan only when data is already unsorted
                self.points.iter().position(|(ts, _)| *ts == timestamp)
            }
        } else {
            None
        };

        if let Some(idx) = existing_idx {
            // Update existing point directly by index
            self.points[idx].1 = value;
            self.overwrites += 1;
            true
        } else {
            // Append out-of-order point (will be sorted on drain)
            self.points.push((timestamp, value));
            // Update last_timestamp only if this is the new maximum
            if timestamp > self.last_timestamp.unwrap_or(i64::MIN) {
                self.last_timestamp = Some(timestamp);
            }
            false
        }
    }

    /// Add a point to the buffer (legacy API, no validation)
    ///
    /// If a point with the same timestamp already exists, it will be overwritten.
    /// Returns true if this was an overwrite.
    ///
    /// # Deprecated
    ///
    /// Use `add_validated` for production code to ensure data integrity.
    #[inline]
    pub fn add(&mut self, timestamp: i64, value: f64) -> bool {
        self.add_unchecked(timestamp, value)
    }

    /// Add multiple points to the buffer
    ///
    /// # Arguments
    ///
    /// * `points` - Iterator of (timestamp, value) pairs
    pub fn add_batch(&mut self, points: impl IntoIterator<Item = (i64, f64)>) {
        for (timestamp, value) in points {
            self.add(timestamp, value);
        }
    }

    /// Ensure the points are sorted by timestamp
    ///
    /// Called automatically before drain, but can be called manually
    /// if sorted access is needed.
    fn ensure_sorted(&mut self) {
        if !self.is_sorted {
            // Sort by timestamp, keeping last value for duplicates
            self.points.sort_by_key(|(ts, _)| *ts);
            self.points.dedup_by(|(ts1, _), (ts2, _)| {
                // Deduplicate by timestamp - keep the later value
                *ts1 == *ts2
            });
            self.is_sorted = true;
        }
    }

    /// Number of points in the buffer
    ///
    /// Note: This may include duplicates until drain() is called.
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Age of the buffer since creation
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Time since last modification
    pub fn idle_time(&self) -> Duration {
        self.last_modified.elapsed()
    }

    /// Drain all points from the buffer, returning them as sorted DataPoints
    ///
    /// Points are sorted by timestamp and duplicates are removed (last value wins).
    pub fn drain(&mut self) -> Vec<DataPoint> {
        // Ensure sorted before draining
        self.ensure_sorted();

        let series_id = self.series_id;
        let points: Vec<DataPoint> = self
            .points
            .drain(..)
            .map(|(timestamp, value)| DataPoint::new(series_id, timestamp, value))
            .collect();

        // Reset state
        self.is_sorted = true;
        self.last_timestamp = None;

        points
    }

    /// Get buffer statistics
    pub fn stats(&self) -> SeriesBufferStats {
        SeriesBufferStats {
            series_id: self.series_id,
            point_count: self.points.len(),
            total_added: self.total_added,
            overwrites: self.overwrites,
            age: self.age(),
            idle_time: self.idle_time(),
        }
    }

    /// Get count of out-of-order insertions
    pub fn out_of_order_count(&self) -> u64 {
        self.out_of_order_count
    }

    /// Estimated memory usage in bytes
    pub fn memory_size(&self) -> usize {
        // Vec capacity * (timestamp + value) + struct overhead
        // Each (i64, f64) is 16 bytes, plus Vec overhead
        std::mem::size_of::<Self>() + self.points.capacity() * 16
    }
}

/// Statistics for a series buffer
///
/// Snapshot of buffer metrics for monitoring and debugging.
#[derive(Debug, Clone)]
pub struct SeriesBufferStats {
    /// Series identifier
    pub series_id: SeriesId,
    /// Current number of unique points
    pub point_count: usize,
    /// Total points ever added
    pub total_added: u64,
    /// Number of timestamp overwrites
    pub overwrites: u64,
    /// Age of the buffer
    pub age: Duration,
    /// Time since last modification
    pub idle_time: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_buffer_new() {
        let buffer = SeriesBuffer::new(1);
        assert_eq!(buffer.series_id, 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_series_buffer_with_capacity() {
        let buffer = SeriesBuffer::with_capacity(42, 100);
        assert_eq!(buffer.series_id, 42);
        assert!(buffer.is_empty());
        assert!(buffer.points.capacity() >= 100);
    }

    #[test]
    fn test_series_buffer_add() {
        let mut buffer = SeriesBuffer::new(1);

        // Add first point
        let overwrite = buffer.add(1000, 42.0);
        assert!(!overwrite);
        assert_eq!(buffer.len(), 1);

        // Add second point
        let overwrite = buffer.add(1001, 43.0);
        assert!(!overwrite);
        assert_eq!(buffer.len(), 2);

        // Overwrite first point
        let overwrite = buffer.add(1000, 44.0);
        assert!(overwrite);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.overwrites, 1);
    }

    #[test]
    fn test_series_buffer_add_validated_valid() {
        let mut buffer = SeriesBuffer::new(1);
        let result = buffer.add_validated(1000, 42.5);
        assert!(result.is_ok());
        assert!(!result.unwrap());
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_series_buffer_add_validated_invalid_timestamp_negative() {
        let mut buffer = SeriesBuffer::new(1);
        let result = buffer.add_validated(-1, 42.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_series_buffer_add_validated_invalid_timestamp_too_large() {
        let mut buffer = SeriesBuffer::new(1);
        let result = buffer.add_validated(MAX_VALID_TIMESTAMP + 1, 42.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_series_buffer_add_validated_nan() {
        let mut buffer = SeriesBuffer::new(1);
        let result = buffer.add_validated(1000, f64::NAN);
        assert!(result.is_err());
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_series_buffer_add_validated_infinity() {
        let mut buffer = SeriesBuffer::new(1);
        let result = buffer.add_validated(1000, f64::INFINITY);
        assert!(result.is_err());

        let result = buffer.add_validated(1000, f64::NEG_INFINITY);
        assert!(result.is_err());
    }

    #[test]
    fn test_series_buffer_add_validated_overwrite() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add_validated(1000, 42.0).unwrap();
        let result = buffer.add_validated(1000, 99.0);
        assert!(result.is_ok());
        assert!(result.unwrap()); // Was an overwrite
    }

    #[test]
    fn test_series_buffer_out_of_order() {
        let mut buffer = SeriesBuffer::new(1);

        // Add points out of order
        buffer.add(1002, 44.0);
        buffer.add(1000, 42.0);
        buffer.add(1001, 43.0);

        // Drain should return sorted
        let points = buffer.drain();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].timestamp, 1000);
        assert_eq!(points[1].timestamp, 1001);
        assert_eq!(points[2].timestamp, 1002);
    }

    #[test]
    fn test_series_buffer_out_of_order_count() {
        let mut buffer = SeriesBuffer::new(1);

        buffer.add(1000, 1.0);
        buffer.add(1001, 2.0);
        assert_eq!(buffer.out_of_order_count(), 0);

        buffer.add(999, 0.0); // Out of order
        assert_eq!(buffer.out_of_order_count(), 1);

        buffer.add(998, -1.0); // Out of order
        assert_eq!(buffer.out_of_order_count(), 2);
    }

    #[test]
    fn test_series_buffer_drain() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 42.0);
        buffer.add(1001, 43.0);

        let points = buffer.drain();
        assert_eq!(points.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_series_buffer_drain_empty() {
        let mut buffer = SeriesBuffer::new(1);
        let points = buffer.drain();
        assert!(points.is_empty());
    }

    #[test]
    fn test_series_buffer_drain_resets_state() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 42.0);

        buffer.drain();
        assert!(buffer.is_sorted);
        assert!(buffer.last_timestamp.is_none());
    }

    #[test]
    fn test_series_buffer_add_batch() {
        let mut buffer = SeriesBuffer::new(1);
        let points = vec![(1000, 1.0), (1001, 2.0), (1002, 3.0)];

        buffer.add_batch(points);
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_series_buffer_stats() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 42.0);
        buffer.add(1000, 43.0); // Overwrite

        let stats = buffer.stats();
        assert_eq!(stats.series_id, 1);
        assert_eq!(stats.point_count, 1);
        assert_eq!(stats.total_added, 2);
        assert_eq!(stats.overwrites, 1);
    }

    #[test]
    fn test_series_buffer_memory_size() {
        let mut buffer = SeriesBuffer::new(1);
        let empty_size = buffer.memory_size();

        buffer.add(1000, 42.0);
        buffer.add(1001, 43.0);

        let with_points_size = buffer.memory_size();
        assert!(with_points_size > empty_size);
    }

    #[test]
    fn test_series_buffer_age() {
        let buffer = SeriesBuffer::new(1);
        std::thread::sleep(Duration::from_millis(10));
        let age = buffer.age();
        assert!(age >= Duration::from_millis(10));
    }

    #[test]
    fn test_series_buffer_idle_time() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 42.0);
        std::thread::sleep(Duration::from_millis(10));
        let idle = buffer.idle_time();
        assert!(idle >= Duration::from_millis(10));
    }

    #[test]
    fn test_series_buffer_deduplicate_on_drain() {
        let mut buffer = SeriesBuffer::new(1);

        // Add points with duplicates
        buffer.add(1000, 1.0);
        buffer.add(1001, 2.0);
        buffer.add(1000, 3.0); // Overwrite

        let points = buffer.drain();
        // After dedup, should have 2 unique timestamps
        assert_eq!(points.len(), 2);
    }

    #[test]
    fn test_series_buffer_boundary_timestamps() {
        let mut buffer = SeriesBuffer::new(1);

        // Test boundary valid timestamps
        let result = buffer.add_validated(MIN_VALID_TIMESTAMP, 1.0);
        assert!(result.is_ok());

        let result = buffer.add_validated(MAX_VALID_TIMESTAMP, 2.0);
        assert!(result.is_ok());

        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_series_buffer_stats_clone() {
        let stats = SeriesBufferStats {
            series_id: 42,
            point_count: 100,
            total_added: 150,
            overwrites: 50,
            age: Duration::from_secs(10),
            idle_time: Duration::from_secs(1),
        };

        let cloned = stats.clone();
        assert_eq!(cloned.series_id, 42);
        assert_eq!(cloned.point_count, 100);
        assert_eq!(cloned.overwrites, 50);
    }

    #[test]
    fn test_series_buffer_stats_debug() {
        let stats = SeriesBufferStats {
            series_id: 1,
            point_count: 10,
            total_added: 20,
            overwrites: 5,
            age: Duration::from_millis(100),
            idle_time: Duration::from_millis(50),
        };

        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("SeriesBufferStats"));
        assert!(debug_str.contains("point_count: 10"));
    }

    #[test]
    fn test_series_buffer_many_points() {
        let mut buffer = SeriesBuffer::new(1);

        for i in 0..1000 {
            buffer.add(i, i as f64);
        }

        assert_eq!(buffer.len(), 1000);
        let points = buffer.drain();
        assert_eq!(points.len(), 1000);

        // Verify ordering
        for (i, point) in points.iter().enumerate() {
            assert_eq!(point.timestamp, i as i64);
        }
    }

    #[test]
    fn test_series_buffer_negative_values() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, -42.5);
        buffer.add(1001, -0.0);

        let points = buffer.drain();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].value, -42.5);
    }

    #[test]
    fn test_series_buffer_very_small_values() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, f64::MIN_POSITIVE);
        buffer.add(1001, -f64::MIN_POSITIVE);

        let points = buffer.drain();
        assert_eq!(points.len(), 2);
    }

    #[test]
    fn test_series_buffer_zero_values() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 0.0);
        buffer.add(1001, -0.0);

        let points = buffer.drain();
        assert_eq!(points.len(), 2);
    }
}
