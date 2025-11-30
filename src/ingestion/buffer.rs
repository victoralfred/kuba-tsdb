//! Write buffer manager for per-series buffering
//!
//! Manages per-series write buffers with support for out-of-order point handling.
//! Points are sorted within each series buffer before being flushed to storage.

use crate::error::IngestionError;
use crate::types::{DataPoint, SeriesId};

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, info, warn};

use super::backpressure::BackpressureController;
use super::batch::PointBatch;
use super::metrics::IngestionMetrics;

/// Minimum valid timestamp (Unix epoch 1970-01-01 in milliseconds)
pub const MIN_VALID_TIMESTAMP: i64 = 0;
/// Maximum valid timestamp (year 2100 in milliseconds)
pub const MAX_VALID_TIMESTAMP: i64 = 4_102_444_800_000;
/// Maximum allowed series count to prevent memory exhaustion
pub const MAX_SERIES_COUNT: usize = 10_000_000;
/// Maximum points per series limit
pub const MAX_POINTS_PER_SERIES: usize = 1_000_000;

/// Configuration for the write buffer manager
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum points to buffer per series before flushing
    pub max_points_per_series: usize,
    /// Maximum total memory for all buffers (bytes)
    pub max_total_memory: usize,
    /// Flush interval for time-based flushing
    pub flush_interval: Duration,
    /// Maximum age of buffered points before forced flush
    pub max_buffer_age: Duration,
    /// Initial capacity for series buffers
    pub initial_series_capacity: usize,
    /// Maximum number of unique series to buffer (prevents memory exhaustion)
    pub max_series_count: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_points_per_series: 10_000,
            max_total_memory: 512 * 1024 * 1024, // 512 MB
            flush_interval: Duration::from_secs(1),
            max_buffer_age: Duration::from_secs(10),
            initial_series_capacity: 1_000,
            max_series_count: 100_000, // 100K series default limit
        }
    }
}

impl BufferConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_points_per_series == 0 {
            return Err("max_points_per_series must be > 0".to_string());
        }
        if self.max_points_per_series > MAX_POINTS_PER_SERIES {
            return Err(format!(
                "max_points_per_series {} exceeds maximum {}",
                self.max_points_per_series, MAX_POINTS_PER_SERIES
            ));
        }
        if self.max_total_memory == 0 {
            return Err("max_total_memory must be > 0".to_string());
        }
        if self.flush_interval.is_zero() {
            return Err("flush_interval must be > 0".to_string());
        }
        if self.max_series_count == 0 {
            return Err("max_series_count must be > 0".to_string());
        }
        if self.max_series_count > MAX_SERIES_COUNT {
            return Err(format!(
                "max_series_count {} exceeds maximum {}",
                self.max_series_count, MAX_SERIES_COUNT
            ));
        }
        Ok(())
    }
}

/// Per-series write buffer
///
/// Optimized for time-series data which is typically mostly-ordered.
/// Uses a hybrid approach:
/// - Fast path: Vec append for in-order timestamps (O(1))
/// - Slow path: Insertion for out-of-order data, with deferred sorting
///
/// This provides significant performance improvement over BTreeMap
/// for the common case of monotonically increasing timestamps.
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
    overwrites: u64,
    /// Count of out-of-order insertions (for monitoring)
    out_of_order_count: u64,
}

impl SeriesBuffer {
    /// Create a new series buffer
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

/// Batch of points ready to be written, grouped by series
#[derive(Debug)]
pub struct WriteBatch {
    /// Series ID for this batch
    pub series_id: SeriesId,
    /// Sorted points to write
    pub points: Vec<DataPoint>,
    /// Sequence number for ordering
    pub sequence: u64,
}

/// Write buffer manager
///
/// Manages per-series buffers and coordinates flushing to storage.
pub struct WriteBufferManager {
    /// Configuration
    config: BufferConfig,
    /// Per-series buffers (concurrent access)
    buffers: DashMap<SeriesId, SeriesBuffer>,
    /// Input channel for batches
    input: RwLock<Option<mpsc::Receiver<PointBatch>>>,
    /// Output channel for write batches
    output: mpsc::Sender<WriteBatch>,
    /// Backpressure controller
    backpressure: Arc<BackpressureController>,
    /// Metrics collector
    metrics: Arc<IngestionMetrics>,
    /// Total memory used by all buffers (approximate)
    memory_used: AtomicUsize,
    /// Total points currently buffered
    total_buffered: AtomicUsize,
    /// Write batch sequence counter
    sequence: AtomicU64,
}

impl WriteBufferManager {
    /// Create a new write buffer manager
    pub fn new(
        config: BufferConfig,
        input: mpsc::Receiver<PointBatch>,
        output: mpsc::Sender<WriteBatch>,
        backpressure: Arc<BackpressureController>,
        metrics: Arc<IngestionMetrics>,
    ) -> Self {
        Self {
            config,
            buffers: DashMap::new(),
            input: RwLock::new(Some(input)),
            output,
            backpressure,
            metrics,
            memory_used: AtomicUsize::new(0),
            total_buffered: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
        }
    }

    /// Run the buffer manager (processes incoming batches)
    ///
    /// # Errors
    ///
    /// Returns error if called more than once (double-start).
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), IngestionError> {
        // Take ownership of the input receiver
        let mut input = match self.input.write().await.take() {
            Some(rx) => rx,
            None => {
                warn!("WriteBufferManager input already taken - double start detected");
                return Err(IngestionError::ConfigError(
                    "WriteBufferManager already running (double start)".to_string(),
                ));
            }
        };

        let mut flush_interval = tokio::time::interval(self.config.flush_interval);

        info!("WriteBufferManager started");

        loop {
            tokio::select! {
                // Process incoming batches
                Some(batch) = input.recv() => {
                    if let Err(e) = self.process_batch(batch).await {
                        warn!("Error processing batch: {}", e);
                    }
                }

                // Periodic flush check
                _ = flush_interval.tick() => {
                    if let Err(e) = self.flush_aged().await {
                        warn!("Error in periodic flush: {}", e);
                    }
                }

                // Shutdown signal
                _ = shutdown.recv() => {
                    info!("WriteBufferManager shutting down");
                    break;
                }
            }
        }

        // Final flush on shutdown
        if let Err(e) = self.flush_all().await {
            warn!("Error in final flush: {}", e);
        }

        info!("WriteBufferManager stopped");
        Ok(())
    }

    /// Process an incoming point batch
    ///
    /// Optimized to batch atomic counter updates instead of updating per-point.
    /// This reduces cache line bouncing under high throughput.
    async fn process_batch(&self, batch: PointBatch) -> Result<(), IngestionError> {
        let point_count = batch.len();

        // Track counters locally to batch atomic updates
        let mut new_points: usize = 0;
        let mut overwrites: u64 = 0;
        let mut series_to_flush: Vec<(SeriesId, Vec<DataPoint>)> = Vec::new();

        for point in batch.points {
            let result = self.add_point_internal(point, &mut series_to_flush)?;
            if result {
                overwrites += 1;
            } else {
                new_points += 1;
            }
        }

        // Batch atomic updates - single update instead of per-point
        if new_points > 0 {
            self.total_buffered.fetch_add(new_points, Ordering::Relaxed);
            // Approximate memory increase: 16 bytes per point (timestamp + value)
            self.memory_used
                .fetch_add(new_points.saturating_mul(16), Ordering::Relaxed);
        }

        // Record overwrites in metrics
        for _ in 0..overwrites {
            self.metrics.record_overwrite();
        }

        // Flush any series that exceeded their threshold
        for (series_id, points) in series_to_flush {
            self.send_write_batch(series_id, points).await?;
        }

        // Update backpressure state once per batch
        self.backpressure
            .update_memory_usage(self.memory_used.load(Ordering::Relaxed));

        debug!(
            "Processed batch with {} points ({} new, {} overwrites)",
            point_count, new_points, overwrites
        );
        Ok(())
    }

    /// Internal point addition without atomic updates (for batching)
    ///
    /// Returns true if this was an overwrite, false if new point.
    /// Collects series that need flushing into the provided vector.
    fn add_point_internal(
        &self,
        point: DataPoint,
        flush_queue: &mut Vec<(SeriesId, Vec<DataPoint>)>,
    ) -> Result<bool, IngestionError> {
        let series_id = point.series_id;

        // Check series count limit before creating new buffer
        let is_new_series = !self.buffers.contains_key(&series_id);
        if is_new_series && self.buffers.len() >= self.config.max_series_count {
            return Err(IngestionError::ValidationError(format!(
                "Maximum series count {} exceeded",
                self.config.max_series_count
            )));
        }

        // Get or create buffer for this series
        let mut buffer = self
            .buffers
            .entry(series_id)
            .or_insert_with(|| SeriesBuffer::new(series_id));

        // Use validated add to check timestamp and value
        let overwrite = buffer.add_validated(point.timestamp, point.value)?;

        // Check if buffer should be flushed
        if buffer.len() >= self.config.max_points_per_series {
            let points = buffer.drain();
            drop(buffer); // Release lock before adding to flush queue
            flush_queue.push((series_id, points));
        }

        Ok(overwrite)
    }

    /// Add a single point to the appropriate series buffer (public API)
    ///
    /// # Errors
    ///
    /// Returns `IngestionError::ValidationError` if:
    /// - Series count limit is exceeded (new series would exceed max_series_count)
    /// - Timestamp is out of valid range
    /// - Value is NaN or Infinity
    pub async fn add_point(&self, point: DataPoint) -> Result<(), IngestionError> {
        let series_id = point.series_id;

        // Check series count limit before creating new buffer
        let is_new_series = !self.buffers.contains_key(&series_id);
        if is_new_series && self.buffers.len() >= self.config.max_series_count {
            return Err(IngestionError::ValidationError(format!(
                "Maximum series count {} exceeded",
                self.config.max_series_count
            )));
        }

        // Get or create buffer for this series
        let mut buffer = self
            .buffers
            .entry(series_id)
            .or_insert_with(|| SeriesBuffer::new(series_id));

        // Use validated add to check timestamp and value
        let overwrite = buffer.add_validated(point.timestamp, point.value)?;

        // Update counters
        if !overwrite {
            self.total_buffered.fetch_add(1, Ordering::Relaxed);
            self.memory_used.fetch_add(16, Ordering::Relaxed);
        } else {
            self.metrics.record_overwrite();
        }

        // Check if buffer should be flushed
        if buffer.len() >= self.config.max_points_per_series {
            let points = buffer.drain();
            drop(buffer); // Release lock before sending
            self.send_write_batch(series_id, points).await?;
        } else {
            drop(buffer);
        }

        // Update backpressure state
        self.backpressure
            .update_memory_usage(self.memory_used.load(Ordering::Relaxed));

        Ok(())
    }

    /// Flush all buffers that have exceeded max age
    async fn flush_aged(&self) -> Result<(), IngestionError> {
        let mut to_flush = Vec::new();

        // Collect series that need flushing
        for entry in self.buffers.iter() {
            let buffer = entry.value();
            if !buffer.is_empty() && buffer.age() >= self.config.max_buffer_age {
                to_flush.push(*entry.key());
            }
        }

        // Flush collected series
        for series_id in to_flush {
            self.flush_series(series_id).await?;
        }

        Ok(())
    }

    /// Flush a specific series buffer
    async fn flush_series(&self, series_id: SeriesId) -> Result<(), IngestionError> {
        if let Some(mut buffer) = self.buffers.get_mut(&series_id) {
            if !buffer.is_empty() {
                let points = buffer.drain();
                let count = points.len();

                drop(buffer); // Release lock before sending

                self.send_write_batch(series_id, points).await?;

                // Update counters using saturating subtraction to prevent underflow
                // Use fetch_update for safe saturating subtraction
                let _ = self.total_buffered.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_sub(count)),
                );
                // Memory: 16 bytes per point (timestamp + value)
                let _ = self.memory_used.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_sub(count.saturating_mul(16))),
                );
            }
        }

        Ok(())
    }

    /// Flush all buffers
    pub async fn flush_all(&self) -> Result<(), IngestionError> {
        let series_ids: Vec<SeriesId> = self.buffers.iter().map(|e| *e.key()).collect();

        for series_id in series_ids {
            self.flush_series(series_id).await?;
        }

        debug!("Flushed all {} series buffers", self.buffers.len());
        Ok(())
    }

    /// Send a write batch to the output channel
    async fn send_write_batch(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
    ) -> Result<(), IngestionError> {
        if points.is_empty() {
            return Ok(());
        }

        let count = points.len() as u64;
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        let batch = WriteBatch {
            series_id,
            points,
            sequence,
        };

        self.output.send(batch).await.map_err(|_| {
            IngestionError::ChannelClosed("Write output channel closed".to_string())
        })?;

        self.metrics.record_buffer_flushed(count);

        Ok(())
    }

    /// Get total number of buffered points
    pub fn total_buffered(&self) -> usize {
        self.total_buffered.load(Ordering::Relaxed)
    }

    /// Get number of active series
    pub fn active_series_count(&self) -> usize {
        self.buffers.len()
    }

    /// Get current memory usage in bytes
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get statistics for all buffers
    pub fn all_stats(&self) -> Vec<SeriesBufferStats> {
        self.buffers
            .iter()
            .map(|entry| entry.value().stats())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== BufferConfig tests =====

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.max_points_per_series, 10_000);
        assert_eq!(config.max_total_memory, 512 * 1024 * 1024);
        assert_eq!(config.flush_interval, Duration::from_secs(1));
        assert_eq!(config.max_buffer_age, Duration::from_secs(10));
        assert_eq!(config.initial_series_capacity, 1_000);
        assert_eq!(config.max_series_count, 100_000);
    }

    #[test]
    fn test_buffer_config_validation() {
        let config = BufferConfig {
            max_points_per_series: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_buffer_config_validation_zero_memory() {
        let config = BufferConfig {
            max_total_memory: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_total_memory must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_zero_flush_interval() {
        let config = BufferConfig {
            flush_interval: Duration::ZERO,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("flush_interval must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_zero_series_count() {
        let config = BufferConfig {
            max_series_count: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_series_count must be > 0"));
    }

    #[test]
    fn test_buffer_config_validation_exceeds_max_points() {
        let config = BufferConfig {
            max_points_per_series: MAX_POINTS_PER_SERIES + 1,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_buffer_config_validation_exceeds_max_series() {
        let config = BufferConfig {
            max_series_count: MAX_SERIES_COUNT + 1,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_buffer_config_clone() {
        let config = BufferConfig {
            max_points_per_series: 5000,
            max_total_memory: 1024,
            flush_interval: Duration::from_millis(500),
            max_buffer_age: Duration::from_secs(5),
            initial_series_capacity: 100,
            max_series_count: 1000,
        };

        let cloned = config.clone();
        assert_eq!(cloned.max_points_per_series, 5000);
        assert_eq!(cloned.max_total_memory, 1024);
        assert_eq!(cloned.flush_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_buffer_config_debug() {
        let config = BufferConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("BufferConfig"));
        assert!(debug_str.contains("max_points_per_series"));
    }

    // ===== SeriesBuffer tests =====

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

    // ===== SeriesBufferStats tests =====

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

    // ===== WriteBatch tests =====

    #[test]
    fn test_write_batch_debug() {
        let batch = WriteBatch {
            series_id: 1,
            points: vec![DataPoint::new(1, 1000, 42.0)],
            sequence: 0,
        };

        let debug_str = format!("{:?}", batch);
        assert!(debug_str.contains("WriteBatch"));
        assert!(debug_str.contains("series_id: 1"));
    }

    // ===== Constant tests =====

    #[test]
    fn test_constants() {
        assert_eq!(MIN_VALID_TIMESTAMP, 0);
        assert_eq!(MAX_SERIES_COUNT, 10_000_000);
        assert_eq!(MAX_POINTS_PER_SERIES, 1_000_000);
    }

    #[test]
    fn test_max_valid_timestamp_is_reasonable() {
        // MAX_VALID_TIMESTAMP should be year 2100 in milliseconds
        // 2100-01-01 is approximately 4102444800 seconds since epoch
        assert_eq!(MAX_VALID_TIMESTAMP, 4_102_444_800_000);
    }

    // ===== Edge case tests =====

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
