//! Write buffer manager for per-series buffering
//!
//! Manages per-series write buffers with support for out-of-order point handling.
//! Points are sorted within each series buffer before being flushed to storage.

use crate::error::IngestionError;
use crate::types::{DataPoint, SeriesId};

use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, info, warn};

use super::backpressure::BackpressureController;
use super::batch::PointBatch;
use super::metrics::IngestionMetrics;

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
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_points_per_series: 10_000,
            max_total_memory: 512 * 1024 * 1024, // 512 MB
            flush_interval: Duration::from_secs(1),
            max_buffer_age: Duration::from_secs(10),
            initial_series_capacity: 1_000,
        }
    }
}

impl BufferConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_points_per_series == 0 {
            return Err("max_points_per_series must be > 0".to_string());
        }
        if self.max_total_memory == 0 {
            return Err("max_total_memory must be > 0".to_string());
        }
        if self.flush_interval.is_zero() {
            return Err("flush_interval must be > 0".to_string());
        }
        Ok(())
    }
}

/// Per-series write buffer
///
/// Uses a BTreeMap to maintain sorted order by timestamp,
/// allowing efficient handling of out-of-order points.
pub struct SeriesBuffer {
    /// Series identifier
    series_id: SeriesId,
    /// Points sorted by timestamp (handles out-of-order automatically)
    points: BTreeMap<i64, f64>,
    /// When this buffer was created
    created_at: Instant,
    /// When this buffer was last modified
    last_modified: Instant,
    /// Total points ever added (including overwrites)
    total_added: u64,
    /// Points that overwrote existing timestamps
    overwrites: u64,
}

impl SeriesBuffer {
    /// Create a new series buffer
    pub fn new(series_id: SeriesId) -> Self {
        let now = Instant::now();
        Self {
            series_id,
            points: BTreeMap::new(),
            created_at: now,
            last_modified: now,
            total_added: 0,
            overwrites: 0,
        }
    }

    /// Add a point to the buffer
    ///
    /// If a point with the same timestamp already exists, it will be overwritten.
    /// Returns true if this was an overwrite.
    pub fn add(&mut self, timestamp: i64, value: f64) -> bool {
        self.last_modified = Instant::now();
        self.total_added += 1;

        let overwrite = self.points.insert(timestamp, value).is_some();
        if overwrite {
            self.overwrites += 1;
        }
        overwrite
    }

    /// Add multiple points to the buffer
    pub fn add_batch(&mut self, points: impl IntoIterator<Item = (i64, f64)>) {
        for (timestamp, value) in points {
            self.add(timestamp, value);
        }
    }

    /// Number of unique points in the buffer
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
    pub fn drain(&mut self) -> Vec<DataPoint> {
        let points: Vec<DataPoint> = self
            .points
            .iter()
            .map(|(&timestamp, &value)| DataPoint::new(self.series_id, timestamp, value))
            .collect();

        self.points.clear();
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

    /// Estimated memory usage in bytes
    pub fn memory_size(&self) -> usize {
        // BTreeMap node overhead + (timestamp, value) per entry
        // Rough estimate: 64 bytes per entry for BTreeMap overhead
        std::mem::size_of::<Self>() + self.points.len() * 80
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
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        // Take ownership of the input receiver
        let mut input = match self.input.write().await.take() {
            Some(rx) => rx,
            None => {
                warn!("WriteBufferManager input already taken");
                return;
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
    }

    /// Process an incoming point batch
    async fn process_batch(&self, batch: PointBatch) -> Result<(), IngestionError> {
        let point_count = batch.len();

        for point in batch.points {
            self.add_point(point).await?;
        }

        debug!("Processed batch with {} points", point_count);
        Ok(())
    }

    /// Add a single point to the appropriate series buffer
    async fn add_point(&self, point: DataPoint) -> Result<(), IngestionError> {
        let series_id = point.series_id;

        // Get or create buffer for this series
        let mut buffer = self.buffers.entry(series_id).or_insert_with(|| {
            SeriesBuffer::new(series_id)
        });

        let overwrite = buffer.add(point.timestamp, point.value);

        // Update counters
        if !overwrite {
            self.total_buffered.fetch_add(1, Ordering::Relaxed);
            // Approximate memory increase
            self.memory_used.fetch_add(80, Ordering::Relaxed);
        }

        if overwrite {
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
        self.backpressure.update_memory_usage(self.memory_used.load(Ordering::Relaxed));

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

                // Update counters
                self.total_buffered.fetch_sub(count, Ordering::Relaxed);
                self.memory_used.fetch_sub(count * 80, Ordering::Relaxed);
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

        self.output
            .send(batch)
            .await
            .map_err(|_| IngestionError::ChannelClosed("Write output channel closed".to_string()))?;

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

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_buffer_config_validation() {
        let mut config = BufferConfig::default();
        config.max_points_per_series = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_series_buffer_new() {
        let buffer = SeriesBuffer::new(1);
        assert_eq!(buffer.series_id, 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
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
    fn test_series_buffer_drain() {
        let mut buffer = SeriesBuffer::new(1);
        buffer.add(1000, 42.0);
        buffer.add(1001, 43.0);

        let points = buffer.drain();
        assert_eq!(points.len(), 2);
        assert!(buffer.is_empty());
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
}
