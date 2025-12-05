//! Write buffer manager for per-series buffering
//!
//! Manages per-series write buffers with support for out-of-order point handling.
//! Points are sorted within each series buffer before being flushed to storage.
//!
//! # Architecture
//!
//! This module is split into submodules for maintainability:
//!
//! - `config` - Buffer configuration and constants
//! - `series` - Per-series buffer implementation
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::ingestion::{BufferConfig, WriteBufferManager, SeriesBuffer};
//!
//! let mut buffer = SeriesBuffer::new(1);
//! buffer.add(1000, 42.0);
//! buffer.add(1001, 43.0);
//!
//! let points = buffer.drain();
//! assert_eq!(points.len(), 2);
//! ```

mod config;
mod series;

pub use config::{
    BufferConfig, MAX_POINTS_PER_SERIES, MAX_SERIES_COUNT, MAX_VALID_TIMESTAMP, MIN_VALID_TIMESTAMP,
};
pub use series::{SeriesBuffer, SeriesBufferStats};

use crate::error::IngestionError;
use crate::types::{DataPoint, SeriesId};

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, warn};

use super::backpressure::BackpressureController;
use super::batch::PointBatch;
use super::metrics::IngestionMetrics;

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
/// Provides concurrent access to series buffers using DashMap.
///
/// # Thread Safety
///
/// Uses DashMap for concurrent access to per-series buffers without
/// requiring a global lock. Safe for multi-threaded ingestion.
///
/// # Backpressure
///
/// Integrates with BackpressureController to signal memory pressure
/// and enable flow control upstream.
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
    ///
    /// # Arguments
    ///
    /// * `config` - Buffer configuration
    /// * `input` - Channel to receive point batches from batcher
    /// * `output` - Channel to send write batches to writer
    /// * `backpressure` - Backpressure controller for flow control
    /// * `metrics` - Metrics collector for monitoring
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
    /// This is the main event loop that processes incoming batches
    /// and performs periodic flushes.
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
            },
        };

        let mut flush_interval = tokio::time::interval(self.config.flush_interval);

        debug!("WriteBufferManager started");

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
                    debug!("WriteBufferManager shutting down");
                    break;
                }
            }
        }

        // Final flush on shutdown
        if let Err(e) = self.flush_all().await {
            warn!("Error in final flush: {}", e);
        }

        debug!("WriteBufferManager stopped");
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
    ///
    /// Forces all buffered data to be sent to the writer.
    /// Called during shutdown to ensure no data is lost.
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
}
