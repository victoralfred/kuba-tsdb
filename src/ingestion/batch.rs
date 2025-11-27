//! Point batching for efficient ingestion
//!
//! Groups individual data points into batches for more efficient processing.
//! Supports both size-based and time-based batch triggers.

use crate::error::IngestionError;
use crate::types::DataPoint;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use super::metrics::IngestionMetrics;

/// Configuration for point batching
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum points per batch before flushing
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing a partial batch
    pub max_batch_timeout: Duration,
    /// Pre-allocate capacity for batches
    pub initial_capacity: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 10_000,
            max_batch_timeout: Duration::from_millis(100),
            initial_capacity: 1_000,
        }
    }
}

impl BatchConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_batch_size == 0 {
            return Err("max_batch_size must be > 0".to_string());
        }
        if self.max_batch_timeout.is_zero() {
            return Err("max_batch_timeout must be > 0".to_string());
        }
        Ok(())
    }
}

/// A batch of data points ready for processing
#[derive(Debug, Clone)]
pub struct PointBatch {
    /// The data points in this batch
    pub points: Vec<DataPoint>,
    /// When this batch was created
    pub created_at: Instant,
    /// Sequence number for ordering
    pub sequence: u64,
}

impl PointBatch {
    /// Create a new point batch
    pub fn new(points: Vec<DataPoint>, sequence: u64) -> Self {
        Self {
            points,
            created_at: Instant::now(),
            sequence,
        }
    }

    /// Create an empty batch with pre-allocated capacity
    pub fn with_capacity(capacity: usize, sequence: u64) -> Self {
        Self {
            points: Vec::with_capacity(capacity),
            created_at: Instant::now(),
            sequence,
        }
    }

    /// Number of points in the batch
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Age of the batch since creation
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Add a point to the batch
    pub fn push(&mut self, point: DataPoint) {
        self.points.push(point);
    }

    /// Extend batch with multiple points
    pub fn extend(&mut self, points: impl IntoIterator<Item = DataPoint>) {
        self.points.extend(points);
    }

    /// Take ownership of the points, leaving the batch empty
    pub fn take_points(&mut self) -> Vec<DataPoint> {
        std::mem::take(&mut self.points)
    }

    /// Estimated memory size of the batch in bytes
    pub fn memory_size(&self) -> usize {
        // DataPoint is Copy, so it's stack allocated within the Vec
        // Each DataPoint: series_id (16) + timestamp (8) + value (8) = 32 bytes
        std::mem::size_of::<Self>() + self.points.capacity() * std::mem::size_of::<DataPoint>()
    }
}

/// Internal state for the batcher
struct BatcherState {
    /// Current batch being built
    current_batch: PointBatch,
    /// When the current batch was started
    batch_start: Instant,
}

/// Point batcher that groups points into batches
///
/// Flushes batches when either:
/// - The batch reaches `max_batch_size` points
/// - The batch age exceeds `max_batch_timeout`
pub struct Batcher {
    /// Configuration
    config: BatchConfig,
    /// Internal state protected by mutex
    state: Mutex<BatcherState>,
    /// Output channel for completed batches
    output: mpsc::Sender<PointBatch>,
    /// Sequence counter for batch ordering
    sequence: AtomicU64,
    /// Metrics collector
    metrics: Arc<IngestionMetrics>,
}

impl Batcher {
    /// Create a new batcher
    ///
    /// # Arguments
    ///
    /// * `config` - Batch configuration
    /// * `output` - Channel to send completed batches
    /// * `metrics` - Metrics collector
    pub fn new(
        config: BatchConfig,
        output: mpsc::Sender<PointBatch>,
        metrics: Arc<IngestionMetrics>,
    ) -> Self {
        let sequence = AtomicU64::new(0);
        let seq = sequence.fetch_add(1, Ordering::SeqCst);

        let state = BatcherState {
            current_batch: PointBatch::with_capacity(config.initial_capacity, seq),
            batch_start: Instant::now(),
        };

        Self {
            config,
            state: Mutex::new(state),
            output,
            sequence,
            metrics,
        }
    }

    /// Add a single point to the current batch
    ///
    /// May trigger a batch flush if size threshold is reached.
    pub async fn add_point(&self, point: DataPoint) -> Result<(), IngestionError> {
        let mut state = self.state.lock().await;

        state.current_batch.push(point);

        // Check if batch is full
        if state.current_batch.len() >= self.config.max_batch_size {
            self.flush_internal(&mut state).await?;
        }

        Ok(())
    }

    /// Add multiple points to the current batch
    ///
    /// More efficient than adding points one at a time.
    /// May trigger multiple batch flushes if input is large.
    pub async fn add_batch(&self, points: Vec<DataPoint>) -> Result<(), IngestionError> {
        if points.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock().await;

        // If adding all points would exceed batch size, flush in chunks
        let remaining_capacity = self.config.max_batch_size.saturating_sub(state.current_batch.len());

        if points.len() <= remaining_capacity {
            // All points fit in current batch
            state.current_batch.extend(points);

            if state.current_batch.len() >= self.config.max_batch_size {
                self.flush_internal(&mut state).await?;
            }
        } else {
            // Need to split across batches
            let mut points_iter = points.into_iter();

            // Fill current batch
            for point in points_iter.by_ref().take(remaining_capacity) {
                state.current_batch.push(point);
            }
            self.flush_internal(&mut state).await?;

            // Create full batches from remaining points
            let mut remaining: Vec<DataPoint> = points_iter.collect();

            while remaining.len() >= self.config.max_batch_size {
                let batch_points: Vec<DataPoint> = remaining.drain(..self.config.max_batch_size).collect();
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
                let batch = PointBatch::new(batch_points, seq);

                self.send_batch(batch).await?;
            }

            // Put leftover points in current batch
            if !remaining.is_empty() {
                state.current_batch.extend(remaining);
            }
        }

        Ok(())
    }

    /// Flush the current batch if not empty
    pub async fn flush(&self) -> Result<(), IngestionError> {
        let mut state = self.state.lock().await;
        if !state.current_batch.is_empty() {
            self.flush_internal(&mut state).await?;
        }
        Ok(())
    }

    /// Check if the current batch should be flushed due to timeout
    pub async fn check_timeout(&self) -> Result<(), IngestionError> {
        let mut state = self.state.lock().await;

        if !state.current_batch.is_empty()
            && state.batch_start.elapsed() >= self.config.max_batch_timeout
        {
            self.flush_internal(&mut state).await?;
        }

        Ok(())
    }

    /// Internal flush implementation (caller must hold lock)
    async fn flush_internal(&self, state: &mut BatcherState) -> Result<(), IngestionError> {
        if state.current_batch.is_empty() {
            return Ok(());
        }

        // Take the current batch
        let batch = std::mem::replace(
            &mut state.current_batch,
            PointBatch::with_capacity(
                self.config.initial_capacity,
                self.sequence.fetch_add(1, Ordering::SeqCst),
            ),
        );

        state.batch_start = Instant::now();

        // Send the batch
        self.send_batch(batch).await
    }

    /// Send a batch to the output channel
    async fn send_batch(&self, batch: PointBatch) -> Result<(), IngestionError> {
        let point_count = batch.len() as u64;
        let batch_age = batch.age();

        debug!(
            "Flushing batch {} with {} points (age: {:?})",
            batch.sequence,
            point_count,
            batch_age
        );

        self.output
            .send(batch)
            .await
            .map_err(|_| IngestionError::ChannelClosed("Batch output channel closed".to_string()))?;

        self.metrics.record_batch_flushed(point_count, batch_age);

        Ok(())
    }

    /// Get the number of points in the current batch
    pub async fn pending_count(&self) -> usize {
        self.state.lock().await.current_batch.len()
    }

    /// Get the age of the current batch
    pub async fn current_batch_age(&self) -> Duration {
        self.state.lock().await.batch_start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.max_batch_size, 10_000);
    }

    #[test]
    fn test_batch_config_validation() {
        let mut config = BatchConfig::default();
        config.max_batch_size = 0;
        assert!(config.validate().is_err());

        config.max_batch_size = 100;
        config.max_batch_timeout = Duration::ZERO;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_point_batch_creation() {
        let points = vec![
            DataPoint::new(1, 1000, 42.0),
            DataPoint::new(1, 1001, 43.0),
        ];

        let batch = PointBatch::new(points, 0);
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_eq!(batch.sequence, 0);
    }

    #[test]
    fn test_point_batch_with_capacity() {
        let batch = PointBatch::with_capacity(100, 5);
        assert!(batch.is_empty());
        assert_eq!(batch.sequence, 5);
    }

    #[test]
    fn test_point_batch_push() {
        let mut batch = PointBatch::with_capacity(10, 0);
        batch.push(DataPoint::new(1, 1000, 42.0));
        batch.push(DataPoint::new(1, 1001, 43.0));

        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_point_batch_extend() {
        let mut batch = PointBatch::with_capacity(10, 0);
        let points = vec![
            DataPoint::new(1, 1000, 42.0),
            DataPoint::new(1, 1001, 43.0),
        ];

        batch.extend(points);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_point_batch_take_points() {
        let mut batch = PointBatch::new(
            vec![DataPoint::new(1, 1000, 42.0)],
            0,
        );

        let points = batch.take_points();
        assert_eq!(points.len(), 1);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_point_batch_memory_size() {
        let batch = PointBatch::with_capacity(100, 0);
        let size = batch.memory_size();
        // Should include Vec overhead plus capacity * DataPoint size
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_batcher_add_point() {
        let (tx, mut rx) = mpsc::channel(10);
        let metrics = Arc::new(IngestionMetrics::new());
        let config = BatchConfig {
            max_batch_size: 2,
            max_batch_timeout: Duration::from_secs(60),
            initial_capacity: 10,
        };

        let batcher = Batcher::new(config, tx, metrics);

        // Add first point - should not flush
        batcher.add_point(DataPoint::new(1, 1000, 42.0)).await.unwrap();
        assert_eq!(batcher.pending_count().await, 1);

        // Add second point - should trigger flush
        batcher.add_point(DataPoint::new(1, 1001, 43.0)).await.unwrap();
        assert_eq!(batcher.pending_count().await, 0);

        // Should have received a batch
        let batch = rx.try_recv().unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_batcher_add_batch() {
        let (tx, mut rx) = mpsc::channel(10);
        let metrics = Arc::new(IngestionMetrics::new());
        let config = BatchConfig {
            max_batch_size: 3,
            max_batch_timeout: Duration::from_secs(60),
            initial_capacity: 10,
        };

        let batcher = Batcher::new(config, tx, metrics);

        // Add batch of 5 points with max_batch_size=3
        let points = vec![
            DataPoint::new(1, 1000, 42.0),
            DataPoint::new(1, 1001, 43.0),
            DataPoint::new(1, 1002, 44.0),
            DataPoint::new(1, 1003, 45.0),
            DataPoint::new(1, 1004, 46.0),
        ];

        batcher.add_batch(points).await.unwrap();

        // Should have flushed one full batch and have 2 pending
        let batch1 = rx.try_recv().unwrap();
        assert_eq!(batch1.len(), 3);

        // Remaining 2 points should be pending
        assert_eq!(batcher.pending_count().await, 2);
    }

    #[tokio::test]
    async fn test_batcher_flush() {
        let (tx, mut rx) = mpsc::channel(10);
        let metrics = Arc::new(IngestionMetrics::new());
        let config = BatchConfig {
            max_batch_size: 100,
            max_batch_timeout: Duration::from_secs(60),
            initial_capacity: 10,
        };

        let batcher = Batcher::new(config, tx, metrics);

        // Add single point
        batcher.add_point(DataPoint::new(1, 1000, 42.0)).await.unwrap();
        assert_eq!(batcher.pending_count().await, 1);

        // Manual flush
        batcher.flush().await.unwrap();
        assert_eq!(batcher.pending_count().await, 0);

        // Should have received the batch
        let batch = rx.try_recv().unwrap();
        assert_eq!(batch.len(), 1);
    }
}
