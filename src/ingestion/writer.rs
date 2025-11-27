//! Parallel write workers for storage persistence
//!
//! Provides multi-threaded write workers that compress and persist
//! data points to storage with high throughput.

use crate::error::IngestionError;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tracing::{debug, error, info, warn};

use super::buffer::WriteBatch;
use super::metrics::IngestionMetrics;

/// Configuration for write workers
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Number of parallel write workers
    pub num_workers: usize,
    /// Maximum concurrent writes per worker
    pub max_concurrent_writes: usize,
    /// Write timeout
    pub write_timeout: Duration,
    /// Whether to compress before writing
    pub compress: bool,
    /// Retry count on failure
    pub max_retries: usize,
    /// Delay between retries
    pub retry_delay: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            max_concurrent_writes: 16,
            write_timeout: Duration::from_secs(30),
            compress: true,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl WriterConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.num_workers == 0 {
            return Err("num_workers must be > 0".to_string());
        }
        if self.max_concurrent_writes == 0 {
            return Err("max_concurrent_writes must be > 0".to_string());
        }
        Ok(())
    }
}

/// Statistics for write operations
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    /// Total batches written
    pub batches_written: u64,
    /// Total points written
    pub points_written: u64,
    /// Total bytes written (compressed)
    pub bytes_written: u64,
    /// Total write errors
    pub write_errors: u64,
    /// Total retries
    pub retries: u64,
    /// Average write latency in microseconds
    pub avg_latency_us: u64,
}

/// A single write worker
pub struct WriteWorker {
    /// Worker ID
    id: usize,
    /// Configuration (shared via Arc to avoid cloning per batch)
    config: Arc<WriterConfig>,
    /// Metrics collector
    metrics: Arc<IngestionMetrics>,
    /// Batches written
    batches_written: AtomicU64,
    /// Points written
    points_written: AtomicU64,
    /// Bytes written (compressed)
    bytes_written: AtomicU64,
    /// Write errors
    write_errors: AtomicU64,
    /// Total retries
    retries: AtomicU64,
    /// Total latency in microseconds (for computing average)
    total_latency_us: AtomicU64,
}

impl WriteWorker {
    /// Create a new write worker with an owned config
    pub fn new(id: usize, config: WriterConfig, metrics: Arc<IngestionMetrics>) -> Self {
        Self::with_shared_config(id, Arc::new(config), metrics)
    }

    /// Create a new write worker with a shared config
    ///
    /// This is more efficient when creating multiple workers, as the config
    /// is not cloned for each worker.
    pub fn with_shared_config(
        id: usize,
        config: Arc<WriterConfig>,
        metrics: Arc<IngestionMetrics>,
    ) -> Self {
        Self {
            id,
            config,
            metrics,
            batches_written: AtomicU64::new(0),
            points_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
        }
    }

    /// Process a write batch
    ///
    /// Compresses (if configured) and writes points to storage.
    pub async fn process_batch(&self, batch: WriteBatch) -> Result<(), IngestionError> {
        let start = Instant::now();
        let point_count = batch.points.len() as u64;

        debug!(
            "Worker {} processing batch {} ({} points for series {})",
            self.id, batch.sequence, point_count, batch.series_id
        );

        // Attempt write with retries
        let mut last_error = None;
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay).await;
                debug!(
                    "Worker {} retry {} for batch {}",
                    self.id, attempt, batch.sequence
                );
            }

            match self.write_points(&batch).await {
                Ok(bytes_written) => {
                    let latency = start.elapsed();
                    let latency_us = latency.as_micros() as u64;

                    // Update all tracked stats
                    self.batches_written.fetch_add(1, Ordering::Relaxed);
                    self.points_written
                        .fetch_add(point_count, Ordering::Relaxed);
                    self.bytes_written
                        .fetch_add(bytes_written, Ordering::Relaxed);
                    self.total_latency_us
                        .fetch_add(latency_us, Ordering::Relaxed);

                    self.metrics
                        .record_write(point_count, bytes_written, latency);

                    debug!(
                        "Worker {} wrote batch {} ({} points, {} bytes) in {:?}",
                        self.id, batch.sequence, point_count, bytes_written, latency
                    );

                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        self.retries.fetch_add(1, Ordering::Relaxed);
                        self.metrics.record_retry();
                    }
                }
            }
        }

        // All retries failed
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_write_error();

        Err(last_error
            .unwrap_or_else(|| IngestionError::WriteError("Unknown write error".to_string())))
    }

    /// Write points to storage (stub - will integrate with storage layer)
    async fn write_points(&self, batch: &WriteBatch) -> Result<u64, IngestionError> {
        // TODO: Integrate with actual storage layer
        // For now, simulate write with compression size estimation

        let points = &batch.points;
        if points.is_empty() {
            return Ok(0);
        }

        // Simulate compression - Gorilla typically achieves ~1.37 bytes/point
        let compressed_size = if self.config.compress {
            // Estimate: 2 bytes per point with Gorilla compression
            (points.len() * 2) as u64
        } else {
            // Uncompressed: 24 bytes per point (timestamp + value + overhead)
            (points.len() * 24) as u64
        };

        // Simulate write latency (remove this when integrating with real storage)
        // In production, this would call the storage engine
        tokio::time::sleep(Duration::from_micros(10)).await;

        Ok(compressed_size)
    }

    /// Get worker statistics
    pub fn stats(&self) -> WriteStats {
        let batches = self.batches_written.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);

        // Calculate average latency (avoid division by zero)
        let avg_latency_us = if batches > 0 {
            total_latency / batches
        } else {
            0
        };

        WriteStats {
            batches_written: batches,
            points_written: self.points_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            avg_latency_us,
        }
    }
}

/// Parallel writer that distributes work across multiple workers
pub struct ParallelWriter {
    /// Configuration (shared via Arc to avoid cloning per batch)
    config: Arc<WriterConfig>,
    /// Input channel for write batches
    input: tokio::sync::RwLock<Option<mpsc::Receiver<WriteBatch>>>,
    /// Metrics collector
    metrics: Arc<IngestionMetrics>,
    /// Concurrency limiter
    semaphore: Arc<Semaphore>,
    /// Active write count (wrapped in Arc for safe sharing across tasks)
    active_writes: Arc<AtomicUsize>,
    /// Total batches processed (wrapped in Arc for safe sharing across tasks)
    batches_processed: Arc<AtomicU64>,
    /// Total points written (wrapped in Arc for safe sharing across tasks)
    points_written: Arc<AtomicU64>,
}

impl ParallelWriter {
    /// Create a new parallel writer
    pub fn new(
        config: WriterConfig,
        input: mpsc::Receiver<WriteBatch>,
        metrics: Arc<IngestionMetrics>,
    ) -> Self {
        let max_concurrent = config.num_workers * config.max_concurrent_writes;
        let semaphore = Arc::new(Semaphore::new(max_concurrent));

        Self {
            config: Arc::new(config),
            input: tokio::sync::RwLock::new(Some(input)),
            metrics,
            semaphore,
            active_writes: Arc::new(AtomicUsize::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            points_written: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Run the parallel writer
    ///
    /// # Errors
    ///
    /// Returns error if called more than once (double-start).
    pub async fn run(
        &self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<(), crate::error::IngestionError> {
        // Take ownership of the input receiver
        let mut input = match self.input.write().await.take() {
            Some(rx) => rx,
            None => {
                warn!("ParallelWriter input already taken - double start detected");
                return Err(crate::error::IngestionError::ConfigError(
                    "ParallelWriter already running (double start)".to_string(),
                ));
            }
        };

        info!(
            "ParallelWriter started with {} workers, {} max concurrent",
            self.config.num_workers,
            self.config.num_workers * self.config.max_concurrent_writes
        );

        loop {
            tokio::select! {
                Some(batch) = input.recv() => {
                    let permit = match self.semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            error!("Semaphore closed unexpectedly");
                            break;
                        }
                    };

                    self.active_writes.fetch_add(1, Ordering::Relaxed);

                    let metrics = Arc::clone(&self.metrics);
                    // Use Arc::clone instead of config.clone() to avoid allocation
                    let config = Arc::clone(&self.config);

                    // Clone Arc references to move into spawned task
                    let batches_processed = Arc::clone(&self.batches_processed);
                    let points_written = Arc::clone(&self.points_written);
                    let active_writes = Arc::clone(&self.active_writes);

                    // Spawn task to process the batch
                    let point_count = batch.points.len() as u64;
                    let batch_seq = batch.sequence;

                    // Create worker for this batch using shared config
                    let worker = WriteWorker::with_shared_config(0, config, metrics);

                    tokio::spawn(async move {
                        let result = worker.process_batch(batch).await;

                        if let Err(e) = result {
                            error!("Failed to write batch {}: {}", batch_seq, e);
                        }

                        // Update counters AFTER task completes (fixes race condition)
                        batches_processed.fetch_add(1, Ordering::Relaxed);
                        points_written.fetch_add(point_count, Ordering::Relaxed);
                        active_writes.fetch_sub(1, Ordering::Relaxed);

                        drop(permit);
                    });
                }

                _ = shutdown.recv() => {
                    info!("ParallelWriter shutting down");
                    break;
                }
            }
        }

        // Wait for active writes to complete (with timeout)
        if let Err(e) = self.wait_for_completion().await {
            warn!("Error during shutdown wait: {}", e);
        }

        info!("ParallelWriter stopped");
        Ok(())
    }

    /// Wait for all active writes to complete with timeout
    ///
    /// Waits up to 30 seconds for all active writes to complete.
    /// Returns error if timeout is exceeded.
    async fn wait_for_completion(&self) -> Result<(), crate::error::IngestionError> {
        let max_concurrent = self.config.num_workers * self.config.max_concurrent_writes;
        let timeout = Duration::from_secs(30);
        let start = Instant::now();

        // Wait until all permits are available (meaning no active writes)
        loop {
            let available = self.semaphore.available_permits();
            if available == max_concurrent {
                return Ok(());
            }

            // Check timeout
            if start.elapsed() > timeout {
                let remaining = max_concurrent - available;
                warn!(
                    "Shutdown timeout: {} writes still active after {:?}",
                    remaining, timeout
                );
                return Err(crate::error::IngestionError::ShutdownError(format!(
                    "Timeout waiting for {} active writes to complete",
                    remaining
                )));
            }

            debug!(
                "Waiting for {} active writes to complete",
                max_concurrent - available
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Get the number of currently active writes
    pub fn active_writes(&self) -> usize {
        self.active_writes.load(Ordering::Relaxed)
    }

    /// Get total batches processed
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed.load(Ordering::Relaxed)
    }

    /// Get total points written
    pub fn points_written(&self) -> u64 {
        self.points_written.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataPoint;

    #[test]
    fn test_writer_config_default() {
        let config = WriterConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.num_workers, 4);
    }

    #[test]
    fn test_writer_config_validation() {
        let mut config = WriterConfig::default();
        config.num_workers = 0;
        assert!(config.validate().is_err());

        config.num_workers = 4;
        config.max_concurrent_writes = 0;
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_write_worker_process_batch() {
        let metrics = Arc::new(IngestionMetrics::new());
        let config = WriterConfig::default();
        let worker = WriteWorker::new(0, config, metrics);

        let batch = WriteBatch {
            series_id: 1,
            points: vec![DataPoint::new(1, 1000, 42.0), DataPoint::new(1, 1001, 43.0)],
            sequence: 0,
        };

        let result = worker.process_batch(batch).await;
        assert!(result.is_ok());
        assert_eq!(worker.batches_written.load(Ordering::Relaxed), 1);
        assert_eq!(worker.points_written.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_write_worker_empty_batch() {
        let metrics = Arc::new(IngestionMetrics::new());
        let config = WriterConfig::default();
        let worker = WriteWorker::new(0, config, metrics);

        let batch = WriteBatch {
            series_id: 1,
            points: vec![],
            sequence: 0,
        };

        let result = worker.process_batch(batch).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_parallel_writer_creation() {
        let (_tx, rx) = mpsc::channel(100);
        let metrics = Arc::new(IngestionMetrics::new());
        let config = WriterConfig::default();

        let writer = ParallelWriter::new(config.clone(), rx, metrics);

        assert_eq!(writer.active_writes(), 0);
        assert_eq!(writer.batches_processed(), 0);
    }
}
