//! Ingestion-specific metrics collection
//!
//! Provides detailed metrics for monitoring ingestion pipeline performance,
//! throughput, latency, and health.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Ingestion metrics collector
///
/// Thread-safe metrics collection for the ingestion pipeline.
/// Uses atomic operations for lock-free updates.
pub struct IngestionMetrics {
    // === Throughput Counters ===
    /// Total points received
    points_received: AtomicU64,
    /// Total points written to storage
    points_written: AtomicU64,
    /// Total points rejected due to backpressure
    points_rejected: AtomicU64,
    /// Total points dropped (validation failures, etc.)
    points_dropped: AtomicU64,

    // === Batch Metrics ===
    /// Total batches processed
    batches_processed: AtomicU64,
    /// Total batch flush operations
    batches_flushed: AtomicU64,

    // === Buffer Metrics ===
    /// Total buffer flushes
    buffer_flushes: AtomicU64,
    /// Total timestamp overwrites (duplicate timestamps)
    overwrites: AtomicU64,

    // === Write Metrics ===
    /// Total bytes written (compressed)
    bytes_written: AtomicU64,
    /// Total write errors
    write_errors: AtomicU64,
    /// Total write retries
    write_retries: AtomicU64,

    // === Latency Tracking (simple average) ===
    /// Sum of write latencies in microseconds
    write_latency_sum_us: AtomicU64,
    /// Count of write latency samples
    write_latency_count: AtomicU64,

    /// Sum of batch ages in microseconds when flushed
    batch_age_sum_us: AtomicU64,
    /// Count of batch age samples
    batch_age_count: AtomicU64,

    // === Backpressure Metrics ===
    /// Times backpressure was activated
    backpressure_activations: AtomicU64,
    /// Times backpressure was deactivated
    backpressure_deactivations: AtomicU64,

    // === Timing ===
    /// When metrics collection started
    start_time: Instant,
}

impl IngestionMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            points_received: AtomicU64::new(0),
            points_written: AtomicU64::new(0),
            points_rejected: AtomicU64::new(0),
            points_dropped: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            buffer_flushes: AtomicU64::new(0),
            overwrites: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            write_retries: AtomicU64::new(0),
            write_latency_sum_us: AtomicU64::new(0),
            write_latency_count: AtomicU64::new(0),
            batch_age_sum_us: AtomicU64::new(0),
            batch_age_count: AtomicU64::new(0),
            backpressure_activations: AtomicU64::new(0),
            backpressure_deactivations: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    // === Recording Methods ===

    /// Record points received
    #[inline]
    pub fn record_received(&self, count: u64) {
        // Use saturating arithmetic to prevent overflow
        let _ =
            self.points_received
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(count))
                });
    }

    /// Record points rejected due to backpressure
    #[inline]
    pub fn record_rejected(&self, count: u64) {
        self.points_rejected.fetch_add(count, Ordering::Relaxed);
    }

    /// Record points dropped (validation failure, etc.)
    #[inline]
    pub fn record_dropped(&self, count: u64) {
        self.points_dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a batch flush
    #[inline]
    pub fn record_batch_flushed(&self, _point_count: u64, age: Duration) {
        self.batches_flushed.fetch_add(1, Ordering::Relaxed);

        let age_us = age.as_micros() as u64;
        self.batch_age_sum_us.fetch_add(age_us, Ordering::Relaxed);
        self.batch_age_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a buffer flush
    #[inline]
    pub fn record_buffer_flushed(&self, _point_count: u64) {
        self.buffer_flushes.fetch_add(1, Ordering::Relaxed);
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timestamp overwrite
    #[inline]
    pub fn record_overwrite(&self) {
        self.overwrites.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful write
    #[inline]
    pub fn record_write(&self, point_count: u64, bytes: u64, latency: Duration) {
        // Use saturating arithmetic for critical counters to prevent overflow
        let _ = self
            .points_written
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(point_count))
            });
        let _ = self
            .bytes_written
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            });

        let latency_us = latency.as_micros() as u64;
        let _ = self.write_latency_sum_us.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(latency_us)),
        );
        self.write_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write error
    #[inline]
    pub fn record_write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write retry
    #[inline]
    pub fn record_retry(&self) {
        self.write_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record backpressure activation
    #[inline]
    pub fn record_backpressure_activated(&self) {
        self.backpressure_activations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record backpressure deactivation
    #[inline]
    pub fn record_backpressure_deactivated(&self) {
        self.backpressure_deactivations
            .fetch_add(1, Ordering::Relaxed);
    }

    // === Query Methods ===

    /// Get total points received
    pub fn points_received(&self) -> u64 {
        self.points_received.load(Ordering::Relaxed)
    }

    /// Get total points written
    pub fn points_written(&self) -> u64 {
        self.points_written.load(Ordering::Relaxed)
    }

    /// Get total points rejected
    pub fn points_rejected(&self) -> u64 {
        self.points_rejected.load(Ordering::Relaxed)
    }

    /// Get total points dropped
    pub fn points_dropped(&self) -> u64 {
        self.points_dropped.load(Ordering::Relaxed)
    }

    /// Get total batches processed
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed.load(Ordering::Relaxed)
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get total write errors
    pub fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }

    /// Get total overwrites
    pub fn overwrites(&self) -> u64 {
        self.overwrites.load(Ordering::Relaxed)
    }

    /// Get average write latency in microseconds
    pub fn avg_write_latency_us(&self) -> u64 {
        let sum = self.write_latency_sum_us.load(Ordering::Relaxed);
        let count = self.write_latency_count.load(Ordering::Relaxed);
        if count > 0 {
            sum / count
        } else {
            0
        }
    }

    /// Get average batch age in microseconds when flushed
    pub fn avg_batch_age_us(&self) -> u64 {
        let sum = self.batch_age_sum_us.load(Ordering::Relaxed);
        let count = self.batch_age_count.load(Ordering::Relaxed);
        if count > 0 {
            sum / count
        } else {
            0
        }
    }

    /// Get uptime since metrics collection started
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Calculate points per second throughput
    pub fn points_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.points_written.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Calculate bytes per second throughput
    pub fn bytes_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.bytes_written.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Get a complete metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            points_received: self.points_received.load(Ordering::Relaxed),
            points_written: self.points_written.load(Ordering::Relaxed),
            points_rejected: self.points_rejected.load(Ordering::Relaxed),
            points_dropped: self.points_dropped.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            batches_flushed: self.batches_flushed.load(Ordering::Relaxed),
            buffer_flushes: self.buffer_flushes.load(Ordering::Relaxed),
            overwrites: self.overwrites.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            write_retries: self.write_retries.load(Ordering::Relaxed),
            avg_write_latency_us: self.avg_write_latency_us(),
            avg_batch_age_us: self.avg_batch_age_us(),
            backpressure_activations: self.backpressure_activations.load(Ordering::Relaxed),
            backpressure_deactivations: self.backpressure_deactivations.load(Ordering::Relaxed),
            uptime: self.uptime(),
            points_per_second: self.points_per_second(),
            bytes_per_second: self.bytes_per_second(),
        }
    }

    /// Reset all metrics (for testing)
    #[cfg(test)]
    pub fn reset(&self) {
        self.points_received.store(0, Ordering::Relaxed);
        self.points_written.store(0, Ordering::Relaxed);
        self.points_rejected.store(0, Ordering::Relaxed);
        self.points_dropped.store(0, Ordering::Relaxed);
        self.batches_processed.store(0, Ordering::Relaxed);
        self.batches_flushed.store(0, Ordering::Relaxed);
        self.buffer_flushes.store(0, Ordering::Relaxed);
        self.overwrites.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.write_errors.store(0, Ordering::Relaxed);
        self.write_retries.store(0, Ordering::Relaxed);
        self.write_latency_sum_us.store(0, Ordering::Relaxed);
        self.write_latency_count.store(0, Ordering::Relaxed);
        self.batch_age_sum_us.store(0, Ordering::Relaxed);
        self.batch_age_count.store(0, Ordering::Relaxed);
        self.backpressure_activations.store(0, Ordering::Relaxed);
        self.backpressure_deactivations.store(0, Ordering::Relaxed);
    }
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total points received
    pub points_received: u64,
    /// Total points written to storage
    pub points_written: u64,
    /// Total points rejected due to backpressure
    pub points_rejected: u64,
    /// Total points dropped
    pub points_dropped: u64,
    /// Total batches processed
    pub batches_processed: u64,
    /// Total batches flushed
    pub batches_flushed: u64,
    /// Total buffer flushes
    pub buffer_flushes: u64,
    /// Total timestamp overwrites
    pub overwrites: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total write errors
    pub write_errors: u64,
    /// Total write retries
    pub write_retries: u64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: u64,
    /// Average batch age in microseconds
    pub avg_batch_age_us: u64,
    /// Backpressure activation count
    pub backpressure_activations: u64,
    /// Backpressure deactivation count
    pub backpressure_deactivations: u64,
    /// Time since metrics collection started
    pub uptime: Duration,
    /// Points written per second
    pub points_per_second: f64,
    /// Bytes written per second
    pub bytes_per_second: f64,
}

impl MetricsSnapshot {
    /// Calculate the write success rate (0.0 - 1.0)
    pub fn write_success_rate(&self) -> f64 {
        let total = self.points_received;
        if total > 0 {
            self.points_written as f64 / total as f64
        } else {
            1.0
        }
    }

    /// Calculate the rejection rate (0.0 - 1.0)
    pub fn rejection_rate(&self) -> f64 {
        let total = self.points_received;
        if total > 0 {
            self.points_rejected as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Calculate the error rate (0.0 - 1.0)
    pub fn error_rate(&self) -> f64 {
        let total = self.batches_processed;
        if total > 0 {
            self.write_errors as f64 / total as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = IngestionMetrics::new();
        assert_eq!(metrics.points_received(), 0);
        assert_eq!(metrics.points_written(), 0);
    }

    #[test]
    fn test_metrics_record_received() {
        let metrics = IngestionMetrics::new();
        metrics.record_received(100);
        assert_eq!(metrics.points_received(), 100);

        metrics.record_received(50);
        assert_eq!(metrics.points_received(), 150);
    }

    #[test]
    fn test_metrics_record_write() {
        let metrics = IngestionMetrics::new();
        metrics.record_write(100, 500, Duration::from_micros(1000));

        assert_eq!(metrics.points_written(), 100);
        assert_eq!(metrics.bytes_written(), 500);
        assert_eq!(metrics.avg_write_latency_us(), 1000);
    }

    #[test]
    fn test_metrics_avg_latency() {
        let metrics = IngestionMetrics::new();

        // No writes yet
        assert_eq!(metrics.avg_write_latency_us(), 0);

        // Record multiple writes
        metrics.record_write(10, 100, Duration::from_micros(1000));
        metrics.record_write(10, 100, Duration::from_micros(2000));
        metrics.record_write(10, 100, Duration::from_micros(3000));

        // Average should be 2000
        assert_eq!(metrics.avg_write_latency_us(), 2000);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = IngestionMetrics::new();
        metrics.record_received(100);
        metrics.record_write(80, 400, Duration::from_micros(500));
        metrics.record_rejected(10);
        metrics.record_dropped(10);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.points_received, 100);
        assert_eq!(snapshot.points_written, 80);
        assert_eq!(snapshot.points_rejected, 10);
        assert_eq!(snapshot.points_dropped, 10);
    }

    #[test]
    fn test_metrics_snapshot_rates() {
        let metrics = IngestionMetrics::new();
        metrics.record_received(100);
        metrics.record_write(80, 400, Duration::from_micros(500));
        metrics.record_rejected(10);

        let snapshot = metrics.snapshot();
        assert!((snapshot.write_success_rate() - 0.8).abs() < 0.001);
        assert!((snapshot.rejection_rate() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_metrics_backpressure() {
        let metrics = IngestionMetrics::new();
        metrics.record_backpressure_activated();
        metrics.record_backpressure_deactivated();
        metrics.record_backpressure_activated();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.backpressure_activations, 2);
        assert_eq!(snapshot.backpressure_deactivations, 1);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = IngestionMetrics::new();
        metrics.record_received(100);
        metrics.record_write(80, 400, Duration::from_micros(500));

        metrics.reset();

        assert_eq!(metrics.points_received(), 0);
        assert_eq!(metrics.points_written(), 0);
    }
}
