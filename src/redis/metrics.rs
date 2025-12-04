//! Redis metrics and monitoring
//!
//! Provides comprehensive metrics for Redis operations including:
//! - Connection pool statistics
//! - Operation latencies
//! - Error rates
//! - Cache hit rates
//! - Script execution times
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::metrics::{RedisMetrics, MetricsConfig};
//!
//! let metrics = RedisMetrics::new(MetricsConfig::default());
//!
//! // Record an operation
//! metrics.record_operation("query", 150, true);
//!
//! // Get statistics
//! let stats = metrics.stats();
//! println!("Total operations: {}", stats.total_operations);
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for metrics collection
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    /// Number of buckets for latency histogram
    pub histogram_buckets: usize,

    /// Maximum latency to track in microseconds
    pub max_latency_us: u64,

    /// Enable detailed per-operation metrics
    pub detailed_metrics: bool,

    /// Sliding window size for rate calculations
    pub rate_window_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            histogram_buckets: 20,
            max_latency_us: 1_000_000, // 1 second
            detailed_metrics: true,
            rate_window_secs: 60,
        }
    }
}

/// Latency histogram for tracking operation times
struct LatencyHistogram {
    /// Bucket boundaries in microseconds
    boundaries: Vec<u64>,
    /// Count per bucket
    buckets: Vec<AtomicU64>,
    /// Total sum of latencies
    sum: AtomicU64,
    /// Total count
    count: AtomicU64,
    /// Min latency seen
    min: AtomicU64,
    /// Max latency seen
    max: AtomicU64,
}

impl LatencyHistogram {
    fn new(num_buckets: usize, max_latency_us: u64) -> Self {
        // Create exponential bucket boundaries
        let mut boundaries = Vec::with_capacity(num_buckets);
        let base = (max_latency_us as f64).powf(1.0 / num_buckets as f64);

        for i in 0..num_buckets {
            boundaries.push(base.powf(i as f64) as u64);
        }

        let buckets: Vec<_> = (0..=num_buckets).map(|_| AtomicU64::new(0)).collect();

        Self {
            boundaries,
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    fn record(&self, latency_us: u64) {
        // Find the bucket
        let bucket_idx = self
            .boundaries
            .iter()
            .position(|&b| latency_us <= b)
            .unwrap_or(self.boundaries.len());

        // SEC-006: Use saturating arithmetic to prevent overflow
        // This caps values at u64::MAX instead of wrapping
        let _ = self.buckets[bucket_idx].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_add(1))
        });
        let _ = self
            .sum
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(latency_us))
            });
        let _ = self
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            });

        // Update min/max
        self.min.fetch_min(latency_us, Ordering::Relaxed);
        self.max.fetch_max(latency_us, Ordering::Relaxed);
    }

    fn percentile(&self, p: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }

        let target = (total as f64 * p) as u64;
        let mut cumulative = 0u64;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return if i < self.boundaries.len() {
                    self.boundaries[i]
                } else {
                    self.max.load(Ordering::Relaxed)
                };
            }
        }

        self.max.load(Ordering::Relaxed)
    }

    fn average(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.sum.load(Ordering::Relaxed) as f64 / count as f64
    }

    /// Reset the histogram to initial state
    ///
    /// Used for periodic metrics cleanup or when starting a new monitoring window.
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.min.store(u64::MAX, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

/// Per-operation metrics
struct OperationMetrics {
    /// Total operations
    count: AtomicU64,
    /// Successful operations
    successes: AtomicU64,
    /// Failed operations
    failures: AtomicU64,
    /// Latency histogram
    latency: LatencyHistogram,
}

impl OperationMetrics {
    fn new(config: &MetricsConfig) -> Self {
        Self {
            count: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            latency: LatencyHistogram::new(config.histogram_buckets, config.max_latency_us),
        }
    }

    fn record(&self, latency_us: u64, success: bool) {
        // SEC-006: Use saturating arithmetic to prevent overflow
        let _ = self
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            });
        if success {
            let _ = self
                .successes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
        } else {
            let _ = self
                .failures
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
        }
        self.latency.record(latency_us);
    }

    fn success_rate(&self) -> f64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        self.successes.load(Ordering::Relaxed) as f64 / total as f64
    }

    fn stats(&self) -> OperationStats {
        let count = self.count.load(Ordering::Relaxed);
        let successes = self.successes.load(Ordering::Relaxed);
        let failures = self.failures.load(Ordering::Relaxed);

        OperationStats {
            count,
            successes,
            failures,
            success_rate: self.success_rate(),
            avg_latency_us: self.latency.average(),
            p50_latency_us: self.latency.percentile(0.5),
            p95_latency_us: self.latency.percentile(0.95),
            p99_latency_us: self.latency.percentile(0.99),
            min_latency_us: self.latency.min.load(Ordering::Relaxed),
            max_latency_us: self.latency.max.load(Ordering::Relaxed),
        }
    }
}

/// Statistics for an operation type
#[derive(Debug, Clone)]
pub struct OperationStats {
    /// Total operations
    pub count: u64,
    /// Successful operations
    pub successes: u64,
    /// Failed operations
    pub failures: u64,
    /// Success rate (0.0 - 1.0)
    pub success_rate: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// 50th percentile latency
    pub p50_latency_us: u64,
    /// 95th percentile latency
    pub p95_latency_us: u64,
    /// 99th percentile latency
    pub p99_latency_us: u64,
    /// Minimum latency seen
    pub min_latency_us: u64,
    /// Maximum latency seen
    pub max_latency_us: u64,
}

/// Rate window for tracking operations per second
struct RateWindow {
    /// Window entries: (timestamp_secs, count)
    entries: RwLock<Vec<(u64, u64)>>,
    /// Window size in seconds
    window_secs: u64,
}

impl RateWindow {
    fn new(window_secs: u64) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            window_secs,
        }
    }

    fn record(&self, count: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut entries = self.entries.write();

        // Remove old entries
        let cutoff = now.saturating_sub(self.window_secs);
        entries.retain(|(ts, _)| *ts > cutoff);

        // Add new entry or update existing
        if let Some((ts, c)) = entries.last_mut() {
            if *ts == now {
                *c += count;
                return;
            }
        }

        entries.push((now, count));
    }

    fn rate(&self) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entries = self.entries.read();
        let cutoff = now.saturating_sub(self.window_secs);

        let total: u64 = entries
            .iter()
            .filter(|(ts, _)| *ts > cutoff)
            .map(|(_, c)| c)
            .sum();

        total as f64 / self.window_secs as f64
    }
}

/// Redis metrics collector
///
/// Collects and aggregates metrics for all Redis operations.
pub struct RedisMetrics {
    /// Configuration
    config: MetricsConfig,

    /// Per-operation metrics
    operations: RwLock<HashMap<String, OperationMetrics>>,

    /// Global operation counter
    total_operations: AtomicU64,

    /// Global error counter
    total_errors: AtomicU64,

    /// Connection metrics
    connections_created: AtomicU64,
    connections_failed: AtomicU64,
    connections_active: AtomicU64,

    /// Cache metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,

    /// Script execution metrics
    script_executions: AtomicU64,
    script_errors: AtomicU64,

    /// Rate tracking
    operation_rate: RateWindow,
    error_rate: RateWindow,

    /// Start time for uptime calculation
    start_time: Instant,
}

impl RedisMetrics {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            operation_rate: RateWindow::new(config.rate_window_secs),
            error_rate: RateWindow::new(config.rate_window_secs),
            config,
            operations: RwLock::new(HashMap::new()),
            total_operations: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            connections_created: AtomicU64::new(0),
            connections_failed: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            script_executions: AtomicU64::new(0),
            script_errors: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Record an operation
    ///
    /// # Arguments
    ///
    /// * `operation` - Name of the operation (e.g., "query", "insert")
    /// * `latency_us` - Latency in microseconds
    /// * `success` - Whether the operation succeeded
    pub fn record_operation(&self, operation: &str, latency_us: u64, success: bool) {
        // SEC-006: Use saturating arithmetic to prevent overflow
        let _ = self
            .total_operations
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            });
        self.operation_rate.record(1);

        if !success {
            let _ = self
                .total_errors
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
            self.error_rate.record(1);
        }

        if self.config.detailed_metrics {
            let mut ops = self.operations.write();
            let metrics = ops
                .entry(operation.to_string())
                .or_insert_with(|| OperationMetrics::new(&self.config));
            metrics.record(latency_us, success);
        }
    }

    /// Record a connection event
    pub fn record_connection(&self, success: bool) {
        // SEC-006: Use saturating arithmetic to prevent overflow
        if success {
            let _ =
                self.connections_created
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_add(1))
                    });
            let _ =
                self.connections_active
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_add(1))
                    });
        } else {
            let _ =
                self.connections_failed
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_add(1))
                    });
        }
    }

    /// Record a connection being released
    pub fn record_connection_released(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a cache access
    pub fn record_cache_access(&self, hit: bool) {
        // SEC-006: Use saturating arithmetic to prevent overflow
        if hit {
            let _ = self
                .cache_hits
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
        } else {
            let _ = self
                .cache_misses
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
        }
    }

    /// Record a script execution
    pub fn record_script_execution(&self, success: bool) {
        // SEC-006: Use saturating arithmetic to prevent overflow
        let _ = self
            .script_executions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            });
        if !success {
            let _ = self
                .script_errors
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_add(1))
                });
        }
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;

        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }

    /// Get error rate
    pub fn error_rate(&self) -> f64 {
        let total = self.total_operations.load(Ordering::Relaxed) as f64;
        let errors = self.total_errors.load(Ordering::Relaxed) as f64;

        if total == 0.0 {
            0.0
        } else {
            errors / total
        }
    }

    /// Get operations per second
    pub fn ops_per_second(&self) -> f64 {
        self.operation_rate.rate()
    }

    /// Get errors per second
    pub fn errors_per_second(&self) -> f64 {
        self.error_rate.rate()
    }

    /// Get statistics for a specific operation
    pub fn operation_stats(&self, operation: &str) -> Option<OperationStats> {
        let ops = self.operations.read();
        ops.get(operation).map(|m| m.stats())
    }

    /// Get all operation statistics
    pub fn all_operation_stats(&self) -> HashMap<String, OperationStats> {
        let ops = self.operations.read();
        ops.iter().map(|(k, v)| (k.clone(), v.stats())).collect()
    }

    /// Get overall statistics
    pub fn stats(&self) -> RedisMetricsStats {
        RedisMetricsStats {
            total_operations: self.total_operations.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            error_rate: self.error_rate(),
            ops_per_second: self.ops_per_second(),
            errors_per_second: self.errors_per_second(),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_failed: self.connections_failed.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_hit_rate: self.cache_hit_rate(),
            script_executions: self.script_executions.load(Ordering::Relaxed),
            script_errors: self.script_errors.load(Ordering::Relaxed),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_operations.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        self.connections_created.store(0, Ordering::Relaxed);
        self.connections_failed.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.script_executions.store(0, Ordering::Relaxed);
        self.script_errors.store(0, Ordering::Relaxed);

        let mut ops = self.operations.write();
        ops.clear();
    }

    /// Reset only latency histograms for rolling window metrics
    ///
    /// This resets the latency distribution buckets while keeping operation
    /// counts intact. Useful for periodic histogram snapshots in monitoring
    /// systems that want fresh histogram data for each scrape interval.
    pub fn reset_histograms(&self) {
        let ops = self.operations.read();
        for metrics in ops.values() {
            metrics.latency.reset();
        }
    }
}

impl Default for RedisMetrics {
    fn default() -> Self {
        Self::new(MetricsConfig::default())
    }
}

/// Overall Redis metrics statistics
#[derive(Debug, Clone)]
pub struct RedisMetricsStats {
    /// Total operations performed
    pub total_operations: u64,
    /// Total errors encountered
    pub total_errors: u64,
    /// Overall error rate
    pub error_rate: f64,
    /// Operations per second
    pub ops_per_second: f64,
    /// Errors per second
    pub errors_per_second: f64,
    /// Total connections created
    pub connections_created: u64,
    /// Total connection failures
    pub connections_failed: u64,
    /// Currently active connections
    pub connections_active: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Script executions
    pub script_executions: u64,
    /// Script errors
    pub script_errors: u64,
    /// Uptime in seconds
    pub uptime_secs: u64,
}

/// Helper to time operations
pub struct OperationTimer {
    start: Instant,
    operation: String,
    metrics: Option<std::sync::Arc<RedisMetrics>>,
}

impl OperationTimer {
    /// Start timing an operation
    pub fn start(operation: impl Into<String>, metrics: std::sync::Arc<RedisMetrics>) -> Self {
        Self {
            start: Instant::now(),
            operation: operation.into(),
            metrics: Some(metrics),
        }
    }

    /// Complete the operation successfully
    pub fn success(mut self) {
        if let Some(metrics) = self.metrics.take() {
            let latency = self.start.elapsed().as_micros() as u64;
            metrics.record_operation(&self.operation, latency, true);
        }
    }

    /// Complete the operation with failure
    pub fn failure(mut self) {
        if let Some(metrics) = self.metrics.take() {
            let latency = self.start.elapsed().as_micros() as u64;
            metrics.record_operation(&self.operation, latency, false);
        }
    }

    /// Get elapsed time without recording
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert_eq!(config.histogram_buckets, 20);
        assert!(config.detailed_metrics);
    }

    #[test]
    fn test_record_operation() {
        let metrics = RedisMetrics::new(MetricsConfig::default());

        metrics.record_operation("test", 100, true);
        metrics.record_operation("test", 200, false);

        assert_eq!(metrics.total_operations.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.total_errors.load(Ordering::Relaxed), 1);

        let stats = metrics.operation_stats("test").unwrap();
        assert_eq!(stats.count, 2);
        assert_eq!(stats.successes, 1);
        assert_eq!(stats.failures, 1);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = RedisMetrics::new(MetricsConfig::default());

        metrics.record_cache_access(true);
        metrics.record_cache_access(true);
        metrics.record_cache_access(false);

        let rate = metrics.cache_hit_rate();
        assert!((rate - 0.666666).abs() < 0.01);
    }

    #[test]
    fn test_error_rate() {
        let metrics = RedisMetrics::new(MetricsConfig::default());

        metrics.record_operation("test", 100, true);
        metrics.record_operation("test", 100, true);
        metrics.record_operation("test", 100, false);

        let rate = metrics.error_rate();
        assert!((rate - 0.333333).abs() < 0.01);
    }

    #[test]
    fn test_latency_histogram() {
        let histogram = LatencyHistogram::new(10, 1_000_000);

        histogram.record(100);
        histogram.record(200);
        histogram.record(300);

        assert_eq!(histogram.count.load(Ordering::Relaxed), 3);
        assert!((histogram.average() - 200.0).abs() < 0.01);
        assert_eq!(histogram.min.load(Ordering::Relaxed), 100);
        assert_eq!(histogram.max.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = RedisMetrics::new(MetricsConfig::default());

        metrics.record_operation("test", 100, true);
        metrics.record_cache_access(true);

        assert_eq!(metrics.total_operations.load(Ordering::Relaxed), 1);

        metrics.reset();

        assert_eq!(metrics.total_operations.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_overall_stats() {
        let metrics = RedisMetrics::new(MetricsConfig::default());

        metrics.record_operation("query", 100, true);
        metrics.record_connection(true);
        metrics.record_cache_access(true);
        metrics.record_script_execution(true);

        let stats = metrics.stats();

        assert_eq!(stats.total_operations, 1);
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.script_executions, 1);
    }
}
