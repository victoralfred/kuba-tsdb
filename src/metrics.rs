///! Metrics and telemetry for Gorilla TSDB
///!
///! This module provides Prometheus metrics for monitoring system performance,
///! resource usage, errors, and data integrity.

use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    CounterVec, Gauge, GaugeVec, HistogramVec, TextEncoder, Encoder,
};

lazy_static! {
    // === Performance Counters ===

    /// Total write operations
    pub static ref WRITES_TOTAL: CounterVec = register_counter_vec!(
        "tsdb_writes_total",
        "Total write operations",
        &["series_id", "status"]
    ).unwrap();

    /// Total read operations
    pub static ref READS_TOTAL: CounterVec = register_counter_vec!(
        "tsdb_reads_total",
        "Total read operations",
        &["series_id", "operation"]
    ).unwrap();

    /// Total seal operations
    pub static ref SEALS_TOTAL: CounterVec = register_counter_vec!(
        "tsdb_seals_total",
        "Total seal operations",
        &["status"]
    ).unwrap();

    /// Total decompressions
    pub static ref DECOMPRESSIONS_TOTAL: CounterVec = register_counter_vec!(
        "tsdb_decompressions_total",
        "Total decompress operations",
        &["status"]
    ).unwrap();

    // === Latency Histograms ===

    /// Write operation duration
    pub static ref WRITE_DURATION: HistogramVec = register_histogram_vec!(
        "tsdb_write_duration_seconds",
        "Write operation latency in seconds",
        &["series_id"],
        vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0]
    ).unwrap();

    /// Read operation duration
    pub static ref READ_DURATION: HistogramVec = register_histogram_vec!(
        "tsdb_read_duration_seconds",
        "Read operation latency in seconds",
        &["operation"],
        vec![0.001, 0.01, 0.1, 0.5, 1.0]
    ).unwrap();

    /// Seal operation duration
    pub static ref SEAL_DURATION: HistogramVec = register_histogram_vec!(
        "tsdb_seal_duration_seconds",
        "Seal operation latency in seconds",
        &["status"],
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
    ).unwrap();

    /// Lock wait duration
    pub static ref LOCK_WAIT_DURATION: HistogramVec = register_histogram_vec!(
        "tsdb_lock_wait_seconds",
        "Time spent waiting for locks",
        &["lock_type"],
        vec![0.0001, 0.001, 0.01, 0.1, 1.0]
    ).unwrap();

    // === Resource Gauges ===

    /// Active chunks count
    pub static ref ACTIVE_CHUNKS: Gauge = register_gauge!(
        "tsdb_active_chunks",
        "Number of active (in-memory) chunks"
    ).unwrap();

    /// Sealed chunks count
    pub static ref SEALED_CHUNKS: Gauge = register_gauge!(
        "tsdb_sealed_chunks",
        "Number of sealed (on-disk) chunks"
    ).unwrap();

    /// Memory usage by type
    pub static ref MEMORY_BYTES: GaugeVec = register_gauge_vec!(
        "tsdb_memory_bytes",
        "Memory usage in bytes by type",
        &["type"]
    ).unwrap();

    /// Disk usage by type
    pub static ref DISK_BYTES: GaugeVec = register_gauge_vec!(
        "tsdb_disk_bytes",
        "Disk usage in bytes by type",
        &["type"]
    ).unwrap();

    /// Open file descriptors
    pub static ref OPEN_FILES: Gauge = register_gauge!(
        "tsdb_open_files",
        "Number of open file descriptors"
    ).unwrap();

    /// Chunk state distribution
    pub static ref CHUNKS_BY_STATE: GaugeVec = register_gauge_vec!(
        "tsdb_chunks_by_state",
        "Number of chunks by state",
        &["state"]
    ).unwrap();

    // === Error Counters ===

    /// Total errors by type
    pub static ref ERRORS_TOTAL: CounterVec = register_counter_vec!(
        "tsdb_errors_total",
        "Total errors by type and operation",
        &["error_type", "operation"]
    ).unwrap();

    /// Seal failures
    pub static ref SEAL_FAILURES: CounterVec = register_counter_vec!(
        "tsdb_seal_failures_total",
        "Total seal failures by reason",
        &["reason"]
    ).unwrap();

    /// Duplicate timestamps
    pub static ref DUPLICATE_TIMESTAMPS: CounterVec = register_counter_vec!(
        "tsdb_duplicate_timestamps_total",
        "Total duplicate timestamp rejections",
        &["series_id"]
    ).unwrap();

    // === Data Integrity Counters ===

    /// Checksum failures
    pub static ref CHECKSUM_FAILURES: CounterVec = register_counter_vec!(
        "tsdb_checksum_failures_total",
        "Total checksum validation failures",
        &["chunk_id"]
    ).unwrap();

    /// Series validation failures
    pub static ref SERIES_VALIDATION_FAILURES: CounterVec = register_counter_vec!(
        "tsdb_series_validation_failures_total",
        "Total series ID validation failures",
        &["series_id"]
    ).unwrap();

    /// Overflow events
    pub static ref OVERFLOW_EVENTS: CounterVec = register_counter_vec!(
        "tsdb_overflow_events_total",
        "Total overflow events by type",
        &["type"]
    ).unwrap();

    // === System Health ===

    /// System uptime
    pub static ref UPTIME_SECONDS: Gauge = register_gauge!(
        "tsdb_uptime_seconds",
        "System uptime in seconds"
    ).unwrap();

    /// Health status (0=unhealthy, 1=healthy)
    pub static ref HEALTH_STATUS: Gauge = register_gauge!(
        "tsdb_health_status",
        "System health status (0=unhealthy, 1=healthy)"
    ).unwrap();

    /// Points per second (write throughput)
    pub static ref POINTS_PER_SECOND: Gauge = register_gauge!(
        "tsdb_points_per_second",
        "Current write throughput in points/second"
    ).unwrap();
}

/// Initialize metrics system
pub fn init() {
    // Set initial health status
    HEALTH_STATUS.set(1.0);

    // Initialize uptime
    UPTIME_SECONDS.set(0.0);

    tracing::info!("Metrics system initialized");
}

/// Get metrics in Prometheus text format
///
/// # Returns
///
/// Result containing the formatted metrics string, or an error if encoding fails
pub fn gather_metrics() -> Result<String, String> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    encoder.encode(&metric_families, &mut buffer)
        .map_err(|e| format!("Failed to encode metrics: {}", e))?;

    String::from_utf8(buffer)
        .map_err(|e| format!("Metrics contain invalid UTF-8: {}", e))
}

/// Record a write operation
#[inline]
pub fn record_write(series_id: u128, duration_secs: f64, success: bool) {
    let status = if success { "success" } else { "error" };

    WRITES_TOTAL
        .with_label_values(&[&series_id.to_string(), status])
        .inc();

    WRITE_DURATION
        .with_label_values(&[&series_id.to_string()])
        .observe(duration_secs);
}

/// Record a seal operation
#[inline]
pub fn record_seal(duration_secs: f64, success: bool) {
    let status = if success { "success" } else { "error" };

    SEALS_TOTAL
        .with_label_values(&[status])
        .inc();

    SEAL_DURATION
        .with_label_values(&[status])
        .observe(duration_secs);
}

/// Record an error
#[inline]
pub fn record_error(error_type: &str, operation: &str) {
    ERRORS_TOTAL
        .with_label_values(&[error_type, operation])
        .inc();
}

/// Record duplicate timestamp
#[inline]
pub fn record_duplicate_timestamp(series_id: u128) {
    DUPLICATE_TIMESTAMPS
        .with_label_values(&[&series_id.to_string()])
        .inc();
}

/// Update active chunks count
#[inline]
pub fn update_active_chunks(count: usize) {
    ACTIVE_CHUNKS.set(count as f64);
}

/// Update sealed chunks count
#[inline]
pub fn update_sealed_chunks(count: usize) {
    SEALED_CHUNKS.set(count as f64);
}

/// Update memory usage
#[inline]
pub fn update_memory(memory_type: &str, bytes: usize) {
    MEMORY_BYTES
        .with_label_values(&[memory_type])
        .set(bytes as f64);
}

/// Update uptime
#[inline]
pub fn update_uptime(seconds: f64) {
    UPTIME_SECONDS.set(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        init();
        assert_eq!(HEALTH_STATUS.get(), 1.0);
    }

    #[test]
    fn test_record_write() {
        record_write(1, 0.001, true);
        let metrics = gather_metrics().expect("Failed to gather metrics");
        assert!(metrics.contains("tsdb_writes_total"));
    }

    #[test]
    fn test_gather_metrics() {
        init(); // Initialize metrics first
        let metrics = gather_metrics().expect("Failed to gather metrics");
        assert!(metrics.contains("tsdb_health_status"));
        assert!(metrics.contains("tsdb_uptime_seconds"));
    }
}
