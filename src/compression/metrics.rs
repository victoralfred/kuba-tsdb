//! Compression Metrics and Observability
//!
//! Provides detailed metrics about compression performance for monitoring
//! and optimization purposes.
//!
//! # Metrics Collected
//!
//! - Compression ratio per codec type
//! - Encoding/decoding latency histograms
//! - Bits per sample achieved
//! - Codec selection frequency
//! - Error rates
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::metrics::{CompressionMetrics, CodecType};
//!
//! let metrics = CompressionMetrics::new();
//!
//! // Record a compression operation
//! metrics.record_compression(CodecType::Ahpac, 16000, 2000, 125);
//!
//! // Get current stats
//! let stats = metrics.snapshot();
//! println!("Overall compression ratio: {:.2}x", stats.overall_compression_ratio);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Compression codec types for metric tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecType {
    /// Kuba/Gorilla XOR compression
    Kuba,
    /// AHPAC adaptive compression
    Ahpac,
    /// ANS entropy coding
    Ans,
    /// Chimp compression
    Chimp,
    /// ALP (Adaptive Lossless Floating-Point)
    Alp,
    /// Delta + LZ4
    DeltaLz4,
    /// Snappy
    Snappy,
    /// No compression (raw)
    None,
}

impl CodecType {
    /// Get codec name as string
    pub fn name(&self) -> &'static str {
        match self {
            CodecType::Kuba => "kuba",
            CodecType::Ahpac => "ahpac",
            CodecType::Ans => "ans",
            CodecType::Chimp => "chimp",
            CodecType::Alp => "alp",
            CodecType::DeltaLz4 => "delta_lz4",
            CodecType::Snappy => "snappy",
            CodecType::None => "none",
        }
    }

    /// Get all codec types
    pub fn all() -> &'static [CodecType] {
        &[
            CodecType::Kuba,
            CodecType::Ahpac,
            CodecType::Ans,
            CodecType::Chimp,
            CodecType::Alp,
            CodecType::DeltaLz4,
            CodecType::Snappy,
            CodecType::None,
        ]
    }
}

/// Atomic counters for a single codec's metrics
#[derive(Debug, Default)]
pub struct CodecMetrics {
    /// Total compression operations
    pub compress_count: AtomicU64,
    /// Total decompression operations
    pub decompress_count: AtomicU64,
    /// Total bytes before compression
    pub original_bytes: AtomicU64,
    /// Total bytes after compression
    pub compressed_bytes: AtomicU64,
    /// Total compression time in microseconds
    pub compress_time_us: AtomicU64,
    /// Total decompression time in microseconds
    pub decompress_time_us: AtomicU64,
    /// Total points compressed
    pub points_compressed: AtomicU64,
    /// Total compression errors
    pub compress_errors: AtomicU64,
    /// Total decompression errors
    pub decompress_errors: AtomicU64,
}

impl CodecMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful compression operation
    pub fn record_compression(
        &self,
        original_size: u64,
        compressed_size: u64,
        duration_us: u64,
        point_count: u64,
    ) {
        self.compress_count.fetch_add(1, Ordering::Relaxed);
        self.original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);
        self.compress_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.points_compressed
            .fetch_add(point_count, Ordering::Relaxed);
    }

    /// Record a successful decompression operation
    pub fn record_decompression(&self, compressed_size: u64, original_size: u64, duration_us: u64) {
        self.decompress_count.fetch_add(1, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);
        self.original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.decompress_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a compression error
    pub fn record_compress_error(&self) {
        self.compress_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decompression error
    pub fn record_decompress_error(&self) {
        self.decompress_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        let compressed = self.compressed_bytes.load(Ordering::Relaxed);
        let original = self.original_bytes.load(Ordering::Relaxed);
        if compressed == 0 {
            0.0
        } else {
            original as f64 / compressed as f64
        }
    }

    /// Get bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        let compressed = self.compressed_bytes.load(Ordering::Relaxed);
        let points = self.points_compressed.load(Ordering::Relaxed);
        if points == 0 {
            0.0
        } else {
            (compressed * 8) as f64 / points as f64
        }
    }

    /// Get average compression time in microseconds
    pub fn avg_compress_time_us(&self) -> f64 {
        let count = self.compress_count.load(Ordering::Relaxed);
        let time = self.compress_time_us.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            time as f64 / count as f64
        }
    }

    /// Get average decompression time in microseconds
    pub fn avg_decompress_time_us(&self) -> f64 {
        let count = self.decompress_count.load(Ordering::Relaxed);
        let time = self.decompress_time_us.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            time as f64 / count as f64
        }
    }

    /// Create a snapshot of current metrics
    pub fn snapshot(&self) -> CodecMetricsSnapshot {
        CodecMetricsSnapshot {
            compress_count: self.compress_count.load(Ordering::Relaxed),
            decompress_count: self.decompress_count.load(Ordering::Relaxed),
            original_bytes: self.original_bytes.load(Ordering::Relaxed),
            compressed_bytes: self.compressed_bytes.load(Ordering::Relaxed),
            compress_time_us: self.compress_time_us.load(Ordering::Relaxed),
            decompress_time_us: self.decompress_time_us.load(Ordering::Relaxed),
            points_compressed: self.points_compressed.load(Ordering::Relaxed),
            compress_errors: self.compress_errors.load(Ordering::Relaxed),
            decompress_errors: self.decompress_errors.load(Ordering::Relaxed),
        }
    }
}

/// Non-atomic snapshot of codec metrics for serialization
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CodecMetricsSnapshot {
    /// Total compression operations
    pub compress_count: u64,
    /// Total decompression operations
    pub decompress_count: u64,
    /// Total bytes before compression
    pub original_bytes: u64,
    /// Total bytes after compression
    pub compressed_bytes: u64,
    /// Total compression time in microseconds
    pub compress_time_us: u64,
    /// Total decompression time in microseconds
    pub decompress_time_us: u64,
    /// Total points compressed
    pub points_compressed: u64,
    /// Total compression errors
    pub compress_errors: u64,
    /// Total decompression errors
    pub decompress_errors: u64,
}

impl CodecMetricsSnapshot {
    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            0.0
        } else {
            self.original_bytes as f64 / self.compressed_bytes as f64
        }
    }

    /// Get bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        if self.points_compressed == 0 {
            0.0
        } else {
            (self.compressed_bytes * 8) as f64 / self.points_compressed as f64
        }
    }

    /// Get average compression time in microseconds
    pub fn avg_compress_time_us(&self) -> f64 {
        if self.compress_count == 0 {
            0.0
        } else {
            self.compress_time_us as f64 / self.compress_count as f64
        }
    }

    /// Get average decompression time in microseconds
    pub fn avg_decompress_time_us(&self) -> f64 {
        if self.decompress_count == 0 {
            0.0
        } else {
            self.decompress_time_us as f64 / self.decompress_count as f64
        }
    }
}

/// Global compression metrics collector
pub struct CompressionMetrics {
    /// Metrics per codec type
    pub kuba: CodecMetrics,
    /// AHPAC metrics
    pub ahpac: CodecMetrics,
    /// ANS metrics
    pub ans: CodecMetrics,
    /// Chimp metrics
    pub chimp: CodecMetrics,
    /// ALP metrics
    pub alp: CodecMetrics,
    /// Delta+LZ4 metrics
    pub delta_lz4: CodecMetrics,
    /// Snappy metrics
    pub snappy: CodecMetrics,
    /// No compression metrics
    pub none: CodecMetrics,
    /// Creation timestamp
    created_at: Instant,
}

impl CompressionMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            kuba: CodecMetrics::new(),
            ahpac: CodecMetrics::new(),
            ans: CodecMetrics::new(),
            chimp: CodecMetrics::new(),
            alp: CodecMetrics::new(),
            delta_lz4: CodecMetrics::new(),
            snappy: CodecMetrics::new(),
            none: CodecMetrics::new(),
            created_at: Instant::now(),
        }
    }

    /// Get metrics for a specific codec type
    pub fn get(&self, codec: CodecType) -> &CodecMetrics {
        match codec {
            CodecType::Kuba => &self.kuba,
            CodecType::Ahpac => &self.ahpac,
            CodecType::Ans => &self.ans,
            CodecType::Chimp => &self.chimp,
            CodecType::Alp => &self.alp,
            CodecType::DeltaLz4 => &self.delta_lz4,
            CodecType::Snappy => &self.snappy,
            CodecType::None => &self.none,
        }
    }

    /// Record a compression operation
    pub fn record_compression(
        &self,
        codec: CodecType,
        original_size: u64,
        compressed_size: u64,
        duration_us: u64,
        point_count: u64,
    ) {
        self.get(codec).record_compression(
            original_size,
            compressed_size,
            duration_us,
            point_count,
        );
    }

    /// Record a decompression operation
    pub fn record_decompression(
        &self,
        codec: CodecType,
        compressed_size: u64,
        original_size: u64,
        duration_us: u64,
    ) {
        self.get(codec)
            .record_decompression(compressed_size, original_size, duration_us);
    }

    /// Record a compression error
    pub fn record_compress_error(&self, codec: CodecType) {
        self.get(codec).record_compress_error();
    }

    /// Record a decompression error
    pub fn record_decompress_error(&self, codec: CodecType) {
        self.get(codec).record_decompress_error();
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Create a full snapshot of all metrics
    pub fn snapshot(&self) -> CompressionMetricsSnapshot {
        let mut per_codec = std::collections::HashMap::new();
        for codec in CodecType::all() {
            per_codec.insert(codec.name().to_string(), self.get(*codec).snapshot());
        }

        // Calculate totals
        let mut total = CodecMetricsSnapshot::default();
        for snapshot in per_codec.values() {
            total.compress_count += snapshot.compress_count;
            total.decompress_count += snapshot.decompress_count;
            total.original_bytes += snapshot.original_bytes;
            total.compressed_bytes += snapshot.compressed_bytes;
            total.compress_time_us += snapshot.compress_time_us;
            total.decompress_time_us += snapshot.decompress_time_us;
            total.points_compressed += snapshot.points_compressed;
            total.compress_errors += snapshot.compress_errors;
            total.decompress_errors += snapshot.decompress_errors;
        }

        let overall_compression_ratio = total.compression_ratio();
        let overall_bits_per_sample = total.bits_per_sample();

        CompressionMetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            per_codec,
            total,
            overall_compression_ratio,
            overall_bits_per_sample,
        }
    }

    /// Format as Prometheus metrics
    pub fn to_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = String::new();

        // Total compression ratio
        output.push_str("# HELP compression_ratio Overall compression ratio\n");
        output.push_str("# TYPE compression_ratio gauge\n");
        output.push_str(&format!(
            "compression_ratio {:.4}\n",
            snapshot.overall_compression_ratio
        ));

        // Bits per sample
        output.push_str("# HELP compression_bits_per_sample Average bits per sample\n");
        output.push_str("# TYPE compression_bits_per_sample gauge\n");
        output.push_str(&format!(
            "compression_bits_per_sample {:.4}\n",
            snapshot.overall_bits_per_sample
        ));

        // Per-codec metrics
        output.push_str(
            "# HELP compression_operations_total Total compression operations by codec\n",
        );
        output.push_str("# TYPE compression_operations_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.compress_count > 0 {
                output.push_str(&format!(
                    "compression_operations_total{{codec=\"{}\",operation=\"compress\"}} {}\n",
                    codec, metrics.compress_count
                ));
            }
            if metrics.decompress_count > 0 {
                output.push_str(&format!(
                    "compression_operations_total{{codec=\"{}\",operation=\"decompress\"}} {}\n",
                    codec, metrics.decompress_count
                ));
            }
        }

        // Bytes processed
        output.push_str("# HELP compression_bytes_total Total bytes processed by codec\n");
        output.push_str("# TYPE compression_bytes_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.original_bytes > 0 {
                output.push_str(&format!(
                    "compression_bytes_total{{codec=\"{}\",type=\"original\"}} {}\n",
                    codec, metrics.original_bytes
                ));
                output.push_str(&format!(
                    "compression_bytes_total{{codec=\"{}\",type=\"compressed\"}} {}\n",
                    codec, metrics.compressed_bytes
                ));
            }
        }

        // Errors
        output.push_str("# HELP compression_errors_total Total compression errors by codec\n");
        output.push_str("# TYPE compression_errors_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.compress_errors > 0 || metrics.decompress_errors > 0 {
                output.push_str(&format!(
                    "compression_errors_total{{codec=\"{}\",operation=\"compress\"}} {}\n",
                    codec, metrics.compress_errors
                ));
                output.push_str(&format!(
                    "compression_errors_total{{codec=\"{}\",operation=\"decompress\"}} {}\n",
                    codec, metrics.decompress_errors
                ));
            }
        }

        output
    }
}

impl Default for CompressionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Full snapshot of compression metrics for serialization
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompressionMetricsSnapshot {
    /// Uptime in seconds
    pub uptime_secs: u64,
    /// Per-codec metrics
    pub per_codec: std::collections::HashMap<String, CodecMetricsSnapshot>,
    /// Total metrics across all codecs
    pub total: CodecMetricsSnapshot,
    /// Overall compression ratio
    pub overall_compression_ratio: f64,
    /// Overall bits per sample
    pub overall_bits_per_sample: f64,
}

/// RAII guard for timing compression operations
pub struct CompressionTimer {
    start: Instant,
    codec: CodecType,
    original_size: u64,
    point_count: u64,
    metrics: Arc<CompressionMetrics>,
    completed: bool,
}

impl CompressionTimer {
    /// Create a new timer for a compression operation
    pub fn new(
        metrics: Arc<CompressionMetrics>,
        codec: CodecType,
        original_size: u64,
        point_count: u64,
    ) -> Self {
        Self {
            start: Instant::now(),
            codec,
            original_size,
            point_count,
            metrics,
            completed: false,
        }
    }

    /// Complete the timer with the compressed size
    pub fn complete(mut self, compressed_size: u64) {
        let duration_us = self.start.elapsed().as_micros() as u64;
        self.metrics.record_compression(
            self.codec,
            self.original_size,
            compressed_size,
            duration_us,
            self.point_count,
        );
        self.completed = true;
    }

    /// Mark as failed
    pub fn fail(mut self) {
        self.metrics.record_compress_error(self.codec);
        self.completed = true;
    }
}

impl Drop for CompressionTimer {
    fn drop(&mut self) {
        if !self.completed {
            // If not completed, assume failure
            self.metrics.record_compress_error(self.codec);
        }
    }
}

/// Global metrics instance (lazy initialization)
static GLOBAL_METRICS: std::sync::OnceLock<Arc<CompressionMetrics>> = std::sync::OnceLock::new();

/// Get the global compression metrics instance
pub fn global_metrics() -> Arc<CompressionMetrics> {
    GLOBAL_METRICS
        .get_or_init(|| Arc::new(CompressionMetrics::new()))
        .clone()
}

// ============================================================================
// Sealing Metrics
// ============================================================================

/// Histogram bucket boundaries for seal duration (in seconds)
pub const SEAL_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Histogram bucket boundaries for compression ratio
pub const COMPRESSION_RATIO_BUCKETS: [f64; 10] =
    [1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, 20.0, 50.0];

/// Histogram for tracking value distributions
#[derive(Debug)]
pub struct Histogram {
    /// Bucket boundaries
    buckets: Vec<f64>,
    /// Counts per bucket (includes values <= bucket boundary)
    counts: Vec<AtomicU64>,
    /// Total sum of observed values
    sum: AtomicU64,
    /// Total count of observations
    count: AtomicU64,
}

impl Histogram {
    /// Create a new histogram with specified bucket boundaries
    pub fn new(buckets: &[f64]) -> Self {
        let bucket_count = buckets.len() + 1; // +1 for +Inf bucket
        let counts = (0..bucket_count).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets: buckets.to_vec(),
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record an observation
    pub fn observe(&self, value: f64) {
        // Update sum and count
        let value_bits = (value * 1_000_000.0) as u64; // Store as micros for precision
        self.sum.fetch_add(value_bits, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // Update bucket counts
        for (i, &boundary) in self.buckets.iter().enumerate() {
            if value <= boundary {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Value exceeds all buckets, goes to +Inf bucket
        self.counts[self.buckets.len()].fetch_add(1, Ordering::Relaxed);
    }

    /// Get histogram snapshot
    pub fn snapshot(&self) -> HistogramSnapshot {
        let bucket_counts: Vec<u64> = self
            .counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect();

        HistogramSnapshot {
            buckets: self.buckets.clone(),
            bucket_counts,
            sum: self.sum.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            count: self.count.load(Ordering::Relaxed),
        }
    }

    /// Reset histogram
    pub fn reset(&self) {
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        for count in &self.counts {
            count.store(0, Ordering::Relaxed);
        }
    }
}

/// Snapshot of histogram data
#[derive(Debug, Clone, serde::Serialize)]
pub struct HistogramSnapshot {
    /// Bucket boundaries
    pub buckets: Vec<f64>,
    /// Cumulative counts per bucket
    pub bucket_counts: Vec<u64>,
    /// Sum of all observations
    pub sum: f64,
    /// Total count of observations
    pub count: u64,
}

impl HistogramSnapshot {
    /// Get average value
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Estimate a percentile (approximation based on histogram)
    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 || self.bucket_counts.is_empty() {
            return 0.0;
        }

        let target = (self.count as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &count) in self.bucket_counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                if i < self.buckets.len() {
                    return self.buckets[i];
                } else {
                    // In +Inf bucket, return last bucket boundary
                    return *self.buckets.last().unwrap_or(&0.0);
                }
            }
        }

        *self.buckets.last().unwrap_or(&0.0)
    }
}

/// Sealing operation metrics for monitoring parallel sealing performance
#[derive(Debug)]
pub struct SealingMetrics {
    /// Duration histogram for seal operations
    pub seal_duration: Histogram,
    /// Compression ratio histogram
    pub compression_ratio: Histogram,
    /// Total successful seals
    pub seals_total_success: AtomicU64,
    /// Total failed seals
    pub seals_total_failed: AtomicU64,
    /// Current queue depth
    pub queue_depth: AtomicU64,
    /// Currently active seals
    pub active_seals: AtomicU64,
    /// Memory budget used in bytes
    pub memory_used_bytes: AtomicU64,
    /// Total points sealed
    pub points_sealed: AtomicU64,
    /// Total original bytes before compression
    pub original_bytes: AtomicU64,
    /// Total compressed bytes
    pub compressed_bytes: AtomicU64,
    /// Background seal check cycles
    pub background_checks: AtomicU64,
    /// Creation timestamp
    created_at: Instant,
}

impl SealingMetrics {
    /// Create new sealing metrics
    pub fn new() -> Self {
        Self {
            seal_duration: Histogram::new(&SEAL_DURATION_BUCKETS),
            compression_ratio: Histogram::new(&COMPRESSION_RATIO_BUCKETS),
            seals_total_success: AtomicU64::new(0),
            seals_total_failed: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            active_seals: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            points_sealed: AtomicU64::new(0),
            original_bytes: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            background_checks: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Record a successful seal operation
    pub fn record_seal_success(
        &self,
        duration_secs: f64,
        original_size: u64,
        compressed_size: u64,
        point_count: u64,
    ) {
        self.seals_total_success.fetch_add(1, Ordering::Relaxed);
        self.seal_duration.observe(duration_secs);
        self.points_sealed.fetch_add(point_count, Ordering::Relaxed);
        self.original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);

        // Record compression ratio
        if compressed_size > 0 {
            let ratio = original_size as f64 / compressed_size as f64;
            self.compression_ratio.observe(ratio);
        }
    }

    /// Record a failed seal operation
    pub fn record_seal_failure(&self, duration_secs: f64) {
        self.seals_total_failed.fetch_add(1, Ordering::Relaxed);
        self.seal_duration.observe(duration_secs);
    }

    /// Set current queue depth
    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Set current active seals
    pub fn set_active_seals(&self, active: u64) {
        self.active_seals.store(active, Ordering::Relaxed);
    }

    /// Set current memory usage
    pub fn set_memory_used(&self, bytes: u64) {
        self.memory_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Increment background check counter
    pub fn record_background_check(&self) {
        self.background_checks.fetch_add(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Create a snapshot of current metrics
    pub fn snapshot(&self) -> SealingMetricsSnapshot {
        SealingMetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            seal_duration: self.seal_duration.snapshot(),
            compression_ratio: self.compression_ratio.snapshot(),
            seals_total_success: self.seals_total_success.load(Ordering::Relaxed),
            seals_total_failed: self.seals_total_failed.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            active_seals: self.active_seals.load(Ordering::Relaxed),
            memory_used_bytes: self.memory_used_bytes.load(Ordering::Relaxed),
            points_sealed: self.points_sealed.load(Ordering::Relaxed),
            original_bytes: self.original_bytes.load(Ordering::Relaxed),
            compressed_bytes: self.compressed_bytes.load(Ordering::Relaxed),
            background_checks: self.background_checks.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.seal_duration.reset();
        self.compression_ratio.reset();
        self.seals_total_success.store(0, Ordering::Relaxed);
        self.seals_total_failed.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.active_seals.store(0, Ordering::Relaxed);
        self.memory_used_bytes.store(0, Ordering::Relaxed);
        self.points_sealed.store(0, Ordering::Relaxed);
        self.original_bytes.store(0, Ordering::Relaxed);
        self.compressed_bytes.store(0, Ordering::Relaxed);
        self.background_checks.store(0, Ordering::Relaxed);
    }

    /// Format as Prometheus metrics
    pub fn to_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = String::new();

        // Seal duration histogram
        output.push_str("# HELP tsdb_seal_duration_seconds Time to seal a chunk\n");
        output.push_str("# TYPE tsdb_seal_duration_seconds histogram\n");
        let mut cumulative = 0u64;
        for (i, &boundary) in snapshot.seal_duration.buckets.iter().enumerate() {
            cumulative += snapshot
                .seal_duration
                .bucket_counts
                .get(i)
                .copied()
                .unwrap_or(0);
            output.push_str(&format!(
                "tsdb_seal_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                boundary, cumulative
            ));
        }
        // Add +Inf bucket
        cumulative += snapshot
            .seal_duration
            .bucket_counts
            .last()
            .copied()
            .unwrap_or(0);
        output.push_str(&format!(
            "tsdb_seal_duration_seconds_bucket{{le=\"+Inf\"}} {}\n",
            cumulative
        ));
        output.push_str(&format!(
            "tsdb_seal_duration_seconds_sum {}\n",
            snapshot.seal_duration.sum
        ));
        output.push_str(&format!(
            "tsdb_seal_duration_seconds_count {}\n",
            snapshot.seal_duration.count
        ));

        // Queue depth
        output.push_str("# HELP tsdb_seal_queue_depth Pending seal tasks\n");
        output.push_str("# TYPE tsdb_seal_queue_depth gauge\n");
        output.push_str(&format!("tsdb_seal_queue_depth {}\n", snapshot.queue_depth));

        // Active seals
        output.push_str("# HELP tsdb_seal_active Currently sealing chunks\n");
        output.push_str("# TYPE tsdb_seal_active gauge\n");
        output.push_str(&format!("tsdb_seal_active {}\n", snapshot.active_seals));

        // Total seals by status
        output.push_str("# HELP tsdb_seal_total Total seals by status\n");
        output.push_str("# TYPE tsdb_seal_total counter\n");
        output.push_str(&format!(
            "tsdb_seal_total{{status=\"success\"}} {}\n",
            snapshot.seals_total_success
        ));
        output.push_str(&format!(
            "tsdb_seal_total{{status=\"failed\"}} {}\n",
            snapshot.seals_total_failed
        ));

        // Compression ratio histogram
        output.push_str("# HELP tsdb_compression_ratio Compression ratios achieved\n");
        output.push_str("# TYPE tsdb_compression_ratio histogram\n");
        let mut cumulative = 0u64;
        for (i, &boundary) in snapshot.compression_ratio.buckets.iter().enumerate() {
            cumulative += snapshot
                .compression_ratio
                .bucket_counts
                .get(i)
                .copied()
                .unwrap_or(0);
            output.push_str(&format!(
                "tsdb_compression_ratio_bucket{{le=\"{}\"}} {}\n",
                boundary, cumulative
            ));
        }
        cumulative += snapshot
            .compression_ratio
            .bucket_counts
            .last()
            .copied()
            .unwrap_or(0);
        output.push_str(&format!(
            "tsdb_compression_ratio_bucket{{le=\"+Inf\"}} {}\n",
            cumulative
        ));
        output.push_str(&format!(
            "tsdb_compression_ratio_sum {}\n",
            snapshot.compression_ratio.sum
        ));
        output.push_str(&format!(
            "tsdb_compression_ratio_count {}\n",
            snapshot.compression_ratio.count
        ));

        // Bytes totals
        output
            .push_str("# HELP tsdb_compression_bytes_total Total bytes before/after compression\n");
        output.push_str("# TYPE tsdb_compression_bytes_total counter\n");
        output.push_str(&format!(
            "tsdb_compression_bytes_total{{type=\"original\"}} {}\n",
            snapshot.original_bytes
        ));
        output.push_str(&format!(
            "tsdb_compression_bytes_total{{type=\"compressed\"}} {}\n",
            snapshot.compressed_bytes
        ));

        // Points sealed
        output.push_str("# HELP tsdb_seal_points_total Total points sealed\n");
        output.push_str("# TYPE tsdb_seal_points_total counter\n");
        output.push_str(&format!(
            "tsdb_seal_points_total {}\n",
            snapshot.points_sealed
        ));

        // Memory budget used
        output.push_str("# HELP tsdb_memory_budget_used_bytes Memory used by pending seals\n");
        output.push_str("# TYPE tsdb_memory_budget_used_bytes gauge\n");
        output.push_str(&format!(
            "tsdb_memory_budget_used_bytes {}\n",
            snapshot.memory_used_bytes
        ));

        // Background checks
        output.push_str("# HELP tsdb_background_seal_checks Background seal check cycles\n");
        output.push_str("# TYPE tsdb_background_seal_checks counter\n");
        output.push_str(&format!(
            "tsdb_background_seal_checks {}\n",
            snapshot.background_checks
        ));

        output
    }
}

impl Default for SealingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of sealing metrics for serialization
#[derive(Debug, Clone, serde::Serialize)]
pub struct SealingMetricsSnapshot {
    /// Uptime in seconds
    pub uptime_secs: u64,
    /// Seal duration histogram
    pub seal_duration: HistogramSnapshot,
    /// Compression ratio histogram
    pub compression_ratio: HistogramSnapshot,
    /// Total successful seals
    pub seals_total_success: u64,
    /// Total failed seals
    pub seals_total_failed: u64,
    /// Current queue depth
    pub queue_depth: u64,
    /// Currently active seals
    pub active_seals: u64,
    /// Memory budget used in bytes
    pub memory_used_bytes: u64,
    /// Total points sealed
    pub points_sealed: u64,
    /// Total original bytes
    pub original_bytes: u64,
    /// Total compressed bytes
    pub compressed_bytes: u64,
    /// Background seal check cycles
    pub background_checks: u64,
}

impl SealingMetricsSnapshot {
    /// Get overall compression ratio
    pub fn overall_compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            0.0
        } else {
            self.original_bytes as f64 / self.compressed_bytes as f64
        }
    }

    /// Get average seal duration in seconds
    pub fn avg_seal_duration_secs(&self) -> f64 {
        self.seal_duration.avg()
    }

    /// Get p50 seal duration
    pub fn p50_seal_duration_secs(&self) -> f64 {
        self.seal_duration.percentile(50.0)
    }

    /// Get p99 seal duration
    pub fn p99_seal_duration_secs(&self) -> f64 {
        self.seal_duration.percentile(99.0)
    }

    /// Get total seals
    pub fn total_seals(&self) -> u64 {
        self.seals_total_success + self.seals_total_failed
    }

    /// Get success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.total_seals();
        if total == 0 {
            100.0
        } else {
            (self.seals_total_success as f64 / total as f64) * 100.0
        }
    }
}

/// Global sealing metrics instance
static GLOBAL_SEALING_METRICS: std::sync::OnceLock<Arc<SealingMetrics>> =
    std::sync::OnceLock::new();

/// Get the global sealing metrics instance
pub fn global_sealing_metrics() -> Arc<SealingMetrics> {
    GLOBAL_SEALING_METRICS
        .get_or_init(|| Arc::new(SealingMetrics::new()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_metrics_basic() {
        let metrics = CodecMetrics::new();

        metrics.record_compression(16000, 2000, 100, 1000);
        metrics.record_compression(16000, 2000, 100, 1000);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.compress_count, 2);
        assert_eq!(snapshot.original_bytes, 32000);
        assert_eq!(snapshot.compressed_bytes, 4000);
        assert_eq!(snapshot.points_compressed, 2000);
        assert!((snapshot.compression_ratio() - 8.0).abs() < 0.01);
    }

    #[test]
    fn test_compression_metrics_per_codec() {
        let metrics = CompressionMetrics::new();

        metrics.record_compression(CodecType::Ahpac, 16000, 2000, 100, 1000);
        metrics.record_compression(CodecType::Kuba, 16000, 4000, 50, 1000);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.per_codec["ahpac"].compress_count, 1);
        assert_eq!(snapshot.per_codec["kuba"].compress_count, 1);
        assert_eq!(snapshot.total.compress_count, 2);
    }

    #[test]
    fn test_bits_per_sample() {
        let metrics = CodecMetrics::new();

        // 1000 points, 2000 bytes compressed = 16 bits per sample
        metrics.record_compression(16000, 2000, 100, 1000);

        assert!((metrics.bits_per_sample() - 16.0).abs() < 0.01);
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = CompressionMetrics::new();
        metrics.record_compression(CodecType::Ahpac, 16000, 2000, 100, 1000);

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("compression_ratio"));
        assert!(prometheus.contains("ahpac"));
    }

    #[test]
    fn test_timer_complete() {
        let metrics = Arc::new(CompressionMetrics::new());
        let timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);

        timer.complete(2000);

        assert_eq!(metrics.ahpac.compress_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_timer_fail() {
        let metrics = Arc::new(CompressionMetrics::new());
        let timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);

        timer.fail();

        assert_eq!(metrics.ahpac.compress_count.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_timer_drop_records_error() {
        let metrics = Arc::new(CompressionMetrics::new());
        {
            let _timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);
            // Timer dropped without calling complete() or fail()
        }

        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 1);
    }

    // =========================================================================
    // Histogram Tests
    // =========================================================================

    #[test]
    fn test_histogram_basic() {
        let hist = Histogram::new(&[1.0, 5.0, 10.0]);

        hist.observe(0.5); // <= 1.0 bucket
        hist.observe(3.0); // <= 5.0 bucket
        hist.observe(7.0); // <= 10.0 bucket
        hist.observe(15.0); // +Inf bucket

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.count, 4);
        assert!((snapshot.sum - 25.5).abs() < 0.01);
        assert_eq!(snapshot.bucket_counts[0], 1); // <= 1.0
        assert_eq!(snapshot.bucket_counts[1], 1); // <= 5.0
        assert_eq!(snapshot.bucket_counts[2], 1); // <= 10.0
        assert_eq!(snapshot.bucket_counts[3], 1); // +Inf
    }

    #[test]
    fn test_histogram_avg() {
        let hist = Histogram::new(&[10.0]);
        hist.observe(2.0);
        hist.observe(4.0);
        hist.observe(6.0);

        let snapshot = hist.snapshot();
        assert!((snapshot.avg() - 4.0).abs() < 0.01);
    }

    #[test]
    fn test_histogram_percentile() {
        let hist = Histogram::new(&[1.0, 2.0, 3.0, 4.0, 5.0]);

        // Add 100 observations distributed across buckets
        for _ in 0..20 {
            hist.observe(0.5);
        }
        for _ in 0..20 {
            hist.observe(1.5);
        }
        for _ in 0..20 {
            hist.observe(2.5);
        }
        for _ in 0..20 {
            hist.observe(3.5);
        }
        for _ in 0..20 {
            hist.observe(4.5);
        }

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.count, 100);

        // p50 should be around bucket <= 3.0 (50 values)
        let p50 = snapshot.percentile(50.0);
        assert!((2.0..=3.0).contains(&p50));

        // p99 should be around bucket <= 5.0
        let p99 = snapshot.percentile(99.0);
        assert!(p99 >= 4.0);
    }

    #[test]
    fn test_histogram_reset() {
        let hist = Histogram::new(&[10.0]);
        hist.observe(5.0);
        hist.observe(5.0);

        hist.reset();

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.count, 0);
        assert_eq!(snapshot.sum, 0.0);
    }

    // =========================================================================
    // Sealing Metrics Tests
    // =========================================================================

    #[test]
    fn test_sealing_metrics_creation() {
        let metrics = SealingMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.seals_total_success, 0);
        assert_eq!(snapshot.seals_total_failed, 0);
        assert_eq!(snapshot.queue_depth, 0);
    }

    #[test]
    fn test_sealing_metrics_record_success() {
        let metrics = SealingMetrics::new();

        metrics.record_seal_success(0.5, 10000, 2000, 500);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.seals_total_success, 1);
        assert_eq!(snapshot.seals_total_failed, 0);
        assert_eq!(snapshot.original_bytes, 10000);
        assert_eq!(snapshot.compressed_bytes, 2000);
        assert_eq!(snapshot.points_sealed, 500);
        assert_eq!(snapshot.seal_duration.count, 1);
        assert_eq!(snapshot.compression_ratio.count, 1);
    }

    #[test]
    fn test_sealing_metrics_record_failure() {
        let metrics = SealingMetrics::new();

        metrics.record_seal_failure(1.0);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.seals_total_success, 0);
        assert_eq!(snapshot.seals_total_failed, 1);
        assert_eq!(snapshot.seal_duration.count, 1);
    }

    #[test]
    fn test_sealing_metrics_gauges() {
        let metrics = SealingMetrics::new();

        metrics.set_queue_depth(10);
        metrics.set_active_seals(3);
        metrics.set_memory_used(1024 * 1024);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.queue_depth, 10);
        assert_eq!(snapshot.active_seals, 3);
        assert_eq!(snapshot.memory_used_bytes, 1024 * 1024);
    }

    #[test]
    fn test_sealing_metrics_background_checks() {
        let metrics = SealingMetrics::new();

        metrics.record_background_check();
        metrics.record_background_check();
        metrics.record_background_check();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.background_checks, 3);
    }

    #[test]
    fn test_sealing_metrics_reset() {
        let metrics = SealingMetrics::new();

        metrics.record_seal_success(0.1, 1000, 500, 100);
        metrics.set_queue_depth(5);

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.seals_total_success, 0);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.seal_duration.count, 0);
    }

    #[test]
    fn test_sealing_metrics_prometheus() {
        let metrics = SealingMetrics::new();
        metrics.record_seal_success(0.05, 10000, 2000, 500);
        metrics.set_queue_depth(3);
        metrics.set_active_seals(1);

        let prometheus = metrics.to_prometheus();

        assert!(prometheus.contains("tsdb_seal_duration_seconds"));
        assert!(prometheus.contains("tsdb_seal_queue_depth"));
        assert!(prometheus.contains("tsdb_seal_active"));
        assert!(prometheus.contains("tsdb_seal_total"));
        assert!(prometheus.contains("tsdb_compression_ratio"));
        assert!(prometheus.contains("tsdb_seal_points_total"));
        assert!(prometheus.contains("tsdb_memory_budget_used_bytes"));
        assert!(prometheus.contains("tsdb_background_seal_checks"));
    }

    #[test]
    fn test_sealing_metrics_snapshot_calculations() {
        let metrics = SealingMetrics::new();

        // Record 10 successful seals, 2 failed
        for _ in 0..10 {
            metrics.record_seal_success(0.1, 1000, 200, 100);
        }
        for _ in 0..2 {
            metrics.record_seal_failure(0.05);
        }

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.total_seals(), 12);
        assert!((snapshot.success_rate() - 83.333).abs() < 1.0); // ~83.3%
        assert!((snapshot.overall_compression_ratio() - 5.0).abs() < 0.1); // 10000/2000 = 5x
    }

    #[test]
    fn test_global_sealing_metrics() {
        let metrics1 = global_sealing_metrics();
        let metrics2 = global_sealing_metrics();

        // Should be the same instance
        assert!(Arc::ptr_eq(&metrics1, &metrics2));
    }
}
