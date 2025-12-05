//! Adaptive Concurrency Control for Parallel Sealing
//!
//! This module provides dynamic adjustment of concurrency limits based on system load,
//! memory pressure, and I/O latency. The goal is to maximize throughput while preventing
//! resource exhaustion.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                     Adaptive Concurrency Controller                      │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐     ┌─────────────────────┐     ┌──────────────────┐ │
//! │  │ LoadSampler  │────▶│ ConcurrencyController│────▶│  Dynamic Limit   │ │
//! │  │ (background) │     │  (adjustment logic)  │     │  (semaphore)     │ │
//! │  └──────────────┘     └─────────────────────┘     └──────────────────┘ │
//! │         │                       │                                       │
//! │         ▼                       ▼                                       │
//! │  ┌──────────────┐     ┌─────────────────────┐                          │
//! │  │SystemMetrics │     │  AdaptiveConfig     │                          │
//! │  │ CPU/Mem/IO   │     │  thresholds/bounds  │                          │
//! │  └──────────────┘     └─────────────────────┘                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::adaptive_concurrency::{
//!     AdaptiveConfig, ConcurrencyController,
//! };
//!
//! let config = AdaptiveConfig::default();
//! let controller = ConcurrencyController::new(config);
//!
//! // Get current limit
//! let limit = controller.current_limit();
//!
//! // Adjust based on current conditions
//! controller.adjust();
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for adaptive concurrency control
///
/// Controls how the system adjusts concurrency based on load metrics.
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Minimum concurrency limit (floor)
    ///
    /// The system will never reduce concurrency below this value.
    /// Default: 1
    pub min_concurrency: usize,

    /// Maximum concurrency limit (ceiling)
    ///
    /// The system will never increase concurrency above this value.
    /// Default: 2 * num_cpus
    pub max_concurrency: usize,

    /// Initial concurrency limit
    ///
    /// Starting point before any adjustments are made.
    /// Default: num_cpus
    pub initial_concurrency: usize,

    /// Interval between adjustment checks
    ///
    /// How often to sample metrics and potentially adjust concurrency.
    /// Default: 1 second
    pub adjustment_interval: Duration,

    /// CPU usage threshold for reducing concurrency (0.0-1.0)
    ///
    /// If CPU usage exceeds this, reduce concurrency.
    /// Default: 0.85 (85%)
    pub cpu_high_threshold: f64,

    /// CPU usage threshold for increasing concurrency (0.0-1.0)
    ///
    /// If CPU usage is below this, consider increasing concurrency.
    /// Default: 0.50 (50%)
    pub cpu_low_threshold: f64,

    /// Memory pressure threshold for reducing concurrency (0.0-1.0)
    ///
    /// If memory usage exceeds this, reduce concurrency.
    /// Default: 0.80 (80%)
    pub memory_high_threshold: f64,

    /// I/O latency threshold for reducing concurrency (milliseconds)
    ///
    /// If average I/O latency exceeds this, reduce concurrency.
    /// Default: 100ms
    pub io_latency_threshold_ms: u64,

    /// Number of samples to average for metrics
    ///
    /// Larger values provide smoother adjustments but slower response.
    /// Default: 5
    pub sample_window: usize,

    /// Step size for concurrency adjustments
    ///
    /// How much to increase/decrease concurrency at each adjustment.
    /// Default: 1
    pub adjustment_step: usize,

    /// Cooldown period after adjustment
    ///
    /// Minimum time to wait before making another adjustment.
    /// Default: 5 seconds
    pub cooldown_period: Duration,

    /// Enable adaptive adjustment
    ///
    /// If false, concurrency remains fixed at initial_concurrency.
    /// Default: true
    pub enabled: bool,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        let num_cpus = num_cpus::get();
        Self {
            min_concurrency: 1,
            max_concurrency: num_cpus * 2,
            initial_concurrency: num_cpus,
            adjustment_interval: Duration::from_secs(1),
            cpu_high_threshold: 0.85,
            cpu_low_threshold: 0.50,
            memory_high_threshold: 0.80,
            io_latency_threshold_ms: 100,
            sample_window: 5,
            adjustment_step: 1,
            cooldown_period: Duration::from_secs(5),
            enabled: true,
        }
    }
}

impl AdaptiveConfig {
    /// Configuration optimized for high-throughput workloads
    ///
    /// Allows higher concurrency and is more aggressive about scaling up.
    pub fn high_throughput() -> Self {
        let num_cpus = num_cpus::get();
        Self {
            min_concurrency: 2,
            max_concurrency: num_cpus * 4,
            initial_concurrency: num_cpus * 2,
            cpu_high_threshold: 0.90,
            cpu_low_threshold: 0.40,
            memory_high_threshold: 0.85,
            io_latency_threshold_ms: 200,
            cooldown_period: Duration::from_secs(3),
            ..Default::default()
        }
    }

    /// Configuration optimized for low-latency workloads
    ///
    /// More conservative to prevent latency spikes.
    pub fn low_latency() -> Self {
        let num_cpus = num_cpus::get();
        Self {
            min_concurrency: 1,
            max_concurrency: num_cpus,
            initial_concurrency: num_cpus / 2,
            cpu_high_threshold: 0.70,
            cpu_low_threshold: 0.30,
            memory_high_threshold: 0.70,
            io_latency_threshold_ms: 50,
            cooldown_period: Duration::from_secs(10),
            ..Default::default()
        }
    }

    /// Configuration for memory-constrained environments
    pub fn low_memory() -> Self {
        Self {
            min_concurrency: 1,
            max_concurrency: 2,
            initial_concurrency: 1,
            memory_high_threshold: 0.60,
            ..Default::default()
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_concurrency == 0 {
            return Err("min_concurrency must be at least 1".to_string());
        }

        if self.max_concurrency < self.min_concurrency {
            return Err("max_concurrency must be >= min_concurrency".to_string());
        }

        if self.initial_concurrency < self.min_concurrency
            || self.initial_concurrency > self.max_concurrency
        {
            return Err("initial_concurrency must be between min and max concurrency".to_string());
        }

        if self.cpu_high_threshold <= self.cpu_low_threshold {
            return Err("cpu_high_threshold must be > cpu_low_threshold".to_string());
        }

        if self.cpu_high_threshold > 1.0 || self.cpu_low_threshold < 0.0 {
            return Err("CPU thresholds must be between 0.0 and 1.0".to_string());
        }

        if self.memory_high_threshold > 1.0 || self.memory_high_threshold < 0.0 {
            return Err("memory_high_threshold must be between 0.0 and 1.0".to_string());
        }

        if self.sample_window == 0 {
            return Err("sample_window must be at least 1".to_string());
        }

        if self.adjustment_step == 0 {
            return Err("adjustment_step must be at least 1".to_string());
        }

        Ok(())
    }
}

// ============================================================================
// System Metrics
// ============================================================================

/// Snapshot of system metrics at a point in time
#[derive(Debug, Clone, Default)]
pub struct SystemMetrics {
    /// CPU usage as a fraction (0.0-1.0)
    pub cpu_usage: f64,

    /// Memory usage as a fraction (0.0-1.0)
    pub memory_usage: f64,

    /// Available memory in bytes
    pub available_memory_bytes: u64,

    /// Average I/O latency in milliseconds
    pub io_latency_ms: f64,

    /// Number of active seal operations
    pub active_seals: u64,

    /// Pending seal operations in queue
    pub pending_seals: u64,

    /// Timestamp when metrics were sampled
    pub sampled_at: Option<Instant>,
}

impl SystemMetrics {
    /// Create a new metrics snapshot with current system values
    ///
    /// Note: Actual system metrics collection requires platform-specific code.
    /// This implementation provides estimates based on available information.
    pub fn sample() -> Self {
        Self::sample_with_seals(0, 0)
    }

    /// Sample metrics with known seal counts
    pub fn sample_with_seals(active_seals: u64, pending_seals: u64) -> Self {
        // Get memory info using sys-info if available, otherwise estimate
        let (memory_usage, available_memory_bytes) = Self::get_memory_info();

        // CPU usage estimation (simplified - real implementation would use
        // platform-specific APIs like /proc/stat on Linux)
        let cpu_usage = Self::estimate_cpu_usage();

        Self {
            cpu_usage,
            memory_usage,
            available_memory_bytes,
            io_latency_ms: 0.0, // Set externally based on actual I/O measurements
            active_seals,
            pending_seals,
            sampled_at: Some(Instant::now()),
        }
    }

    /// Get memory information
    fn get_memory_info() -> (f64, u64) {
        // Try to get actual system memory info
        // For now, return conservative estimates
        // In production, use sysinfo crate or platform-specific APIs

        // Default: assume 50% usage and 4GB available
        (0.5, 4 * 1024 * 1024 * 1024)
    }

    /// Estimate CPU usage
    fn estimate_cpu_usage() -> f64 {
        // Simplified estimation
        // Real implementation would read /proc/stat on Linux
        // or use platform-specific APIs

        // Default: assume moderate usage
        0.5
    }

    /// Check if metrics indicate high load
    pub fn is_high_load(&self, config: &AdaptiveConfig) -> bool {
        self.cpu_usage > config.cpu_high_threshold
            || self.memory_usage > config.memory_high_threshold
            || self.io_latency_ms > config.io_latency_threshold_ms as f64
    }

    /// Check if metrics indicate low load
    pub fn is_low_load(&self, config: &AdaptiveConfig) -> bool {
        self.cpu_usage < config.cpu_low_threshold
            && self.memory_usage < config.memory_high_threshold * 0.5
            && self.io_latency_ms < config.io_latency_threshold_ms as f64 * 0.5
    }
}

// ============================================================================
// Load Sampler
// ============================================================================

/// Background service that periodically samples system metrics
pub struct LoadSampler {
    /// Configuration
    config: AdaptiveConfig,

    /// Recent samples for averaging
    samples: RwLock<Vec<SystemMetrics>>,

    /// Whether the sampler is running
    running: AtomicBool,

    /// External I/O latency tracker (updated by seal operations)
    io_latency_sum_ns: AtomicU64,
    io_latency_count: AtomicU64,

    /// External active seal count
    active_seals: AtomicU64,

    /// External pending seal count
    pending_seals: AtomicU64,
}

impl LoadSampler {
    /// Create a new load sampler
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            config,
            samples: RwLock::new(Vec::new()),
            running: AtomicBool::new(false),
            io_latency_sum_ns: AtomicU64::new(0),
            io_latency_count: AtomicU64::new(0),
            active_seals: AtomicU64::new(0),
            pending_seals: AtomicU64::new(0),
        }
    }

    /// Record an I/O operation's latency
    pub fn record_io_latency(&self, duration: Duration) {
        self.io_latency_sum_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.io_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update active seal count
    pub fn set_active_seals(&self, count: u64) {
        self.active_seals.store(count, Ordering::Relaxed);
    }

    /// Update pending seal count
    pub fn set_pending_seals(&self, count: u64) {
        self.pending_seals.store(count, Ordering::Relaxed);
    }

    /// Take a single sample
    pub async fn take_sample(&self) {
        let active = self.active_seals.load(Ordering::Relaxed);
        let pending = self.pending_seals.load(Ordering::Relaxed);

        let mut metrics = SystemMetrics::sample_with_seals(active, pending);

        // Calculate average I/O latency
        let count = self.io_latency_count.load(Ordering::Relaxed);
        if count > 0 {
            let sum = self.io_latency_sum_ns.load(Ordering::Relaxed);
            metrics.io_latency_ms = (sum as f64 / count as f64) / 1_000_000.0;

            // Reset counters for next window
            self.io_latency_sum_ns.store(0, Ordering::Relaxed);
            self.io_latency_count.store(0, Ordering::Relaxed);
        }

        let mut samples = self.samples.write().await;
        samples.push(metrics);

        // Keep only the most recent samples
        while samples.len() > self.config.sample_window {
            samples.remove(0);
        }
    }

    /// Get the latest metrics sample
    pub async fn latest(&self) -> Option<SystemMetrics> {
        let samples = self.samples.read().await;
        samples.last().cloned()
    }

    /// Get averaged metrics over the sample window
    pub async fn averaged(&self) -> SystemMetrics {
        let samples = self.samples.read().await;
        if samples.is_empty() {
            return SystemMetrics::default();
        }

        let count = samples.len() as f64;
        let mut avg = SystemMetrics::default();

        for sample in samples.iter() {
            avg.cpu_usage += sample.cpu_usage;
            avg.memory_usage += sample.memory_usage;
            avg.available_memory_bytes += sample.available_memory_bytes;
            avg.io_latency_ms += sample.io_latency_ms;
            avg.active_seals += sample.active_seals;
            avg.pending_seals += sample.pending_seals;
        }

        avg.cpu_usage /= count;
        avg.memory_usage /= count;
        avg.available_memory_bytes = (avg.available_memory_bytes as f64 / count) as u64;
        avg.io_latency_ms /= count;
        avg.active_seals = (avg.active_seals as f64 / count) as u64;
        avg.pending_seals = (avg.pending_seals as f64 / count) as u64;
        avg.sampled_at = Some(Instant::now());

        avg
    }

    /// Check if the sampler is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Start the background sampling task
    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::Relaxed);
        let sampler = Arc::clone(self);
        let interval = self.config.adjustment_interval;

        tokio::spawn(async move {
            while sampler.running.load(Ordering::Relaxed) {
                sampler.take_sample().await;
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Stop the background sampling task
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Get sample count
    pub async fn sample_count(&self) -> usize {
        self.samples.read().await.len()
    }

    /// Clear all samples
    pub async fn clear(&self) {
        self.samples.write().await.clear();
    }
}

// ============================================================================
// Concurrency Controller
// ============================================================================

/// Adjustment direction for concurrency
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustmentDirection {
    /// Increase concurrency
    Increase,
    /// Decrease concurrency
    Decrease,
    /// No change needed
    None,
}

/// Result of a concurrency adjustment
#[derive(Debug, Clone)]
pub struct AdjustmentResult {
    /// Previous concurrency limit
    pub previous: usize,
    /// New concurrency limit
    pub current: usize,
    /// Direction of adjustment
    pub direction: AdjustmentDirection,
    /// Reason for the adjustment
    pub reason: String,
}

/// Statistics for the concurrency controller
#[derive(Debug, Clone, Default)]
pub struct ControllerStatsSnapshot {
    /// Current concurrency limit
    pub current_limit: usize,
    /// Total adjustments made
    pub total_adjustments: u64,
    /// Total increases
    pub total_increases: u64,
    /// Total decreases
    pub total_decreases: u64,
    /// Time since last adjustment
    pub time_since_last_adjustment_ms: u64,
}

/// Controller for dynamic concurrency adjustment
///
/// Monitors system metrics and adjusts the concurrency limit to optimize
/// throughput while preventing resource exhaustion.
pub struct ConcurrencyController {
    /// Configuration
    config: AdaptiveConfig,

    /// Current concurrency limit
    current_limit: AtomicUsize,

    /// Load sampler for metrics
    sampler: Arc<LoadSampler>,

    /// Last adjustment time
    last_adjustment: RwLock<Instant>,

    /// Statistics
    total_adjustments: AtomicU64,
    total_increases: AtomicU64,
    total_decreases: AtomicU64,
}

impl ConcurrencyController {
    /// Create a new concurrency controller
    pub fn new(config: AdaptiveConfig) -> Self {
        let initial = config.initial_concurrency;
        let sampler = Arc::new(LoadSampler::new(config.clone()));

        info!(
            initial = initial,
            min = config.min_concurrency,
            max = config.max_concurrency,
            "Creating adaptive concurrency controller"
        );

        Self {
            config,
            current_limit: AtomicUsize::new(initial),
            sampler,
            last_adjustment: RwLock::new(Instant::now()),
            total_adjustments: AtomicU64::new(0),
            total_increases: AtomicU64::new(0),
            total_decreases: AtomicU64::new(0),
        }
    }

    /// Get the current concurrency limit
    pub fn current_limit(&self) -> usize {
        self.current_limit.load(Ordering::Relaxed)
    }

    /// Force a specific concurrency limit
    ///
    /// The limit is clamped to [min, max].
    pub fn force_limit(&self, limit: usize) {
        let clamped = limit.clamp(self.config.min_concurrency, self.config.max_concurrency);
        self.current_limit.store(clamped, Ordering::Relaxed);
        debug!(limit = clamped, "Concurrency limit forced");
    }

    /// Reset to initial concurrency
    pub fn reset(&self) {
        self.current_limit
            .store(self.config.initial_concurrency, Ordering::Relaxed);
        debug!(limit = self.config.initial_concurrency, "Concurrency reset");
    }

    /// Adjust concurrency based on current metrics
    ///
    /// Returns the adjustment result if an adjustment was made.
    pub async fn adjust(&self) -> Option<AdjustmentResult> {
        if !self.config.enabled {
            return None;
        }

        // Check cooldown
        {
            let last = self.last_adjustment.read().await;
            if last.elapsed() < self.config.cooldown_period {
                return None;
            }
        }

        // Get averaged metrics
        let metrics = self.sampler.averaged().await;

        // Determine adjustment direction
        let (direction, reason) = self.determine_adjustment(&metrics);

        if direction == AdjustmentDirection::None {
            return None;
        }

        // Apply adjustment
        let previous = self.current_limit.load(Ordering::Relaxed);
        let new_limit = match direction {
            AdjustmentDirection::Increase => {
                (previous + self.config.adjustment_step).min(self.config.max_concurrency)
            },
            AdjustmentDirection::Decrease => previous
                .saturating_sub(self.config.adjustment_step)
                .max(self.config.min_concurrency),
            AdjustmentDirection::None => previous,
        };

        // Only update if there's an actual change
        if new_limit == previous {
            return None;
        }

        self.current_limit.store(new_limit, Ordering::Relaxed);
        *self.last_adjustment.write().await = Instant::now();

        // Update statistics
        self.total_adjustments.fetch_add(1, Ordering::Relaxed);
        match direction {
            AdjustmentDirection::Increase => {
                self.total_increases.fetch_add(1, Ordering::Relaxed);
            },
            AdjustmentDirection::Decrease => {
                self.total_decreases.fetch_add(1, Ordering::Relaxed);
            },
            AdjustmentDirection::None => {},
        }

        info!(
            previous = previous,
            new = new_limit,
            reason = %reason,
            "Adjusted concurrency limit"
        );

        Some(AdjustmentResult {
            previous,
            current: new_limit,
            direction,
            reason,
        })
    }

    /// Determine if and how to adjust concurrency
    fn determine_adjustment(&self, metrics: &SystemMetrics) -> (AdjustmentDirection, String) {
        // Check for high load conditions (reduce concurrency)
        if metrics.cpu_usage > self.config.cpu_high_threshold {
            return (
                AdjustmentDirection::Decrease,
                format!("CPU usage high: {:.1}%", metrics.cpu_usage * 100.0),
            );
        }

        if metrics.memory_usage > self.config.memory_high_threshold {
            return (
                AdjustmentDirection::Decrease,
                format!("Memory usage high: {:.1}%", metrics.memory_usage * 100.0),
            );
        }

        if metrics.io_latency_ms > self.config.io_latency_threshold_ms as f64 {
            return (
                AdjustmentDirection::Decrease,
                format!("I/O latency high: {:.1}ms", metrics.io_latency_ms),
            );
        }

        // Check for low load conditions (increase concurrency)
        if metrics.is_low_load(&self.config) {
            let current = self.current_limit.load(Ordering::Relaxed);
            if current < self.config.max_concurrency {
                return (
                    AdjustmentDirection::Increase,
                    format!(
                        "Low load: CPU {:.1}%, Mem {:.1}%",
                        metrics.cpu_usage * 100.0,
                        metrics.memory_usage * 100.0
                    ),
                );
            }
        }

        (AdjustmentDirection::None, String::new())
    }

    /// Get the load sampler
    pub fn sampler(&self) -> &Arc<LoadSampler> {
        &self.sampler
    }

    /// Get the configuration
    pub fn config(&self) -> &AdaptiveConfig {
        &self.config
    }

    /// Get statistics snapshot
    pub async fn stats(&self) -> ControllerStatsSnapshot {
        let last = self.last_adjustment.read().await;
        ControllerStatsSnapshot {
            current_limit: self.current_limit.load(Ordering::Relaxed),
            total_adjustments: self.total_adjustments.load(Ordering::Relaxed),
            total_increases: self.total_increases.load(Ordering::Relaxed),
            total_decreases: self.total_decreases.load(Ordering::Relaxed),
            time_since_last_adjustment_ms: last.elapsed().as_millis() as u64,
        }
    }

    /// Record I/O latency from a seal operation
    pub fn record_io_latency(&self, duration: Duration) {
        self.sampler.record_io_latency(duration);
    }

    /// Update seal counts for metrics
    pub fn update_seal_counts(&self, active: u64, pending: u64) {
        self.sampler.set_active_seals(active);
        self.sampler.set_pending_seals(pending);
    }

    /// Check if adaptive adjustment is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

impl Default for ConcurrencyController {
    fn default() -> Self {
        Self::new(AdaptiveConfig::default())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Configuration Tests
    // ========================================================================

    #[test]
    fn test_config_default() {
        let config = AdaptiveConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.min_concurrency >= 1);
        assert!(config.max_concurrency >= config.min_concurrency);
        assert!(config.initial_concurrency >= config.min_concurrency);
        assert!(config.initial_concurrency <= config.max_concurrency);
    }

    #[test]
    fn test_config_high_throughput() {
        let config = AdaptiveConfig::high_throughput();
        assert!(config.validate().is_ok());
        assert!(config.max_concurrency > AdaptiveConfig::default().max_concurrency);
    }

    #[test]
    fn test_config_low_latency() {
        let config = AdaptiveConfig::low_latency();
        assert!(config.validate().is_ok());
        assert!(config.cpu_high_threshold < AdaptiveConfig::default().cpu_high_threshold);
    }

    #[test]
    fn test_config_low_memory() {
        let config = AdaptiveConfig::low_memory();
        assert!(config.validate().is_ok());
        assert!(config.max_concurrency <= 2);
    }

    #[test]
    fn test_config_validation_min_zero() {
        let config = AdaptiveConfig {
            min_concurrency: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_max_less_than_min() {
        let config = AdaptiveConfig {
            min_concurrency: 5,
            max_concurrency: 3,
            initial_concurrency: 4,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_initial_out_of_range() {
        let config = AdaptiveConfig {
            min_concurrency: 2,
            max_concurrency: 10,
            initial_concurrency: 15,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_cpu_thresholds() {
        let config = AdaptiveConfig {
            cpu_high_threshold: 0.3,
            cpu_low_threshold: 0.5, // Higher than high threshold
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_memory_threshold_out_of_range() {
        let config = AdaptiveConfig {
            memory_high_threshold: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_sample_window_zero() {
        let config = AdaptiveConfig {
            sample_window: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_adjustment_step_zero() {
        let config = AdaptiveConfig {
            adjustment_step: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // ========================================================================
    // System Metrics Tests
    // ========================================================================

    #[test]
    fn test_metrics_default() {
        let metrics = SystemMetrics::default();
        assert_eq!(metrics.cpu_usage, 0.0);
        assert_eq!(metrics.memory_usage, 0.0);
        assert!(metrics.sampled_at.is_none());
    }

    #[test]
    fn test_metrics_sample() {
        let metrics = SystemMetrics::sample();
        assert!(metrics.sampled_at.is_some());
    }

    #[test]
    fn test_metrics_sample_with_seals() {
        let metrics = SystemMetrics::sample_with_seals(5, 10);
        assert_eq!(metrics.active_seals, 5);
        assert_eq!(metrics.pending_seals, 10);
    }

    #[test]
    fn test_metrics_is_high_load() {
        let config = AdaptiveConfig::default();

        let high_cpu = SystemMetrics {
            cpu_usage: 0.95,
            ..Default::default()
        };
        assert!(high_cpu.is_high_load(&config));

        let high_memory = SystemMetrics {
            memory_usage: 0.95,
            ..Default::default()
        };
        assert!(high_memory.is_high_load(&config));

        let high_io = SystemMetrics {
            io_latency_ms: 200.0,
            ..Default::default()
        };
        assert!(high_io.is_high_load(&config));

        let normal = SystemMetrics {
            cpu_usage: 0.5,
            memory_usage: 0.5,
            io_latency_ms: 50.0,
            ..Default::default()
        };
        assert!(!normal.is_high_load(&config));
    }

    #[test]
    fn test_metrics_is_low_load() {
        let config = AdaptiveConfig::default();

        let low = SystemMetrics {
            cpu_usage: 0.2,
            memory_usage: 0.2,
            io_latency_ms: 10.0,
            ..Default::default()
        };
        assert!(low.is_low_load(&config));

        let normal = SystemMetrics {
            cpu_usage: 0.6,
            memory_usage: 0.5,
            io_latency_ms: 50.0,
            ..Default::default()
        };
        assert!(!normal.is_low_load(&config));
    }

    // ========================================================================
    // Load Sampler Tests
    // ========================================================================

    #[tokio::test]
    async fn test_sampler_creation() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());
        assert!(!sampler.is_running());
        assert_eq!(sampler.sample_count().await, 0);
    }

    #[tokio::test]
    async fn test_sampler_take_sample() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());
        sampler.take_sample().await;
        assert_eq!(sampler.sample_count().await, 1);
    }

    #[tokio::test]
    async fn test_sampler_latest() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());
        assert!(sampler.latest().await.is_none());

        sampler.take_sample().await;
        assert!(sampler.latest().await.is_some());
    }

    #[tokio::test]
    async fn test_sampler_averaged() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());

        // Take multiple samples
        for _ in 0..3 {
            sampler.take_sample().await;
        }

        let avg = sampler.averaged().await;
        assert!(avg.sampled_at.is_some());
    }

    #[tokio::test]
    async fn test_sampler_window_limit() {
        let config = AdaptiveConfig {
            sample_window: 3,
            ..Default::default()
        };
        let sampler = LoadSampler::new(config);

        // Take more samples than window size
        for _ in 0..10 {
            sampler.take_sample().await;
        }

        assert_eq!(sampler.sample_count().await, 3);
    }

    #[tokio::test]
    async fn test_sampler_record_io_latency() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());

        sampler.record_io_latency(Duration::from_millis(50));
        sampler.record_io_latency(Duration::from_millis(100));

        sampler.take_sample().await;

        let metrics = sampler.latest().await.unwrap();
        // Average of 50ms and 100ms = 75ms
        assert!((metrics.io_latency_ms - 75.0).abs() < 1.0);
    }

    #[tokio::test]
    async fn test_sampler_clear() {
        let sampler = LoadSampler::new(AdaptiveConfig::default());
        sampler.take_sample().await;
        assert_eq!(sampler.sample_count().await, 1);

        sampler.clear().await;
        assert_eq!(sampler.sample_count().await, 0);
    }

    // ========================================================================
    // Concurrency Controller Tests
    // ========================================================================

    #[test]
    fn test_controller_creation() {
        let config = AdaptiveConfig {
            initial_concurrency: 4,
            ..Default::default()
        };
        let controller = ConcurrencyController::new(config);
        assert_eq!(controller.current_limit(), 4);
    }

    #[test]
    fn test_controller_default() {
        let controller = ConcurrencyController::default();
        assert!(controller.current_limit() > 0);
    }

    #[test]
    fn test_controller_force_limit() {
        let config = AdaptiveConfig {
            min_concurrency: 2,
            max_concurrency: 10,
            initial_concurrency: 5,
            ..Default::default()
        };
        let controller = ConcurrencyController::new(config);

        controller.force_limit(8);
        assert_eq!(controller.current_limit(), 8);

        // Test clamping to max
        controller.force_limit(100);
        assert_eq!(controller.current_limit(), 10);

        // Test clamping to min
        controller.force_limit(0);
        assert_eq!(controller.current_limit(), 2);
    }

    #[test]
    fn test_controller_reset() {
        let config = AdaptiveConfig {
            initial_concurrency: 5,
            ..Default::default()
        };
        let controller = ConcurrencyController::new(config);

        controller.force_limit(10);
        assert_eq!(controller.current_limit(), 10);

        controller.reset();
        assert_eq!(controller.current_limit(), 5);
    }

    #[tokio::test]
    async fn test_controller_adjust_disabled() {
        let config = AdaptiveConfig {
            enabled: false,
            ..Default::default()
        };
        let controller = ConcurrencyController::new(config);

        let result = controller.adjust().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_controller_stats() {
        let controller = ConcurrencyController::default();
        let stats = controller.stats().await;

        assert!(stats.current_limit > 0);
        assert_eq!(stats.total_adjustments, 0);
    }

    #[test]
    fn test_controller_is_enabled() {
        let enabled_config = AdaptiveConfig {
            enabled: true,
            ..Default::default()
        };
        let disabled_config = AdaptiveConfig {
            enabled: false,
            ..Default::default()
        };

        let enabled = ConcurrencyController::new(enabled_config);
        let disabled = ConcurrencyController::new(disabled_config);

        assert!(enabled.is_enabled());
        assert!(!disabled.is_enabled());
    }

    #[test]
    fn test_controller_record_io_latency() {
        let controller = ConcurrencyController::default();
        // Should not panic
        controller.record_io_latency(Duration::from_millis(100));
    }

    #[test]
    fn test_controller_update_seal_counts() {
        let controller = ConcurrencyController::default();
        // Should not panic
        controller.update_seal_counts(5, 10);
    }

    // ========================================================================
    // Adjustment Direction Tests
    // ========================================================================

    #[test]
    fn test_adjustment_direction_equality() {
        assert_eq!(AdjustmentDirection::Increase, AdjustmentDirection::Increase);
        assert_eq!(AdjustmentDirection::Decrease, AdjustmentDirection::Decrease);
        assert_eq!(AdjustmentDirection::None, AdjustmentDirection::None);
        assert_ne!(AdjustmentDirection::Increase, AdjustmentDirection::Decrease);
    }
}
