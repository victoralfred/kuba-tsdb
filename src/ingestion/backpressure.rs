//! Backpressure management for flow control
//!
//! Provides flow control mechanisms to prevent memory exhaustion
//! and ensure graceful handling of overload conditions.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::metrics::IngestionMetrics;

/// Backpressure strategy when limits are reached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Reject new data (return error to caller)
    #[default]
    Reject,
    /// Block until space is available
    Block,
    /// Drop oldest data to make room
    DropOldest,
    /// Drop newest data (the incoming data)
    DropNewest,
}

/// Configuration for backpressure management
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum memory usage in bytes before triggering backpressure
    pub memory_limit: usize,
    /// Memory threshold for warning (percentage of limit, 0-100)
    pub memory_warning_threshold: u8,
    /// Maximum queue depth before triggering backpressure
    pub queue_limit: usize,
    /// Queue threshold for warning (percentage of limit, 0-100)
    pub queue_warning_threshold: u8,
    /// Strategy when backpressure is triggered
    pub strategy: BackpressureStrategy,
    /// How long to wait before rechecking when blocking
    pub block_check_interval: Duration,
    /// Maximum time to block before rejecting
    pub max_block_time: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            memory_limit: 1024 * 1024 * 1024, // 1 GB
            memory_warning_threshold: 80,
            queue_limit: 1_000_000,
            queue_warning_threshold: 80,
            strategy: BackpressureStrategy::Reject,
            block_check_interval: Duration::from_millis(10),
            max_block_time: Duration::from_secs(30),
        }
    }
}

/// Maximum allowed memory limit (100 GB) to prevent absurd configurations
pub const MAX_MEMORY_LIMIT: usize = 100 * 1024 * 1024 * 1024;

/// Maximum allowed queue limit to prevent excessive memory usage
pub const MAX_QUEUE_LIMIT: usize = 100_000_000;

impl BackpressureConfig {
    /// Validate the configuration
    ///
    /// Checks both minimum and maximum bounds to prevent misconfiguration.
    pub fn validate(&self) -> Result<(), String> {
        if self.memory_limit == 0 {
            return Err("memory_limit must be > 0".to_string());
        }
        if self.memory_limit > MAX_MEMORY_LIMIT {
            return Err(format!(
                "memory_limit {} exceeds maximum allowed {}",
                self.memory_limit, MAX_MEMORY_LIMIT
            ));
        }
        if self.memory_warning_threshold > 100 {
            return Err("memory_warning_threshold must be <= 100".to_string());
        }
        if self.queue_limit == 0 {
            return Err("queue_limit must be > 0".to_string());
        }
        if self.queue_limit > MAX_QUEUE_LIMIT {
            return Err(format!(
                "queue_limit {} exceeds maximum allowed {}",
                self.queue_limit, MAX_QUEUE_LIMIT
            ));
        }
        if self.queue_warning_threshold > 100 {
            return Err("queue_warning_threshold must be <= 100".to_string());
        }
        Ok(())
    }

    /// Get the memory warning threshold in bytes
    ///
    /// Uses saturating arithmetic to prevent overflow with large limits.
    pub fn memory_warning_bytes(&self) -> usize {
        // Use saturating multiplication to prevent overflow
        let threshold =
            (self.memory_limit as u64).saturating_mul(self.memory_warning_threshold as u64) / 100;
        threshold.min(usize::MAX as u64) as usize
    }

    /// Get the queue warning threshold
    ///
    /// Uses saturating arithmetic to prevent overflow with large limits.
    pub fn queue_warning_count(&self) -> usize {
        // Use saturating multiplication to prevent overflow
        let threshold =
            (self.queue_limit as u64).saturating_mul(self.queue_warning_threshold as u64) / 100;
        threshold.min(usize::MAX as u64) as usize
    }
}

/// Backpressure state snapshot
#[derive(Debug, Clone)]
pub struct BackpressureState {
    /// Whether backpressure is currently active
    pub active: bool,
    /// Current memory usage in bytes
    pub memory_used: usize,
    /// Memory limit
    pub memory_limit: usize,
    /// Memory usage percentage (0-100)
    pub memory_percent: u8,
    /// Current queue depth
    pub queue_depth: usize,
    /// Queue limit
    pub queue_limit: usize,
    /// Queue usage percentage (0-100)
    pub queue_percent: u8,
    /// Total rejections due to backpressure
    pub total_rejections: u64,
    /// When backpressure was last activated
    pub last_activation: Option<Instant>,
}

/// Backpressure controller
///
/// Monitors memory and queue depth to trigger flow control
/// when the system is under pressure.
pub struct BackpressureController {
    /// Configuration
    config: BackpressureConfig,
    /// Whether backpressure is currently active
    active: AtomicBool,
    /// Current memory usage
    memory_used: AtomicUsize,
    /// Current queue depth
    queue_depth: AtomicUsize,
    /// Total rejections
    rejections: AtomicU64,
    /// Metrics collector
    metrics: Arc<IngestionMetrics>,
    /// Whether we've logged a warning for memory
    memory_warning_logged: AtomicBool,
    /// Whether we've logged a warning for queue
    queue_warning_logged: AtomicBool,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new(config: BackpressureConfig, metrics: Arc<IngestionMetrics>) -> Self {
        Self {
            config,
            active: AtomicBool::new(false),
            memory_used: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
            metrics,
            memory_warning_logged: AtomicBool::new(false),
            queue_warning_logged: AtomicBool::new(false),
        }
    }

    /// Update memory usage and check backpressure
    pub fn update_memory_usage(&self, bytes: usize) {
        self.memory_used.store(bytes, Ordering::Relaxed);
        self.check_backpressure();
    }

    /// Update queue depth and check backpressure
    pub fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
        self.check_backpressure();
    }

    /// Increment queue depth by one
    pub fn increment_queue(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        self.check_backpressure();
    }

    /// Decrement queue depth by one
    ///
    /// Uses saturating subtraction to prevent underflow if called
    /// more times than increment_queue.
    pub fn decrement_queue(&self) {
        // Use fetch_update with saturating_sub to prevent underflow
        let _ = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
        self.check_backpressure();
    }

    /// Check if backpressure should be activated or deactivated
    fn check_backpressure(&self) {
        let memory = self.memory_used.load(Ordering::Relaxed);
        let queue = self.queue_depth.load(Ordering::Relaxed);

        let memory_exceeded = memory >= self.config.memory_limit;
        let queue_exceeded = queue >= self.config.queue_limit;

        let should_activate = memory_exceeded || queue_exceeded;
        let was_active = self.active.swap(should_activate, Ordering::SeqCst);

        // Log state changes
        if should_activate && !was_active {
            if memory_exceeded {
                warn!(
                    "Backpressure activated: memory {} bytes >= limit {} bytes",
                    memory, self.config.memory_limit
                );
            }
            if queue_exceeded {
                warn!(
                    "Backpressure activated: queue {} >= limit {}",
                    queue, self.config.queue_limit
                );
            }
            self.metrics.record_backpressure_activated();
        } else if !should_activate && was_active {
            debug!("Backpressure deactivated");
            self.metrics.record_backpressure_deactivated();
        }

        // Log warnings at thresholds
        self.check_warnings(memory, queue);
    }

    /// Check and log warning thresholds
    fn check_warnings(&self, memory: usize, queue: usize) {
        let memory_warning_threshold = self.config.memory_warning_bytes();
        let queue_warning_threshold = self.config.queue_warning_count();

        // Memory warning
        if memory >= memory_warning_threshold {
            if !self.memory_warning_logged.swap(true, Ordering::Relaxed) {
                warn!(
                    "Memory usage high: {} bytes ({}% of limit)",
                    memory,
                    memory * 100 / self.config.memory_limit
                );
            }
        } else {
            self.memory_warning_logged.store(false, Ordering::Relaxed);
        }

        // Queue warning
        if queue >= queue_warning_threshold {
            if !self.queue_warning_logged.swap(true, Ordering::Relaxed) {
                warn!(
                    "Queue depth high: {} ({}% of limit)",
                    queue,
                    queue * 100 / self.config.queue_limit
                );
            }
        } else {
            self.queue_warning_logged.store(false, Ordering::Relaxed);
        }
    }

    /// Check if new data should be rejected
    pub fn should_reject(&self) -> bool {
        if !self.active.load(Ordering::Relaxed) {
            return false;
        }

        match self.config.strategy {
            BackpressureStrategy::Reject | BackpressureStrategy::DropNewest => {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                true
            },
            BackpressureStrategy::Block | BackpressureStrategy::DropOldest => false,
        }
    }

    /// Try to reserve space for incoming points (atomic check-and-reserve)
    ///
    /// This addresses the TOCTOU race in the check-then-act pattern by atomically
    /// checking limits and updating counters. If the reservation fails due to
    /// exceeding limits, no state is modified.
    ///
    /// # Arguments
    ///
    /// * `point_count` - Number of points to reserve space for
    /// * `memory_bytes` - Approximate memory bytes needed
    ///
    /// # Returns
    ///
    /// `true` if reservation succeeded, `false` if it would exceed limits.
    pub fn try_reserve(&self, point_count: usize, memory_bytes: usize) -> bool {
        // Atomic check-and-update for queue depth
        let queue_reserved =
            self.queue_depth
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    let new_depth = current.saturating_add(point_count);
                    if new_depth > self.config.queue_limit {
                        None // Reject: would exceed limit
                    } else {
                        Some(new_depth)
                    }
                });

        if queue_reserved.is_err() {
            // Queue limit would be exceeded
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        // Atomic check-and-update for memory
        let memory_reserved =
            self.memory_used
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    let new_memory = current.saturating_add(memory_bytes);
                    if new_memory > self.config.memory_limit {
                        None // Reject: would exceed limit
                    } else {
                        Some(new_memory)
                    }
                });

        if memory_reserved.is_err() {
            // Memory limit would be exceeded - rollback queue reservation
            let _ = self
                .queue_depth
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    Some(current.saturating_sub(point_count))
                });
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        // Check and update backpressure state after successful reservation
        self.check_backpressure();
        true
    }

    /// Release previously reserved space (call on error or after processing)
    ///
    /// # Arguments
    ///
    /// * `point_count` - Number of points to release
    /// * `memory_bytes` - Memory bytes to release
    pub fn release(&self, point_count: usize, memory_bytes: usize) {
        let _ = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(point_count))
            });
        let _ = self
            .memory_used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(memory_bytes))
            });
        self.check_backpressure();
    }

    /// Wait until backpressure is relieved (for Block strategy)
    ///
    /// Returns Ok(()) when space is available, or Err if timeout exceeded.
    pub async fn wait_for_space(&self) -> Result<(), String> {
        if self.config.strategy != BackpressureStrategy::Block {
            return Ok(());
        }

        let start = Instant::now();

        while self.active.load(Ordering::Relaxed) {
            if start.elapsed() > self.config.max_block_time {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                return Err("Backpressure block timeout exceeded".to_string());
            }

            tokio::time::sleep(self.config.block_check_interval).await;
        }

        Ok(())
    }

    /// Check if backpressure is currently active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Get current memory usage
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Get total rejections
    pub fn total_rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Get current backpressure state
    pub fn state(&self) -> BackpressureState {
        let memory_used = self.memory_used.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);

        let memory_percent = if self.config.memory_limit > 0 {
            ((memory_used as u64 * 100) / self.config.memory_limit as u64) as u8
        } else {
            0
        };

        let queue_percent = if self.config.queue_limit > 0 {
            ((queue_depth as u64 * 100) / self.config.queue_limit as u64) as u8
        } else {
            0
        };

        BackpressureState {
            active: self.active.load(Ordering::Relaxed),
            memory_used,
            memory_limit: self.config.memory_limit,
            memory_percent,
            queue_depth,
            queue_limit: self.config.queue_limit,
            queue_percent,
            total_rejections: self.rejections.load(Ordering::Relaxed),
            last_activation: None, // Would need additional tracking
        }
    }

    /// Reset the controller (for testing)
    #[cfg(test)]
    pub fn reset(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.memory_used.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.rejections.store(0, Ordering::Relaxed);
        self.memory_warning_logged.store(false, Ordering::Relaxed);
        self.queue_warning_logged.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_controller() -> BackpressureController {
        let config = BackpressureConfig {
            memory_limit: 1000,
            memory_warning_threshold: 80,
            queue_limit: 100,
            queue_warning_threshold: 80,
            strategy: BackpressureStrategy::Reject,
            ..Default::default()
        };
        let metrics = Arc::new(IngestionMetrics::new());
        BackpressureController::new(config, metrics)
    }

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_backpressure_config_validation() {
        let config = BackpressureConfig {
            memory_limit: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = BackpressureConfig {
            memory_limit: 1000,
            memory_warning_threshold: 101,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_backpressure_config_thresholds() {
        let config = BackpressureConfig {
            memory_limit: 1000,
            memory_warning_threshold: 80,
            queue_limit: 100,
            queue_warning_threshold: 75,
            ..Default::default()
        };

        assert_eq!(config.memory_warning_bytes(), 800);
        assert_eq!(config.queue_warning_count(), 75);
    }

    #[test]
    fn test_backpressure_controller_creation() {
        let controller = create_controller();
        assert!(!controller.is_active());
        assert_eq!(controller.memory_used(), 0);
        assert_eq!(controller.queue_depth(), 0);
    }

    #[test]
    fn test_backpressure_memory_trigger() {
        let controller = create_controller();

        // Below limit - no backpressure
        controller.update_memory_usage(500);
        assert!(!controller.is_active());

        // At limit - backpressure activated
        controller.update_memory_usage(1000);
        assert!(controller.is_active());

        // Below limit - backpressure deactivated
        controller.update_memory_usage(500);
        assert!(!controller.is_active());
    }

    #[test]
    fn test_backpressure_queue_trigger() {
        let controller = create_controller();

        // Below limit - no backpressure
        controller.update_queue_depth(50);
        assert!(!controller.is_active());

        // At limit - backpressure activated
        controller.update_queue_depth(100);
        assert!(controller.is_active());

        // Below limit - backpressure deactivated
        controller.update_queue_depth(50);
        assert!(!controller.is_active());
    }

    #[test]
    fn test_backpressure_should_reject() {
        let controller = create_controller();

        // Not active - should not reject
        assert!(!controller.should_reject());

        // Activate backpressure
        controller.update_memory_usage(1000);
        assert!(controller.is_active());

        // Should reject with Reject strategy
        assert!(controller.should_reject());
        assert_eq!(controller.total_rejections(), 1);
    }

    #[test]
    fn test_backpressure_increment_decrement() {
        let controller = create_controller();

        controller.increment_queue();
        assert_eq!(controller.queue_depth(), 1);

        controller.increment_queue();
        assert_eq!(controller.queue_depth(), 2);

        controller.decrement_queue();
        assert_eq!(controller.queue_depth(), 1);
    }

    #[test]
    fn test_backpressure_state() {
        let controller = create_controller();
        controller.update_memory_usage(500);
        controller.update_queue_depth(25);

        let state = controller.state();
        assert!(!state.active);
        assert_eq!(state.memory_used, 500);
        assert_eq!(state.memory_limit, 1000);
        assert_eq!(state.memory_percent, 50);
        assert_eq!(state.queue_depth, 25);
        assert_eq!(state.queue_limit, 100);
        assert_eq!(state.queue_percent, 25);
    }

    #[test]
    fn test_backpressure_reset() {
        let controller = create_controller();

        controller.update_memory_usage(1000);
        controller.update_queue_depth(100);
        assert!(controller.is_active());

        controller.reset();
        assert!(!controller.is_active());
        assert_eq!(controller.memory_used(), 0);
        assert_eq!(controller.queue_depth(), 0);
    }
}
