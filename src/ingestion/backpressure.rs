//! Backpressure management for flow control
//!
//! Provides flow control mechanisms to prevent memory exhaustion
//! and ensure graceful handling of overload conditions.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
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
    /// Hysteresis threshold for deactivation (percentage of limit, 0-100)
    /// Backpressure deactivates when usage falls below this percentage.
    pub deactivation_threshold: u8,
    /// Minimum interval between warning logs (rate limiting)
    pub warning_cooldown: Duration,
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
            deactivation_threshold: 90,
            warning_cooldown: Duration::from_secs(60),
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
        if self.block_check_interval.is_zero() {
            return Err("block_check_interval must be > 0".to_string());
        }
        if self.max_block_time.is_zero() {
            return Err("max_block_time must be > 0".to_string());
        }
        if self.block_check_interval > self.max_block_time {
            return Err("block_check_interval must be <= max_block_time".to_string());
        }
        if self.deactivation_threshold > 100 {
            return Err("deactivation_threshold must be <= 100".to_string());
        }
        if self.deactivation_threshold < self.memory_warning_threshold {
            return Err(
                "deactivation_threshold should be >= memory_warning_threshold for proper hysteresis"
                    .to_string(),
            );
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

    /// Get the memory deactivation threshold in bytes (for hysteresis)
    pub fn memory_deactivation_bytes(&self) -> usize {
        let threshold =
            (self.memory_limit as u64).saturating_mul(self.deactivation_threshold as u64) / 100;
        threshold.min(usize::MAX as u64) as usize
    }

    /// Get the queue deactivation threshold (for hysteresis)
    pub fn queue_deactivation_count(&self) -> usize {
        let threshold =
            (self.queue_limit as u64).saturating_mul(self.deactivation_threshold as u64) / 100;
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
    /// Total duration backpressure has been active (cumulative)
    pub total_active_duration: Duration,
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
    /// Last time a memory warning was logged (epoch nanos, 0 = never)
    last_memory_warning_nanos: AtomicU64,
    /// Last time a queue warning was logged (epoch nanos, 0 = never)
    last_queue_warning_nanos: AtomicU64,
    /// Timestamp when backpressure was last activated (elapsed nanos)
    last_activation_nanos: AtomicU64,
    /// Total cumulative duration of backpressure (in nanoseconds)
    total_active_nanos: AtomicU64,
    /// Notifier for waking up blocked waiters when backpressure is relieved
    space_available: Arc<Notify>,
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
            last_memory_warning_nanos: AtomicU64::new(0),
            last_queue_warning_nanos: AtomicU64::new(0),
            last_activation_nanos: AtomicU64::new(0),
            total_active_nanos: AtomicU64::new(0),
            space_available: Arc::new(Notify::new()),
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
    ///
    /// Uses saturating addition to prevent overflow at usize::MAX.
    pub fn increment_queue(&self) {
        let _ = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(1))
            });
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
    ///
    /// Uses hysteresis to prevent rapid on/off toggling:
    /// - Activates when usage >= 100% of limit
    /// - Deactivates when usage falls below deactivation_threshold (default 90%)
    fn check_backpressure(&self) {
        let memory = self.memory_used.load(Ordering::Relaxed);
        let queue = self.queue_depth.load(Ordering::Relaxed);
        let was_active = self.active.load(Ordering::Relaxed);

        // Check if we should activate (at limit)
        let memory_exceeded = memory >= self.config.memory_limit;
        let queue_exceeded = queue >= self.config.queue_limit;

        // Check if we should deactivate (below hysteresis threshold)
        let memory_below_hysteresis = memory < self.config.memory_deactivation_bytes();
        let queue_below_hysteresis = queue < self.config.queue_deactivation_count();

        let should_activate = if was_active {
            // Already active: stay active unless BOTH are below hysteresis threshold
            !(memory_below_hysteresis && queue_below_hysteresis)
        } else {
            // Not active: activate if EITHER exceeds limit
            memory_exceeded || queue_exceeded
        };

        // Only update state if it changes
        if should_activate != was_active {
            self.active.store(should_activate, Ordering::SeqCst);

            if should_activate {
                // Record activation timestamp using SystemTime for absolute time
                let now_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                self.last_activation_nanos
                    .store(now_nanos, Ordering::Relaxed);

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
            } else {
                // Calculate and accumulate active duration
                let activation_nanos = self.last_activation_nanos.load(Ordering::Relaxed);
                if activation_nanos > 0 {
                    let now_nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    let duration = now_nanos.saturating_sub(activation_nanos);
                    self.total_active_nanos
                        .fetch_add(duration, Ordering::Relaxed);
                }

                debug!(
                    "Backpressure deactivated (hysteresis): memory {}% queue {}%",
                    ((memory as u128) * 100 / (self.config.memory_limit as u128)) as u8,
                    ((queue as u128) * 100 / (self.config.queue_limit as u128)) as u8
                );
                self.metrics.record_backpressure_deactivated();

                // Notify any blocked waiters that space is available
                self.space_available.notify_waiters();
            }
        }

        // Log warnings at thresholds (with rate limiting)
        self.check_warnings(memory, queue);
    }

    /// Check and log warning thresholds with rate limiting
    fn check_warnings(&self, memory: usize, queue: usize) {
        let memory_warning_threshold = self.config.memory_warning_bytes();
        let queue_warning_threshold = self.config.queue_warning_count();
        let cooldown_nanos = self.config.warning_cooldown.as_nanos() as u64;
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Memory warning - use u128 to prevent overflow in percentage calculation
        if memory >= memory_warning_threshold {
            let last_warning = self.last_memory_warning_nanos.load(Ordering::Relaxed);
            // Log if never logged (0) or cooldown has passed
            if last_warning == 0 || now_nanos.saturating_sub(last_warning) >= cooldown_nanos {
                // Try to update the timestamp (only one thread will succeed)
                if self
                    .last_memory_warning_nanos
                    .compare_exchange(last_warning, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    let percent =
                        ((memory as u128) * 100 / (self.config.memory_limit as u128)) as u8;
                    warn!(
                        "Memory usage high: {} bytes ({}% of limit)",
                        memory, percent
                    );
                }
            }
        } else {
            // Reset when below threshold so next crossing will log
            self.last_memory_warning_nanos.store(0, Ordering::Relaxed);
        }

        // Queue warning - use u128 to prevent overflow in percentage calculation
        if queue >= queue_warning_threshold {
            let last_warning = self.last_queue_warning_nanos.load(Ordering::Relaxed);
            // Log if never logged (0) or cooldown has passed
            if last_warning == 0 || now_nanos.saturating_sub(last_warning) >= cooldown_nanos {
                // Try to update the timestamp (only one thread will succeed)
                if self
                    .last_queue_warning_nanos
                    .compare_exchange(last_warning, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    let percent = ((queue as u128) * 100 / (self.config.queue_limit as u128)) as u8;
                    warn!("Queue depth high: {} ({}% of limit)", queue, percent);
                }
            }
        } else {
            // Reset when below threshold so next crossing will log
            self.last_queue_warning_nanos.store(0, Ordering::Relaxed);
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
    /// Uses notification-based waiting instead of polling for efficiency.
    /// Returns Ok(()) when space is available, or Err if timeout exceeded.
    pub async fn wait_for_space(&self) -> Result<(), String> {
        if self.config.strategy != BackpressureStrategy::Block {
            return Ok(());
        }

        // Fast path: if not active, return immediately
        if !self.active.load(Ordering::Relaxed) {
            return Ok(());
        }

        let deadline = Instant::now() + self.config.max_block_time;

        loop {
            // Check if backpressure is relieved
            if !self.active.load(Ordering::Relaxed) {
                return Ok(());
            }

            // Check timeout
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                return Err("Backpressure block timeout exceeded".to_string());
            }

            // Wait for notification with timeout
            let wait_duration = remaining.min(self.config.block_check_interval);

            tokio::select! {
                _ = self.space_available.notified() => {
                    // Notified that space may be available, loop to check
                }
                _ = tokio::time::sleep(wait_duration) => {
                    // Timeout expired, loop to check again
                }
            }
        }
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

    /// Get total duration backpressure has been active
    pub fn total_active_duration(&self) -> Duration {
        Duration::from_nanos(self.total_active_nanos.load(Ordering::Relaxed))
    }

    /// Get current backpressure state
    pub fn state(&self) -> BackpressureState {
        let memory_used = self.memory_used.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let is_active = self.active.load(Ordering::Relaxed);

        let memory_percent = if self.config.memory_limit > 0 {
            ((memory_used as u128 * 100) / self.config.memory_limit as u128) as u8
        } else {
            0
        };

        let queue_percent = if self.config.queue_limit > 0 {
            ((queue_depth as u128 * 100) / self.config.queue_limit as u128) as u8
        } else {
            0
        };

        // Reconstruct last_activation from stored nanos
        // Note: We store absolute time (SystemTime nanos) but return Instant
        // This is an approximation - for exact tracking, consider storing Instant directly
        let activation_nanos = self.last_activation_nanos.load(Ordering::Relaxed);
        let last_activation = if activation_nanos > 0 {
            // Approximate: assume activation was recent (within reasonable bounds)
            // For exact tracking, we'd need to store Instant, but that requires
            // more complex synchronization. This approximation is acceptable for metrics.
            let now_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let duration_since_activation = now_nanos.saturating_sub(activation_nanos);
            // Convert to Instant by subtracting from now
            Some(
                Instant::now()
                    .checked_sub(Duration::from_nanos(duration_since_activation))
                    .unwrap_or_else(Instant::now),
            )
        } else {
            None
        };

        // Calculate total active duration (include current active period if applicable)
        let mut total_nanos = self.total_active_nanos.load(Ordering::Relaxed);
        if is_active && activation_nanos > 0 {
            let now_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            total_nanos = total_nanos.saturating_add(now_nanos.saturating_sub(activation_nanos));
        }

        BackpressureState {
            active: is_active,
            memory_used,
            memory_limit: self.config.memory_limit,
            memory_percent,
            queue_depth,
            queue_limit: self.config.queue_limit,
            queue_percent,
            total_rejections: self.rejections.load(Ordering::Relaxed),
            last_activation,
            total_active_duration: Duration::from_nanos(total_nanos),
        }
    }

    /// Reset the controller (for testing)
    #[cfg(test)]
    pub fn reset(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.memory_used.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.rejections.store(0, Ordering::Relaxed);
        self.last_memory_warning_nanos.store(0, Ordering::Relaxed);
        self.last_queue_warning_nanos.store(0, Ordering::Relaxed);
        self.last_activation_nanos.store(0, Ordering::Relaxed);
        self.total_active_nanos.store(0, Ordering::Relaxed);
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

        // Above hysteresis threshold (90% = 900) - still active
        controller.update_memory_usage(900);
        assert!(controller.is_active());

        // Below hysteresis threshold - backpressure deactivated
        controller.update_memory_usage(899);
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

        // Above hysteresis threshold (90% = 90) - still active
        controller.update_queue_depth(90);
        assert!(controller.is_active());

        // Below hysteresis threshold - backpressure deactivated
        controller.update_queue_depth(89);
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

    #[test]
    fn test_hysteresis_prevents_toggling() {
        let controller = create_controller();

        // Activate at limit
        controller.update_memory_usage(1000);
        assert!(controller.is_active());

        // Drop to 95% - still active due to hysteresis
        controller.update_memory_usage(950);
        assert!(controller.is_active());

        // Drop to 91% - still active (threshold is 90%)
        controller.update_memory_usage(910);
        assert!(controller.is_active());

        // Drop to 89% - now deactivates
        controller.update_memory_usage(890);
        assert!(!controller.is_active());
    }

    #[test]
    fn test_deactivation_threshold_config() {
        let config = BackpressureConfig {
            memory_limit: 1000,
            queue_limit: 100,
            deactivation_threshold: 90,
            ..Default::default()
        };

        assert_eq!(config.memory_deactivation_bytes(), 900);
        assert_eq!(config.queue_deactivation_count(), 90);
    }
}
