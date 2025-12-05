//! Prioritized sealing for chunk compression
//!
//! This module provides priority-based ordering for chunk sealing operations.
//! Chunks are prioritized based on age, series importance, memory pressure,
//! and other configurable factors.
//!
//! # Priority Levels
//!
//! - **Critical**: Must be sealed immediately (e.g., shutdown, memory emergency)
//! - **High**: Should be sealed soon (e.g., old chunks, high-priority series)
//! - **Normal**: Standard sealing priority
//! - **Low**: Can wait for resources (e.g., small chunks, low-priority series)
//! - **Background**: Seal only when idle (e.g., optimization, compaction)
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::priority_sealing::{PriorityCalculator, PriorityConfig, SealPriority};
//!
//! let config = PriorityConfig::default();
//! let calculator = PriorityCalculator::new(config);
//!
//! // Calculate priority for a chunk
//! let priority = calculator.calculate_priority(&chunk, series_id);
//!
//! // Override priority for specific series
//! calculator.set_series_priority(critical_series_id, SealPriority::High);
//! ```

use crate::storage::ActiveChunk;
use crate::types::SeriesId;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ============================================================================
// Priority Types
// ============================================================================

/// Priority levels for seal operations
///
/// Higher priority chunks are sealed before lower priority ones.
/// Within the same priority level, older tasks are processed first (FIFO).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SealPriority {
    /// Must be sealed immediately (shutdown, memory emergency)
    Critical = 100,
    /// Should be sealed soon (old chunks, important series)
    High = 75,
    /// Standard sealing priority
    #[default]
    Normal = 50,
    /// Can wait for resources
    Low = 25,
    /// Seal only when idle
    Background = 0,
}

impl SealPriority {
    /// Get the numeric priority value (higher = more urgent)
    pub fn value(&self) -> u8 {
        *self as u8
    }

    /// Create priority from a numeric score (0-100)
    pub fn from_score(score: u8) -> Self {
        match score {
            90..=100 => SealPriority::Critical,
            70..=89 => SealPriority::High,
            40..=69 => SealPriority::Normal,
            20..=39 => SealPriority::Low,
            _ => SealPriority::Background,
        }
    }
}

impl PartialOrd for SealPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SealPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for priority calculation
#[derive(Debug, Clone)]
pub struct PriorityConfig {
    /// Age threshold for high priority (chunks older than this get boosted)
    pub high_priority_age: Duration,

    /// Age threshold for critical priority
    pub critical_priority_age: Duration,

    /// Point count threshold for priority boost
    pub high_point_count_threshold: usize,

    /// Memory pressure threshold (0.0-1.0) for priority escalation
    pub memory_pressure_threshold: f64,

    /// Weight for age factor in priority calculation (0.0-1.0)
    pub age_weight: f64,

    /// Weight for point count factor (0.0-1.0)
    pub point_count_weight: f64,

    /// Weight for series priority factor (0.0-1.0)
    pub series_priority_weight: f64,

    /// Enable dynamic priority adjustment based on system load
    pub enable_dynamic_adjustment: bool,
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            high_priority_age: Duration::from_secs(60),
            critical_priority_age: Duration::from_secs(300),
            high_point_count_threshold: 5000,
            memory_pressure_threshold: 0.8,
            age_weight: 0.4,
            point_count_weight: 0.3,
            series_priority_weight: 0.3,
            enable_dynamic_adjustment: true,
        }
    }
}

impl PriorityConfig {
    /// Configuration for latency-sensitive workloads
    pub fn low_latency() -> Self {
        Self {
            high_priority_age: Duration::from_secs(30),
            critical_priority_age: Duration::from_secs(120),
            high_point_count_threshold: 2000,
            memory_pressure_threshold: 0.7,
            age_weight: 0.5,
            point_count_weight: 0.2,
            series_priority_weight: 0.3,
            enable_dynamic_adjustment: true,
        }
    }

    /// Configuration for throughput-optimized workloads
    pub fn high_throughput() -> Self {
        Self {
            high_priority_age: Duration::from_secs(120),
            critical_priority_age: Duration::from_secs(600),
            high_point_count_threshold: 10000,
            memory_pressure_threshold: 0.9,
            age_weight: 0.3,
            point_count_weight: 0.4,
            series_priority_weight: 0.3,
            enable_dynamic_adjustment: true,
        }
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), String> {
        if self.age_weight + self.point_count_weight + self.series_priority_weight > 1.0 {
            return Err("Priority weights must sum to <= 1.0".to_string());
        }
        if self.memory_pressure_threshold < 0.0 || self.memory_pressure_threshold > 1.0 {
            return Err("Memory pressure threshold must be between 0.0 and 1.0".to_string());
        }
        if self.high_priority_age >= self.critical_priority_age {
            return Err("High priority age must be less than critical priority age".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Priority Calculator
// ============================================================================

/// Calculates priority for seal operations based on chunk characteristics
pub struct PriorityCalculator {
    config: PriorityConfig,

    /// Per-series priority overrides
    series_priorities: RwLock<HashMap<SeriesId, SealPriority>>,

    /// Current memory pressure (0.0-1.0)
    memory_pressure: AtomicU64,

    /// Statistics
    calculations_performed: AtomicU64,
}

impl PriorityCalculator {
    /// Create a new priority calculator with the given configuration
    pub fn new(config: PriorityConfig) -> Self {
        Self {
            config,
            series_priorities: RwLock::new(HashMap::new()),
            memory_pressure: AtomicU64::new(0),
            calculations_performed: AtomicU64::new(0),
        }
    }

    /// Calculate priority for a chunk
    ///
    /// Priority is computed based on:
    /// - Chunk age (older = higher priority)
    /// - Point count (more points = higher priority)
    /// - Series priority override (if set)
    /// - Memory pressure (higher pressure = higher priority)
    pub fn calculate_priority(&self, chunk: &ActiveChunk, series_id: SeriesId) -> SealPriority {
        self.calculations_performed
            .fetch_add(1, AtomicOrdering::Relaxed);

        // Check for series priority override first
        if let Some(override_priority) = self.get_series_priority(series_id) {
            return override_priority;
        }

        let mut score: f64 = 0.0;

        // Age factor (0-100)
        let age = chunk.age();
        let age_score = self.calculate_age_score(age);
        score += age_score * self.config.age_weight;

        // Point count factor (0-100)
        let point_count = chunk.point_count() as usize;
        let point_score = self.calculate_point_count_score(point_count);
        score += point_score * self.config.point_count_weight;

        // Memory pressure adjustment
        if self.config.enable_dynamic_adjustment {
            let pressure = self.get_memory_pressure();
            if pressure > self.config.memory_pressure_threshold {
                // Escalate priority under memory pressure
                let pressure_boost = (pressure - self.config.memory_pressure_threshold)
                    / (1.0 - self.config.memory_pressure_threshold);
                score += pressure_boost * 30.0; // Up to 30 point boost
            }
        }

        // Clamp score to 0-100
        let final_score = (score.clamp(0.0, 100.0)) as u8;
        SealPriority::from_score(final_score)
    }

    /// Calculate priority based on series ID and point count only
    ///
    /// This is a simplified version of `calculate_priority` for use cases
    /// where an `ActiveChunk` reference is not available (e.g., parallel sealing).
    /// Priority is based on:
    /// - Point count (more points = higher priority)
    /// - Series priority override (if set)
    /// - Memory pressure (higher pressure = higher priority)
    ///
    /// Note: Age-based calculation is not available in this simplified version.
    pub fn calculate_priority_simple(
        &self,
        series_id: SeriesId,
        point_count: usize,
    ) -> SealPriority {
        self.calculations_performed
            .fetch_add(1, AtomicOrdering::Relaxed);

        // Check for series priority override first
        if let Some(override_priority) = self.get_series_priority(series_id) {
            return override_priority;
        }

        let mut score: f64 = 0.0;

        // Point count factor (0-100)
        let point_score = self.calculate_point_count_score(point_count);
        // Use full weight since we don't have age info
        score += point_score * (self.config.point_count_weight + self.config.age_weight);

        // Memory pressure adjustment
        if self.config.enable_dynamic_adjustment {
            let pressure = self.get_memory_pressure();
            if pressure > self.config.memory_pressure_threshold {
                // Escalate priority under memory pressure
                let pressure_boost = (pressure - self.config.memory_pressure_threshold)
                    / (1.0 - self.config.memory_pressure_threshold);
                score += pressure_boost * 30.0; // Up to 30 point boost
            }
        }

        // Clamp score to 0-100
        let final_score = (score.clamp(0.0, 100.0)) as u8;
        SealPriority::from_score(final_score)
    }

    /// Calculate age-based score (0-100)
    fn calculate_age_score(&self, age: Duration) -> f64 {
        let critical_ms = self.config.critical_priority_age.as_millis() as f64;
        let age_ms = age.as_millis() as f64;

        if age >= self.config.critical_priority_age {
            100.0
        } else if age >= self.config.high_priority_age {
            75.0 + (25.0 * (age_ms / critical_ms))
        } else {
            // Linear scale from 0 to high_priority_age
            let high_ms = self.config.high_priority_age.as_millis() as f64;
            (age_ms / high_ms) * 75.0
        }
    }

    /// Calculate point count-based score (0-100)
    fn calculate_point_count_score(&self, point_count: usize) -> f64 {
        let threshold = self.config.high_point_count_threshold as f64;
        if point_count as f64 >= threshold {
            100.0
        } else {
            (point_count as f64 / threshold) * 100.0
        }
    }

    /// Set priority override for a specific series
    pub fn set_series_priority(&self, series_id: SeriesId, priority: SealPriority) {
        let mut priorities = self.series_priorities.write().unwrap();
        priorities.insert(series_id, priority);
    }

    /// Remove priority override for a series
    pub fn clear_series_priority(&self, series_id: SeriesId) {
        let mut priorities = self.series_priorities.write().unwrap();
        priorities.remove(&series_id);
    }

    /// Get priority override for a series (if any)
    pub fn get_series_priority(&self, series_id: SeriesId) -> Option<SealPriority> {
        let priorities = self.series_priorities.read().unwrap();
        priorities.get(&series_id).copied()
    }

    /// Update current memory pressure (0.0-1.0)
    pub fn set_memory_pressure(&self, pressure: f64) {
        let pressure_bits = pressure.to_bits();
        self.memory_pressure
            .store(pressure_bits, AtomicOrdering::Relaxed);
    }

    /// Get current memory pressure
    pub fn get_memory_pressure(&self) -> f64 {
        let bits = self.memory_pressure.load(AtomicOrdering::Relaxed);
        f64::from_bits(bits)
    }

    /// Adjust priority based on memory pressure
    ///
    /// Escalates priority when memory pressure exceeds threshold
    pub fn adjust_for_memory_pressure(
        &self,
        priority: SealPriority,
        pressure: f64,
    ) -> SealPriority {
        if pressure <= self.config.memory_pressure_threshold {
            return priority;
        }

        // Escalate by one level for each 10% above threshold
        let excess = pressure - self.config.memory_pressure_threshold;
        let levels_to_escalate = (excess / 0.1).ceil() as u8;

        match priority {
            SealPriority::Background => {
                if levels_to_escalate >= 2 {
                    SealPriority::Normal
                } else {
                    SealPriority::Low
                }
            },
            SealPriority::Low => {
                if levels_to_escalate >= 2 {
                    SealPriority::High
                } else {
                    SealPriority::Normal
                }
            },
            SealPriority::Normal => {
                if levels_to_escalate >= 2 {
                    SealPriority::Critical
                } else {
                    SealPriority::High
                }
            },
            SealPriority::High => SealPriority::Critical,
            SealPriority::Critical => SealPriority::Critical,
        }
    }

    /// Get number of priority calculations performed
    pub fn calculations_count(&self) -> u64 {
        self.calculations_performed.load(AtomicOrdering::Relaxed)
    }

    /// Get the configuration
    pub fn config(&self) -> &PriorityConfig {
        &self.config
    }
}

impl Default for PriorityCalculator {
    fn default() -> Self {
        Self::new(PriorityConfig::default())
    }
}

// ============================================================================
// Priority Seal Task
// ============================================================================

/// A seal task with priority information
#[derive(Debug)]
pub struct PrioritySealTask<T> {
    /// The underlying task data
    pub task: T,

    /// Task priority
    pub priority: SealPriority,

    /// Sequence number for FIFO ordering within same priority
    pub sequence: u64,

    /// When the task was created
    pub created_at: Instant,

    /// Series ID for the task
    pub series_id: SeriesId,
}

impl<T> PrioritySealTask<T> {
    /// Create a new priority seal task
    pub fn new(task: T, priority: SealPriority, sequence: u64, series_id: SeriesId) -> Self {
        Self {
            task,
            priority,
            sequence,
            created_at: Instant::now(),
            series_id,
        }
    }

    /// Get the age of this task
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl<T> PartialEq for PrioritySealTask<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<T> Eq for PrioritySealTask<T> {}

impl<T> PartialOrd for PrioritySealTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PrioritySealTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => {
                // For same priority, lower sequence (older) first
                // Reverse because BinaryHeap is a max-heap
                other.sequence.cmp(&self.sequence)
            },
            other_ord => other_ord,
        }
    }
}

// ============================================================================
// Priority Seal Queue
// ============================================================================

/// A priority queue for seal tasks
///
/// Tasks are ordered by priority (highest first), then by insertion order (FIFO).
pub struct PrioritySealQueue<T> {
    /// The priority queue
    heap: RwLock<BinaryHeap<PrioritySealTask<T>>>,

    /// Sequence counter for FIFO ordering
    next_sequence: AtomicU64,

    /// Maximum queue size (0 = unlimited)
    max_size: usize,

    /// Priority calculator for reordering
    calculator: Arc<PriorityCalculator>,

    /// Statistics
    stats: PriorityQueueStats,
}

/// Statistics for the priority queue
#[derive(Debug, Default)]
pub struct PriorityQueueStats {
    /// Total tasks pushed
    pub total_pushed: AtomicU64,
    /// Total tasks popped
    pub total_popped: AtomicU64,
    /// Tasks rejected due to queue full
    pub rejected_full: AtomicU64,
    /// Reorder operations performed
    pub reorders: AtomicU64,
}

impl<T> PrioritySealQueue<T> {
    /// Create a new priority seal queue
    pub fn new(calculator: Arc<PriorityCalculator>, max_size: usize) -> Self {
        Self {
            heap: RwLock::new(BinaryHeap::new()),
            next_sequence: AtomicU64::new(0),
            max_size,
            calculator,
            stats: PriorityQueueStats::default(),
        }
    }

    /// Push a task with the given priority
    ///
    /// Returns `Err` if queue is full
    pub fn push(
        &self,
        task: T,
        priority: SealPriority,
        series_id: SeriesId,
    ) -> Result<(), QueueFullError> {
        let mut heap = self.heap.write().unwrap();

        if self.max_size > 0 && heap.len() >= self.max_size {
            self.stats
                .rejected_full
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Err(QueueFullError {
                max_size: self.max_size,
            });
        }

        let sequence = self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let priority_task = PrioritySealTask::new(task, priority, sequence, series_id);
        heap.push(priority_task);
        self.stats
            .total_pushed
            .fetch_add(1, AtomicOrdering::Relaxed);

        Ok(())
    }

    /// Pop the highest priority task
    pub fn pop(&self) -> Option<PrioritySealTask<T>> {
        let mut heap = self.heap.write().unwrap();
        let task = heap.pop();
        if task.is_some() {
            self.stats
                .total_popped
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        task
    }

    /// Peek at the highest priority task without removing it
    pub fn peek(&self) -> Option<SealPriority> {
        let heap = self.heap.read().unwrap();
        heap.peek().map(|t| t.priority)
    }

    /// Get the current queue length
    pub fn len(&self) -> usize {
        let heap = self.heap.read().unwrap();
        heap.len()
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all tasks from the queue
    pub fn clear(&self) {
        let mut heap = self.heap.write().unwrap();
        heap.clear();
    }

    /// Get queue statistics
    pub fn stats(&self) -> PriorityQueueStatsSnapshot {
        PriorityQueueStatsSnapshot {
            total_pushed: self.stats.total_pushed.load(AtomicOrdering::Relaxed),
            total_popped: self.stats.total_popped.load(AtomicOrdering::Relaxed),
            rejected_full: self.stats.rejected_full.load(AtomicOrdering::Relaxed),
            reorders: self.stats.reorders.load(AtomicOrdering::Relaxed),
            current_size: self.len(),
        }
    }

    /// Get the priority calculator
    pub fn calculator(&self) -> &Arc<PriorityCalculator> {
        &self.calculator
    }
}

impl<T: Send> PrioritySealQueue<T> {
    /// Reorder queue based on current memory pressure
    ///
    /// This recalculates priorities for all tasks based on current conditions.
    /// Expensive operation - use sparingly.
    pub fn reorder_on_pressure(&self, pressure: f64) {
        let mut heap = self.heap.write().unwrap();
        self.stats.reorders.fetch_add(1, AtomicOrdering::Relaxed);

        // Update calculator's memory pressure
        self.calculator.set_memory_pressure(pressure);

        // Drain and reinsert with adjusted priorities
        let tasks: Vec<_> = heap.drain().collect();
        for mut task in tasks {
            task.priority = self
                .calculator
                .adjust_for_memory_pressure(task.priority, pressure);
            heap.push(task);
        }
    }
}

/// Error returned when queue is full
#[derive(Debug, Clone)]
pub struct QueueFullError {
    /// Maximum allowed queue size
    pub max_size: usize,
}

impl std::fmt::Display for QueueFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Queue full (max size: {})", self.max_size)
    }
}

impl std::error::Error for QueueFullError {}

/// Snapshot of queue statistics
#[derive(Debug, Clone)]
pub struct PriorityQueueStatsSnapshot {
    /// Total tasks pushed to queue
    pub total_pushed: u64,
    /// Total tasks popped from queue
    pub total_popped: u64,
    /// Tasks rejected due to queue being full
    pub rejected_full: u64,
    /// Number of reorder operations performed
    pub reorders: u64,
    /// Current queue size
    pub current_size: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::active_chunk::SealConfig;

    // -------------------------------------------------------------------------
    // SealPriority Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_priority_values() {
        assert_eq!(SealPriority::Critical.value(), 100);
        assert_eq!(SealPriority::High.value(), 75);
        assert_eq!(SealPriority::Normal.value(), 50);
        assert_eq!(SealPriority::Low.value(), 25);
        assert_eq!(SealPriority::Background.value(), 0);
    }

    #[test]
    fn test_priority_from_score() {
        assert_eq!(SealPriority::from_score(100), SealPriority::Critical);
        assert_eq!(SealPriority::from_score(95), SealPriority::Critical);
        assert_eq!(SealPriority::from_score(75), SealPriority::High);
        assert_eq!(SealPriority::from_score(50), SealPriority::Normal);
        assert_eq!(SealPriority::from_score(30), SealPriority::Low);
        assert_eq!(SealPriority::from_score(10), SealPriority::Background);
        assert_eq!(SealPriority::from_score(0), SealPriority::Background);
    }

    #[test]
    fn test_priority_ordering() {
        assert!(SealPriority::Critical > SealPriority::High);
        assert!(SealPriority::High > SealPriority::Normal);
        assert!(SealPriority::Normal > SealPriority::Low);
        assert!(SealPriority::Low > SealPriority::Background);
    }

    #[test]
    fn test_priority_default() {
        assert_eq!(SealPriority::default(), SealPriority::Normal);
    }

    // -------------------------------------------------------------------------
    // PriorityConfig Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_config_default() {
        let config = PriorityConfig::default();
        assert_eq!(config.high_priority_age, Duration::from_secs(60));
        assert_eq!(config.critical_priority_age, Duration::from_secs(300));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_low_latency() {
        let config = PriorityConfig::low_latency();
        assert!(config.high_priority_age < PriorityConfig::default().high_priority_age);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_high_throughput() {
        let config = PriorityConfig::high_throughput();
        assert!(config.high_priority_age > PriorityConfig::default().high_priority_age);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_weights() {
        let config = PriorityConfig {
            age_weight: 0.5,
            point_count_weight: 0.5,
            series_priority_weight: 0.5, // Sum > 1.0
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_pressure() {
        let config = PriorityConfig {
            memory_pressure_threshold: 1.5, // > 1.0
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_age_order() {
        let config = PriorityConfig {
            high_priority_age: Duration::from_secs(300),
            critical_priority_age: Duration::from_secs(60),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // -------------------------------------------------------------------------
    // PriorityCalculator Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_calculator_creation() {
        let calculator = PriorityCalculator::new(PriorityConfig::default());
        assert_eq!(calculator.calculations_count(), 0);
        assert_eq!(calculator.get_memory_pressure(), 0.0);
    }

    #[test]
    fn test_calculator_default() {
        let calculator = PriorityCalculator::default();
        assert_eq!(
            calculator.config().high_priority_age,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_priority_calculation() {
        let calculator = PriorityCalculator::new(PriorityConfig::default());

        let seal_config = SealConfig {
            max_points: 10000,
            max_duration_ms: 300_000,
            max_size_bytes: 10 * 1024 * 1024,
        };
        let chunk = ActiveChunk::new(1, 10000, seal_config);

        let priority = calculator.calculate_priority(&chunk, 1);
        assert_eq!(calculator.calculations_count(), 1);

        // New chunk with no points should have low priority
        assert!(priority <= SealPriority::Normal);
    }

    #[test]
    fn test_series_priority_override() {
        let calculator = PriorityCalculator::new(PriorityConfig::default());

        // Set override
        calculator.set_series_priority(42, SealPriority::Critical);
        assert_eq!(
            calculator.get_series_priority(42),
            Some(SealPriority::Critical)
        );

        // Clear override
        calculator.clear_series_priority(42);
        assert_eq!(calculator.get_series_priority(42), None);
    }

    #[test]
    fn test_memory_pressure_adjustment() {
        let calculator = PriorityCalculator::new(PriorityConfig::default());

        // Low pressure - no change
        let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Normal, 0.5);
        assert_eq!(adjusted, SealPriority::Normal);

        // High pressure - escalate
        let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Normal, 0.95);
        assert!(adjusted > SealPriority::Normal);
    }

    #[test]
    fn test_memory_pressure_set_get() {
        let calculator = PriorityCalculator::new(PriorityConfig::default());

        calculator.set_memory_pressure(0.75);
        let pressure = calculator.get_memory_pressure();
        assert!((pressure - 0.75).abs() < 0.001);
    }

    // -------------------------------------------------------------------------
    // PrioritySealTask Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_task_creation() {
        let task = PrioritySealTask::new("test", SealPriority::High, 1, 42);
        assert_eq!(task.priority, SealPriority::High);
        assert_eq!(task.sequence, 1);
        assert_eq!(task.series_id, 42);
    }

    #[test]
    fn test_task_ordering_by_priority() {
        let task1 = PrioritySealTask::new("low", SealPriority::Low, 1, 1);
        let task2 = PrioritySealTask::new("high", SealPriority::High, 2, 2);

        // Higher priority should come first
        assert!(task2 > task1);
    }

    #[test]
    fn test_task_ordering_same_priority_fifo() {
        let task1 = PrioritySealTask::new("first", SealPriority::Normal, 1, 1);
        let task2 = PrioritySealTask::new("second", SealPriority::Normal, 2, 2);

        // Same priority, lower sequence (older) should come first
        assert!(task1 > task2);
    }

    #[test]
    fn test_task_age() {
        let task = PrioritySealTask::new("test", SealPriority::Normal, 1, 1);
        std::thread::sleep(Duration::from_millis(10));
        assert!(task.age() >= Duration::from_millis(10));
    }

    // -------------------------------------------------------------------------
    // PrioritySealQueue Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_queue_push() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<String> = PrioritySealQueue::new(calculator, 100);

        assert!(queue
            .push("task1".to_string(), SealPriority::Normal, 1)
            .is_ok());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_queue_pop_order() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<&str> = PrioritySealQueue::new(calculator, 100);

        queue.push("low", SealPriority::Low, 1).unwrap();
        queue.push("high", SealPriority::High, 2).unwrap();
        queue.push("normal", SealPriority::Normal, 3).unwrap();

        // Should pop in priority order: high, normal, low
        assert_eq!(queue.pop().unwrap().task, "high");
        assert_eq!(queue.pop().unwrap().task, "normal");
        assert_eq!(queue.pop().unwrap().task, "low");
    }

    #[test]
    fn test_queue_fifo_same_priority() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<&str> = PrioritySealQueue::new(calculator, 100);

        queue.push("first", SealPriority::Normal, 1).unwrap();
        queue.push("second", SealPriority::Normal, 2).unwrap();
        queue.push("third", SealPriority::Normal, 3).unwrap();

        // Same priority - should be FIFO
        assert_eq!(queue.pop().unwrap().task, "first");
        assert_eq!(queue.pop().unwrap().task, "second");
        assert_eq!(queue.pop().unwrap().task, "third");
    }

    #[test]
    fn test_queue_peek() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<&str> = PrioritySealQueue::new(calculator, 100);

        assert!(queue.peek().is_none());

        queue.push("task", SealPriority::High, 1).unwrap();
        assert_eq!(queue.peek(), Some(SealPriority::High));
        assert_eq!(queue.len(), 1); // Peek doesn't remove
    }

    #[test]
    fn test_queue_len() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<i32> = PrioritySealQueue::new(calculator, 100);

        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        queue.push(1, SealPriority::Normal, 1).unwrap();
        queue.push(2, SealPriority::Normal, 2).unwrap();
        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());
    }

    #[test]
    fn test_queue_max_size() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<i32> = PrioritySealQueue::new(calculator, 2);

        assert!(queue.push(1, SealPriority::Normal, 1).is_ok());
        assert!(queue.push(2, SealPriority::Normal, 2).is_ok());
        assert!(queue.push(3, SealPriority::Normal, 3).is_err()); // Should fail
    }

    #[test]
    fn test_queue_clear() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<i32> = PrioritySealQueue::new(calculator, 100);

        queue.push(1, SealPriority::Normal, 1).unwrap();
        queue.push(2, SealPriority::Normal, 2).unwrap();
        assert_eq!(queue.len(), 2);

        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_stats() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<i32> = PrioritySealQueue::new(calculator, 2);

        queue.push(1, SealPriority::Normal, 1).unwrap();
        queue.push(2, SealPriority::Normal, 2).unwrap();
        let _ = queue.push(3, SealPriority::Normal, 3); // Rejected

        queue.pop();

        let stats = queue.stats();
        assert_eq!(stats.total_pushed, 2);
        assert_eq!(stats.total_popped, 1);
        assert_eq!(stats.rejected_full, 1);
        assert_eq!(stats.current_size, 1);
    }

    #[test]
    fn test_queue_reorder() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<&str> = PrioritySealQueue::new(calculator, 100);

        queue.push("task1", SealPriority::Low, 1).unwrap();
        queue.push("task2", SealPriority::Normal, 2).unwrap();

        // Reorder with high pressure - should escalate priorities
        queue.reorder_on_pressure(0.95);

        let stats = queue.stats();
        assert_eq!(stats.reorders, 1);
    }

    #[test]
    fn test_empty_queue_pop() {
        let calculator = Arc::new(PriorityCalculator::default());
        let queue: PrioritySealQueue<i32> = PrioritySealQueue::new(calculator, 100);

        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_concurrent_priority_updates() {
        use std::thread;

        let calculator = Arc::new(PriorityCalculator::default());
        let calc1 = Arc::clone(&calculator);
        let calc2 = Arc::clone(&calculator);

        let handle1 = thread::spawn(move || {
            for i in 0..100 {
                calc1.set_series_priority(i, SealPriority::High);
            }
        });

        let handle2 = thread::spawn(move || {
            for i in 0..100 {
                calc2.set_series_priority(i + 100, SealPriority::Low);
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Should have both sets of priorities
        assert_eq!(calculator.get_series_priority(50), Some(SealPriority::High));
        assert_eq!(calculator.get_series_priority(150), Some(SealPriority::Low));
    }
}
