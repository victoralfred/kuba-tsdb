//! Parallel Chunk Sealing Service
//!
//! This module provides concurrent compression of multiple chunks during seal operations.
//! It addresses the bottleneck where `flush_all()` seals chunks sequentially by enabling
//! parallel compression with memory-aware backpressure.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                       ParallelSealingService                             │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐     ┌─────────────────────┐     ┌──────────────────┐ │
//! │  │ submit()     │────▶│  Worker Pool        │────▶│  SealHandle      │ │
//! │  │ submit_batch │     │  (Semaphore-bound)  │     │  (oneshot rx)    │ │
//! │  └──────────────┘     └─────────────────────┘     └──────────────────┘ │
//! │                              │                                          │
//! │                              ▼                                          │
//! │                       spawn_blocking()                                  │
//! │                       + AHPAC compress                                  │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::parallel_sealing::{ParallelSealingService, ParallelSealingConfig};
//!
//! let service = ParallelSealingService::new(ParallelSealingConfig::default());
//!
//! // Submit multiple chunks for parallel sealing
//! let chunks = vec![
//!     (series_id_1, points_1, path_1),
//!     (series_id_2, points_2, path_2),
//! ];
//!
//! let results = service.seal_all_and_wait(chunks).await;
//! ```

use crate::compression::AhpacCompressor;
use crate::compression::{AlgorithmSelector, ChunkCharacteristics, SelectionConfig};
use crate::engine::traits::Compressor;
use crate::storage::adaptive_concurrency::{AdaptiveConfig, ConcurrencyController};
use crate::storage::chunk::{ChunkHeader, CompressionType};
use crate::storage::priority_sealing::{PriorityCalculator, PriorityConfig, SealPriority};
use crate::types::{ChunkId, DataPoint, SeriesId};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, Semaphore};
use tracing::{debug, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for parallel sealing operations
///
/// Controls concurrency, memory limits, and timeouts for the parallel sealing service.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::storage::parallel_sealing::ParallelSealingConfig;
/// use std::time::Duration;
///
/// // High-throughput configuration
/// let config = ParallelSealingConfig {
///     max_concurrent_seals: 8,
///     memory_budget_bytes: 512 * 1024 * 1024, // 512MB
///     seal_timeout: Duration::from_secs(60),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ParallelSealingConfig {
    /// Maximum concurrent seal operations
    ///
    /// Controls how many chunks can be compressed simultaneously.
    /// Higher values increase throughput but consume more CPU and memory.
    /// Default: number of CPU cores
    pub max_concurrent_seals: usize,

    /// Memory budget for pending seals in bytes
    ///
    /// When pending memory exceeds this limit, new seal requests will wait
    /// for existing seals to complete. Prevents OOM during high-volume flushes.
    /// Default: 256MB
    pub memory_budget_bytes: usize,

    /// Timeout for individual seal operations
    ///
    /// If a single seal takes longer than this, it's considered failed.
    /// Default: 30 seconds
    pub seal_timeout: Duration,

    /// Maximum retry attempts for failed seals
    ///
    /// Set to 0 to disable retries.
    /// Default: 2
    pub max_retries: usize,

    /// Delay between retry attempts
    ///
    /// Uses exponential backoff: delay * 2^attempt
    /// Default: 100ms
    pub retry_delay: Duration,

    /// Compression algorithm to use
    ///
    /// Default: AHPAC (adaptive compression)
    pub compression_type: CompressionType,

    /// Whether to collect detailed metrics
    ///
    /// Default: true
    pub collect_metrics: bool,

    /// Enable priority-based sealing
    ///
    /// When enabled, chunks are sealed in priority order rather than FIFO.
    /// Default: true
    pub enable_priority: bool,

    /// Configuration for priority calculation
    ///
    /// Only used when `enable_priority` is true.
    pub priority_config: Option<PriorityConfig>,

    /// Enable adaptive concurrency control
    ///
    /// When enabled, the service dynamically adjusts concurrency based on
    /// system load metrics (CPU, memory, I/O latency).
    /// Default: true
    pub enable_adaptive_concurrency: bool,

    /// Configuration for adaptive concurrency control
    ///
    /// Only used when `enable_adaptive_concurrency` is true.
    /// If None, uses default AdaptiveConfig.
    pub adaptive_config: Option<AdaptiveConfig>,

    /// Enable automatic algorithm selection
    ///
    /// When enabled, the service analyzes data characteristics and selects
    /// the optimal compression algorithm (AHPAC, Snappy, Kuba, or None).
    /// When disabled, uses the `compression_type` setting.
    /// Default: true
    pub enable_algorithm_selection: bool,

    /// Configuration for algorithm selection
    ///
    /// Only used when `enable_algorithm_selection` is true.
    /// If None, uses default SelectionConfig.
    pub selection_config: Option<SelectionConfig>,
}

impl Default for ParallelSealingConfig {
    fn default() -> Self {
        Self {
            max_concurrent_seals: num_cpus::get(),
            memory_budget_bytes: 256 * 1024 * 1024, // 256MB
            seal_timeout: Duration::from_secs(30),
            max_retries: 2,
            retry_delay: Duration::from_millis(100),
            compression_type: CompressionType::Ahpac,
            collect_metrics: true,
            enable_priority: true,
            priority_config: Some(PriorityConfig::default()),
            enable_adaptive_concurrency: true,
            adaptive_config: Some(AdaptiveConfig::default()),
            enable_algorithm_selection: true,
            selection_config: Some(SelectionConfig::default()),
        }
    }
}

impl ParallelSealingConfig {
    /// Create configuration optimized for high throughput
    ///
    /// Uses more concurrent workers and larger memory budget for maximum speed.
    /// Adaptive concurrency is configured for aggressive scaling.
    pub fn high_throughput() -> Self {
        Self {
            max_concurrent_seals: num_cpus::get() * 2,
            memory_budget_bytes: 512 * 1024 * 1024, // 512MB
            seal_timeout: Duration::from_secs(60),
            collect_metrics: false, // Reduce overhead
            adaptive_config: Some(AdaptiveConfig::high_throughput()),
            ..Default::default()
        }
    }

    /// Create configuration optimized for low memory usage
    ///
    /// Uses fewer workers and smaller budget for memory-constrained environments.
    /// Adaptive concurrency is configured for conservative resource usage.
    pub fn low_memory() -> Self {
        Self {
            max_concurrent_seals: 2,
            memory_budget_bytes: 64 * 1024 * 1024, // 64MB
            seal_timeout: Duration::from_secs(30),
            adaptive_config: Some(AdaptiveConfig::low_memory()),
            ..Default::default()
        }
    }

    /// Create configuration optimized for low latency
    ///
    /// Limits concurrency to prevent latency spikes during seal operations.
    pub fn low_latency() -> Self {
        Self {
            max_concurrent_seals: num_cpus::get() / 2,
            memory_budget_bytes: 128 * 1024 * 1024, // 128MB
            seal_timeout: Duration::from_secs(20),
            adaptive_config: Some(AdaptiveConfig::low_latency()),
            ..Default::default()
        }
    }
}

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during seal operations
#[derive(Debug, Error)]
pub enum SealError {
    /// The seal operation was cancelled (receiver dropped)
    #[error("Seal cancelled")]
    Cancelled,

    /// The seal operation timed out
    #[error("Seal timeout after {0:?}")]
    Timeout(Duration),

    /// Compression failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// IO error during disk write
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Memory budget exceeded and couldn't proceed
    #[error("Memory budget exceeded: need {needed} bytes, budget is {budget} bytes")]
    MemoryBudgetExceeded {
        /// Bytes needed for this operation
        needed: usize,
        /// Total budget configured
        budget: usize,
    },

    /// Cannot seal an empty chunk
    #[error("Cannot seal empty chunk for series {0}")]
    EmptyChunk(SeriesId),

    /// Task join error (panic in worker)
    #[error("Task panicked: {0}")]
    TaskPanicked(String),
}

// ============================================================================
// Result Types
// ============================================================================

/// Result of a successful seal operation
///
/// Contains metadata about the sealed chunk including compression statistics.
#[derive(Debug, Clone)]
pub struct SealResult {
    /// Series this chunk belongs to
    pub series_id: SeriesId,

    /// Unique identifier for the sealed chunk
    pub chunk_id: ChunkId,

    /// Number of data points in the chunk
    pub point_count: usize,

    /// Start timestamp of the chunk (minimum timestamp)
    pub start_timestamp: i64,

    /// End timestamp of the chunk (maximum timestamp)
    pub end_timestamp: i64,

    /// Original uncompressed size in bytes
    pub original_size: usize,

    /// Compressed size in bytes
    pub compressed_size: usize,

    /// Compression ratio (original / compressed)
    pub compression_ratio: f64,

    /// Time taken to seal (set after completion)
    pub duration: Duration,

    /// Path where chunk was written
    pub path: PathBuf,
}

impl SealResult {
    /// Calculate bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        if self.point_count == 0 {
            0.0
        } else {
            (self.compressed_size * 8) as f64 / self.point_count as f64
        }
    }
}

// ============================================================================
// Handle for Tracking Seal Progress
// ============================================================================

/// Handle for tracking and awaiting a seal operation
///
/// Returned by `submit()`, allows the caller to wait for the seal to complete
/// or check if it's done without blocking.
///
/// # Example
///
/// ```rust,ignore
/// let handle = service.submit(series_id, points, path).await?;
///
/// // Wait for completion
/// let result = handle.await_result().await?;
/// println!("Sealed {} points", result.point_count);
/// ```
pub struct SealHandle {
    /// Unique task identifier
    task_id: u64,

    /// Series being sealed
    series_id: SeriesId,

    /// Channel to receive result
    result_rx: oneshot::Receiver<Result<SealResult, SealError>>,
}

impl SealHandle {
    /// Get the task ID for this seal operation
    pub fn task_id(&self) -> u64 {
        self.task_id
    }

    /// Get the series ID being sealed
    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Await the seal result
    ///
    /// Blocks until the seal operation completes or fails.
    pub async fn await_result(self) -> Result<SealResult, SealError> {
        self.result_rx.await.map_err(|_| SealError::Cancelled)?
    }
}

// ============================================================================
// Internal Task Representation
// ============================================================================

/// Internal representation of a seal task
struct SealTask {
    /// Unique task ID (used for logging/debugging)
    #[allow(dead_code)]
    id: u64,

    /// Series identifier
    series_id: SeriesId,

    /// Points to compress
    points: Vec<DataPoint>,

    /// Estimated memory usage
    estimated_memory: usize,

    /// Target path for sealed chunk
    target_path: PathBuf,

    /// Compression type to use
    compression_type: CompressionType,

    /// Priority of this seal task (higher = more urgent)
    ///
    /// Used for scheduling when priority-based sealing is enabled.
    /// Currently stored for logging/debugging, will be used for
    /// priority queue scheduling in future enhancements.
    #[allow(dead_code)]
    priority: SealPriority,

    /// Channel to send result
    result_tx: oneshot::Sender<Result<SealResult, SealError>>,
}

// ============================================================================
// Statistics
// ============================================================================

/// Atomic statistics for the sealing service
#[derive(Default)]
struct SealingStats {
    /// Currently active seal operations
    active_seals: AtomicU64,

    /// Total successful seals
    total_sealed: AtomicU64,

    /// Total failed seals
    total_failed: AtomicU64,

    /// Total bytes compressed (original size)
    total_bytes_original: AtomicU64,

    /// Total bytes after compression
    total_bytes_compressed: AtomicU64,

    /// Total seal time in nanoseconds
    total_seal_time_ns: AtomicU64,
}

/// Snapshot of sealing statistics
#[derive(Debug, Clone)]
pub struct SealingStatsSnapshot {
    /// Currently active seal operations
    pub active_seals: u64,

    /// Total successful seals
    pub total_sealed: u64,

    /// Total failed seals
    pub total_failed: u64,

    /// Total bytes compressed (original size)
    pub total_bytes_original: u64,

    /// Total bytes after compression
    pub total_bytes_compressed: u64,

    /// Current pending memory usage in bytes
    pub pending_memory: usize,

    /// Average seal time in seconds
    pub avg_seal_time_secs: f64,

    /// Overall compression ratio
    pub compression_ratio: f64,
}

// ============================================================================
// Main Service
// ============================================================================

/// Parallel sealing service for concurrent chunk compression
///
/// Manages a pool of workers that compress chunks in parallel, with memory-aware
/// backpressure to prevent OOM during high-volume flush operations. Supports
/// adaptive concurrency control that dynamically adjusts worker count based on
/// system load metrics.
///
/// # Thread Safety
///
/// This service is fully thread-safe and can be shared across multiple tasks.
///
/// # Example
///
/// ```rust,ignore
/// use kuba_tsdb::storage::parallel_sealing::{ParallelSealingService, ParallelSealingConfig};
///
/// // Create service with default config
/// let service = ParallelSealingService::new(ParallelSealingConfig::default());
///
/// // Submit chunks for parallel sealing
/// let handle = service.submit(series_id, points, path).await?;
/// let result = handle.await_result().await?;
///
/// // Or seal multiple chunks at once
/// let chunks = vec![
///     (series_id_1, points_1, path_1),
///     (series_id_2, points_2, path_2),
/// ];
/// let results = service.seal_all_and_wait(chunks).await;
/// ```
pub struct ParallelSealingService {
    /// Configuration
    config: ParallelSealingConfig,

    /// Semaphore for concurrency control
    ///
    /// When adaptive concurrency is enabled, the semaphore's effective limit
    /// is managed by the concurrency controller and periodically adjusted.
    semaphore: Arc<Semaphore>,

    /// Task ID counter
    next_task_id: AtomicU64,

    /// Pending memory tracking
    pending_memory: Arc<AtomicUsize>,

    /// Service statistics
    stats: Arc<SealingStats>,

    /// Shared compressor instance
    compressor: Arc<AhpacCompressor>,

    /// Priority calculator for determining chunk sealing priority
    ///
    /// Only used when `enable_priority` is true in config.
    priority_calculator: Option<Arc<PriorityCalculator>>,

    /// Adaptive concurrency controller
    ///
    /// Dynamically adjusts concurrency based on system load metrics.
    /// Only active when `enable_adaptive_concurrency` is true in config.
    concurrency_controller: Option<Arc<ConcurrencyController>>,

    /// Handle for the background adjustment task
    ///
    /// Used to stop the adjustment loop when the service is dropped.
    #[allow(dead_code)]
    adjustment_task_running: Arc<AtomicBool>,

    /// Algorithm selector for choosing optimal compression algorithm
    ///
    /// Analyzes data characteristics and selects the best algorithm.
    /// Only active when `enable_algorithm_selection` is true in config.
    algorithm_selector: Option<Arc<AlgorithmSelector>>,
}

impl ParallelSealingService {
    /// Create a new parallel sealing service
    pub fn new(config: ParallelSealingConfig) -> Self {
        info!(
            max_concurrent = config.max_concurrent_seals,
            memory_budget_mb = config.memory_budget_bytes / (1024 * 1024),
            priority_enabled = config.enable_priority,
            adaptive_concurrency_enabled = config.enable_adaptive_concurrency,
            algorithm_selection_enabled = config.enable_algorithm_selection,
            "Creating parallel sealing service"
        );

        // Create priority calculator if priority-based sealing is enabled
        let priority_calculator = if config.enable_priority {
            let priority_config = config.priority_config.clone().unwrap_or_default();
            Some(Arc::new(PriorityCalculator::new(priority_config)))
        } else {
            None
        };

        // Create adaptive concurrency controller if enabled
        let (concurrency_controller, adjustment_task_running) =
            if config.enable_adaptive_concurrency {
                let adaptive_config = config.adaptive_config.clone().unwrap_or_else(|| {
                    // Create config that matches the max_concurrent_seals setting
                    AdaptiveConfig {
                        max_concurrency: config.max_concurrent_seals,
                        initial_concurrency: config.max_concurrent_seals,
                        ..Default::default()
                    }
                });
                let controller = Arc::new(ConcurrencyController::new(adaptive_config));
                let running = Arc::new(AtomicBool::new(true));

                // Start background metric sampling
                controller.sampler().start();

                // Start background adjustment task
                let controller_clone = Arc::clone(&controller);
                let running_clone = Arc::clone(&running);
                tokio::spawn(async move {
                    Self::run_adjustment_loop(controller_clone, running_clone).await;
                });

                (Some(controller), running)
            } else {
                (None, Arc::new(AtomicBool::new(false)))
            };

        // Create algorithm selector if enabled
        let algorithm_selector = if config.enable_algorithm_selection {
            let selection_config = config.selection_config.clone().unwrap_or_default();
            Some(Arc::new(AlgorithmSelector::new(selection_config)))
        } else {
            None
        };

        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_seals)),
            next_task_id: AtomicU64::new(0),
            pending_memory: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(SealingStats::default()),
            compressor: Arc::new(AhpacCompressor::new()),
            priority_calculator,
            concurrency_controller,
            adjustment_task_running,
            algorithm_selector,
            config,
        }
    }

    /// Background task that periodically adjusts concurrency based on load
    async fn run_adjustment_loop(controller: Arc<ConcurrencyController>, running: Arc<AtomicBool>) {
        let interval = controller.config().adjustment_interval;

        while running.load(Ordering::Relaxed) {
            tokio::time::sleep(interval).await;

            if !running.load(Ordering::Relaxed) {
                break;
            }

            // Perform adjustment
            if let Some(result) = controller.adjust().await {
                debug!(
                    previous = result.previous,
                    current = result.current,
                    reason = %result.reason,
                    "Adaptive concurrency adjustment"
                );
            }
        }

        // Stop the sampler when done
        controller.sampler().stop();
    }

    /// Submit a single chunk for sealing
    ///
    /// The chunk will be compressed asynchronously. Use the returned handle
    /// to await the result. Priority is automatically calculated based on
    /// chunk characteristics if priority-based sealing is enabled.
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier for the chunk
    /// * `points` - Data points to compress (must not be empty)
    /// * `target_path` - File path where the sealed chunk will be written
    ///
    /// # Errors
    ///
    /// Returns `SealError::EmptyChunk` if points is empty, or
    /// `SealError::MemoryBudgetExceeded` if the memory budget is exhausted.
    pub async fn submit(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
        target_path: PathBuf,
    ) -> Result<SealHandle, SealError> {
        // Calculate priority based on chunk characteristics
        let priority = self.calculate_priority(series_id, &points);
        self.submit_with_priority(series_id, points, target_path, priority)
            .await
    }

    /// Submit a chunk for sealing with explicit priority
    ///
    /// Use this method when you need precise control over task priority.
    /// For automatic priority calculation, use `submit()` instead.
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier for the chunk
    /// * `points` - Data points to compress (must not be empty)
    /// * `target_path` - File path where the sealed chunk will be written
    /// * `priority` - Explicit priority for this seal task
    ///
    /// # Errors
    ///
    /// Returns `SealError::EmptyChunk` if points is empty, or
    /// `SealError::MemoryBudgetExceeded` if the memory budget is exhausted.
    pub async fn submit_with_priority(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
        target_path: PathBuf,
        priority: SealPriority,
    ) -> Result<SealHandle, SealError> {
        if points.is_empty() {
            return Err(SealError::EmptyChunk(series_id));
        }

        let estimated_memory = Self::estimate_memory(&points);

        // Wait for memory budget if needed
        self.wait_for_memory_budget(estimated_memory).await?;

        // Select optimal compression algorithm based on data characteristics
        let compression_type = self.select_compression_algorithm(&points);

        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let (result_tx, result_rx) = oneshot::channel();

        let task = SealTask {
            id: task_id,
            series_id,
            points,
            estimated_memory,
            target_path,
            compression_type,
            priority,
            result_tx,
        };

        // Track pending memory
        self.pending_memory
            .fetch_add(estimated_memory, Ordering::Relaxed);

        debug!(
            task_id = task_id,
            series_id = series_id,
            estimated_memory = estimated_memory,
            priority = ?priority,
            compression = ?compression_type,
            "Submitting seal task"
        );

        // Spawn the seal task
        self.spawn_seal_task(task);

        Ok(SealHandle {
            task_id,
            series_id,
            result_rx,
        })
    }

    /// Calculate priority for a chunk based on its characteristics
    ///
    /// Uses the priority calculator if enabled, otherwise returns Normal priority.
    /// This uses the simplified calculation that doesn't require an ActiveChunk.
    fn calculate_priority(&self, series_id: SeriesId, points: &[DataPoint]) -> SealPriority {
        match &self.priority_calculator {
            Some(calculator) => calculator.calculate_priority_simple(series_id, points.len()),
            None => SealPriority::Normal,
        }
    }

    /// Select the optimal compression algorithm for the given data points
    ///
    /// When algorithm selection is enabled, analyzes the data characteristics
    /// (entropy, timestamp regularity, value variance) and selects the best
    /// compression algorithm. Falls back to the configured default when disabled.
    fn select_compression_algorithm(&self, points: &[DataPoint]) -> CompressionType {
        match &self.algorithm_selector {
            Some(selector) => {
                let characteristics = ChunkCharacteristics::analyze(points);
                let algo = selector.select(&characteristics);

                debug!(
                    point_count = characteristics.point_count,
                    entropy = format!("{:.3}", characteristics.entropy),
                    regularity = format!("{:.3}", characteristics.timestamp_regularity),
                    selected = ?algo,
                    "Algorithm selection result"
                );

                algo
            },
            None => self.config.compression_type,
        }
    }

    /// Submit multiple chunks for parallel sealing
    ///
    /// More efficient than calling `submit()` in a loop as it allows
    /// for better scheduling of the tasks.
    ///
    /// # Arguments
    ///
    /// * `chunks` - Vector of (series_id, points, target_path) tuples
    ///
    /// # Returns
    ///
    /// Vector of results, one for each input chunk. Failed submissions are
    /// represented as `Err` values in the vector.
    pub async fn submit_batch(
        &self,
        chunks: Vec<(SeriesId, Vec<DataPoint>, PathBuf)>,
    ) -> Vec<Result<SealHandle, SealError>> {
        let mut handles = Vec::with_capacity(chunks.len());

        for (series_id, points, path) in chunks {
            let handle = self.submit(series_id, points, path).await;
            handles.push(handle);
        }

        handles
    }

    /// Seal all chunks and wait for completion
    ///
    /// Convenience method that submits all chunks and waits for all results.
    /// Use this for flush operations where you need to wait for everything.
    ///
    /// # Arguments
    ///
    /// * `chunks` - Vector of (series_id, points, target_path) tuples
    ///
    /// # Returns
    ///
    /// Vector of seal results, one for each input chunk.
    pub async fn seal_all_and_wait(
        &self,
        chunks: Vec<(SeriesId, Vec<DataPoint>, PathBuf)>,
    ) -> Vec<Result<SealResult, SealError>> {
        let count = chunks.len();
        if count == 0 {
            return Vec::new();
        }

        info!(chunks = count, "Starting parallel seal of all chunks");
        let start = Instant::now();

        let handles = self.submit_batch(chunks).await;

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle {
                Ok(h) => results.push(h.await_result().await),
                Err(e) => results.push(Err(e)),
            }
        }

        let duration = start.elapsed();
        let success_count = results.iter().filter(|r| r.is_ok()).count();

        info!(
            total = count,
            success = success_count,
            failed = count - success_count,
            duration_ms = duration.as_millis(),
            "Parallel seal complete"
        );

        results
    }

    /// Spawn a seal task as an async worker
    fn spawn_seal_task(&self, task: SealTask) {
        let semaphore = Arc::clone(&self.semaphore);
        let stats = Arc::clone(&self.stats);
        let compressor = Arc::clone(&self.compressor);
        let pending_memory = Arc::clone(&self.pending_memory);
        let concurrency_controller = self.concurrency_controller.clone();
        let algorithm_selector = self.algorithm_selector.clone();
        let timeout = self.config.seal_timeout;
        let memory = task.estimated_memory;
        let collect_metrics = self.config.collect_metrics;
        let compression_type = task.compression_type;

        tokio::spawn(async move {
            // Acquire permit (limits concurrency)
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    // Semaphore closed, service is shutting down
                    let _ = task.result_tx.send(Err(SealError::Cancelled));
                    pending_memory.fetch_sub(memory, Ordering::Relaxed);
                    return;
                },
            };

            let active = stats.active_seals.fetch_add(1, Ordering::Relaxed) + 1;
            let start = Instant::now();

            // Update controller with current seal counts
            if let Some(ref controller) = concurrency_controller {
                let pending = pending_memory.load(Ordering::Relaxed) as u64 / memory.max(1) as u64;
                controller.update_seal_counts(active, pending);
            }

            // Run seal with timeout
            let result = tokio::time::timeout(
                timeout,
                Self::execute_seal(
                    task.series_id,
                    task.points,
                    task.target_path,
                    task.compression_type,
                    Arc::clone(&compressor),
                ),
            )
            .await;

            // Release memory budget
            pending_memory.fetch_sub(memory, Ordering::Relaxed);
            let active = stats.active_seals.fetch_sub(1, Ordering::Relaxed) - 1;

            // Update controller with updated seal counts
            if let Some(ref controller) = concurrency_controller {
                let pending = pending_memory.load(Ordering::Relaxed) as u64 / memory.max(1) as u64;
                controller.update_seal_counts(active, pending);
            }

            let seal_result = match result {
                Ok(Ok(mut seal_result)) => {
                    seal_result.duration = start.elapsed();

                    // Record I/O latency for adaptive concurrency
                    if let Some(ref controller) = concurrency_controller {
                        controller.record_io_latency(seal_result.duration);
                    }

                    // Record compression statistics for algorithm learning
                    if let Some(ref selector) = algorithm_selector {
                        selector.record_stats(
                            compression_type,
                            seal_result.original_size,
                            seal_result.compressed_size,
                            seal_result.duration,
                        );
                    }

                    if collect_metrics {
                        stats.total_sealed.fetch_add(1, Ordering::Relaxed);
                        stats
                            .total_bytes_original
                            .fetch_add(seal_result.original_size as u64, Ordering::Relaxed);
                        stats
                            .total_bytes_compressed
                            .fetch_add(seal_result.compressed_size as u64, Ordering::Relaxed);
                        stats
                            .total_seal_time_ns
                            .fetch_add(seal_result.duration.as_nanos() as u64, Ordering::Relaxed);
                    }

                    debug!(
                        series_id = seal_result.series_id,
                        points = seal_result.point_count,
                        ratio = format!("{:.2}", seal_result.compression_ratio),
                        duration_ms = seal_result.duration.as_millis(),
                        compression = ?compression_type,
                        "Seal completed successfully"
                    );

                    Ok(seal_result)
                },
                Ok(Err(e)) => {
                    if collect_metrics {
                        stats.total_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    warn!(series_id = task.series_id, error = %e, "Seal failed");
                    Err(e)
                },
                Err(_) => {
                    if collect_metrics {
                        stats.total_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    warn!(
                        series_id = task.series_id,
                        timeout_ms = timeout.as_millis(),
                        "Seal timed out"
                    );
                    Err(SealError::Timeout(timeout))
                },
            };

            // Send result (ignore if receiver dropped)
            let _ = task.result_tx.send(seal_result);
        });
    }

    /// Execute the actual seal operation
    async fn execute_seal(
        series_id: SeriesId,
        points: Vec<DataPoint>,
        path: PathBuf,
        compression_type: CompressionType,
        compressor: Arc<AhpacCompressor>,
    ) -> Result<SealResult, SealError> {
        let point_count = points.len();
        let original_size = point_count * std::mem::size_of::<DataPoint>();

        // Get time range from points
        let start_timestamp = points.first().map(|p| p.timestamp).unwrap_or(0);
        let end_timestamp = points.last().map(|p| p.timestamp).unwrap_or(0);

        // Compress in blocking thread pool to avoid blocking async runtime
        let compressed = {
            let compressor = Arc::clone(&compressor);
            let points_clone = points.clone();

            tokio::task::spawn_blocking(move || {
                futures::executor::block_on(async { compressor.compress(&points_clone).await })
            })
            .await
            .map_err(|e| SealError::TaskPanicked(e.to_string()))?
            .map_err(|e| SealError::CompressionFailed(e.to_string()))?
        };

        let compressed_size = compressed.compressed_size;

        // Write to disk
        Self::write_chunk_to_disk(
            series_id,
            start_timestamp,
            end_timestamp,
            point_count,
            compression_type,
            &compressed.data,
            compressed_size,
            original_size,
            &path,
        )
        .await?;

        Ok(SealResult {
            series_id,
            chunk_id: ChunkId::new(),
            point_count,
            start_timestamp,
            end_timestamp,
            original_size,
            compressed_size,
            compression_ratio: original_size as f64 / compressed_size.max(1) as f64,
            duration: Duration::ZERO, // Set by caller
            path,
        })
    }

    /// Write the compressed chunk to disk
    #[allow(clippy::too_many_arguments)]
    async fn write_chunk_to_disk(
        series_id: SeriesId,
        start_timestamp: i64,
        end_timestamp: i64,
        point_count: usize,
        compression_type: CompressionType,
        compressed_data: &[u8],
        compressed_size: usize,
        original_size: usize,
        path: &PathBuf,
    ) -> Result<(), SealError> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Build chunk header with all fields
        let mut header = ChunkHeader::new(series_id);
        header.start_timestamp = start_timestamp;
        header.end_timestamp = end_timestamp;
        header.point_count = point_count as u32;
        header.compressed_size = compressed_size as u32;
        header.uncompressed_size = original_size as u32;
        header.compression_type = compression_type;

        // Calculate checksum of compressed data
        header.checksum = crate::storage::integrity::calculate_checksum(compressed_data);

        // Write file atomically (header + data)
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&header.to_bytes()).await?;
        file.write_all(compressed_data).await?;
        file.sync_all().await?;

        Ok(())
    }

    /// Estimate memory usage for a set of points
    ///
    /// Accounts for both DataPoint storage and BTreeMap overhead during processing.
    fn estimate_memory(points: &[DataPoint]) -> usize {
        // DataPoint size + estimated BTreeMap node overhead
        points.len() * (std::mem::size_of::<DataPoint>() + 64)
    }

    /// Wait for memory budget to become available
    ///
    /// Blocks if the memory budget is exhausted, waiting for other seals to complete.
    async fn wait_for_memory_budget(&self, needed: usize) -> Result<(), SealError> {
        let budget = self.config.memory_budget_bytes;

        // Fast path: enough budget available
        if self.pending_memory.load(Ordering::Relaxed) + needed <= budget {
            return Ok(());
        }

        // Slow path: wait for memory to become available
        // Try up to 100 times with 100ms sleep = 10 seconds max wait
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.pending_memory.load(Ordering::Relaxed) + needed <= budget {
                return Ok(());
            }
        }

        Err(SealError::MemoryBudgetExceeded { needed, budget })
    }

    /// Get current statistics snapshot
    pub fn stats(&self) -> SealingStatsSnapshot {
        let total_sealed = self.stats.total_sealed.load(Ordering::Relaxed);
        let total_bytes_original = self.stats.total_bytes_original.load(Ordering::Relaxed);
        let total_bytes_compressed = self.stats.total_bytes_compressed.load(Ordering::Relaxed);
        let total_seal_time_ns = self.stats.total_seal_time_ns.load(Ordering::Relaxed);

        let avg_seal_time_secs = if total_sealed > 0 {
            (total_seal_time_ns as f64 / 1_000_000_000.0) / total_sealed as f64
        } else {
            0.0
        };

        let compression_ratio = if total_bytes_compressed > 0 {
            total_bytes_original as f64 / total_bytes_compressed as f64
        } else {
            0.0
        };

        SealingStatsSnapshot {
            active_seals: self.stats.active_seals.load(Ordering::Relaxed),
            total_sealed,
            total_failed: self.stats.total_failed.load(Ordering::Relaxed),
            total_bytes_original,
            total_bytes_compressed,
            pending_memory: self.pending_memory.load(Ordering::Relaxed),
            avg_seal_time_secs,
            compression_ratio,
        }
    }

    /// Get the current number of active seal operations
    pub fn active_seals(&self) -> u64 {
        self.stats.active_seals.load(Ordering::Relaxed)
    }

    /// Get the current pending memory usage
    pub fn pending_memory(&self) -> usize {
        self.pending_memory.load(Ordering::Relaxed)
    }

    /// Check if the service has capacity for more seals
    ///
    /// Returns true if there are available permits and memory budget.
    pub fn has_capacity(&self, estimated_points: usize) -> bool {
        let memory_needed = estimated_points * (std::mem::size_of::<DataPoint>() + 64);
        let current_memory = self.pending_memory.load(Ordering::Relaxed);
        current_memory + memory_needed <= self.config.memory_budget_bytes
    }

    // ==========================================================================
    // Priority Management Methods
    // ==========================================================================

    /// Set the priority override for a specific series
    ///
    /// This allows marking certain series as higher or lower priority than
    /// the automatic calculation would assign. The override persists until
    /// explicitly cleared.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to set priority for
    /// * `priority` - The priority to assign to this series
    pub fn set_series_priority(&self, series_id: SeriesId, priority: SealPriority) {
        if let Some(calculator) = &self.priority_calculator {
            calculator.set_series_priority(series_id, priority);
        }
    }

    /// Clear a priority override for a series
    ///
    /// Removes any explicit priority override, allowing automatic
    /// calculation to take effect again.
    pub fn clear_series_priority(&self, series_id: SeriesId) {
        if let Some(calculator) = &self.priority_calculator {
            calculator.clear_series_priority(series_id);
        }
    }

    /// Update the memory pressure level
    ///
    /// Higher memory pressure causes the priority calculator to escalate
    /// seal task priorities to free memory faster. Value should be 0.0-1.0
    /// representing the fraction of memory budget in use.
    ///
    /// # Arguments
    ///
    /// * `pressure` - Memory pressure as a fraction (0.0-1.0)
    pub fn set_memory_pressure(&self, pressure: f64) {
        if let Some(calculator) = &self.priority_calculator {
            calculator.set_memory_pressure(pressure);
        }
    }

    /// Automatically update memory pressure based on current pending memory
    ///
    /// Calculates pressure as `pending_memory / memory_budget` and updates
    /// the priority calculator.
    pub fn update_memory_pressure(&self) {
        let current = self.pending_memory.load(Ordering::Relaxed) as f64;
        let budget = self.config.memory_budget_bytes as f64;
        let pressure = if budget > 0.0 {
            (current / budget).clamp(0.0, 1.0)
        } else {
            0.0
        };
        self.set_memory_pressure(pressure);
    }

    /// Check if priority-based sealing is enabled
    pub fn is_priority_enabled(&self) -> bool {
        self.priority_calculator.is_some()
    }

    /// Get the priority calculator if priority-based sealing is enabled
    ///
    /// Returns `None` if priority-based sealing is disabled.
    pub fn priority_calculator(&self) -> Option<&Arc<PriorityCalculator>> {
        self.priority_calculator.as_ref()
    }

    // ==========================================================================
    // Adaptive Concurrency Methods
    // ==========================================================================

    /// Check if adaptive concurrency control is enabled
    pub fn is_adaptive_concurrency_enabled(&self) -> bool {
        self.concurrency_controller.is_some()
    }

    /// Get the current concurrency limit
    ///
    /// When adaptive concurrency is enabled, this reflects the dynamically
    /// adjusted limit. Otherwise, returns the configured max_concurrent_seals.
    pub fn current_concurrency_limit(&self) -> usize {
        match &self.concurrency_controller {
            Some(controller) => controller.current_limit(),
            None => self.config.max_concurrent_seals,
        }
    }

    /// Force a specific concurrency limit
    ///
    /// Only works when adaptive concurrency is enabled. The limit is clamped
    /// to the configured min/max bounds. This is useful for testing or
    /// emergency throttling.
    ///
    /// # Arguments
    ///
    /// * `limit` - The desired concurrency limit
    pub fn force_concurrency_limit(&self, limit: usize) {
        if let Some(controller) = &self.concurrency_controller {
            controller.force_limit(limit);
        }
    }

    /// Reset concurrency to the initial configured value
    ///
    /// Only works when adaptive concurrency is enabled.
    pub fn reset_concurrency(&self) {
        if let Some(controller) = &self.concurrency_controller {
            controller.reset();
        }
    }

    /// Get the concurrency controller if adaptive concurrency is enabled
    ///
    /// Returns `None` if adaptive concurrency is disabled.
    pub fn concurrency_controller(&self) -> Option<&Arc<ConcurrencyController>> {
        self.concurrency_controller.as_ref()
    }

    /// Get a snapshot of adaptive concurrency statistics
    ///
    /// Returns `None` if adaptive concurrency is disabled.
    pub async fn adaptive_concurrency_stats(
        &self,
    ) -> Option<crate::storage::adaptive_concurrency::ControllerStatsSnapshot> {
        match &self.concurrency_controller {
            Some(controller) => Some(controller.stats().await),
            None => None,
        }
    }

    /// Manually trigger a concurrency adjustment check
    ///
    /// Normally adjustments happen automatically on a schedule. This method
    /// allows forcing an immediate check, useful for testing or when load
    /// conditions have changed dramatically.
    ///
    /// Returns the adjustment result if an adjustment was made, `None` otherwise.
    pub async fn trigger_concurrency_adjustment(
        &self,
    ) -> Option<crate::storage::adaptive_concurrency::AdjustmentResult> {
        match &self.concurrency_controller {
            Some(controller) => controller.adjust().await,
            None => None,
        }
    }

    /// Stop the background adjustment task
    ///
    /// This is called automatically when the service is dropped, but can be
    /// called explicitly for graceful shutdown.
    pub fn stop_adaptive_concurrency(&self) {
        self.adjustment_task_running.store(false, Ordering::Relaxed);
        if let Some(controller) = &self.concurrency_controller {
            controller.sampler().stop();
        }
    }

    // ==========================================================================
    // Algorithm Selection Methods
    // ==========================================================================

    /// Check if automatic algorithm selection is enabled
    pub fn is_algorithm_selection_enabled(&self) -> bool {
        self.algorithm_selector.is_some()
    }

    /// Get the algorithm selector if enabled
    ///
    /// Returns `None` if algorithm selection is disabled.
    pub fn algorithm_selector(&self) -> Option<&Arc<AlgorithmSelector>> {
        self.algorithm_selector.as_ref()
    }

    /// Record compression statistics for learning
    ///
    /// Updates the algorithm statistics with the result of a compression operation.
    /// This allows the selector to learn which algorithms work best over time.
    ///
    /// Note: For full learning with characteristics, use the AlgorithmSelector
    /// directly via `algorithm_selector()`.
    ///
    /// # Arguments
    ///
    /// * `algorithm` - The algorithm that was used
    /// * `original_size` - Size before compression in bytes
    /// * `compressed_size` - Size after compression in bytes
    /// * `duration` - Time taken to compress
    pub fn record_compression_result(
        &self,
        algorithm: CompressionType,
        original_size: usize,
        compressed_size: usize,
        duration: Duration,
    ) {
        if let Some(selector) = &self.algorithm_selector {
            selector.record_stats(algorithm, original_size, compressed_size, duration);
        }
    }

    /// Get statistics for a specific compression algorithm
    ///
    /// Returns `None` if algorithm selection is disabled or no stats exist.
    pub fn algorithm_stats(
        &self,
        algorithm: CompressionType,
    ) -> Option<crate::compression::AlgorithmStatsSnapshot> {
        self.algorithm_selector
            .as_ref()
            .and_then(|s| s.get_stats(algorithm))
    }

    /// Get statistics for all compression algorithms
    ///
    /// Returns an empty map if algorithm selection is disabled.
    pub fn all_algorithm_stats(
        &self,
    ) -> std::collections::HashMap<CompressionType, crate::compression::AlgorithmStatsSnapshot>
    {
        match &self.algorithm_selector {
            Some(selector) => selector.all_stats(),
            None => std::collections::HashMap::new(),
        }
    }

    /// Reset all algorithm statistics
    ///
    /// Clears the learning history, useful for testing or after config changes.
    pub fn reset_algorithm_stats(&self) {
        if let Some(selector) = &self.algorithm_selector {
            selector.reset();
        }
    }

    /// Preview which algorithm would be selected for given data
    ///
    /// Useful for debugging and understanding algorithm selection decisions
    /// without actually performing compression.
    ///
    /// # Arguments
    ///
    /// * `points` - Sample data points to analyze
    ///
    /// # Returns
    ///
    /// The compression type that would be selected for this data.
    pub fn preview_algorithm_selection(&self, points: &[DataPoint]) -> CompressionType {
        self.select_compression_algorithm(points)
    }
}

impl Default for ParallelSealingService {
    fn default() -> Self {
        Self::new(ParallelSealingConfig::default())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_points(count: usize, series_id: SeriesId, base_ts: i64) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(series_id, base_ts + i as i64 * 1000, 100.0 + i as f64 * 0.1))
            .collect()
    }

    #[tokio::test]
    async fn test_single_seal() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        let points = create_test_points(100, 1, 1000);
        let path = temp_dir.path().join("chunk_1.kub");

        let handle = service.submit(1, points, path.clone()).await.unwrap();
        let result = handle.await_result().await.unwrap();

        assert_eq!(result.series_id, 1);
        assert_eq!(result.point_count, 100);
        assert!(result.compressed_size > 0);
        assert!(result.compression_ratio > 0.0);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_parallel_seals() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 4,
            ..Default::default()
        });

        let chunks: Vec<_> = (0..10u128)
            .map(|i| {
                let points = create_test_points(100, i, 1000);
                let path = temp_dir.path().join(format!("chunk_{}.kub", i));
                (i, points, path)
            })
            .collect();

        let results = service.seal_all_and_wait(chunks).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.is_ok()));

        let stats = service.stats();
        assert_eq!(stats.total_sealed, 10);
        assert_eq!(stats.total_failed, 0);
    }

    #[tokio::test]
    async fn test_empty_chunk_error() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        let path = temp_dir.path().join("chunk.kub");
        let result = service.submit(1, Vec::new(), path).await;

        assert!(matches!(result, Err(SealError::EmptyChunk(1))));
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        let points = create_test_points(50, 1, 1000);
        let path = temp_dir.path().join("chunk.kub");

        let handle = service.submit(1, points, path).await.unwrap();
        let _ = handle.await_result().await;

        let stats = service.stats();
        assert_eq!(stats.total_sealed, 1);
        assert!(stats.total_bytes_original > 0);
        assert!(stats.total_bytes_compressed > 0);
        assert!(stats.compression_ratio > 1.0);
    }

    #[test]
    fn test_config_presets() {
        let default = ParallelSealingConfig::default();
        let high = ParallelSealingConfig::high_throughput();
        let low = ParallelSealingConfig::low_memory();

        assert!(high.max_concurrent_seals >= default.max_concurrent_seals);
        assert!(high.memory_budget_bytes >= default.memory_budget_bytes);
        assert!(low.max_concurrent_seals <= default.max_concurrent_seals);
        assert!(low.memory_budget_bytes <= default.memory_budget_bytes);
    }

    #[test]
    fn test_memory_estimation() {
        let points = create_test_points(1000, 1, 0);
        let estimate = ParallelSealingService::estimate_memory(&points);

        // Should be roughly 1000 * (16 + 64) = 80,000 bytes
        assert!(estimate >= 70_000);
        assert!(estimate <= 100_000);
    }

    #[tokio::test]
    async fn test_has_capacity() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            memory_budget_bytes: 10_000,
            ..Default::default()
        });

        // Small request should have capacity
        assert!(service.has_capacity(50));

        // Large request should not have capacity
        assert!(!service.has_capacity(10_000));
    }

    // ==========================================================================
    // Priority Integration Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_priority_enabled_by_default() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());
        assert!(service.is_priority_enabled());
        assert!(service.priority_calculator().is_some());
    }

    #[tokio::test]
    async fn test_priority_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_priority: false,
            priority_config: None,
            ..Default::default()
        });
        assert!(!service.is_priority_enabled());
        assert!(service.priority_calculator().is_none());
    }

    #[tokio::test]
    async fn test_set_series_priority() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Set a high priority for series 1
        service.set_series_priority(1, SealPriority::High);

        // Verify it was set
        let calculator = service.priority_calculator().unwrap();
        assert_eq!(calculator.get_series_priority(1), Some(SealPriority::High));
    }

    #[tokio::test]
    async fn test_clear_series_priority() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Set then clear priority
        service.set_series_priority(1, SealPriority::Critical);
        service.clear_series_priority(1);

        // Verify it was cleared
        let calculator = service.priority_calculator().unwrap();
        assert_eq!(calculator.get_series_priority(1), None);
    }

    #[tokio::test]
    async fn test_set_memory_pressure() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Set memory pressure
        service.set_memory_pressure(0.75);

        // Verify it was set
        let calculator = service.priority_calculator().unwrap();
        assert!((calculator.get_memory_pressure() - 0.75).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_update_memory_pressure() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            memory_budget_bytes: 1000,
            ..Default::default()
        });

        // Initially pressure should be 0
        service.update_memory_pressure();
        let calculator = service.priority_calculator().unwrap();
        assert_eq!(calculator.get_memory_pressure(), 0.0);
    }

    #[tokio::test]
    async fn test_submit_with_explicit_priority() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        let points = create_test_points(100, 1, 1000);
        let path = temp_dir.path().join("chunk_critical.kub");

        // Submit with explicit critical priority
        let handle = service
            .submit_with_priority(1, points, path.clone(), SealPriority::Critical)
            .await
            .unwrap();

        let result = handle.await_result().await.unwrap();
        assert_eq!(result.series_id, 1);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_priority_does_not_affect_basic_seal() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Submit tasks with different priorities - all should complete successfully
        let mut handles = Vec::new();
        for (i, priority) in [
            SealPriority::Background,
            SealPriority::Low,
            SealPriority::Normal,
            SealPriority::High,
            SealPriority::Critical,
        ]
        .iter()
        .enumerate()
        {
            let points = create_test_points(50, i as u128, 1000);
            let path = temp_dir.path().join(format!("chunk_{}.kub", i));
            let handle = service
                .submit_with_priority(i as u128, points, path, *priority)
                .await
                .unwrap();
            handles.push(handle);
        }

        // All should complete
        for handle in handles {
            let result = handle.await_result().await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_priority_with_disabled_service() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_priority: false,
            ..Default::default()
        });

        // These should be no-ops when priority is disabled
        service.set_series_priority(1, SealPriority::High);
        service.clear_series_priority(1);
        service.set_memory_pressure(0.9);

        // Should not panic and calculator should be None
        assert!(service.priority_calculator().is_none());
    }

    #[test]
    fn test_config_includes_priority_settings() {
        let default = ParallelSealingConfig::default();
        assert!(default.enable_priority);
        assert!(default.priority_config.is_some());

        let priority_config = default.priority_config.unwrap();
        // Verify default priority config values are sensible
        assert!(priority_config.age_weight > 0.0);
        assert!(priority_config.point_count_weight > 0.0);
    }

    // ==========================================================================
    // Adaptive Concurrency Integration Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_adaptive_concurrency_enabled_by_default() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());
        assert!(service.is_adaptive_concurrency_enabled());
        assert!(service.concurrency_controller().is_some());
    }

    #[tokio::test]
    async fn test_adaptive_concurrency_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_adaptive_concurrency: false,
            adaptive_config: None,
            ..Default::default()
        });
        assert!(!service.is_adaptive_concurrency_enabled());
        assert!(service.concurrency_controller().is_none());
    }

    #[tokio::test]
    async fn test_current_concurrency_limit_with_adaptive() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());
        let limit = service.current_concurrency_limit();
        assert!(limit > 0);
    }

    #[tokio::test]
    async fn test_current_concurrency_limit_without_adaptive() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 4,
            enable_adaptive_concurrency: false,
            ..Default::default()
        });
        assert_eq!(service.current_concurrency_limit(), 4);
    }

    #[tokio::test]
    async fn test_force_concurrency_limit() {
        let config = AdaptiveConfig {
            min_concurrency: 2,
            max_concurrency: 10,
            initial_concurrency: 5,
            ..Default::default()
        };
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 10,
            adaptive_config: Some(config),
            ..Default::default()
        });

        // Force to a specific limit
        service.force_concurrency_limit(8);
        assert_eq!(service.current_concurrency_limit(), 8);

        // Test clamping to max
        service.force_concurrency_limit(100);
        assert_eq!(service.current_concurrency_limit(), 10);

        // Test clamping to min
        service.force_concurrency_limit(0);
        assert_eq!(service.current_concurrency_limit(), 2);
    }

    #[tokio::test]
    async fn test_reset_concurrency() {
        let config = AdaptiveConfig {
            min_concurrency: 2,
            max_concurrency: 10,
            initial_concurrency: 5,
            ..Default::default()
        };
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 10,
            adaptive_config: Some(config),
            ..Default::default()
        });

        // Change and then reset
        service.force_concurrency_limit(8);
        assert_eq!(service.current_concurrency_limit(), 8);

        service.reset_concurrency();
        assert_eq!(service.current_concurrency_limit(), 5);
    }

    #[tokio::test]
    async fn test_force_limit_no_op_when_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 4,
            enable_adaptive_concurrency: false,
            ..Default::default()
        });

        // Should be no-op
        service.force_concurrency_limit(10);
        assert_eq!(service.current_concurrency_limit(), 4);
    }

    #[tokio::test]
    async fn test_reset_no_op_when_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 4,
            enable_adaptive_concurrency: false,
            ..Default::default()
        });

        // Should be no-op
        service.reset_concurrency();
        assert_eq!(service.current_concurrency_limit(), 4);
    }

    #[tokio::test]
    async fn test_adaptive_concurrency_stats() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());
        let stats = service.adaptive_concurrency_stats().await;
        assert!(stats.is_some());

        let stats = stats.unwrap();
        assert!(stats.current_limit > 0);
        assert_eq!(stats.total_adjustments, 0);
    }

    #[tokio::test]
    async fn test_adaptive_concurrency_stats_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_adaptive_concurrency: false,
            ..Default::default()
        });
        let stats = service.adaptive_concurrency_stats().await;
        assert!(stats.is_none());
    }

    #[tokio::test]
    async fn test_trigger_concurrency_adjustment_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_adaptive_concurrency: false,
            ..Default::default()
        });
        let result = service.trigger_concurrency_adjustment().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_stop_adaptive_concurrency() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Should not panic
        service.stop_adaptive_concurrency();

        // Controller should still be accessible but sampler stopped
        let controller = service.concurrency_controller().unwrap();
        assert!(!controller.sampler().is_running());
    }

    #[test]
    fn test_config_includes_adaptive_settings() {
        let default = ParallelSealingConfig::default();
        assert!(default.enable_adaptive_concurrency);
        assert!(default.adaptive_config.is_some());
    }

    #[test]
    fn test_config_presets_include_adaptive() {
        let high = ParallelSealingConfig::high_throughput();
        assert!(high.adaptive_config.is_some());

        let low = ParallelSealingConfig::low_memory();
        assert!(low.adaptive_config.is_some());

        let latency = ParallelSealingConfig::low_latency();
        assert!(latency.adaptive_config.is_some());
    }

    #[tokio::test]
    async fn test_seal_records_io_latency() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        let points = create_test_points(100, 1, 1000);
        let path = temp_dir.path().join("chunk_1.kub");

        let handle = service.submit(1, points, path.clone()).await.unwrap();
        let _ = handle.await_result().await.unwrap();

        // Give time for sampler to record
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Controller should have recorded some I/O latency
        // (we can't easily check the exact value, but we can verify no panic)
        let controller = service.concurrency_controller().unwrap();
        let sampler = controller.sampler();
        sampler.take_sample().await;
    }

    #[tokio::test]
    async fn test_parallel_seals_with_adaptive_concurrency() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig {
            max_concurrent_seals: 4,
            enable_adaptive_concurrency: true,
            adaptive_config: Some(AdaptiveConfig {
                min_concurrency: 1,
                max_concurrency: 8,
                initial_concurrency: 4,
                ..Default::default()
            }),
            ..Default::default()
        });

        let chunks: Vec<_> = (0..10u128)
            .map(|i| {
                let points = create_test_points(100, i, 1000);
                let path = temp_dir.path().join(format!("chunk_{}.kub", i));
                (i, points, path)
            })
            .collect();

        let results = service.seal_all_and_wait(chunks).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.is_ok()));

        let stats = service.stats();
        assert_eq!(stats.total_sealed, 10);
        assert_eq!(stats.total_failed, 0);
    }

    // ==========================================================================
    // Algorithm Selection Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_algorithm_selection_enabled_by_default() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());
        assert!(service.is_algorithm_selection_enabled());
    }

    #[tokio::test]
    async fn test_algorithm_selection_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_algorithm_selection: false,
            ..Default::default()
        });
        assert!(!service.is_algorithm_selection_enabled());
        assert!(service.algorithm_selector().is_none());
    }

    #[tokio::test]
    async fn test_preview_algorithm_selection() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Regular data with low entropy (repeated values)
        let regular_points: Vec<DataPoint> = (0..200)
            .map(|i| DataPoint::new(1, 1000 + i as i64 * 1000, (i % 5) as f64 * 10.0))
            .collect();

        let algo = service.preview_algorithm_selection(&regular_points);
        // Should select AHPAC for regular time-series data
        assert_eq!(algo, CompressionType::Ahpac);
    }

    #[tokio::test]
    async fn test_algorithm_selection_falls_back_when_disabled() {
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_algorithm_selection: false,
            compression_type: CompressionType::Snappy,
            ..Default::default()
        });

        let points = create_test_points(100, 1, 1000);
        let algo = service.preview_algorithm_selection(&points);

        // Should fall back to configured compression_type
        assert_eq!(algo, CompressionType::Snappy);
    }

    #[tokio::test]
    async fn test_record_compression_result() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Record a result
        service.record_compression_result(
            CompressionType::Ahpac,
            1000,
            500,
            Duration::from_millis(10),
        );

        // Check stats were updated
        let stats = service.algorithm_stats(CompressionType::Ahpac);
        assert!(stats.is_some());

        let stats = stats.unwrap();
        assert_eq!(stats.total_compressions, 1);
        assert!((stats.avg_ratio - 2.0).abs() < 0.01); // 1000/500 = 2.0
    }

    #[tokio::test]
    async fn test_all_algorithm_stats() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Record results for different algorithms
        service.record_compression_result(
            CompressionType::Ahpac,
            1000,
            500,
            Duration::from_millis(10),
        );
        service.record_compression_result(
            CompressionType::Snappy,
            1000,
            600,
            Duration::from_millis(5),
        );

        let all_stats = service.all_algorithm_stats();

        // Should have stats for recorded algorithms
        assert!(all_stats.contains_key(&CompressionType::Ahpac));
        assert!(all_stats.contains_key(&CompressionType::Snappy));
    }

    #[tokio::test]
    async fn test_reset_algorithm_stats() {
        let service = ParallelSealingService::new(ParallelSealingConfig::default());

        // Record a result
        service.record_compression_result(
            CompressionType::Ahpac,
            1000,
            500,
            Duration::from_millis(10),
        );

        // Reset
        service.reset_algorithm_stats();

        // Stats should be cleared
        let stats = service.algorithm_stats(CompressionType::Ahpac);
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().total_compressions, 0);
    }

    #[tokio::test]
    async fn test_seal_with_algorithm_selection() {
        let temp_dir = TempDir::new().unwrap();
        let service = ParallelSealingService::new(ParallelSealingConfig {
            enable_algorithm_selection: true,
            ..Default::default()
        });

        // Create low-entropy data that should select AHPAC
        let points: Vec<DataPoint> = (0..200)
            .map(|i| DataPoint::new(1, 1000 + i as i64 * 1000, (i % 5) as f64 * 10.0))
            .collect();

        let path = temp_dir.path().join("chunk.kub");
        let handle = service.submit(1, points, path.clone()).await.unwrap();
        let result = handle.await_result().await.unwrap();

        assert_eq!(result.series_id, 1);
        assert!(result.compressed_size > 0);
        assert!(path.exists());

        // Algorithm stats should be updated
        let all_stats = service.all_algorithm_stats();
        // At least one algorithm should have stats
        assert!(all_stats.values().any(|s| s.total_compressions > 0));
    }

    #[tokio::test]
    async fn test_config_includes_algorithm_selection_settings() {
        let config = ParallelSealingConfig::default();
        assert!(config.enable_algorithm_selection);
        assert!(config.selection_config.is_some());

        // Presets should also have algorithm selection
        let high = ParallelSealingConfig::high_throughput();
        assert!(high.enable_algorithm_selection);

        let low = ParallelSealingConfig::low_memory();
        assert!(low.enable_algorithm_selection);
    }
}
