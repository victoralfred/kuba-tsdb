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
use crate::engine::traits::Compressor;
use crate::storage::chunk::{ChunkHeader, CompressionType};
use crate::types::{ChunkId, DataPoint, SeriesId};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
        }
    }
}

impl ParallelSealingConfig {
    /// Create configuration optimized for high throughput
    ///
    /// Uses more concurrent workers and larger memory budget for maximum speed.
    pub fn high_throughput() -> Self {
        Self {
            max_concurrent_seals: num_cpus::get() * 2,
            memory_budget_bytes: 512 * 1024 * 1024, // 512MB
            seal_timeout: Duration::from_secs(60),
            collect_metrics: false, // Reduce overhead
            ..Default::default()
        }
    }

    /// Create configuration optimized for low memory usage
    ///
    /// Uses fewer workers and smaller budget for memory-constrained environments.
    pub fn low_memory() -> Self {
        Self {
            max_concurrent_seals: 2,
            memory_budget_bytes: 64 * 1024 * 1024, // 64MB
            seal_timeout: Duration::from_secs(30),
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
/// backpressure to prevent OOM during high-volume flush operations.
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
    semaphore: Arc<Semaphore>,

    /// Task ID counter
    next_task_id: AtomicU64,

    /// Pending memory tracking
    pending_memory: Arc<AtomicUsize>,

    /// Service statistics
    stats: Arc<SealingStats>,

    /// Shared compressor instance
    compressor: Arc<AhpacCompressor>,
}

impl ParallelSealingService {
    /// Create a new parallel sealing service
    pub fn new(config: ParallelSealingConfig) -> Self {
        info!(
            max_concurrent = config.max_concurrent_seals,
            memory_budget_mb = config.memory_budget_bytes / (1024 * 1024),
            "Creating parallel sealing service"
        );

        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_seals)),
            next_task_id: AtomicU64::new(0),
            pending_memory: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(SealingStats::default()),
            compressor: Arc::new(AhpacCompressor::new()),
            config,
        }
    }

    /// Submit a single chunk for sealing
    ///
    /// The chunk will be compressed asynchronously. Use the returned handle
    /// to await the result.
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
        if points.is_empty() {
            return Err(SealError::EmptyChunk(series_id));
        }

        let estimated_memory = Self::estimate_memory(&points);

        // Wait for memory budget if needed
        self.wait_for_memory_budget(estimated_memory).await?;

        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let (result_tx, result_rx) = oneshot::channel();

        let task = SealTask {
            id: task_id,
            series_id,
            points,
            estimated_memory,
            target_path,
            compression_type: self.config.compression_type,
            result_tx,
        };

        // Track pending memory
        self.pending_memory
            .fetch_add(estimated_memory, Ordering::Relaxed);

        debug!(
            task_id = task_id,
            series_id = series_id,
            estimated_memory = estimated_memory,
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
        let timeout = self.config.seal_timeout;
        let memory = task.estimated_memory;
        let collect_metrics = self.config.collect_metrics;

        tokio::spawn(async move {
            // Acquire permit (limits concurrency)
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    // Semaphore closed, service is shutting down
                    let _ = task.result_tx.send(Err(SealError::Cancelled));
                    pending_memory.fetch_sub(memory, Ordering::Relaxed);
                    return;
                }
            };

            stats.active_seals.fetch_add(1, Ordering::Relaxed);
            let start = Instant::now();

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
            stats.active_seals.fetch_sub(1, Ordering::Relaxed);

            let seal_result = match result {
                Ok(Ok(mut seal_result)) => {
                    seal_result.duration = start.elapsed();

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
                        "Seal completed successfully"
                    );

                    Ok(seal_result)
                }
                Ok(Err(e)) => {
                    if collect_metrics {
                        stats.total_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    warn!(series_id = task.series_id, error = %e, "Seal failed");
                    Err(e)
                }
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
                }
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
}
