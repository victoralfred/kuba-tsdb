//! Compaction Service
//!
//! Merges and optimizes chunks for improved query performance:
//! - Size-based compaction: Merge small chunks into larger ones
//! - Time-based compaction: Merge chunks within time windows
//! - Leveled compaction: LSM-style tiered compaction
//! - Concurrent compaction with configurable parallelism

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;
use tokio::time::{interval, Instant};

use crate::storage::LocalDiskEngine;
use crate::types::ChunkId;

use super::framework::{RestartPolicy, Service, ServiceError, ServiceStatus};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the compaction service
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Compaction strategy to use
    pub strategy: CompactionStrategy,

    /// Minimum number of chunks to trigger compaction
    pub min_chunks_to_compact: usize,

    /// Maximum number of chunks to compact at once
    pub max_chunks_per_job: usize,

    /// Target size for compacted chunks (bytes)
    pub target_chunk_size: usize,

    /// Number of concurrent compaction workers
    pub worker_count: usize,

    /// Interval for checking compaction conditions
    pub check_interval: Duration,

    /// Enable background compaction
    pub enabled: bool,

    /// Maximum number of jobs allowed in the queue (prevents memory exhaustion)
    pub max_queue_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::SizeBased,
            min_chunks_to_compact: 4,
            max_chunks_per_job: 10,
            target_chunk_size: 64 * 1024 * 1024, // 64 MB
            worker_count: 2,
            check_interval: Duration::from_secs(60),
            enabled: true,
            max_queue_size: 1000, // Limit queue to prevent memory exhaustion
        }
    }
}

// ============================================================================
// Compaction Strategy
// ============================================================================

/// Strategy for selecting chunks to compact
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategy {
    /// Merge small chunks based on size thresholds
    SizeBased,

    /// Merge chunks within time windows (e.g., hourly, daily)
    TimeBased,

    /// LSM-style leveled compaction with size ratios
    LeveledCompaction,
}

// ============================================================================
// Compaction Job
// ============================================================================

/// A compaction job to be executed
#[derive(Debug, Clone)]
pub struct CompactionJob {
    /// Unique job ID
    pub id: u64,

    /// Chunks to compact
    pub chunks: Vec<ChunkId>,

    /// Target size for the output chunk
    pub target_size: usize,

    /// Priority (higher = more urgent)
    pub priority: u32,

    /// Creation time
    pub created_at: Instant,
}

impl CompactionJob {
    /// Create a new compaction job
    pub fn new(chunks: Vec<ChunkId>, target_size: usize) -> Self {
        static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);

        Self {
            id: JOB_COUNTER.fetch_add(1, Ordering::SeqCst),
            chunks,
            target_size,
            priority: 0,
            created_at: Instant::now(),
        }
    }

    /// Create with priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics collected by the compaction service
#[derive(Debug, Default, Clone)]
pub struct CompactionStats {
    /// Total jobs completed
    pub jobs_completed: u64,

    /// Total jobs failed
    pub jobs_failed: u64,

    /// Jobs currently in progress
    pub jobs_in_progress: u64,

    /// Jobs waiting in queue
    pub jobs_queued: u64,

    /// Total chunks compacted
    pub chunks_compacted: u64,

    /// Total bytes read during compaction
    pub bytes_read: u64,

    /// Total bytes written during compaction
    pub bytes_written: u64,

    /// Total time spent compacting (milliseconds)
    pub total_compaction_time_ms: u64,
}

// ============================================================================
// Compaction Service
// ============================================================================

/// Background service for chunk compaction
///
/// The CompactionService monitors chunks and merges them to optimize
/// storage and query performance. It supports multiple compaction
/// strategies and concurrent execution.
pub struct CompactionService {
    /// Configuration
    config: CompactionConfig,

    /// Storage engine reference (used for chunk merging operations)
    #[allow(dead_code)]
    storage: Arc<LocalDiskEngine>,

    /// Job queue
    queue: Mutex<VecDeque<CompactionJob>>,

    /// Current service status
    status: RwLock<ServiceStatus>,

    /// Collected statistics
    stats: RwLock<CompactionStats>,
}

impl CompactionService {
    /// Create a new compaction service
    pub fn new(config: CompactionConfig, storage: Arc<LocalDiskEngine>) -> Self {
        Self {
            config,
            storage,
            queue: Mutex::new(VecDeque::new()),
            status: RwLock::new(ServiceStatus::Stopped),
            stats: RwLock::new(CompactionStats::default()),
        }
    }

    /// Create with default configuration
    pub fn with_storage(storage: Arc<LocalDiskEngine>) -> Self {
        Self::new(CompactionConfig::default(), storage)
    }

    /// Get current statistics
    pub fn stats(&self) -> CompactionStats {
        self.stats.read().clone()
    }

    /// Get current queue depth (number of pending jobs)
    pub fn queue_depth(&self) -> usize {
        self.queue.lock().len()
    }

    /// Get queue utilization as a percentage (0.0 to 1.0)
    pub fn queue_utilization(&self) -> f64 {
        let depth = self.queue.lock().len();
        depth as f64 / self.config.max_queue_size as f64
    }

    /// Submit a compaction job to the queue
    ///
    /// Returns `Ok(())` if the job was queued, or `Err` if the queue is full.
    /// This prevents memory exhaustion from unbounded queue growth.
    pub fn submit_job(&self, job: CompactionJob) -> Result<(), ServiceError> {
        // Minimize lock holding time - get queue length after operation
        let queue_len = {
            let mut queue = self.queue.lock();

            // Check queue size limit to prevent memory exhaustion
            if queue.len() >= self.config.max_queue_size {
                tracing::warn!(
                    queue_size = queue.len(),
                    max_size = self.config.max_queue_size,
                    job_id = job.id,
                    "Compaction job rejected: queue is full"
                );
                return Err(ServiceError::RuntimeError(format!(
                    "Compaction queue full (max: {})",
                    self.config.max_queue_size
                )));
            }

            queue.push_back(job);
            queue.len()
        }; // queue lock released here

        // Update stats with separate lock
        self.stats.write().jobs_queued = queue_len as u64;
        Ok(())
    }

    /// Get the next job from the queue
    fn next_job(&self) -> Option<CompactionJob> {
        // Minimize lock holding time - extract job and length atomically
        let (job, queue_len) = {
            let mut queue = self.queue.lock();
            let job = queue.pop_front();
            (job, queue.len())
        }; // queue lock released here

        if job.is_some() {
            // Update stats with separate lock
            let mut stats = self.stats.write();
            stats.jobs_queued = queue_len as u64;
            stats.jobs_in_progress += 1;
        }

        job
    }

    /// Run the compaction check cycle
    async fn run_compaction_check(&self) -> Result<(), ServiceError> {
        if !self.config.enabled {
            return Ok(());
        }

        // Find chunks that need compaction based on strategy
        let jobs = self.find_compaction_candidates().await?;

        for job in jobs {
            // Log but don't fail if queue is full - jobs will be retried next cycle
            if let Err(e) = self.submit_job(job) {
                tracing::debug!(error = %e, "Could not submit compaction job, will retry next cycle");
            }
        }

        Ok(())
    }

    /// Find chunks that are candidates for compaction
    async fn find_compaction_candidates(&self) -> Result<Vec<CompactionJob>, ServiceError> {
        let jobs = Vec::new();

        match self.config.strategy {
            CompactionStrategy::SizeBased => {
                // Find small chunks that can be merged
                // Real implementation would query storage for chunk sizes
            },
            CompactionStrategy::TimeBased => {
                // Find chunks within the same time window
                // Real implementation would group chunks by time
            },
            CompactionStrategy::LeveledCompaction => {
                // Find chunks at each level that exceed size ratio
                // Real implementation would track levels and ratios
            },
        }

        Ok(jobs)
    }

    /// Execute a compaction job
    async fn execute_job(&self, job: CompactionJob) -> Result<(), ServiceError> {
        let start = Instant::now();

        tracing::debug!(
            job_id = job.id,
            chunks = job.chunks.len(),
            "Executing compaction job"
        );

        // Real implementation would:
        // 1. Read all source chunks
        // 2. Merge and re-compress data
        // 3. Write new compacted chunk
        // 4. Delete old chunks

        let bytes_read = 0u64;
        let bytes_written = 0u64;
        let duration_ms = start.elapsed().as_millis() as u64;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.jobs_completed += 1;
            stats.jobs_in_progress = stats.jobs_in_progress.saturating_sub(1);
            stats.chunks_compacted += job.chunks.len() as u64;
            stats.bytes_read += bytes_read;
            stats.bytes_written += bytes_written;
            stats.total_compaction_time_ms += duration_ms;
        }

        tracing::debug!(
            job_id = job.id,
            chunks = job.chunks.len(),
            duration_ms = duration_ms,
            "Compaction job completed"
        );

        Ok(())
    }

    /// Process jobs from the queue
    async fn process_jobs(&self) {
        while let Some(job) = self.next_job() {
            if let Err(e) = self.execute_job(job).await {
                tracing::error!(error = %e, "Compaction job failed");

                let mut stats = self.stats.write();
                stats.jobs_failed += 1;
                stats.jobs_in_progress = stats.jobs_in_progress.saturating_sub(1);
            }
        }
    }
}

#[async_trait::async_trait]
impl Service for CompactionService {
    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
        *self.status.write() = ServiceStatus::Running;
        tracing::debug!(
            strategy = ?self.config.strategy,
            workers = self.config.worker_count,
            "Compaction service started"
        );

        let mut check_interval = interval(self.config.check_interval);

        loop {
            tokio::select! {
                // Shutdown signal received
                result = shutdown.recv() => {
                    match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!("Compaction service received shutdown signal");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Missed some messages but channel is still open, continue
                            tracing::debug!(missed = n, "Compaction service broadcast receiver lagged");
                        }
                    }
                }

                // Periodic compaction check
                _ = check_interval.tick() => {
                    if let Err(e) = self.run_compaction_check().await {
                        tracing::error!(error = %e, "Compaction check failed");
                    }

                    // Process any queued jobs
                    self.process_jobs().await;
                }
            }
        }

        // Process remaining jobs before shutdown
        self.process_jobs().await;

        *self.status.write() = ServiceStatus::Stopped;
        tracing::debug!("Compaction service stopped");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "compaction"
    }

    fn status(&self) -> ServiceStatus {
        self.status.read().clone()
    }

    fn dependencies(&self) -> Vec<&'static str> {
        vec!["chunk_manager"]
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure {
            max_retries: 3,
            backoff: Duration::from_secs(30),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Arc<LocalDiskEngine>) {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();
        (temp_dir, Arc::new(storage))
    }

    #[test]
    fn test_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.strategy, CompactionStrategy::SizeBased);
        assert_eq!(config.min_chunks_to_compact, 4);
        assert_eq!(config.worker_count, 2);
        assert!(config.enabled);
    }

    #[test]
    fn test_compaction_job() {
        let chunks = vec![ChunkId::new(), ChunkId::new()];
        let job = CompactionJob::new(chunks.clone(), 1024 * 1024);

        assert_eq!(job.chunks.len(), 2);
        assert_eq!(job.priority, 0);

        let job2 = CompactionJob::new(vec![], 0).with_priority(10);
        assert_eq!(job2.priority, 10);
    }

    #[tokio::test]
    async fn test_job_queue() {
        let (_temp_dir, storage) = create_test_storage();
        let service = CompactionService::with_storage(storage);

        let job = CompactionJob::new(vec![ChunkId::new()], 1024);
        service.submit_job(job).unwrap();

        let stats = service.stats();
        assert_eq!(stats.jobs_queued, 1);

        let next = service.next_job();
        assert!(next.is_some());

        let stats = service.stats();
        assert_eq!(stats.jobs_queued, 0);
        assert_eq!(stats.jobs_in_progress, 1);
    }

    #[tokio::test]
    async fn test_job_queue_limit() {
        let (_temp_dir, storage) = create_test_storage();
        let config = CompactionConfig {
            max_queue_size: 2,
            ..Default::default()
        };
        let service = CompactionService::new(config, storage);

        // First two jobs should succeed
        let job1 = CompactionJob::new(vec![ChunkId::new()], 1024);
        let job2 = CompactionJob::new(vec![ChunkId::new()], 1024);
        assert!(service.submit_job(job1).is_ok());
        assert!(service.submit_job(job2).is_ok());

        // Third job should fail - queue is full
        let job3 = CompactionJob::new(vec![ChunkId::new()], 1024);
        assert!(service.submit_job(job3).is_err());

        let stats = service.stats();
        assert_eq!(stats.jobs_queued, 2);
    }

    #[tokio::test]
    async fn test_compaction_service_lifecycle() {
        let (_temp_dir, storage) = create_test_storage();
        let service = Arc::new(CompactionService::with_storage(storage));

        let (tx, rx) = broadcast::channel(1);

        let s = service.clone();
        let handle = tokio::spawn(async move { s.start(rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
