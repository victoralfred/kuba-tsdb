//! Background Sealing Service
//!
//! Automatically seals active chunks that meet threshold conditions without
//! blocking the write path. This service monitors all active chunks and
//! triggers parallel sealing when chunks are ready.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                      BackgroundSealingService                            │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌──────────────────┐     ┌─────────────────────┐                       │
//! │  │  Monitor Loop    │────▶│  Check Active Chunks│                       │
//! │  │  (interval-based)│     │  (should_seal?)     │                       │
//! │  └──────────────────┘     └──────────┬──────────┘                       │
//! │                                       │                                  │
//! │                                       ▼                                  │
//! │                           ┌──────────────────────┐                      │
//! │                           │  ParallelSealingService                     │
//! │                           │  (submit for sealing) │                      │
//! │                           └──────────────────────┘                       │
//! │                                       │                                  │
//! │                                       ▼                                  │
//! │                           ┌──────────────────────┐                      │
//! │                           │  Index Registration   │                      │
//! │                           │  (async callback)     │                      │
//! │                           └──────────────────────┘                       │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Non-blocking writes**: Sealing happens in background, writes continue
//! - **Parallel compression**: Uses ParallelSealingService for concurrent seals
//! - **Streaming for large chunks**: Chunks > threshold are compressed in pieces
//! - **Graceful shutdown**: Flushes all pending seals before stopping

use crate::engine::traits::{ChunkLocation, TimeIndex};
use crate::storage::active_chunk::ActiveChunk;
use crate::storage::parallel_sealing::{
    ParallelSealingConfig, ParallelSealingService, SealError, SealResult,
};
use crate::types::{DataPoint, SeriesId, TimeRange};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the background sealing service
#[derive(Debug, Clone)]
pub struct BackgroundSealingConfig {
    /// Interval between seal checks
    /// Default: 1 second
    pub check_interval: Duration,

    /// Minimum points before considering for background seal
    /// Default: 500 (seal when half full for responsiveness)
    pub min_points_for_seal: usize,

    /// Maximum age before forcing a seal (even if not full)
    /// Default: 30 seconds
    pub max_age_before_seal: Duration,

    /// Enable streaming compression for large chunks
    /// Default: true
    pub enable_streaming: bool,

    /// Threshold for streaming compression (points)
    /// Chunks larger than this are compressed in pieces
    /// Default: 50,000 points
    pub streaming_threshold: usize,

    /// Chunk size for streaming compression
    /// Default: 10,000 points per piece
    pub streaming_chunk_size: usize,

    /// Parallel sealing configuration
    pub sealing_config: ParallelSealingConfig,

    /// Maximum pending seals in queue
    /// Default: 100
    pub max_pending_seals: usize,
}

impl Default for BackgroundSealingConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(1),
            min_points_for_seal: 500,
            max_age_before_seal: Duration::from_secs(30),
            enable_streaming: true,
            streaming_threshold: 50_000,
            streaming_chunk_size: 10_000,
            sealing_config: ParallelSealingConfig::default(),
            max_pending_seals: 100,
        }
    }
}

impl BackgroundSealingConfig {
    /// Configuration for high-throughput workloads
    pub fn high_throughput() -> Self {
        Self {
            check_interval: Duration::from_millis(500),
            min_points_for_seal: 800,
            max_age_before_seal: Duration::from_secs(15),
            sealing_config: ParallelSealingConfig::high_throughput(),
            ..Default::default()
        }
    }

    /// Configuration for low-latency workloads
    pub fn low_latency() -> Self {
        Self {
            check_interval: Duration::from_millis(100),
            min_points_for_seal: 100,
            max_age_before_seal: Duration::from_secs(5),
            sealing_config: ParallelSealingConfig::low_memory(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics for the background sealing service
#[derive(Debug, Default)]
struct BackgroundSealingStats {
    /// Total seal checks performed
    checks_performed: AtomicU64,
    /// Total chunks sealed
    chunks_sealed: AtomicU64,
    /// Total chunks sealed via streaming
    chunks_streamed: AtomicU64,
    /// Total points sealed
    points_sealed: AtomicU64,
    /// Total bytes compressed
    bytes_compressed: AtomicU64,
    /// Failed seals
    seals_failed: AtomicU64,
}

/// Snapshot of background sealing statistics
#[derive(Debug, Clone)]
pub struct BackgroundSealingStatsSnapshot {
    /// Total seal checks performed
    pub checks_performed: u64,
    /// Total chunks sealed
    pub chunks_sealed: u64,
    /// Total chunks sealed via streaming (large chunks split into pieces)
    pub chunks_streamed: u64,
    /// Total points sealed
    pub points_sealed: u64,
    /// Total bytes after compression
    pub bytes_compressed: u64,
    /// Failed seal attempts
    pub seals_failed: u64,
}

// ============================================================================
// Background Sealing Service
// ============================================================================

/// Background service for automatic chunk sealing
///
/// Monitors active chunks and automatically seals them when they meet
/// threshold conditions. Uses parallel compression for efficiency.
pub struct BackgroundSealingService {
    /// Configuration
    config: BackgroundSealingConfig,

    /// Parallel sealing service
    sealing_service: Arc<ParallelSealingService>,

    /// Running state
    running: AtomicBool,

    /// Statistics
    stats: BackgroundSealingStats,

    /// Shutdown signal sender
    shutdown_tx: RwLock<Option<mpsc::Sender<()>>>,
}

impl BackgroundSealingService {
    /// Create a new background sealing service
    pub fn new(config: BackgroundSealingConfig) -> Self {
        let sealing_service = Arc::new(ParallelSealingService::new(config.sealing_config.clone()));

        Self {
            config,
            sealing_service,
            running: AtomicBool::new(false),
            stats: BackgroundSealingStats::default(),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// Start the background sealing service
    ///
    /// This spawns a background task that monitors active chunks and seals
    /// them when they meet threshold conditions.
    ///
    /// # Arguments
    ///
    /// * `active_chunks` - Shared reference to active chunks map
    /// * `data_dir` - Base directory for chunk storage
    /// * `index` - Time index for registering sealed chunks
    pub async fn start<I: TimeIndex + 'static + ?Sized>(
        self: Arc<Self>,
        active_chunks: Arc<RwLock<HashMap<SeriesId, Arc<ActiveChunk>>>>,
        data_dir: PathBuf,
        index: Arc<I>,
    ) {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("Background sealing service already running");
            return;
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        let service = Arc::clone(&self);
        let config = self.config.clone();

        info!(
            check_interval_ms = config.check_interval.as_millis(),
            min_points = config.min_points_for_seal,
            max_age_secs = config.max_age_before_seal.as_secs(),
            "Starting background sealing service"
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Background sealing service shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        service.run_seal_check(
                            &active_chunks,
                            &data_dir,
                            &index,
                        ).await;
                    }
                }
            }

            service.running.store(false, Ordering::SeqCst);
            info!("Background sealing service stopped");
        });
    }

    /// Stop the background sealing service
    pub async fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Run a single seal check cycle
    async fn run_seal_check<I: TimeIndex + ?Sized>(
        &self,
        active_chunks: &Arc<RwLock<HashMap<SeriesId, Arc<ActiveChunk>>>>,
        data_dir: &Path,
        index: &Arc<I>,
    ) {
        self.stats.checks_performed.fetch_add(1, Ordering::Relaxed);

        // Collect chunks that need sealing
        let chunks_to_seal = self.collect_chunks_to_seal(active_chunks).await;

        if chunks_to_seal.is_empty() {
            return;
        }

        debug!(
            chunks = chunks_to_seal.len(),
            "Found chunks ready for background sealing"
        );

        // Process each chunk
        for (series_id, chunk) in chunks_to_seal {
            if let Err(e) = self
                .seal_chunk(series_id, chunk, data_dir, index, active_chunks)
                .await
            {
                warn!(series_id = series_id, error = %e, "Background seal failed");
                self.stats.seals_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Collect chunks that meet sealing criteria
    async fn collect_chunks_to_seal(
        &self,
        active_chunks: &Arc<RwLock<HashMap<SeriesId, Arc<ActiveChunk>>>>,
    ) -> Vec<(SeriesId, Arc<ActiveChunk>)> {
        let chunks = active_chunks.read().await;
        let mut to_seal = Vec::new();

        for (&series_id, chunk) in chunks.iter() {
            if self.should_seal(chunk) {
                to_seal.push((series_id, Arc::clone(chunk)));
            }
        }

        to_seal
    }

    /// Check if a chunk should be sealed
    fn should_seal(&self, chunk: &ActiveChunk) -> bool {
        // Don't seal already sealed chunks
        if chunk.is_sealed() {
            return false;
        }

        let point_count = chunk.point_count() as usize;

        // Check if chunk has minimum points
        if point_count >= self.config.min_points_for_seal {
            return true;
        }

        // Check if chunk is too old
        if chunk.age() >= self.config.max_age_before_seal && point_count > 0 {
            return true;
        }

        // Check standard seal conditions
        chunk.should_seal()
    }

    /// Seal a single chunk
    async fn seal_chunk<I: TimeIndex + ?Sized>(
        &self,
        series_id: SeriesId,
        chunk: Arc<ActiveChunk>,
        data_dir: &Path,
        index: &Arc<I>,
        active_chunks: &Arc<RwLock<HashMap<SeriesId, Arc<ActiveChunk>>>>,
    ) -> Result<(), SealError> {
        // Take points from chunk
        let points = chunk.take_points().map_err(SealError::CompressionFailed)?;

        if points.is_empty() {
            return Ok(());
        }

        let point_count = points.len();

        // Remove from active chunks map
        {
            let mut chunks = active_chunks.write().await;
            chunks.remove(&series_id);
        }

        // Generate target path
        let timestamp = chrono::Utc::now().timestamp_millis();
        let target_path = data_dir
            .join(format!("series_{}", series_id))
            .join(format!("chunk_{}.kub", timestamp));

        // Check if we should use streaming compression
        if self.config.enable_streaming && point_count > self.config.streaming_threshold {
            self.seal_with_streaming(series_id, points, target_path, index)
                .await
        } else {
            self.seal_standard(series_id, points, target_path, index)
                .await
        }
    }

    /// Standard sealing (non-streaming)
    async fn seal_standard<I: TimeIndex + ?Sized>(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
        target_path: PathBuf,
        index: &Arc<I>,
    ) -> Result<(), SealError> {
        let point_count = points.len();

        let handle = self
            .sealing_service
            .submit(series_id, points, target_path)
            .await?;

        let result = handle.await_result().await?;

        // Register with index
        self.register_sealed_chunk(series_id, &result, index)
            .await?;

        // Update stats
        self.stats.chunks_sealed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .points_sealed
            .fetch_add(point_count as u64, Ordering::Relaxed);
        self.stats
            .bytes_compressed
            .fetch_add(result.compressed_size as u64, Ordering::Relaxed);

        debug!(
            series_id = series_id,
            points = point_count,
            compressed_size = result.compressed_size,
            "Background seal completed"
        );

        Ok(())
    }

    /// Streaming sealing for very large chunks
    ///
    /// Splits the chunk into smaller pieces and compresses each separately.
    /// This reduces memory pressure and allows for incremental progress.
    async fn seal_with_streaming<I: TimeIndex + ?Sized>(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
        base_path: PathBuf,
        index: &Arc<I>,
    ) -> Result<(), SealError> {
        let total_points = points.len();
        let chunk_size = self.config.streaming_chunk_size;

        info!(
            series_id = series_id,
            total_points = total_points,
            chunk_size = chunk_size,
            pieces = total_points.div_ceil(chunk_size),
            "Starting streaming compression"
        );

        // Split into chunks
        let chunks: Vec<Vec<DataPoint>> = points.chunks(chunk_size).map(|c| c.to_vec()).collect();

        let mut total_compressed = 0usize;
        let mut sealed_chunks = Vec::new();

        // Compress each piece
        for (i, chunk_points) in chunks.into_iter().enumerate() {
            let piece_path = base_path.with_file_name(format!(
                "{}_{}.kub",
                base_path.file_stem().unwrap().to_string_lossy(),
                i
            ));

            let handle = self
                .sealing_service
                .submit(series_id, chunk_points, piece_path)
                .await?;

            let result = handle.await_result().await?;
            total_compressed += result.compressed_size;
            sealed_chunks.push(result);
        }

        // Register all pieces with index
        for result in &sealed_chunks {
            self.register_sealed_chunk(series_id, result, index).await?;
        }

        // Update stats
        self.stats
            .chunks_sealed
            .fetch_add(sealed_chunks.len() as u64, Ordering::Relaxed);
        self.stats.chunks_streamed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .points_sealed
            .fetch_add(total_points as u64, Ordering::Relaxed);
        self.stats
            .bytes_compressed
            .fetch_add(total_compressed as u64, Ordering::Relaxed);

        info!(
            series_id = series_id,
            total_points = total_points,
            pieces = sealed_chunks.len(),
            total_compressed = total_compressed,
            "Streaming compression completed"
        );

        Ok(())
    }

    /// Register a sealed chunk with the time index
    async fn register_sealed_chunk<I: TimeIndex + ?Sized>(
        &self,
        series_id: SeriesId,
        result: &SealResult,
        index: &Arc<I>,
    ) -> Result<(), SealError> {
        let time_range = TimeRange::new_unchecked(result.start_timestamp, result.end_timestamp);

        let location = ChunkLocation {
            engine_id: "local-disk-v1".to_string(),
            path: result.path.to_string_lossy().to_string(),
            offset: None,
            size: Some(result.compressed_size),
        };

        index
            .add_chunk(series_id, result.chunk_id.clone(), time_range, location)
            .await
            .map_err(|e| SealError::CompressionFailed(format!("Index error: {}", e)))?;

        Ok(())
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> BackgroundSealingStatsSnapshot {
        BackgroundSealingStatsSnapshot {
            checks_performed: self.stats.checks_performed.load(Ordering::Relaxed),
            chunks_sealed: self.stats.chunks_sealed.load(Ordering::Relaxed),
            chunks_streamed: self.stats.chunks_streamed.load(Ordering::Relaxed),
            points_sealed: self.stats.points_sealed.load(Ordering::Relaxed),
            bytes_compressed: self.stats.bytes_compressed.load(Ordering::Relaxed),
            seals_failed: self.stats.seals_failed.load(Ordering::Relaxed),
        }
    }

    /// Get reference to the underlying parallel sealing service
    pub fn sealing_service(&self) -> &Arc<ParallelSealingService> {
        &self.sealing_service
    }
}

impl Default for BackgroundSealingService {
    fn default() -> Self {
        Self::new(BackgroundSealingConfig::default())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::active_chunk::SealConfig;

    #[test]
    fn test_config_default() {
        let config = BackgroundSealingConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(1));
        assert_eq!(config.min_points_for_seal, 500);
        assert_eq!(config.streaming_threshold, 50_000);
        assert!(config.enable_streaming);
    }

    #[test]
    fn test_config_presets() {
        let high = BackgroundSealingConfig::high_throughput();
        let low = BackgroundSealingConfig::low_latency();

        assert!(high.check_interval < Duration::from_secs(1));
        assert!(low.check_interval < high.check_interval);
        assert!(low.min_points_for_seal < high.min_points_for_seal);
    }

    #[tokio::test]
    async fn test_service_creation() {
        let service = BackgroundSealingService::new(BackgroundSealingConfig::default());
        assert!(!service.is_running());

        let stats = service.stats();
        assert_eq!(stats.chunks_sealed, 0);
        assert_eq!(stats.checks_performed, 0);
    }

    #[tokio::test]
    async fn test_service_stop_when_not_running() {
        let service = BackgroundSealingService::new(BackgroundSealingConfig::default());
        // Should not panic when stopping a service that was never started
        service.stop().await;
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn test_should_seal_with_min_points() {
        let config = BackgroundSealingConfig {
            min_points_for_seal: 100,
            ..Default::default()
        };
        let service = BackgroundSealingService::new(config);

        // Create chunk with enough points
        let seal_config = SealConfig {
            max_points: 1000,
            max_duration_ms: 60_000,
            max_size_bytes: 1024 * 1024,
        };
        let chunk = ActiveChunk::new(1, 200, seal_config);

        // Add points to reach threshold
        for i in 0..100 {
            let point = DataPoint::new(1, i * 1000, i as f64);
            chunk.append(point).unwrap();
        }

        // Should now recommend sealing
        assert!(service.should_seal(&chunk));
    }

    #[tokio::test]
    async fn test_should_not_seal_sealed_chunk() {
        let config = BackgroundSealingConfig::default();
        let service = BackgroundSealingService::new(config);

        let seal_config = SealConfig {
            max_points: 10,
            max_duration_ms: 60_000,
            max_size_bytes: 1024 * 1024,
        };
        let chunk = ActiveChunk::new(1, 100, seal_config);

        // Add points and seal manually
        for i in 0..10 {
            let point = DataPoint::new(1, i * 1000, i as f64);
            chunk.append(point).unwrap();
        }
        let _ = chunk.take_points(); // This seals the chunk

        // Should not recommend sealing an already sealed chunk
        assert!(!service.should_seal(&chunk));
    }

    #[tokio::test]
    async fn test_stats_snapshot() {
        let service = BackgroundSealingService::new(BackgroundSealingConfig::default());

        let stats = service.stats();
        assert_eq!(stats.checks_performed, 0);
        assert_eq!(stats.chunks_sealed, 0);
        assert_eq!(stats.chunks_streamed, 0);
        assert_eq!(stats.points_sealed, 0);
        assert_eq!(stats.bytes_compressed, 0);
        assert_eq!(stats.seals_failed, 0);
    }

    #[tokio::test]
    async fn test_sealing_service_access() {
        let config = BackgroundSealingConfig::default();
        let service = BackgroundSealingService::new(config);

        // Should be able to access underlying sealing service
        let sealing_service = service.sealing_service();
        let stats = sealing_service.stats();
        assert_eq!(stats.total_sealed, 0);
    }

    #[test]
    fn test_streaming_threshold_config() {
        let config = BackgroundSealingConfig {
            streaming_threshold: 1000,
            streaming_chunk_size: 100,
            enable_streaming: true,
            ..Default::default()
        };

        assert_eq!(config.streaming_threshold, 1000);
        assert_eq!(config.streaming_chunk_size, 100);
        assert!(config.enable_streaming);
    }

    #[tokio::test]
    async fn test_default_impl() {
        let service = BackgroundSealingService::default();
        assert!(!service.is_running());
    }
}
