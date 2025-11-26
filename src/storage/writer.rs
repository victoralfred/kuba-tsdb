//! Chunk writer for managing active chunks and automatic sealing
//!
//! The `ChunkWriter` handles the lifecycle of chunks for a single time series,
//! including:
//! - Point batching for efficient writes
//! - Automatic chunk rotation based on configurable thresholds
//! - Async write pipeline with backpressure handling
//! - Automatic sealing when thresholds are exceeded
use crate::storage::active_chunk::{ActiveChunk, SealConfig};
use crate::storage::chunk::Chunk;
use crate::types::{DataPoint, SeriesId};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

/// Write statistics for monitoring
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    /// Total points written
    pub points_written: u64,

    /// Total chunks sealed
    pub chunks_sealed: u64,

    /// Total write errors
    pub write_errors: u64,

    /// Last write timestamp
    pub last_write: Option<Instant>,

    /// Current active chunk point count
    pub active_chunk_points: u32,
}

/// Chunk writer for a single time series
///
/// Manages the lifecycle of chunks including automatic rotation and sealing.
///
/// # Example
///
/// ```
/// use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
/// use gorilla_tsdb::types::DataPoint;
///
/// # async fn example() -> Result<(), String> {
/// let config = ChunkWriterConfig::default();
/// let mut writer = ChunkWriter::new(1, "/data".into(), config);
///
/// // Write points
/// writer.write(DataPoint {
///     series_id: 1,
///     timestamp: 1000,
///     value: 42.0,
/// }).await?;
///
/// // Check if seal is needed
/// if writer.should_seal().await {
///     let chunk = writer.seal().await?;
///     // Handle sealed chunk
/// }
/// # Ok(())
/// # }
/// ```
pub struct ChunkWriter {
    /// Series ID this writer handles
    series_id: SeriesId,

    /// Base directory for chunk files
    base_path: PathBuf,

    /// Currently active chunk
    active_chunk: Arc<RwLock<Option<Arc<ActiveChunk>>>>,

    /// Writer configuration
    config: ChunkWriterConfig,

    /// Write statistics
    stats: Arc<RwLock<WriteStats>>,

    /// Seal channel sender
    seal_tx: mpsc::UnboundedSender<SealRequest>,

    /// Sealed chunks receiver
    sealed_rx: Arc<RwLock<mpsc::UnboundedReceiver<Chunk>>>,
}

/// Configuration for chunk writer
#[derive(Debug, Clone)]
pub struct ChunkWriterConfig {
    /// Maximum points per chunk before sealing
    pub max_points: usize,

    /// Maximum duration before sealing
    pub max_duration: Duration,

    /// Maximum memory size before sealing
    pub max_size_bytes: usize,

    /// Initial chunk capacity hint
    pub initial_capacity: usize,

    /// Enable automatic sealing
    pub auto_seal: bool,

    /// Write buffer size
    pub write_buffer_size: usize,
}

impl Default for ChunkWriterConfig {
    fn default() -> Self {
        Self {
            max_points: 10_000,
            max_duration: Duration::from_secs(3600), // 1 hour
            max_size_bytes: 1024 * 1024,             // 1 MB
            initial_capacity: 1_000,
            auto_seal: true,
            write_buffer_size: 1_000,
        }
    }
}

impl ChunkWriterConfig {
    /// Convert to SealConfig
    fn to_seal_config(&self) -> SealConfig {
        SealConfig {
            max_points: self.max_points,
            max_duration_ms: self.max_duration.as_millis() as i64,
            max_size_bytes: self.max_size_bytes,
        }
    }
}

/// Internal seal request
struct SealRequest {
    chunk: ActiveChunk,
    path: PathBuf,
    response_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
}

impl ChunkWriter {
    /// Create a new chunk writer
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series ID for this writer
    /// * `base_path` - Base directory for chunk files
    /// * `config` - Writer configuration
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
    ///
    /// # async fn example() {
    /// let config = ChunkWriterConfig {
    ///     max_points: 5_000,
    ///     ..Default::default()
    /// };
    /// let writer = ChunkWriter::new(1, "/data".into(), config);
    /// # }
    /// ```
    pub fn new(series_id: SeriesId, base_path: PathBuf, config: ChunkWriterConfig) -> Self {
        let (seal_tx, mut seal_rx) = mpsc::unbounded_channel::<SealRequest>();
        let (sealed_tx, sealed_rx) = mpsc::unbounded_channel::<Chunk>();

        // Spawn background seal worker
        tokio::spawn(async move {
            while let Some(req) = seal_rx.recv().await {
                let result = req.chunk.seal(req.path).await;

                // Send to sealed channel if successful
                match result {
                    Ok(chunk) => {
                        let _ = req.response_tx.send(Ok(()));
                        let _ = sealed_tx.send(chunk);
                    }
                    Err(e) => {
                        let _ = req.response_tx.send(Err(e));
                    }
                }
            }
        });

        Self {
            series_id,
            base_path,
            active_chunk: Arc::new(RwLock::new(None)),
            config,
            stats: Arc::new(RwLock::new(WriteStats::default())),
            seal_tx,
            sealed_rx: Arc::new(RwLock::new(sealed_rx)),
        }
    }

    /// Write a single point
    ///
    /// This method will automatically create a new chunk if needed and
    /// trigger sealing if thresholds are exceeded (when auto_seal is enabled).
    ///
    /// # Arguments
    ///
    /// * `point` - Data point to write
    ///
    /// # Returns
    ///
    /// - `Ok(())` if write succeeded
    /// - `Err(String)` if write failed
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// # async fn example() -> Result<(), String> {
    /// let mut writer = ChunkWriter::new(1, "/data".into(), ChunkWriterConfig::default());
    ///
    /// writer.write(DataPoint {
    ///     series_id: 1,
    ///     timestamp: 1000,
    ///     value: 42.0,
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, point: DataPoint) -> Result<(), String> {
        // Validate series ID
        if point.series_id != self.series_id {
            crate::metrics::record_error("series_mismatch", "write");
            return Err(format!(
                "Point series_id {} doesn't match writer series_id {}",
                point.series_id, self.series_id
            ));
        }

        // Apply rate limiting if enabled
        if crate::config::Config::default()
            .security
            .enable_rate_limiting
            && !crate::security::check_write_rate_limit()
        {
            crate::metrics::record_error("rate_limit", "write");
            return Err("Write rate limit exceeded".to_string());
        }

        // Get or create active chunk
        let chunk = {
            let mut active = self.active_chunk.write().await;
            if active.is_none() {
                *active = Some(Arc::new(self.create_chunk()));
            }
            Arc::clone(active.as_ref().unwrap())
        };

        // Write point to active chunk
        let result = chunk.append(point);

        // Update statistics and check if seal is needed
        let should_seal = if result.is_ok() {
            let mut stats = self.stats.write().await;
            stats.points_written += 1;
            stats.last_write = Some(Instant::now());
            stats.active_chunk_points = chunk.point_count();
            chunk.should_seal()
        } else {
            false
        };

        // Drop the Arc reference before sealing
        drop(chunk);

        // Check if auto-seal is needed
        if self.config.auto_seal && should_seal {
            // Trigger background seal
            self.seal_async().await?;
        }

        result
    }

    /// Write multiple points in a batch
    ///
    /// This is more efficient than calling `write()` in a loop because
    /// it uses the batch write API internally.
    ///
    /// # Arguments
    ///
    /// * `points` - Vector of data points to write
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of points successfully written
    /// - `Err(String)` - If the operation failed
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// # async fn example() -> Result<(), String> {
    /// let mut writer = ChunkWriter::new(1, "/data".into(), ChunkWriterConfig::default());
    ///
    /// let points = vec![
    ///     DataPoint { series_id: 1, timestamp: 1000, value: 1.0 },
    ///     DataPoint { series_id: 1, timestamp: 2000, value: 2.0 },
    ///     DataPoint { series_id: 1, timestamp: 3000, value: 3.0 },
    /// ];
    ///
    /// let count = writer.write_batch(points).await?;
    /// assert_eq!(count, 3);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_batch(&self, points: Vec<DataPoint>) -> Result<usize, String> {
        if points.is_empty() {
            return Ok(0);
        }

        // Validate all points have correct series ID
        for point in &points {
            if point.series_id != self.series_id {
                return Err(format!(
                    "Point series_id {} doesn't match writer series_id {}",
                    point.series_id, self.series_id
                ));
            }
        }

        // Get or create active chunk
        let chunk = {
            let mut active = self.active_chunk.write().await;
            if active.is_none() {
                *active = Some(Arc::new(self.create_chunk()));
            }
            Arc::clone(active.as_ref().unwrap())
        };

        // Write batch to active chunk
        let count = chunk.append_batch(points)?;

        // Update statistics and check if seal is needed
        let should_seal = {
            let mut stats = self.stats.write().await;
            stats.points_written += count as u64;
            stats.last_write = Some(Instant::now());
            stats.active_chunk_points = chunk.point_count();
            chunk.should_seal()
        };

        // Drop the Arc reference before sealing
        drop(chunk);

        // Check if auto-seal is needed
        if self.config.auto_seal && should_seal {
            self.seal_async().await?;
        }

        Ok(count)
    }

    /// Check if the active chunk should be sealed
    ///
    /// This is a lock-free check that doesn't modify state.
    pub async fn should_seal(&self) -> bool {
        let active = self.active_chunk.read().await;
        if let Some(chunk) = active.as_ref() {
            chunk.should_seal()
        } else {
            false
        }
    }

    /// Manually seal the active chunk
    ///
    /// This removes the active chunk and seals it to disk synchronously.
    ///
    /// # Returns
    ///
    /// - `Ok(Chunk)` - The sealed chunk
    /// - `Err(String)` - If sealing failed or no active chunk exists
    pub async fn seal(&self) -> Result<Chunk, String> {
        // Atomically swap active chunk for a new one
        // This ensures writes during seal go to the new chunk
        let chunk_to_seal = {
            let mut active = self.active_chunk.write().await;
            let old = active.take();

            // Only install new chunk if old chunk had points
            // Otherwise, put the empty chunk back
            if let Some(ref chunk) = old {
                if chunk.point_count() > 0 {
                    *active = Some(Arc::new(self.create_chunk()));
                } else {
                    // Put empty chunk back, don't seal it
                    *active = old.clone();
                    return Err("No active chunk to seal (empty)".to_string());
                }
            }
            old
        };

        if let Some(active_arc) = chunk_to_seal {
            // Try to unwrap Arc to get ownership
            let active = match Arc::try_unwrap(active_arc) {
                Ok(chunk) => chunk,
                Err(arc) => {
                    // Arc has other references - this means a write grabbed it
                    // before we could seal. Wait a tiny bit for writes to complete
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;

                    // Try unwrap again
                    match Arc::try_unwrap(arc) {
                        Ok(chunk) => chunk,
                        Err(arc) => {
                            return Err(format!(
                                "Cannot seal: chunk has {} active references (concurrent access)",
                                Arc::strong_count(&arc)
                            ));
                        }
                    }
                }
            };

            // Now seal the old chunk (new chunk is already active)
            let path = self.generate_chunk_path();
            let sealed = active.seal(path).await?;

            // Update statistics
            {
                let mut stats = self.stats.write().await;
                stats.chunks_sealed += 1;
                // Don't set active_chunk_points to 0 - new chunk may already have points
                // Read from actual active chunk instead
                let active_guard = self.active_chunk.read().await;
                stats.active_chunk_points =
                    active_guard.as_ref().map(|c| c.point_count()).unwrap_or(0);
            }

            Ok(sealed)
        } else {
            Err("No active chunk to seal".to_string())
        }
    }

    /// Seal active chunk asynchronously in the background
    ///
    /// This allows writing to continue to a new chunk immediately.
    async fn seal_async(&self) -> Result<(), String> {
        // Take ownership of active chunk and create new one
        let chunk_to_seal = {
            let mut active = self.active_chunk.write().await;
            let old = active.take();
            *active = Some(Arc::new(self.create_chunk()));
            old
        };

        if let Some(active_arc) = chunk_to_seal {
            // Try to unwrap Arc to get ownership
            let active = match Arc::try_unwrap(active_arc) {
                Ok(chunk) => chunk,
                Err(arc) => {
                    // Arc has other references - put it back and return error
                    // This can happen in rare race conditions
                    let ref_count = Arc::strong_count(&arc);
                    let mut active_guard = self.active_chunk.write().await;
                    *active_guard = Some(arc);
                    return Err(format!(
                        "Cannot seal: chunk has {} active references (concurrent access detected)",
                        ref_count
                    ));
                }
            };

            let path = self.generate_chunk_path();
            let (response_tx, _response_rx) = tokio::sync::oneshot::channel();

            // Send seal request to background worker
            self.seal_tx
                .send(SealRequest {
                    chunk: active,
                    path,
                    response_tx,
                })
                .map_err(|e| format!("Failed to send seal request: {}", e))?;

            // Don't wait for response, continue immediately
            // Background worker will handle the seal

            // Update statistics
            {
                let mut stats = self.stats.write().await;
                stats.chunks_sealed += 1;
                stats.active_chunk_points = 0;
            }

            Ok(())
        } else {
            Ok(()) // No chunk to seal, not an error
        }
    }

    /// Get the next sealed chunk (non-blocking)
    ///
    /// Returns `None` if no sealed chunks are available.
    pub async fn poll_sealed(&self) -> Option<Chunk> {
        let mut rx = self.sealed_rx.write().await;
        rx.try_recv().ok()
    }

    /// Wait for the next sealed chunk
    ///
    /// This will block until a chunk is sealed.
    pub async fn next_sealed(&self) -> Option<Chunk> {
        let mut rx = self.sealed_rx.write().await;
        rx.recv().await
    }

    /// Get current write statistics
    pub async fn stats(&self) -> WriteStats {
        let mut stats = self.stats.read().await.clone();

        // Always read active_chunk_points from live chunk for accuracy
        // This ensures stats reflect concurrent writes during seal
        let active_guard = self.active_chunk.read().await;
        stats.active_chunk_points = active_guard.as_ref().map(|c| c.point_count()).unwrap_or(0);

        stats
    }

    /// Flush all pending writes and seal the active chunk
    ///
    /// This ensures all data is persisted to disk.
    pub async fn flush(&self) -> Result<Option<Chunk>, String> {
        let has_points = {
            let active = self.active_chunk.read().await;
            active.as_ref().map(|c| c.point_count()).unwrap_or(0) > 0
        };

        if has_points {
            Ok(Some(self.seal().await?))
        } else {
            Ok(None)
        }
    }

    /// Create a new active chunk
    fn create_chunk(&self) -> ActiveChunk {
        ActiveChunk::new(
            self.series_id,
            self.config.initial_capacity,
            self.config.to_seal_config(),
        )
    }

    /// Generate a unique path for a chunk file
    fn generate_chunk_path(&self) -> PathBuf {
        use uuid::Uuid;
        let chunk_id = Uuid::new_v4();
        self.base_path.join(format!("chunk_{}.gor", chunk_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        let stats = writer.stats().await;
        assert_eq!(stats.points_written, 0);
        assert_eq!(stats.chunks_sealed, 0);
    }

    #[tokio::test]
    async fn test_write_single_point() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        let point = DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0,
        };

        writer.write(point).await.unwrap();

        let stats = writer.stats().await;
        assert_eq!(stats.points_written, 1);
        assert_eq!(stats.active_chunk_points, 1);
    }

    #[tokio::test]
    async fn test_write_batch() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        let points = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 1.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 2.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 3000,
                value: 3.0,
            },
        ];

        let count = writer.write_batch(points).await.unwrap();
        assert_eq!(count, 3);

        let stats = writer.stats().await;
        assert_eq!(stats.points_written, 3);
        assert_eq!(stats.active_chunk_points, 3);
    }

    #[tokio::test]
    async fn test_write_wrong_series() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        let point = DataPoint {
            series_id: 2, // Wrong series
            timestamp: 1000,
            value: 42.0,
        };

        let result = writer.write(point).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("doesn't match"));
    }

    #[tokio::test]
    async fn test_manual_seal() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        // Write some points
        for i in 0..10 {
            writer
                .write(DataPoint {
                    series_id: 1,
                    timestamp: i * 1000,
                    value: i as f64,
                })
                .await
                .unwrap();
        }

        // Manual seal
        let chunk = writer.seal().await.unwrap();
        assert_eq!(chunk.point_count(), 10);

        let stats = writer.stats().await;
        assert_eq!(stats.chunks_sealed, 1);
        assert_eq!(stats.active_chunk_points, 0);
    }

    #[tokio::test]
    async fn test_auto_seal() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkWriterConfig {
            max_points: 5,
            auto_seal: true,
            ..Default::default()
        };

        let writer = ChunkWriter::new(1, temp_dir.path().to_path_buf(), config);

        // Write enough points to trigger auto-seal
        for i in 0..10 {
            writer
                .write(DataPoint {
                    series_id: 1,
                    timestamp: i * 1000,
                    value: i as f64,
                })
                .await
                .unwrap();
        }

        // Give background seal time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let stats = writer.stats().await;
        assert!(stats.chunks_sealed >= 1);
    }

    #[tokio::test]
    async fn test_should_seal() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChunkWriterConfig {
            max_points: 5,
            auto_seal: false,
            ..Default::default()
        };

        let writer = ChunkWriter::new(1, temp_dir.path().to_path_buf(), config);

        // Initially should not seal
        assert!(!writer.should_seal().await);

        // Write points up to threshold
        for i in 0..5 {
            writer
                .write(DataPoint {
                    series_id: 1,
                    timestamp: i * 1000,
                    value: i as f64,
                })
                .await
                .unwrap();
        }

        // Should now need sealing
        assert!(writer.should_seal().await);
    }

    #[tokio::test]
    async fn test_flush() {
        let temp_dir = TempDir::new().unwrap();
        let writer = ChunkWriter::new(
            1,
            temp_dir.path().to_path_buf(),
            ChunkWriterConfig::default(),
        );

        // Write points
        writer
            .write(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .await
            .unwrap();

        // Flush
        let chunk = writer.flush().await.unwrap();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().point_count(), 1);

        // Flush again with no active chunk
        let chunk2 = writer.flush().await.unwrap();
        assert!(chunk2.is_none());
    }
}
