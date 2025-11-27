//! Write-Ahead Log (WAL) module for durability
//!
//! Provides crash-safe durability by persisting data points before they
//! are written to the main storage. The WAL ensures no data loss even
//! in the event of sudden system failure.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        WAL Manager                               │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
//! │  │  Segment 0  │  │  Segment 1  │  │  Segment 2  │  ...         │
//! │  │  (sealed)   │  │  (sealed)   │  │  (active)   │              │
//! │  └─────────────┘  └─────────────┘  └─────────────┘              │
//! │         │               │               │                        │
//! │         v               v               v                        │
//! │  ┌────────────────────────────────────────────────────────────┐ │
//! │  │                    Segment Files                            │ │
//! │  │  wal-00000001.log  wal-00000002.log  wal-00000003.log       │ │
//! │  └────────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Record Format
//!
//! Each WAL record has the following format:
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬───────────┬──────────┐
//! │  Length  │   Type   │   CRC32  │  Payload  │ Padding  │
//! │  4 bytes │  1 byte  │  4 bytes │  N bytes  │ 0-7 byte │
//! └──────────┴──────────┴──────────┴───────────┴──────────┘
//! ```
//!
//! # Features
//!
//! - **Segment-based storage**: Fixed-size segments for easier management
//! - **CRC32 checksums**: Detect corruption during recovery
//! - **Memory-mapped I/O**: High-performance writes
//! - **Batch coalescing**: Group multiple writes for efficiency
//! - **Automatic rotation**: New segments created when size limit reached
//! - **Parallel recovery**: Multi-threaded segment replay
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::ingestion::wal::{WalManager, WalConfig};
//! use gorilla_tsdb::types::DataPoint;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = WalConfig::default();
//! let wal = WalManager::new(config).await?;
//!
//! // Write a data point
//! let point = DataPoint::new(1, 1700000000000, 42.5);
//! wal.write(point).await?;
//!
//! // Sync to disk
//! wal.sync().await?;
//!
//! // Recover from WAL on restart
//! let recovered = wal.recover().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod record;
pub mod recovery;
pub mod segment;
pub mod writer;

pub use config::{SyncMode, WalConfig};
pub use error::{WalError, WalResult};
pub use record::{RecordType, WalRecord};
pub use recovery::WalRecovery;
pub use segment::{SegmentId, WalSegment};
pub use writer::{WalWriter, WriteRequest};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::types::DataPoint;

/// Sequence number for WAL records
pub type SequenceNumber = u64;

/// WAL Manager coordinates all WAL operations
///
/// Manages segment lifecycle, handles writes, and coordinates recovery.
pub struct WalManager {
    /// Configuration
    config: WalConfig,
    /// Current active segment
    active_segment: RwLock<Option<Arc<WalSegment>>>,
    /// List of sealed segments awaiting cleanup
    sealed_segments: RwLock<Vec<Arc<WalSegment>>>,
    /// Global sequence number
    sequence: AtomicU64,
    /// WAL writer for batched writes (Option because it's moved when started)
    writer: parking_lot::Mutex<Option<WalWriter>>,
    /// Channel for write requests
    write_tx: mpsc::Sender<WriteRequest>,
    /// Statistics
    stats: WalStats,
}

impl WalManager {
    /// Create a new WAL manager
    ///
    /// # Arguments
    ///
    /// * `config` - WAL configuration
    ///
    /// # Errors
    ///
    /// Returns error if the WAL directory cannot be created or accessed.
    pub async fn new(config: WalConfig) -> WalResult<Self> {
        config.validate()?;

        // Ensure WAL directory exists
        std::fs::create_dir_all(&config.directory)?;

        let (write_tx, write_rx) = mpsc::channel(config.write_buffer_size);
        let writer = WalWriter::new(config.clone(), write_rx);

        info!("WAL manager initialized at {:?}", config.directory);

        Ok(Self {
            config,
            active_segment: RwLock::new(None),
            sealed_segments: RwLock::new(Vec::new()),
            sequence: AtomicU64::new(0),
            writer: parking_lot::Mutex::new(Some(writer)),
            write_tx,
            stats: WalStats::default(),
        })
    }

    /// Start the WAL background tasks
    pub async fn start(&self) -> WalResult<()> {
        // Create initial segment
        self.rotate_segment().await?;

        // Take ownership of the writer and start it
        let writer = self.writer.lock().take().ok_or_else(|| WalError::Config {
            message: "WAL writer already started".to_string(),
        })?;

        tokio::spawn(async move {
            if let Err(e) = writer.run().await {
                warn!("WAL writer error: {}", e);
            }
        });

        info!("WAL manager started");
        Ok(())
    }

    /// Write a data point to the WAL
    ///
    /// # Arguments
    ///
    /// * `point` - The data point to write
    ///
    /// # Returns
    ///
    /// The sequence number assigned to this write.
    pub async fn write(&self, point: DataPoint) -> WalResult<SequenceNumber> {
        let seq = self.next_sequence();

        let request = WriteRequest {
            sequence: seq,
            points: vec![point],
        };

        self.write_tx
            .send(request)
            .await
            .map_err(|_| WalError::ChannelClosed)?;

        self.stats.records_written.fetch_add(1, Ordering::Relaxed);
        Ok(seq)
    }

    /// Write a batch of data points to the WAL
    ///
    /// More efficient than writing points individually.
    pub async fn write_batch(&self, points: Vec<DataPoint>) -> WalResult<SequenceNumber> {
        if points.is_empty() {
            return Ok(self.sequence.load(Ordering::Relaxed));
        }

        let seq = self.next_sequence();
        let count = points.len();

        let request = WriteRequest {
            sequence: seq,
            points,
        };

        self.write_tx
            .send(request)
            .await
            .map_err(|_| WalError::ChannelClosed)?;

        self.stats
            .records_written
            .fetch_add(count as u64, Ordering::Relaxed);
        Ok(seq)
    }

    /// Sync WAL to disk
    ///
    /// Ensures all written data is persisted to stable storage.
    pub async fn sync(&self) -> WalResult<()> {
        let segment = self.active_segment.read().clone();
        if let Some(seg) = segment {
            seg.sync().await?;
        }
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Rotate to a new segment
    ///
    /// Seals the current segment and creates a new active segment.
    pub async fn rotate_segment(&self) -> WalResult<()> {
        let new_id = self.next_segment_id();
        let new_segment = Arc::new(
            WalSegment::create(&self.config.directory, new_id, self.config.segment_size).await?,
        );

        // Swap segments
        let old_segment = {
            let mut active = self.active_segment.write();
            let old = active.take();
            *active = Some(Arc::clone(&new_segment));
            old
        };

        // Seal old segment if it exists
        if let Some(old) = old_segment {
            old.seal().await?;
            self.sealed_segments.write().push(old);
        }

        self.stats.rotations.fetch_add(1, Ordering::Relaxed);
        debug!("Rotated to segment {}", new_id);

        Ok(())
    }

    /// Recover data from WAL after restart
    ///
    /// Replays all records from existing segments and returns the recovered points.
    pub async fn recover(&self) -> WalResult<Vec<DataPoint>> {
        let recovery = WalRecovery::new(&self.config);
        let points = recovery.recover_all().await?;

        info!("Recovered {} points from WAL", points.len());
        self.stats
            .records_recovered
            .store(points.len() as u64, Ordering::Relaxed);

        Ok(points)
    }

    /// Clean up old segments that have been checkpointed
    ///
    /// # Arguments
    ///
    /// * `checkpoint_seq` - Sequence number up to which data has been persisted
    pub async fn cleanup(&self, checkpoint_seq: SequenceNumber) -> WalResult<usize> {
        let mut removed = 0;

        // Remove sealed segments that are fully checkpointed
        let to_remove: Vec<_> = {
            let segments = self.sealed_segments.read();
            segments
                .iter()
                .filter(|s| s.max_sequence() <= checkpoint_seq)
                .cloned()
                .collect()
        };

        for segment in to_remove {
            segment.delete().await?;
            removed += 1;
        }

        // Update sealed segments list
        {
            let mut segments = self.sealed_segments.write();
            segments.retain(|s| s.max_sequence() > checkpoint_seq);
        }

        if removed > 0 {
            debug!("Cleaned up {} WAL segments", removed);
        }

        Ok(removed)
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> SequenceNumber {
        self.sequence.load(Ordering::Relaxed)
    }

    /// Get next sequence number
    fn next_sequence(&self) -> SequenceNumber {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }

    /// Get next segment ID
    fn next_segment_id(&self) -> SegmentId {
        let segments = self.sealed_segments.read();
        let active = self.active_segment.read();

        let max_sealed = segments.iter().map(|s| s.id()).max().unwrap_or(0);
        let active_id = active.as_ref().map(|s| s.id()).unwrap_or(0);

        max_sealed.max(active_id) + 1
    }

    /// Get WAL statistics
    pub fn stats(&self) -> WalStatsSnapshot {
        WalStatsSnapshot {
            records_written: self.stats.records_written.load(Ordering::Relaxed),
            records_recovered: self.stats.records_recovered.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            syncs: self.stats.syncs.load(Ordering::Relaxed),
            rotations: self.stats.rotations.load(Ordering::Relaxed),
            active_segment_size: self
                .active_segment
                .read()
                .as_ref()
                .map(|s| s.size())
                .unwrap_or(0),
            sealed_segment_count: self.sealed_segments.read().len(),
        }
    }

    /// Get WAL directory path
    pub fn directory(&self) -> &PathBuf {
        &self.config.directory
    }

    /// Gracefully shutdown the WAL
    pub async fn shutdown(self) -> WalResult<()> {
        info!("Shutting down WAL manager");

        // Final sync
        self.sync().await?;

        // Close writer
        drop(self.write_tx);

        info!("WAL manager shutdown complete");
        Ok(())
    }
}

/// WAL statistics (internal)
#[derive(Default)]
struct WalStats {
    records_written: AtomicU64,
    records_recovered: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
    rotations: AtomicU64,
}

/// WAL statistics snapshot
#[derive(Debug, Clone, Default)]
pub struct WalStatsSnapshot {
    /// Total records written
    pub records_written: u64,
    /// Records recovered on startup
    pub records_recovered: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of sync operations
    pub syncs: u64,
    /// Number of segment rotations
    pub rotations: u64,
    /// Current active segment size
    pub active_segment_size: usize,
    /// Number of sealed segments
    pub sealed_segment_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> WalConfig {
        WalConfig {
            directory: dir.to_path_buf(),
            segment_size: 1024 * 1024, // 1MB
            sync_mode: SyncMode::Immediate,
            write_buffer_size: 1000,
            max_segments: 10,
        }
    }

    #[tokio::test]
    async fn test_wal_manager_creation() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        let wal = WalManager::new(config).await;
        assert!(wal.is_ok());
    }

    #[tokio::test]
    async fn test_sequence_generation() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let wal = WalManager::new(config).await.unwrap();

        let seq1 = wal.next_sequence();
        let seq2 = wal.next_sequence();
        let seq3 = wal.next_sequence();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let wal = WalManager::new(config).await.unwrap();

        let stats = wal.stats();
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.syncs, 0);
    }
}
