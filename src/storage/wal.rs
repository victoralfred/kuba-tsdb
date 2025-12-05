//! Write-Ahead Logging (WAL) Module
//!
//! This module provides durability for data before chunks are sealed, enabling
//! crash recovery of unsealed data. The WAL ensures that no data is lost even
//! if the process crashes before data is compressed and written to chunk files.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                        Write-Ahead Log (WAL)                            │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐     ┌───────────────────────────────────────────────┐ │
//! │  │ WalWriter    │────▶│ Segment Files                                 │ │
//! │  │ append()     │     │ wal-000001.log, wal-000002.log, ...           │ │
//! │  └──────────────┘     └───────────────────────────────────────────────┘ │
//! │         │                              │                                │
//! │         ▼                              ▼                                │
//! │  ┌──────────────┐     ┌───────────────────────────────────────────────┐ │
//! │  │ WalEntry     │     │ WalReader + WalRecovery                       │ │
//! │  │ (checksum)   │     │ (replay on startup)                           │ │
//! │  └──────────────┘     └───────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Recovery Process
//!
//! 1. On startup, `WalRecovery::recover()` scans all WAL segments
//! 2. Each entry is verified via checksum
//! 3. Valid entries are replayed into active chunks
//! 4. After successful recovery, old WAL segments are truncated
//! 5. Normal operation resumes with new WAL writes
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::wal::{WalWriter, WalConfig, WalRecovery};
//!
//! // Create WAL writer
//! let config = WalConfig::default();
//! let writer = WalWriter::new(config, "/data/wal")?;
//!
//! // Write data to WAL before buffering
//! writer.append(series_id, &points)?;
//! writer.sync()?;  // Ensure durability
//!
//! // On startup, recover any unsealed data
//! let recovery = WalRecovery::new("/data/wal", db)?;
//! recovery.recover()?;
//! ```

use crate::types::{DataPoint, SeriesId};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tracing::{debug, error, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Magic bytes identifying a WAL file
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x4B]; // "WALK" (WAL Kuba)

/// Current WAL format version
const WAL_VERSION: u8 = 1;

/// Size of the WAL entry header (magic + version + entry_type + crc + length)
const ENTRY_HEADER_SIZE: usize = 4 + 1 + 1 + 4 + 4; // 14 bytes

/// Default segment file size before rotation (64MB)
const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Minimum segment size (1MB)
const MIN_SEGMENT_SIZE: u64 = 1024 * 1024;

/// Maximum segment size (1GB)
const MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during WAL operations
#[derive(Debug, Error)]
pub enum WalError {
    /// I/O error during WAL operations
    #[error("WAL I/O error: {0}")]
    Io(#[from] io::Error),

    /// Invalid WAL file magic bytes
    #[error("Invalid WAL magic bytes")]
    InvalidMagic,

    /// Unsupported WAL version
    #[error("Unsupported WAL version: {0}")]
    UnsupportedVersion(u8),

    /// Checksum verification failed
    #[error("Checksum verification failed for entry at offset {offset}")]
    ChecksumMismatch {
        /// File offset where checksum mismatch occurred
        offset: u64,
    },

    /// Corrupted WAL entry
    #[error("Corrupted WAL entry at offset {offset}: {reason}")]
    CorruptedEntry {
        /// File offset of corrupted entry
        offset: u64,
        /// Description of corruption
        reason: String,
    },

    /// WAL directory doesn't exist
    #[error("WAL directory does not exist: {0}")]
    DirectoryNotFound(PathBuf),

    /// Segment file not found
    #[error("Segment file not found: {0}")]
    SegmentNotFound(PathBuf),

    /// Entry too large
    #[error("Entry size {size} exceeds maximum {max}")]
    EntryTooLarge {
        /// Actual entry size
        size: usize,
        /// Maximum allowed size
        max: usize,
    },

    /// Invalid configuration
    #[error("Invalid WAL configuration: {0}")]
    InvalidConfig(String),

    /// Recovery failed
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),
}

// ============================================================================
// Configuration
// ============================================================================

/// Synchronization mode for WAL writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// No explicit sync - relies on OS buffer flushing
    /// Fastest but least durable
    NoSync,

    /// Sync after each write batch
    /// Good balance of performance and durability
    #[default]
    BatchSync,

    /// Sync after every write (most durable)
    /// Slowest but guarantees durability
    EveryWrite,
}

/// Configuration for Write-Ahead Logging
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum size of a single segment file before rotation
    ///
    /// Default: 64MB
    pub segment_size: u64,

    /// Sync mode for durability
    ///
    /// Default: BatchSync
    pub sync_mode: SyncMode,

    /// How long to retain WAL segments after data is sealed
    ///
    /// Default: 1 hour
    pub retention_duration: Duration,

    /// Maximum number of entries to buffer before sync
    ///
    /// Only used with BatchSync mode.
    /// Default: 1000
    pub batch_size: usize,

    /// Maximum time to wait before syncing a batch
    ///
    /// Only used with BatchSync mode.
    /// Default: 100ms
    pub batch_timeout: Duration,

    /// Enable compression for WAL entries
    ///
    /// Default: false (entries are small, compression overhead not worth it)
    pub compress_entries: bool,

    /// Maximum entry size in bytes
    ///
    /// Default: 16MB
    pub max_entry_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_mode: SyncMode::BatchSync,
            retention_duration: Duration::from_secs(3600), // 1 hour
            batch_size: 1000,
            batch_timeout: Duration::from_millis(100),
            compress_entries: false,
            max_entry_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

impl WalConfig {
    /// Create configuration optimized for maximum durability
    ///
    /// Uses every-write sync and short retention.
    pub fn high_durability() -> Self {
        Self {
            segment_size: 32 * 1024 * 1024, // Smaller segments
            sync_mode: SyncMode::EveryWrite,
            retention_duration: Duration::from_secs(7200), // 2 hours
            ..Default::default()
        }
    }

    /// Create configuration optimized for performance
    ///
    /// Uses batch sync with larger batches.
    pub fn high_performance() -> Self {
        Self {
            segment_size: 128 * 1024 * 1024, // Larger segments
            sync_mode: SyncMode::BatchSync,
            batch_size: 5000,
            batch_timeout: Duration::from_millis(500),
            retention_duration: Duration::from_secs(1800), // 30 minutes
            ..Default::default()
        }
    }

    /// Create configuration for testing
    ///
    /// Small segments, immediate sync.
    pub fn testing() -> Self {
        Self {
            segment_size: MIN_SEGMENT_SIZE,
            sync_mode: SyncMode::EveryWrite,
            retention_duration: Duration::from_secs(60),
            batch_size: 10,
            batch_timeout: Duration::from_millis(10),
            ..Default::default()
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), WalError> {
        if self.segment_size < MIN_SEGMENT_SIZE {
            return Err(WalError::InvalidConfig(format!(
                "segment_size ({}) must be at least {} bytes",
                self.segment_size, MIN_SEGMENT_SIZE
            )));
        }

        if self.segment_size > MAX_SEGMENT_SIZE {
            return Err(WalError::InvalidConfig(format!(
                "segment_size ({}) must not exceed {} bytes",
                self.segment_size, MAX_SEGMENT_SIZE
            )));
        }

        if self.batch_size == 0 {
            return Err(WalError::InvalidConfig(
                "batch_size must be greater than 0".to_string(),
            ));
        }

        if self.max_entry_size == 0 {
            return Err(WalError::InvalidConfig(
                "max_entry_size must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

// ============================================================================
// WAL Entry
// ============================================================================

/// Type of WAL entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    /// Data points for a series
    Data = 1,

    /// Checkpoint marker (indicates data up to this point is safe)
    Checkpoint = 2,

    /// Seal marker (series chunk was sealed)
    Seal = 3,

    /// Truncation marker (data before this LSN can be removed)
    Truncate = 4,
}

impl TryFrom<u8> for EntryType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EntryType::Data),
            2 => Ok(EntryType::Checkpoint),
            3 => Ok(EntryType::Seal),
            4 => Ok(EntryType::Truncate),
            _ => Err(WalError::CorruptedEntry {
                offset: 0,
                reason: format!("Unknown entry type: {}", value),
            }),
        }
    }
}

/// A single WAL entry containing data or control information
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Log Sequence Number - unique, monotonically increasing ID
    pub lsn: u64,

    /// Type of entry
    pub entry_type: EntryType,

    /// Series ID (for Data and Seal entries)
    pub series_id: SeriesId,

    /// Data points (for Data entries)
    pub points: Vec<DataPoint>,

    /// Timestamp when entry was written
    pub timestamp: i64,

    /// CRC32 checksum of entry data
    pub checksum: u32,
}

impl WalEntry {
    /// Create a new data entry
    pub fn new_data(lsn: u64, series_id: SeriesId, points: Vec<DataPoint>) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut entry = Self {
            lsn,
            entry_type: EntryType::Data,
            series_id,
            points,
            timestamp,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Create a new checkpoint entry
    pub fn new_checkpoint(lsn: u64) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut entry = Self {
            lsn,
            entry_type: EntryType::Checkpoint,
            series_id: 0,
            points: vec![],
            timestamp,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Create a new seal marker entry
    pub fn new_seal(lsn: u64, series_id: SeriesId) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut entry = Self {
            lsn,
            entry_type: EntryType::Seal,
            series_id,
            points: vec![],
            timestamp,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Create a new truncation marker entry
    pub fn new_truncate(lsn: u64, before_lsn: u64) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut entry = Self {
            lsn,
            entry_type: EntryType::Truncate,
            series_id: before_lsn as u128, // Store the truncation LSN in series_id
            points: vec![],
            timestamp,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Serialize entry to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header
        buf.extend_from_slice(&WAL_MAGIC);
        buf.push(WAL_VERSION);
        buf.push(self.entry_type as u8);
        buf.extend_from_slice(&self.checksum.to_le_bytes());

        // Body length placeholder (will fill in later)
        let length_pos = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Body start
        let body_start = buf.len();

        // LSN
        buf.extend_from_slice(&self.lsn.to_le_bytes());

        // Timestamp
        buf.extend_from_slice(&self.timestamp.to_le_bytes());

        // Series ID (as 16 bytes)
        buf.extend_from_slice(&self.series_id.to_le_bytes());

        // Point count
        buf.extend_from_slice(&(self.points.len() as u32).to_le_bytes());

        // Points
        for point in &self.points {
            buf.extend_from_slice(&point.series_id.to_le_bytes());
            buf.extend_from_slice(&point.timestamp.to_le_bytes());
            buf.extend_from_slice(&point.value.to_le_bytes());
        }

        // Fill in body length
        let body_len = (buf.len() - body_start) as u32;
        buf[length_pos..length_pos + 4].copy_from_slice(&body_len.to_le_bytes());

        buf
    }

    /// Deserialize entry from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, WalError> {
        if data.len() < ENTRY_HEADER_SIZE {
            return Err(WalError::CorruptedEntry {
                offset: 0,
                reason: "Entry too short for header".to_string(),
            });
        }

        // Verify magic
        if data[0..4] != WAL_MAGIC {
            return Err(WalError::InvalidMagic);
        }

        // Check version
        let version = data[4];
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        // Entry type
        let entry_type = EntryType::try_from(data[5])?;

        // Checksum
        let checksum = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);

        // Body length
        let body_len = u32::from_le_bytes([data[10], data[11], data[12], data[13]]) as usize;

        if data.len() < ENTRY_HEADER_SIZE + body_len {
            return Err(WalError::CorruptedEntry {
                offset: 0,
                reason: format!(
                    "Entry body truncated: expected {} bytes, got {}",
                    body_len,
                    data.len() - ENTRY_HEADER_SIZE
                ),
            });
        }

        let body = &data[ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + body_len];

        // LSN
        let lsn = u64::from_le_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);

        // Timestamp
        let timestamp = i64::from_le_bytes([
            body[8], body[9], body[10], body[11], body[12], body[13], body[14], body[15],
        ]);

        // Series ID
        let series_id = u128::from_le_bytes([
            body[16], body[17], body[18], body[19], body[20], body[21], body[22], body[23],
            body[24], body[25], body[26], body[27], body[28], body[29], body[30], body[31],
        ]);

        // Point count
        let point_count = u32::from_le_bytes([body[32], body[33], body[34], body[35]]) as usize;

        // Points
        let points_offset = 36;
        let point_size = 16 + 8 + 8; // series_id (16) + timestamp (8) + value (8)
        let mut points = Vec::with_capacity(point_count);

        for i in 0..point_count {
            let offset = points_offset + i * point_size;
            if offset + point_size > body.len() {
                return Err(WalError::CorruptedEntry {
                    offset: 0,
                    reason: format!("Point {} extends beyond entry body", i),
                });
            }

            let point_series_id = u128::from_le_bytes([
                body[offset],
                body[offset + 1],
                body[offset + 2],
                body[offset + 3],
                body[offset + 4],
                body[offset + 5],
                body[offset + 6],
                body[offset + 7],
                body[offset + 8],
                body[offset + 9],
                body[offset + 10],
                body[offset + 11],
                body[offset + 12],
                body[offset + 13],
                body[offset + 14],
                body[offset + 15],
            ]);

            let point_timestamp = i64::from_le_bytes([
                body[offset + 16],
                body[offset + 17],
                body[offset + 18],
                body[offset + 19],
                body[offset + 20],
                body[offset + 21],
                body[offset + 22],
                body[offset + 23],
            ]);

            let point_value = f64::from_le_bytes([
                body[offset + 24],
                body[offset + 25],
                body[offset + 26],
                body[offset + 27],
                body[offset + 28],
                body[offset + 29],
                body[offset + 30],
                body[offset + 31],
            ]);

            points.push(DataPoint {
                series_id: point_series_id,
                timestamp: point_timestamp,
                value: point_value,
            });
        }

        let entry = Self {
            lsn,
            entry_type,
            series_id,
            points,
            timestamp,
            checksum,
        };

        // Verify checksum
        if entry.compute_checksum() != checksum {
            return Err(WalError::ChecksumMismatch { offset: 0 });
        }

        Ok(entry)
    }

    /// Compute CRC32 checksum of entry data
    pub fn compute_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        hasher.update(&self.lsn.to_le_bytes());
        hasher.update(&(self.entry_type as u8).to_le_bytes());
        hasher.update(&self.series_id.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(&(self.points.len() as u32).to_le_bytes());

        for point in &self.points {
            hasher.update(&point.series_id.to_le_bytes());
            hasher.update(&point.timestamp.to_le_bytes());
            hasher.update(&point.value.to_le_bytes());
        }

        hasher.finalize()
    }

    /// Verify the entry's checksum
    pub fn verify_checksum(&self) -> bool {
        self.compute_checksum() == self.checksum
    }

    /// Get the serialized size of this entry
    pub fn serialized_size(&self) -> usize {
        ENTRY_HEADER_SIZE + 8 + 8 + 16 + 4 + (self.points.len() * 32)
    }
}

// ============================================================================
// WAL Segment
// ============================================================================

/// A single WAL segment file
#[derive(Debug)]
pub struct WalSegment {
    /// Path to the segment file
    path: PathBuf,

    /// Segment sequence number
    sequence: u64,

    /// Current file size
    size: u64,

    /// First LSN in this segment
    first_lsn: Option<u64>,

    /// Last LSN in this segment
    last_lsn: Option<u64>,

    /// Creation timestamp
    created_at: SystemTime,
}

impl WalSegment {
    /// Create a new segment file
    pub fn create(dir: &Path, sequence: u64) -> Result<Self, WalError> {
        let path = dir.join(format!("wal-{:06}.log", sequence));

        // Create the file
        let file = File::create(&path)?;
        file.sync_all()?;

        Ok(Self {
            path,
            sequence,
            size: 0,
            first_lsn: None,
            last_lsn: None,
            created_at: SystemTime::now(),
        })
    }

    /// Open an existing segment file
    pub fn open(path: PathBuf) -> Result<Self, WalError> {
        let metadata = fs::metadata(&path)?;

        // Extract sequence number from filename
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| WalError::SegmentNotFound(path.clone()))?;

        let sequence = filename
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| WalError::SegmentNotFound(path.clone()))?;

        Ok(Self {
            path,
            sequence,
            size: metadata.len(),
            first_lsn: None,
            last_lsn: None,
            created_at: metadata.created().unwrap_or(SystemTime::now()),
        })
    }

    /// Get the segment file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the segment sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get the current segment size
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Update the segment size
    pub fn update_size(&mut self, size: u64) {
        self.size = size;
    }

    /// Update the LSN range
    pub fn update_lsn_range(&mut self, lsn: u64) {
        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn);
        }
        self.last_lsn = Some(lsn);
    }

    /// Get the first LSN in this segment
    pub fn first_lsn(&self) -> Option<u64> {
        self.first_lsn
    }

    /// Get the last LSN in this segment
    pub fn last_lsn(&self) -> Option<u64> {
        self.last_lsn
    }

    /// Check if the segment is older than the given duration
    pub fn is_older_than(&self, duration: Duration) -> bool {
        match self.created_at.elapsed() {
            Ok(age) => age > duration,
            Err(_) => false,
        }
    }
}

// ============================================================================
// WAL Writer
// ============================================================================

/// Writer for appending entries to the WAL
pub struct WalWriter {
    /// Configuration
    config: WalConfig,

    /// WAL directory
    dir: PathBuf,

    /// Current active segment
    current_segment: WalSegment,

    /// File handle for current segment
    file: BufWriter<File>,

    /// Next LSN to assign
    next_lsn: AtomicU64,

    /// Entries pending sync (for batch mode)
    pending_entries: Vec<WalEntry>,

    /// Last sync time
    last_sync: Instant,

    /// Total bytes written
    bytes_written: AtomicU64,

    /// Total entries written
    entries_written: AtomicU64,
}

impl WalWriter {
    /// Create a new WAL writer
    ///
    /// # Arguments
    ///
    /// * `config` - WAL configuration
    /// * `dir` - Directory for WAL segment files
    pub fn new(config: WalConfig, dir: impl AsRef<Path>) -> Result<Self, WalError> {
        config.validate()?;

        let dir = dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        fs::create_dir_all(&dir)?;

        // Find existing segments and determine next sequence
        let (next_sequence, next_lsn) = Self::scan_segments(&dir)?;

        // Create new segment
        let segment = WalSegment::create(&dir, next_sequence)?;

        // Open file for writing (append implies write)
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment.path())?;

        Ok(Self {
            config,
            dir,
            current_segment: segment,
            file: BufWriter::new(file),
            next_lsn: AtomicU64::new(next_lsn),
            pending_entries: Vec::new(),
            last_sync: Instant::now(),
            bytes_written: AtomicU64::new(0),
            entries_written: AtomicU64::new(0),
        })
    }

    /// Scan existing segments to determine next sequence and LSN
    fn scan_segments(dir: &Path) -> Result<(u64, u64), WalError> {
        let mut max_sequence = 0u64;
        let mut max_lsn = 0u64;

        if dir.exists() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("wal-") && filename.ends_with(".log") {
                        if let Some(seq_str) = filename
                            .strip_prefix("wal-")
                            .and_then(|s| s.strip_suffix(".log"))
                        {
                            if let Ok(seq) = seq_str.parse::<u64>() {
                                max_sequence = max_sequence.max(seq);

                                // Scan segment for max LSN
                                if let Ok(reader) = WalReader::new(&path) {
                                    if let Ok(entries) = reader.read_all() {
                                        for entry in entries {
                                            max_lsn = max_lsn.max(entry.lsn);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((max_sequence + 1, max_lsn + 1))
    }

    /// Append a data entry to the WAL
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier
    /// * `points` - Data points to write
    ///
    /// # Returns
    ///
    /// The LSN assigned to this entry
    pub fn append(&mut self, series_id: SeriesId, points: &[DataPoint]) -> Result<u64, WalError> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let entry = WalEntry::new_data(lsn, series_id, points.to_vec());
        self.write_entry(entry)?;

        Ok(lsn)
    }

    /// Write a seal marker to the WAL
    ///
    /// Called when a chunk is sealed to indicate that data up to this
    /// point for the series has been durably stored in chunk format.
    pub fn write_seal(&mut self, series_id: SeriesId) -> Result<u64, WalError> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let entry = WalEntry::new_seal(lsn, series_id);
        self.write_entry(entry)?;

        Ok(lsn)
    }

    /// Write a checkpoint entry to the WAL
    ///
    /// Checkpoints indicate that all data up to this point has been
    /// successfully processed.
    pub fn checkpoint(&mut self) -> Result<u64, WalError> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let entry = WalEntry::new_checkpoint(lsn);
        self.write_entry(entry)?;

        // Force sync on checkpoint
        self.sync()?;

        Ok(lsn)
    }

    /// Internal: Write an entry to the current segment
    fn write_entry(&mut self, entry: WalEntry) -> Result<(), WalError> {
        let entry_size = entry.serialized_size();

        // Check entry size limit
        if entry_size > self.config.max_entry_size {
            return Err(WalError::EntryTooLarge {
                size: entry_size,
                max: self.config.max_entry_size,
            });
        }

        // Check if we need to rotate
        if self.current_segment.size() + entry_size as u64 > self.config.segment_size {
            self.rotate()?;
        }

        // Serialize and write
        let data = entry.serialize();
        self.file.write_all(&data)?;

        // Update segment info
        self.current_segment
            .update_size(self.current_segment.size() + data.len() as u64);
        self.current_segment.update_lsn_range(entry.lsn);

        // Update stats
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.entries_written.fetch_add(1, Ordering::Relaxed);

        // Handle sync mode
        match self.config.sync_mode {
            SyncMode::EveryWrite => {
                self.sync()?;
            },
            SyncMode::BatchSync => {
                self.pending_entries.push(entry);
                if self.pending_entries.len() >= self.config.batch_size
                    || self.last_sync.elapsed() >= self.config.batch_timeout
                {
                    self.sync()?;
                }
            },
            SyncMode::NoSync => {},
        }

        Ok(())
    }

    /// Force sync all pending writes to disk
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        self.pending_entries.clear();
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Rotate to a new segment file
    pub fn rotate(&mut self) -> Result<(), WalError> {
        // Sync current segment
        self.sync()?;

        // Create new segment
        let new_sequence = self.current_segment.sequence() + 1;
        let new_segment = WalSegment::create(&self.dir, new_sequence)?;

        // Open new file (append implies write)
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(new_segment.path())?;

        // Switch to new segment
        self.current_segment = new_segment;
        self.file = BufWriter::new(new_file);

        debug!("WAL rotated to segment {}", new_sequence);
        Ok(())
    }

    /// Truncate WAL entries before the given LSN
    ///
    /// Removes old segment files that only contain entries with LSN
    /// less than `before_lsn`.
    pub fn truncate(&mut self, before_lsn: u64) -> Result<(), WalError> {
        // Write truncation marker
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_truncate(lsn, before_lsn);
        self.write_entry(entry)?;

        // Find and remove old segments
        let segments = self.list_segments()?;

        for segment in segments {
            // Don't delete the current segment
            if segment.sequence() == self.current_segment.sequence() {
                continue;
            }

            // Check if all entries in segment are before truncation point
            if let Some(last_lsn) = segment.last_lsn() {
                if last_lsn < before_lsn {
                    debug!("Removing old WAL segment: {:?}", segment.path());
                    fs::remove_file(segment.path())?;
                }
            }
        }

        Ok(())
    }

    /// List all WAL segments in the directory
    fn list_segments(&self) -> Result<Vec<WalSegment>, WalError> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal-") && filename.ends_with(".log") {
                    if let Ok(segment) = WalSegment::open(path) {
                        segments.push(segment);
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.sequence());
        Ok(segments)
    }

    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst)
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get total entries written
    pub fn entries_written(&self) -> u64 {
        self.entries_written.load(Ordering::Relaxed)
    }

    /// Get the WAL directory
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

// ============================================================================
// WAL Reader
// ============================================================================

/// Reader for WAL segment files
pub struct WalReader {
    /// Path to the segment or directory
    path: PathBuf,
}

impl WalReader {
    /// Create a new WAL reader for a segment file or directory
    pub fn new(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(WalError::DirectoryNotFound(path));
        }

        Ok(Self { path })
    }

    /// Read all entries from a single segment file
    pub fn read_all(&self) -> Result<Vec<WalEntry>, WalError> {
        if self.path.is_dir() {
            return self.read_all_segments();
        }

        self.read_segment(&self.path)
    }

    /// Read entries from a specific LSN onwards
    pub fn read_from(&self, from_lsn: u64) -> Result<Vec<WalEntry>, WalError> {
        let entries = self.read_all()?;
        Ok(entries.into_iter().filter(|e| e.lsn >= from_lsn).collect())
    }

    /// Read all entries from all segments in directory
    fn read_all_segments(&self) -> Result<Vec<WalEntry>, WalError> {
        let mut all_entries = Vec::new();

        // Find all segment files
        let mut segment_paths: Vec<PathBuf> = Vec::new();

        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal-") && filename.ends_with(".log") {
                    segment_paths.push(path);
                }
            }
        }

        // Sort by sequence number
        segment_paths.sort();

        // Read each segment
        for segment_path in segment_paths {
            match self.read_segment(&segment_path) {
                Ok(entries) => all_entries.extend(entries),
                Err(e) => {
                    warn!("Error reading segment {:?}: {}", segment_path, e);
                    // Continue with other segments
                },
            }
        }

        // Sort by LSN
        all_entries.sort_by_key(|e| e.lsn);

        Ok(all_entries)
    }

    /// Read all entries from a single segment file
    fn read_segment(&self, path: &Path) -> Result<Vec<WalEntry>, WalError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut offset = 0u64;

        loop {
            // Try to read entry header
            let mut header = [0u8; ENTRY_HEADER_SIZE];
            match reader.read_exact(&mut header) {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WalError::Io(e)),
            }

            // Check magic
            if header[0..4] != WAL_MAGIC {
                // Skip to find next valid entry
                warn!("Invalid magic at offset {}, skipping", offset);
                offset += 1;
                reader.seek(SeekFrom::Start(offset))?;
                continue;
            }

            // Get body length
            let body_len =
                u32::from_le_bytes([header[10], header[11], header[12], header[13]]) as usize;

            // Read body
            let mut body = vec![0u8; body_len];
            match reader.read_exact(&mut body) {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    warn!("Truncated entry at offset {}", offset);
                    break;
                },
                Err(e) => return Err(WalError::Io(e)),
            }

            // Combine header and body
            let mut entry_data = header.to_vec();
            entry_data.extend(body);

            // Deserialize
            match WalEntry::deserialize(&entry_data) {
                Ok(entry) => {
                    entries.push(entry);
                },
                Err(e) => {
                    warn!("Corrupted entry at offset {}: {}", offset, e);
                    // Continue with next entry
                },
            }

            offset = reader.stream_position()?;
        }

        Ok(entries)
    }
}

// ============================================================================
// WAL Recovery
// ============================================================================

/// WAL recovery handler for crash recovery
pub struct WalRecovery {
    /// WAL directory
    wal_dir: PathBuf,

    /// Recovered data by series
    recovered_data: HashMap<SeriesId, Vec<DataPoint>>,

    /// Sealed series (data already durably stored)
    sealed_series: Vec<SeriesId>,

    /// Last checkpoint LSN
    last_checkpoint: Option<u64>,

    /// Last processed LSN
    last_lsn: u64,
}

impl WalRecovery {
    /// Create a new recovery handler
    pub fn new(wal_dir: impl AsRef<Path>) -> Result<Self, WalError> {
        let wal_dir = wal_dir.as_ref().to_path_buf();

        if !wal_dir.exists() {
            return Err(WalError::DirectoryNotFound(wal_dir));
        }

        Ok(Self {
            wal_dir,
            recovered_data: HashMap::new(),
            sealed_series: Vec::new(),
            last_checkpoint: None,
            last_lsn: 0,
        })
    }

    /// Perform recovery from WAL
    ///
    /// Reads all WAL entries and identifies data that needs to be
    /// replayed into active chunks.
    ///
    /// # Returns
    ///
    /// Map of series_id to recovered data points
    pub fn recover(&mut self) -> Result<&HashMap<SeriesId, Vec<DataPoint>>, WalError> {
        info!("Starting WAL recovery from {:?}", self.wal_dir);

        let reader = WalReader::new(&self.wal_dir)?;
        let entries = reader.read_all()?;

        info!("Found {} WAL entries to process", entries.len());

        // First pass: find checkpoints and seals
        for entry in &entries {
            match entry.entry_type {
                EntryType::Checkpoint => {
                    self.last_checkpoint = Some(entry.lsn);
                },
                EntryType::Seal => {
                    self.sealed_series.push(entry.series_id);
                },
                EntryType::Truncate => {
                    // Clear data before truncation point
                    // The truncation LSN is stored in series_id field
                    let _truncate_lsn = entry.series_id as u64;
                    self.recovered_data.retain(|_, points| {
                        // Keep data after truncation
                        !points.is_empty()
                    });
                },
                _ => {},
            }
            self.last_lsn = self.last_lsn.max(entry.lsn);
        }

        // Second pass: recover data not covered by seals
        for entry in entries {
            if entry.entry_type == EntryType::Data {
                // Skip if series was sealed after this entry
                if self.sealed_series.contains(&entry.series_id) {
                    continue;
                }

                // Add data to recovery map
                self.recovered_data
                    .entry(entry.series_id)
                    .or_default()
                    .extend(entry.points);
            }
        }

        // Sort recovered data by timestamp
        for points in self.recovered_data.values_mut() {
            points.sort_by_key(|p| p.timestamp);
        }

        info!(
            "Recovery complete: {} series with data to replay",
            self.recovered_data.len()
        );

        Ok(&self.recovered_data)
    }

    /// Get recovered data for a specific series
    pub fn get_series_data(&self, series_id: SeriesId) -> Option<&Vec<DataPoint>> {
        self.recovered_data.get(&series_id)
    }

    /// Check if a series was sealed (no recovery needed)
    pub fn is_series_sealed(&self, series_id: SeriesId) -> bool {
        self.sealed_series.contains(&series_id)
    }

    /// Get all series that need recovery
    pub fn series_needing_recovery(&self) -> Vec<SeriesId> {
        self.recovered_data.keys().copied().collect()
    }

    /// Get the last processed LSN
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Get the last checkpoint LSN
    pub fn last_checkpoint(&self) -> Option<u64> {
        self.last_checkpoint
    }

    /// Clear recovered data after successful replay
    pub fn clear(&mut self) {
        self.recovered_data.clear();
        self.sealed_series.clear();
        self.last_checkpoint = None;
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics snapshot for WAL operations
#[derive(Debug, Clone)]
pub struct WalStatsSnapshot {
    /// Total bytes written to WAL
    pub bytes_written: u64,

    /// Total entries written
    pub entries_written: u64,

    /// Current LSN
    pub current_lsn: u64,

    /// Number of segment files
    pub segment_count: usize,

    /// Total size of all segments
    pub total_size: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // --- Configuration Tests ---

    #[test]
    fn test_config_default() {
        let config = WalConfig::default();

        assert_eq!(config.segment_size, DEFAULT_SEGMENT_SIZE);
        assert_eq!(config.sync_mode, SyncMode::BatchSync);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_high_durability() {
        let config = WalConfig::high_durability();

        assert_eq!(config.sync_mode, SyncMode::EveryWrite);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_high_performance() {
        let config = WalConfig::high_performance();

        assert!(config.batch_size > WalConfig::default().batch_size);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_testing() {
        let config = WalConfig::testing();

        assert_eq!(config.segment_size, MIN_SEGMENT_SIZE);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_segment_too_small() {
        let config = WalConfig {
            segment_size: 100,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_segment_too_large() {
        let config = WalConfig {
            segment_size: MAX_SEGMENT_SIZE + 1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_batch_size_zero() {
        let config = WalConfig {
            batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // --- WAL Entry Tests ---

    #[test]
    fn test_entry_new_data() {
        let points = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 10.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 20.0,
            },
        ];

        let entry = WalEntry::new_data(1, 1, points.clone());

        assert_eq!(entry.lsn, 1);
        assert_eq!(entry.entry_type, EntryType::Data);
        assert_eq!(entry.series_id, 1);
        assert_eq!(entry.points.len(), 2);
        assert!(entry.verify_checksum());
    }

    #[test]
    fn test_entry_new_checkpoint() {
        let entry = WalEntry::new_checkpoint(5);

        assert_eq!(entry.lsn, 5);
        assert_eq!(entry.entry_type, EntryType::Checkpoint);
        assert!(entry.points.is_empty());
        assert!(entry.verify_checksum());
    }

    #[test]
    fn test_entry_new_seal() {
        let entry = WalEntry::new_seal(10, 42);

        assert_eq!(entry.lsn, 10);
        assert_eq!(entry.entry_type, EntryType::Seal);
        assert_eq!(entry.series_id, 42);
        assert!(entry.verify_checksum());
    }

    #[test]
    fn test_entry_serialize_deserialize() {
        let points = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 10.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 20.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 3000,
                value: 30.0,
            },
        ];

        let original = WalEntry::new_data(42, 1, points);
        let serialized = original.serialize();
        let deserialized = WalEntry::deserialize(&serialized).expect("Deserialization failed");

        assert_eq!(original.lsn, deserialized.lsn);
        assert_eq!(original.entry_type, deserialized.entry_type);
        assert_eq!(original.series_id, deserialized.series_id);
        assert_eq!(original.points.len(), deserialized.points.len());
        assert_eq!(original.checksum, deserialized.checksum);
    }

    #[test]
    fn test_entry_checksum_verification() {
        let entry = WalEntry::new_data(1, 1, vec![]);

        assert!(entry.verify_checksum());

        // Modify entry and check that checksum fails
        let mut tampered = entry.clone();
        tampered.lsn = 999;
        assert!(!tampered.verify_checksum());
    }

    #[test]
    fn test_entry_deserialize_invalid_magic() {
        // Need full header length for magic check (first checks header size)
        let mut data = vec![0x00, 0x00, 0x00, 0x00]; // Invalid magic
        data.extend([0u8; ENTRY_HEADER_SIZE - 4]); // Rest of header

        let result = WalEntry::deserialize(&data);
        assert!(matches!(result, Err(WalError::InvalidMagic)));
    }

    #[test]
    fn test_entry_deserialize_unsupported_version() {
        let mut data = WAL_MAGIC.to_vec();
        data.push(99); // Unsupported version
        data.extend([0u8; 9]); // Rest of header

        let result = WalEntry::deserialize(&data);
        assert!(matches!(result, Err(WalError::UnsupportedVersion(99))));
    }

    // --- WAL Writer Tests ---

    #[test]
    fn test_writer_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let writer = WalWriter::new(config, temp_dir.path());
        assert!(writer.is_ok());
    }

    #[test]
    fn test_writer_append() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];

        let lsn = writer.append(1, &points).expect("Failed to append");
        assert_eq!(lsn, 1);
        assert!(writer.entries_written() >= 1);
    }

    #[test]
    fn test_writer_sync() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        writer.append(1, &points).expect("Failed to append");

        let result = writer.sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_writer_checkpoint() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let lsn = writer.checkpoint().expect("Failed to checkpoint");
        assert!(lsn > 0);
    }

    #[test]
    fn test_writer_rotate() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        // Write enough data to trigger rotation
        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];

        for _ in 0..100 {
            writer.append(1, &points).expect("Failed to append");
        }

        let result = writer.rotate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_writer_seal_marker() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let lsn = writer.write_seal(42).expect("Failed to write seal");
        assert!(lsn > 0);
    }

    // --- WAL Reader Tests ---

    #[test]
    fn test_reader_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        // Create some data first
        let mut writer =
            WalWriter::new(config.clone(), temp_dir.path()).expect("Failed to create writer");
        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        writer.append(1, &points).expect("Failed to append");
        writer.sync().expect("Failed to sync");

        let reader = WalReader::new(temp_dir.path());
        assert!(reader.is_ok());
    }

    #[test]
    fn test_reader_read_all() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer =
            WalWriter::new(config.clone(), temp_dir.path()).expect("Failed to create writer");

        // Write multiple entries
        for i in 0..5 {
            let points = vec![DataPoint {
                series_id: 1,
                timestamp: (i * 1000) as i64,
                value: i as f64,
            }];
            writer.append(1, &points).expect("Failed to append");
        }
        writer.sync().expect("Failed to sync");

        let reader = WalReader::new(temp_dir.path()).expect("Failed to create reader");
        let entries = reader.read_all().expect("Failed to read all");

        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_reader_read_from_lsn() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer =
            WalWriter::new(config.clone(), temp_dir.path()).expect("Failed to create writer");

        for i in 0..10 {
            let points = vec![DataPoint {
                series_id: 1,
                timestamp: (i * 1000) as i64,
                value: i as f64,
            }];
            writer.append(1, &points).expect("Failed to append");
        }
        writer.sync().expect("Failed to sync");

        let reader = WalReader::new(temp_dir.path()).expect("Failed to create reader");
        let entries = reader.read_from(5).expect("Failed to read from LSN");

        // Should only include entries with LSN >= 5
        assert!(entries.iter().all(|e| e.lsn >= 5));
    }

    // --- WAL Recovery Tests ---

    #[test]
    fn test_recovery_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let recovery = WalRecovery::new(temp_dir.path());
        assert!(recovery.is_ok());
    }

    #[test]
    fn test_recovery_recover() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        // Write some data
        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let points1 = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        let points2 = vec![DataPoint {
            series_id: 2,
            timestamp: 2000,
            value: 20.0,
        }];

        writer.append(1, &points1).expect("Failed to append");
        writer.append(2, &points2).expect("Failed to append");
        writer.sync().expect("Failed to sync");

        // Recover
        let mut recovery = WalRecovery::new(temp_dir.path()).expect("Failed to create recovery");
        let recovered = recovery.recover().expect("Failed to recover");

        assert_eq!(recovered.len(), 2);
        assert!(recovered.contains_key(&1));
        assert!(recovered.contains_key(&2));
    }

    #[test]
    fn test_recovery_sealed_series_excluded() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        // Write data for series 1
        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        writer.append(1, &points).expect("Failed to append");

        // Seal series 1
        writer.write_seal(1).expect("Failed to write seal");
        writer.sync().expect("Failed to sync");

        // Recover
        let mut recovery = WalRecovery::new(temp_dir.path()).expect("Failed to create recovery");
        let recovered = recovery.recover().expect("Failed to recover");

        // Series 1 should not be in recovery (it was sealed)
        assert!(recovered.is_empty());
        assert!(recovery.is_series_sealed(1));
    }

    #[test]
    fn test_recovery_checkpoint() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        writer.append(1, &points).expect("Failed to append");
        writer.checkpoint().expect("Failed to checkpoint");
        writer.sync().expect("Failed to sync");

        let mut recovery = WalRecovery::new(temp_dir.path()).expect("Failed to create recovery");
        recovery.recover().expect("Failed to recover");

        assert!(recovery.last_checkpoint().is_some());
    }

    #[test]
    fn test_recovery_clear() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        let mut writer = WalWriter::new(config, temp_dir.path()).expect("Failed to create writer");

        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        }];
        writer.append(1, &points).expect("Failed to append");
        writer.sync().expect("Failed to sync");

        let mut recovery = WalRecovery::new(temp_dir.path()).expect("Failed to create recovery");
        recovery.recover().expect("Failed to recover");

        assert!(!recovery.recovered_data.is_empty());

        recovery.clear();
        assert!(recovery.recovered_data.is_empty());
    }

    // --- Integration Tests ---

    #[test]
    fn test_full_wal_lifecycle() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = WalConfig::testing();

        // Write phase
        {
            let mut writer =
                WalWriter::new(config.clone(), temp_dir.path()).expect("Failed to create writer");

            for i in 0..10 {
                let points = vec![DataPoint {
                    series_id: 1,
                    timestamp: (i * 1000) as i64,
                    value: i as f64,
                }];
                writer.append(1, &points).expect("Failed to append");
            }

            writer.checkpoint().expect("Failed to checkpoint");
            writer.sync().expect("Failed to sync");
        }

        // Read phase
        {
            let reader = WalReader::new(temp_dir.path()).expect("Failed to create reader");
            let entries = reader.read_all().expect("Failed to read all");

            // 10 data entries + 1 checkpoint
            assert_eq!(entries.len(), 11);
        }

        // Recovery phase
        {
            let mut recovery =
                WalRecovery::new(temp_dir.path()).expect("Failed to create recovery");
            let recovered = recovery.recover().expect("Failed to recover");

            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered.get(&1).unwrap().len(), 10);
        }
    }
}
