//! WAL segment management
//!
//! Handles individual WAL segment files including creation, writing,
//! reading, sealing, and deletion. Segments use memory-mapped I/O
//! for high-performance operations.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;
use tracing::{debug, warn};

use super::error::{WalError, WalResult};
use super::record::WalRecord;
use super::SequenceNumber;

/// Segment ID type
pub type SegmentId = u64;

/// Segment file header magic number
const SEGMENT_MAGIC: u32 = 0x57414C53; // "WALS"

/// Segment file version
const SEGMENT_VERSION: u16 = 1;

/// Segment header size
const SEGMENT_HEADER_SIZE: usize = 32;

/// WAL segment file
///
/// Represents a single segment in the WAL. Segments are append-only
/// and immutable once sealed.
pub struct WalSegment {
    /// Segment ID (monotonically increasing)
    id: SegmentId,
    /// File path
    path: PathBuf,
    /// File handle for writing
    writer: Mutex<Option<BufWriter<File>>>,
    /// Current write offset
    offset: AtomicUsize,
    /// Maximum segment size
    max_size: usize,
    /// Whether segment is sealed (no more writes)
    sealed: AtomicBool,
    /// Minimum sequence number in segment
    min_sequence: AtomicU64,
    /// Maximum sequence number in segment
    max_sequence: AtomicU64,
    /// Number of records in segment
    record_count: AtomicUsize,
}

impl WalSegment {
    /// Create a new segment file
    ///
    /// # Arguments
    ///
    /// * `directory` - Directory to create segment in
    /// * `id` - Segment ID
    /// * `max_size` - Maximum segment size in bytes
    pub async fn create(directory: &Path, id: SegmentId, max_size: usize) -> WalResult<Self> {
        let path = directory.join(format!("wal-{:08}.log", id));

        // Create the file with proper options
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| WalError::Io {
                source: e,
                context: format!("creating segment {:?}", path),
            })?;

        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // Write segment header
        Self::write_header(&mut writer, id)?;

        debug!("Created WAL segment {} at {:?}", id, path);

        Ok(Self {
            id,
            path,
            writer: Mutex::new(Some(writer)),
            offset: AtomicUsize::new(SEGMENT_HEADER_SIZE),
            max_size,
            sealed: AtomicBool::new(false),
            min_sequence: AtomicU64::new(u64::MAX),
            max_sequence: AtomicU64::new(0),
            record_count: AtomicUsize::new(0),
        })
    }

    /// Open an existing segment file
    pub async fn open(path: &Path) -> WalResult<Self> {
        let file = File::open(path).map_err(|e| WalError::Io {
            source: e,
            context: format!("opening segment {:?}", path),
        })?;

        let mut reader = BufReader::new(file);

        // Read and validate header
        let (id, _version) = Self::read_header(&mut reader)?;

        // Get file size
        let metadata = fs::metadata(path).map_err(|e| WalError::Io {
            source: e,
            context: format!("getting metadata for {:?}", path),
        })?;
        let file_size = metadata.len() as usize;

        // Scan to find sequence range
        let (min_seq, max_seq, record_count) = Self::scan_sequences(&mut reader)?;

        Ok(Self {
            id,
            path: path.to_path_buf(),
            writer: Mutex::new(None), // Opened as read-only
            offset: AtomicUsize::new(file_size),
            max_size: file_size,           // Use actual size for opened segments
            sealed: AtomicBool::new(true), // Existing segments are sealed
            min_sequence: AtomicU64::new(min_seq),
            max_sequence: AtomicU64::new(max_seq),
            record_count: AtomicUsize::new(record_count),
        })
    }

    /// Write segment header
    fn write_header<W: Write>(writer: &mut W, id: SegmentId) -> WalResult<()> {
        let mut header = [0u8; SEGMENT_HEADER_SIZE];

        // Magic (4 bytes)
        header[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        // Version (2 bytes)
        header[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        // Segment ID (8 bytes)
        header[8..16].copy_from_slice(&id.to_le_bytes());
        // Reserved (16 bytes of zeros)

        writer.write_all(&header).map_err(|e| WalError::Io {
            source: e,
            context: "writing segment header".to_string(),
        })?;

        writer.flush().map_err(|e| WalError::Io {
            source: e,
            context: "flushing segment header".to_string(),
        })?;

        Ok(())
    }

    /// Read and validate segment header
    fn read_header<R: io::Read>(reader: &mut R) -> WalResult<(SegmentId, u16)> {
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| WalError::Io {
            source: e,
            context: "reading segment header".to_string(),
        })?;

        // Validate magic
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != SEGMENT_MAGIC {
            return Err(WalError::Corruption {
                segment_id: 0,
                offset: 0,
                message: format!(
                    "invalid magic: expected {:08x}, got {:08x}",
                    SEGMENT_MAGIC, magic
                ),
            });
        }

        // Read version
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version > SEGMENT_VERSION {
            return Err(WalError::Corruption {
                segment_id: 0,
                offset: 0,
                message: format!("unsupported version: {}", version),
            });
        }

        // Read segment ID
        let id = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);

        Ok((id, version))
    }

    /// Scan segment to find sequence range
    fn scan_sequences<R: io::Read + io::Seek>(reader: &mut R) -> WalResult<(u64, u64, usize)> {
        // Skip header
        reader
            .seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))
            .map_err(|e| WalError::Io {
                source: e,
                context: "seeking past header".to_string(),
            })?;

        let mut min_seq = u64::MAX;
        let mut max_seq = 0u64;
        let mut count = 0usize;

        // For now, we don't store sequence numbers in records
        // This would need to be extended for proper sequence tracking
        loop {
            match WalRecord::read_from(reader) {
                Ok(_record) => {
                    count += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(_) => {
                    // Truncated or corrupted record at end
                    break;
                }
            }
        }

        if count > 0 {
            min_seq = 0;
            max_seq = count as u64 - 1;
        }

        Ok((min_seq, max_seq, count))
    }

    /// Write a record to the segment
    ///
    /// # Arguments
    ///
    /// * `record` - The record to write
    /// * `sequence` - Sequence number for this record
    ///
    /// # Returns
    ///
    /// Returns error if segment is sealed or full.
    pub fn write_record(&self, record: &WalRecord, sequence: SequenceNumber) -> WalResult<usize> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(WalError::SegmentFull {
                segment_id: self.id,
                current_size: self.offset.load(Ordering::Relaxed),
                max_size: self.max_size,
            });
        }

        let record_size = record.disk_size();
        let current_offset = self.offset.load(Ordering::Relaxed);

        // Check if record fits
        if current_offset + record_size > self.max_size {
            return Err(WalError::SegmentFull {
                segment_id: self.id,
                current_size: current_offset,
                max_size: self.max_size,
            });
        }

        // Write the record
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            record.write_to(writer).map_err(|e| WalError::Io {
                source: e,
                context: format!("writing record to segment {}", self.id),
            })?;

            // Update statistics
            self.offset.fetch_add(record_size, Ordering::Relaxed);
            self.record_count.fetch_add(1, Ordering::Relaxed);

            // Update sequence range
            self.min_sequence.fetch_min(sequence, Ordering::Relaxed);
            self.max_sequence.fetch_max(sequence, Ordering::Relaxed);

            Ok(record_size)
        } else {
            Err(WalError::Io {
                source: io::Error::new(io::ErrorKind::NotConnected, "segment not writable"),
                context: "segment writer not initialized".to_string(),
            })
        }
    }

    /// Sync segment to disk
    pub async fn sync(&self) -> WalResult<()> {
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer.flush().map_err(|e| WalError::Io {
                source: e,
                context: format!("flushing segment {}", self.id),
            })?;

            // Sync underlying file
            writer.get_ref().sync_all().map_err(|e| WalError::Io {
                source: e,
                context: format!("syncing segment {}", self.id),
            })?;
        }
        Ok(())
    }

    /// Seal the segment (no more writes allowed)
    pub async fn seal(&self) -> WalResult<()> {
        // Final sync
        self.sync().await?;

        // Mark as sealed
        self.sealed.store(true, Ordering::Release);

        // Close the writer
        let mut writer_guard = self.writer.lock();
        *writer_guard = None;

        debug!("Sealed WAL segment {}", self.id);
        Ok(())
    }

    /// Delete the segment file
    pub async fn delete(&self) -> WalResult<()> {
        // Ensure sealed first
        if !self.sealed.load(Ordering::Acquire) {
            warn!("Deleting unsealed segment {}", self.id);
        }

        fs::remove_file(&self.path).map_err(|e| WalError::Io {
            source: e,
            context: format!("deleting segment {:?}", self.path),
        })?;

        debug!("Deleted WAL segment {}", self.id);
        Ok(())
    }

    /// Read all records from the segment
    pub fn read_all_records(&self) -> WalResult<Vec<WalRecord>> {
        let file = File::open(&self.path).map_err(|e| WalError::Io {
            source: e,
            context: format!("opening segment {:?} for reading", self.path),
        })?;

        let mut reader = BufReader::new(file);

        // Skip header
        reader
            .seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))
            .map_err(|e| WalError::Io {
                source: e,
                context: "seeking past header".to_string(),
            })?;

        let mut records = Vec::new();

        loop {
            match WalRecord::read_from(&mut reader) {
                Ok(record) => {
                    // Verify checksum
                    if !record.verify_checksum() {
                        let pos = reader.stream_position().unwrap_or(0);
                        return Err(WalError::ChecksumMismatch {
                            expected: 0, // We don't store expected separately
                            actual: record.checksum,
                            segment_id: self.id,
                            offset: pos,
                        });
                    }
                    records.push(record);
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    let pos = reader.stream_position().unwrap_or(0);
                    return Err(WalError::Corruption {
                        segment_id: self.id,
                        offset: pos,
                        message: format!("read error: {}", e),
                    });
                }
            }
        }

        Ok(records)
    }

    /// Get segment ID
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Get current size
    pub fn size(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }

    /// Get maximum size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Check if segment is sealed
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    /// Get minimum sequence number
    pub fn min_sequence(&self) -> SequenceNumber {
        self.min_sequence.load(Ordering::Relaxed)
    }

    /// Get maximum sequence number
    pub fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence.load(Ordering::Relaxed)
    }

    /// Get record count
    pub fn record_count(&self) -> usize {
        self.record_count.load(Ordering::Relaxed)
    }

    /// Get remaining capacity
    pub fn remaining_capacity(&self) -> usize {
        self.max_size
            .saturating_sub(self.offset.load(Ordering::Relaxed))
    }

    /// Check if segment has room for a record of given size
    pub fn has_capacity(&self, record_size: usize) -> bool {
        !self.is_sealed() && self.remaining_capacity() >= record_size
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// List all segment files in a directory
pub fn list_segments(directory: &Path) -> WalResult<Vec<PathBuf>> {
    let mut segments = Vec::new();

    if !directory.exists() {
        return Ok(segments);
    }

    for entry in fs::read_dir(directory).map_err(|e| WalError::Io {
        source: e,
        context: format!("reading directory {:?}", directory),
    })? {
        let entry = entry.map_err(|e| WalError::Io {
            source: e,
            context: "reading directory entry".to_string(),
        })?;

        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("wal-") && name.ends_with(".log") {
                segments.push(path);
            }
        }
    }

    // Sort by segment ID
    segments.sort_by(|a, b| {
        let id_a = extract_segment_id(a).unwrap_or(0);
        let id_b = extract_segment_id(b).unwrap_or(0);
        id_a.cmp(&id_b)
    });

    Ok(segments)
}

/// Extract segment ID from filename
fn extract_segment_id(path: &Path) -> Option<SegmentId> {
    path.file_name().and_then(|n| n.to_str()).and_then(|name| {
        name.strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataPoint;
    use tempfile::tempdir;

    fn sample_record() -> WalRecord {
        let points = vec![DataPoint::new(1, 1000, 42.5), DataPoint::new(2, 1001, 43.5)];
        WalRecord::from_points(&points)
    }

    #[tokio::test]
    async fn test_segment_create() {
        let dir = tempdir().unwrap();
        let segment = WalSegment::create(dir.path(), 1, 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(segment.id(), 1);
        assert!(!segment.is_sealed());
        assert_eq!(segment.size(), SEGMENT_HEADER_SIZE);
    }

    #[tokio::test]
    async fn test_segment_write_read() {
        let dir = tempdir().unwrap();
        let segment = WalSegment::create(dir.path(), 1, 1024 * 1024)
            .await
            .unwrap();

        let record = sample_record();
        segment.write_record(&record, 0).unwrap();
        segment.sync().await.unwrap();
        segment.seal().await.unwrap();

        let records = segment.read_all_records().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].verify_checksum());
    }

    #[tokio::test]
    async fn test_segment_full() {
        let dir = tempdir().unwrap();
        // Small segment
        let segment = WalSegment::create(dir.path(), 1, SEGMENT_HEADER_SIZE + 50)
            .await
            .unwrap();

        let record = sample_record();
        let result = segment.write_record(&record, 0);

        // Record too large for segment
        assert!(matches!(result, Err(WalError::SegmentFull { .. })));
    }

    #[tokio::test]
    async fn test_segment_seal() {
        let dir = tempdir().unwrap();
        let segment = WalSegment::create(dir.path(), 1, 1024 * 1024)
            .await
            .unwrap();

        segment.seal().await.unwrap();
        assert!(segment.is_sealed());

        // Writing to sealed segment should fail
        let record = sample_record();
        let result = segment.write_record(&record, 0);
        assert!(matches!(result, Err(WalError::SegmentFull { .. })));
    }

    #[tokio::test]
    async fn test_list_segments() {
        let dir = tempdir().unwrap();

        // Create some segments
        let _seg1 = WalSegment::create(dir.path(), 1, 1024).await.unwrap();
        let _seg2 = WalSegment::create(dir.path(), 3, 1024).await.unwrap();
        let _seg3 = WalSegment::create(dir.path(), 2, 1024).await.unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 3);

        // Should be sorted by ID
        let ids: Vec<_> = segments
            .iter()
            .filter_map(|p| extract_segment_id(p))
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_segment_id() {
        assert_eq!(extract_segment_id(Path::new("wal-00000001.log")), Some(1));
        assert_eq!(
            extract_segment_id(Path::new("wal-00012345.log")),
            Some(12345)
        );
        assert_eq!(extract_segment_id(Path::new("other.log")), None);
    }
}
