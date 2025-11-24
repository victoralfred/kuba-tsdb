//! Chunk definitions and management
//!
//! Chunks are the fundamental storage unit, containing compressed time-series data
//! along with metadata for efficient retrieval and validation.
//!
//! # Chunk Lifecycle
//!
//! ```text
//! Active → Sealed → Compressed
//!   ↓         ↓          ↓
//! Memory   Disk       Disk+Snappy
//! (fast)   (mmap)     (space-optimized)
//! ```

use crate::types::{ChunkId, SeriesId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Magic number identifying Gorilla chunk format: "GORILLA" in hex
pub const CHUNK_MAGIC: u32 = 0x474F5249;  // "GORI" (first 4 bytes of "GORILLA")

/// Current chunk format version
pub const CHUNK_VERSION: u16 = 1;

/// Chunk header containing metadata
///
/// The header is stored at the beginning of each chunk file and contains
/// essential information for reading and validating the chunk.
///
/// # Binary Layout (64 bytes total)
///
/// ```text
/// Offset | Size | Field
/// -------|------|------------------
///   0    |  4   | magic
///   4    |  2   | version
///   6    |  16  | series_id
///  22    |  8   | start_timestamp
///  30    |  8   | end_timestamp
///  38    |  4   | point_count
///  42    |  4   | compressed_size
///  46    |  4   | uncompressed_size
///  50    |  8   | checksum
///  58    |  1   | compression_type
///  59    |  1   | flags
///  60    |  4   | reserved
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    /// Magic number for format identification (0x474F5249)
    pub magic: u32,

    /// Format version number (currently 1)
    pub version: u16,

    /// Series this chunk belongs to
    pub series_id: SeriesId,

    /// First timestamp in chunk (milliseconds)
    pub start_timestamp: i64,

    /// Last timestamp in chunk (milliseconds)
    pub end_timestamp: i64,

    /// Number of data points in chunk
    pub point_count: u32,

    /// Size of compressed data in bytes
    pub compressed_size: u32,

    /// Original uncompressed size in bytes
    pub uncompressed_size: u32,

    /// CRC64 checksum of data
    pub checksum: u64,

    /// Compression algorithm used
    pub compression_type: CompressionType,

    /// Additional chunk flags
    pub flags: ChunkFlags,
}

impl ChunkHeader {
    /// Create a new chunk header
    pub fn new(series_id: SeriesId) -> Self {
        Self {
            magic: CHUNK_MAGIC,
            version: CHUNK_VERSION,
            series_id,
            start_timestamp: 0,
            end_timestamp: 0,
            point_count: 0,
            compressed_size: 0,
            uncompressed_size: 0,
            checksum: 0,
            compression_type: CompressionType::Gorilla,
            flags: ChunkFlags::empty(),
        }
    }

    /// Validate header magic and version
    pub fn validate(&self) -> Result<(), String> {
        if self.magic != CHUNK_MAGIC {
            return Err(format!("Invalid magic number: 0x{:08X}", self.magic));
        }
        if self.version > CHUNK_VERSION {
            return Err(format!("Unsupported version: {}", self.version));
        }
        if self.point_count == 0 {
            return Err("Empty chunk".to_string());
        }
        if self.start_timestamp > self.end_timestamp {
            return Err("Invalid time range".to_string());
        }
        Ok(())
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 0.0;
        }
        self.uncompressed_size as f64 / self.compressed_size as f64
    }

    /// Serialize header to bytes (64 bytes total)
    ///
    /// Binary layout:
    /// ```text
    /// [0-3]   magic (u32)
    /// [4-5]   version (u16)
    /// [6-21]  series_id (u128)
    /// [22-29] start_timestamp (i64)
    /// [30-37] end_timestamp (i64)
    /// [38-41] point_count (u32)
    /// [42-45] compressed_size (u32)
    /// [46-49] uncompressed_size (u32)
    /// [50-57] checksum (u64)
    /// [58]    compression_type (u8)
    /// [59]    flags (u8)
    /// [60-63] reserved (4 bytes, all zeros)
    /// ```
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut bytes = [0u8; 64];

        // Magic number (4 bytes)
        bytes[0..4].copy_from_slice(&self.magic.to_le_bytes());

        // Version (2 bytes)
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());

        // Series ID (16 bytes)
        bytes[6..22].copy_from_slice(&self.series_id.to_le_bytes());

        // Timestamps (8 bytes each)
        bytes[22..30].copy_from_slice(&self.start_timestamp.to_le_bytes());
        bytes[30..38].copy_from_slice(&self.end_timestamp.to_le_bytes());

        // Counts and sizes (4 bytes each)
        bytes[38..42].copy_from_slice(&self.point_count.to_le_bytes());
        bytes[42..46].copy_from_slice(&self.compressed_size.to_le_bytes());
        bytes[46..50].copy_from_slice(&self.uncompressed_size.to_le_bytes());

        // Checksum (8 bytes)
        bytes[50..58].copy_from_slice(&self.checksum.to_le_bytes());

        // Compression type (1 byte)
        bytes[58] = self.compression_type as u8;

        // Flags (1 byte)
        bytes[59] = self.flags.0;

        // Reserved (4 bytes) - already zeroed

        bytes
    }

    /// Deserialize header from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - 64-byte array containing serialized header
    ///
    /// # Returns
    ///
    /// Parsed ChunkHeader or error if invalid
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 64 {
            return Err(format!("Invalid header size: {} bytes (expected 64)", bytes.len()));
        }

        // Parse magic number
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Parse version
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);

        // Parse series ID (16 bytes)
        let series_id = u128::from_le_bytes([
            bytes[6], bytes[7], bytes[8], bytes[9],
            bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15], bytes[16], bytes[17],
            bytes[18], bytes[19], bytes[20], bytes[21],
        ]);

        // Parse timestamps
        let start_timestamp = i64::from_le_bytes([
            bytes[22], bytes[23], bytes[24], bytes[25],
            bytes[26], bytes[27], bytes[28], bytes[29],
        ]);
        let end_timestamp = i64::from_le_bytes([
            bytes[30], bytes[31], bytes[32], bytes[33],
            bytes[34], bytes[35], bytes[36], bytes[37],
        ]);

        // Parse counts and sizes
        let point_count = u32::from_le_bytes([bytes[38], bytes[39], bytes[40], bytes[41]]);
        let compressed_size = u32::from_le_bytes([bytes[42], bytes[43], bytes[44], bytes[45]]);
        let uncompressed_size = u32::from_le_bytes([bytes[46], bytes[47], bytes[48], bytes[49]]);

        // Parse checksum
        let checksum = u64::from_le_bytes([
            bytes[50], bytes[51], bytes[52], bytes[53],
            bytes[54], bytes[55], bytes[56], bytes[57],
        ]);

        // Parse compression type
        let compression_type = match bytes[58] {
            0 => CompressionType::None,
            1 => CompressionType::Gorilla,
            2 => CompressionType::Snappy,
            3 => CompressionType::GorillaSnappy,
            n => return Err(format!("Invalid compression type: {}", n)),
        };

        // Parse flags
        let flags = ChunkFlags(bytes[59]);

        Ok(Self {
            magic,
            version,
            series_id,
            start_timestamp,
            end_timestamp,
            point_count,
            compressed_size,
            uncompressed_size,
            checksum,
            compression_type,
            flags,
        })
    }
}

/// Compression type for chunk data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression
    None = 0,
    /// Gorilla compression (default)
    Gorilla = 1,
    /// Snappy compression (for cold storage)
    Snappy = 2,
    /// Gorilla + Snappy (maximum compression)
    GorillaSnappy = 3,
}

/// Chunk flags for additional metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFlags(u8);

impl ChunkFlags {
    /// No flags set
    pub fn empty() -> Self {
        Self(0)
    }

    /// Chunk is sealed (immutable)
    pub fn sealed() -> Self {
        Self(0x01)
    }

    /// Chunk is compressed with Snappy
    pub fn snappy_compressed() -> Self {
        Self(0x02)
    }

    /// Check if sealed
    pub fn is_sealed(&self) -> bool {
        self.0 & 0x01 != 0
    }

    /// Check if Snappy compressed
    pub fn is_snappy_compressed(&self) -> bool {
        self.0 & 0x02 != 0
    }
}

/// Chunk metadata for tracking stored chunks
///
/// This is a lightweight structure used by the storage engine to track
/// chunks without loading the full data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Unique chunk identifier
    pub chunk_id: ChunkId,

    /// Series this chunk belongs to
    pub series_id: SeriesId,

    /// File path to chunk data
    pub path: PathBuf,

    /// Time range covered by this chunk
    /// Start timestamp in milliseconds
    pub start_timestamp: i64,
    /// End timestamp in milliseconds
    pub end_timestamp: i64,

    /// Number of points in chunk
    pub point_count: u32,

    /// Size on disk
    pub size_bytes: u64,

    /// Compression type used
    pub compression: CompressionType,

    /// When chunk was created
    pub created_at: i64,

    /// When chunk was last accessed
    pub last_accessed: i64,
}

impl ChunkMetadata {
    /// Check if chunk overlaps with time range
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        // Chunk overlaps if: chunk.start <= range.end AND chunk.end >= range.start
        self.start_timestamp <= end && self.end_timestamp >= start
    }

    /// Check if chunk fully contains time range
    pub fn contains(&self, start: i64, end: i64) -> bool {
        self.start_timestamp <= start && self.end_timestamp >= end
    }

    /// Get time span in milliseconds
    pub fn duration_ms(&self) -> i64 {
        self.end_timestamp - self.start_timestamp
    }
}

/// Chunk lifecycle states
///
/// A chunk progresses through these states:
/// - Active: In-memory, accepting writes
/// - Sealed: Immutable, written to disk with Gorilla compression
/// - Compressed: Sealed + additional Snappy compression layer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// Chunk is active and accepting writes in memory
    Active,
    /// Chunk is sealed and stored on disk (Gorilla compressed)
    Sealed,
    /// Chunk is compressed with Snappy for cold storage
    Compressed,
}

/// Internal data storage for chunks
#[derive(Debug)]
enum ChunkData {
    /// Points stored in memory (Active state)
    InMemory(Vec<crate::types::DataPoint>),
    /// Reference to file on disk (Sealed or Compressed state)
    OnDisk(PathBuf),
}

/// Complete chunk representation with full lifecycle management
///
/// # Lifecycle
///
/// ```text
/// Active (RAM) --seal()--> Sealed (Disk) --compress()--> Compressed (Disk+Snappy)
///      ↓                        ↓                              ↓
///   append()                 read()                         read()
/// should_seal()           compress()                      metadata()
///   points()              metadata()
/// ```
///
/// # Example
///
/// ```no_run
/// use gorilla_tsdb::storage::chunk::{Chunk, SealConfig};
/// use gorilla_tsdb::types::DataPoint;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create new active chunk
/// let mut chunk = Chunk::new_active(1, 1000);
///
/// // Append points
/// chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 })?;
/// chunk.append(DataPoint { series_id: 1, timestamp: 2000, value: 43.0 })?;
///
/// // Check if should seal
/// let config = SealConfig::default();
/// if chunk.should_seal(&config) {
///     chunk.seal("/tmp/chunk.gor".into()).await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Chunk {
    /// Chunk metadata
    pub metadata: ChunkMetadata,
    /// Current lifecycle state
    state: ChunkState,
    /// Data storage (in-memory or on-disk reference)
    data: ChunkData,
    /// Capacity for active chunks (pre-allocated size)
    capacity: usize,
}

/// Configuration for when to seal active chunks
#[derive(Debug, Clone)]
pub struct SealConfig {
    /// Maximum number of points before sealing
    pub max_points: usize,
    /// Maximum time duration before sealing
    pub max_duration_ms: i64,
    /// Maximum memory size before sealing (bytes)
    pub max_size_bytes: usize,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            max_points: 10_000,           // 10K points
            max_duration_ms: 3_600_000,   // 1 hour
            max_size_bytes: 1_048_576,    // 1MB
        }
    }
}

impl Chunk {
    /// Create a new active chunk in memory
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier this chunk belongs to
    /// * `capacity` - Pre-allocated capacity for points
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::chunk::Chunk;
    ///
    /// let chunk = Chunk::new_active(1, 1000);
    /// assert!(chunk.is_active());
    /// ```
    pub fn new_active(series_id: SeriesId, capacity: usize) -> Self {
        let chunk_id = ChunkId::new();
        let now = chrono::Utc::now().timestamp_millis();

        Self {
            metadata: ChunkMetadata {
                chunk_id,
                series_id,
                path: PathBuf::new(),  // Will be set when sealed
                start_timestamp: 0,
                end_timestamp: 0,
                point_count: 0,
                size_bytes: 0,
                compression: CompressionType::None,
                created_at: now,
                last_accessed: now,
            },
            state: ChunkState::Active,
            data: ChunkData::InMemory(Vec::with_capacity(capacity)),
            capacity,
        }
    }

    /// Append a point to the chunk
    ///
    /// Only works for Active chunks. Returns error for Sealed or Compressed chunks.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk is not in Active state
    /// - Point belongs to different series
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::chunk::Chunk;
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let mut chunk = Chunk::new_active(1, 1000);
    /// let point = DataPoint { series_id: 1, timestamp: 1000, value: 42.0 };
    ///
    /// chunk.append(point).unwrap();
    /// assert_eq!(chunk.point_count(), 1);
    /// ```
    pub fn append(&mut self, point: crate::types::DataPoint) -> Result<(), String> {
        // Only active chunks can accept writes
        if !self.is_active() {
            return Err(format!("Cannot append to {:?} chunk", self.state));
        }

        // Verify series ID matches
        if point.series_id != self.metadata.series_id {
            return Err(format!(
                "Point series_id {} doesn't match chunk series_id {}",
                point.series_id, self.metadata.series_id
            ));
        }

        // Get mutable access to in-memory points
        if let ChunkData::InMemory(ref mut points) = self.data {
            // Update metadata
            if points.is_empty() {
                self.metadata.start_timestamp = point.timestamp;
            }
            self.metadata.end_timestamp = point.timestamp;
            self.metadata.point_count += 1;

            // Append point
            points.push(point);

            Ok(())
        } else {
            Err("Invalid state: Active chunk without in-memory data".to_string())
        }
    }

    /// Check if chunk should be sealed based on configuration
    ///
    /// Returns true if any threshold is exceeded:
    /// - Point count >= max_points
    /// - Duration >= max_duration_ms
    /// - Memory size >= max_size_bytes
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::chunk::{Chunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let mut chunk = Chunk::new_active(1, 100);
    /// let config = SealConfig { max_points: 10, ..Default::default() };
    ///
    /// for i in 0..10 {
    ///     chunk.append(DataPoint {
    ///         series_id: 1,
    ///         timestamp: i * 1000,
    ///         value: 42.0
    ///     }).unwrap();
    /// }
    ///
    /// assert!(chunk.should_seal(&config));
    /// ```
    pub fn should_seal(&self, config: &SealConfig) -> bool {
        if !self.is_active() {
            return false;
        }

        // Check point count threshold
        if self.metadata.point_count >= config.max_points as u32 {
            return true;
        }

        // Check duration threshold
        let duration = self.metadata.end_timestamp - self.metadata.start_timestamp;
        if duration >= config.max_duration_ms {
            return true;
        }

        // Check memory size threshold
        let size_estimate = self.metadata.point_count as usize * std::mem::size_of::<crate::types::DataPoint>();
        if size_estimate >= config.max_size_bytes {
            return true;
        }

        false
    }

    /// Seal the chunk: transition from Active to Sealed
    ///
    /// This operation:
    /// 1. Compresses points using Gorilla algorithm
    /// 2. Writes to disk with header
    /// 3. Frees in-memory data
    /// 4. Updates state to Sealed
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk is not Active
    /// - Compression fails
    /// - Disk write fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gorilla_tsdb::storage::chunk::Chunk;
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut chunk = Chunk::new_active(1, 1000);
    /// chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 })?;
    ///
    /// chunk.seal("/tmp/chunk.gor".into()).await?;
    /// assert!(chunk.is_sealed());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn seal(&mut self, path: PathBuf) -> Result<(), String> {
        if !self.is_active() {
            return Err(format!("Cannot seal {:?} chunk", self.state));
        }

        // Get points from memory
        let points = if let ChunkData::InMemory(ref points) = self.data {
            points.clone()
        } else {
            return Err("Invalid state: Active chunk without in-memory data".to_string());
        };

        if points.is_empty() {
            return Err("Cannot seal empty chunk".to_string());
        }

        // TODO: Implement actual compression and disk write
        // For now, just update state
        self.metadata.path = path.clone();
        self.metadata.compression = CompressionType::Gorilla;
        self.state = ChunkState::Sealed;
        self.data = ChunkData::OnDisk(path);

        Ok(())
    }

    /// Get current point count
    pub fn point_count(&self) -> u32 {
        self.metadata.point_count
    }

    /// Get points (for Active chunks only)
    ///
    /// Returns reference to in-memory points for Active chunks.
    /// Returns None for Sealed/Compressed chunks (use read() instead).
    pub fn points(&self) -> Option<&[crate::types::DataPoint]> {
        if let ChunkData::InMemory(ref points) = self.data {
            Some(points)
        } else {
            None
        }
    }

    /// Check if chunk is in Active state
    pub fn is_active(&self) -> bool {
        matches!(self.state, ChunkState::Active)
    }

    /// Check if chunk is in Sealed state
    pub fn is_sealed(&self) -> bool {
        matches!(self.state, ChunkState::Sealed)
    }

    /// Check if chunk is in Compressed state
    pub fn is_compressed(&self) -> bool {
        matches!(self.state, ChunkState::Compressed)
    }

    /// Get current state
    pub fn state(&self) -> ChunkState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_header_validation() {
        let mut header = ChunkHeader::new(1);
        header.point_count = 100;
        header.start_timestamp = 1000;
        header.end_timestamp = 2000;

        assert!(header.validate().is_ok());

        // Invalid magic
        let mut bad = header.clone();
        bad.magic = 0x12345678;
        assert!(bad.validate().is_err());

        // Invalid time range
        let mut bad = header.clone();
        bad.start_timestamp = 2000;
        bad.end_timestamp = 1000;
        assert!(bad.validate().is_err());

        // Empty chunk
        let mut bad = header.clone();
        bad.point_count = 0;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_chunk_metadata_overlap() {
        let metadata = ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id: 1,
            path: PathBuf::from("/test"),
            start_timestamp: 1000,
            end_timestamp: 2000,
            point_count: 100,
            size_bytes: 1024,
            compression: CompressionType::Gorilla,
            created_at: 0,
            last_accessed: 0,
        };

        // Overlapping ranges
        assert!(metadata.overlaps(500, 1500));   // Partial overlap (start)
        assert!(metadata.overlaps(1500, 2500));  // Partial overlap (end)
        assert!(metadata.overlaps(1200, 1800));  // Fully contained
        assert!(metadata.overlaps(500, 2500));   // Fully contains chunk

        // Non-overlapping ranges
        assert!(!metadata.overlaps(0, 999));     // Before
        assert!(!metadata.overlaps(2001, 3000)); // After
    }

    #[test]
    fn test_chunk_flags() {
        let empty = ChunkFlags::empty();
        assert!(!empty.is_sealed());
        assert!(!empty.is_snappy_compressed());

        let sealed = ChunkFlags::sealed();
        assert!(sealed.is_sealed());
        assert!(!sealed.is_snappy_compressed());

        let compressed = ChunkFlags::snappy_compressed();
        assert!(!compressed.is_sealed());
        assert!(compressed.is_snappy_compressed());
    }

    #[test]
    fn test_chunk_header_serialization() {
        let mut header = ChunkHeader::new(12345);
        header.start_timestamp = 1000;
        header.end_timestamp = 2000;
        header.point_count = 100;
        header.compressed_size = 1024;
        header.uncompressed_size = 2048;
        header.checksum = 0x123456789ABCDEF0;
        header.compression_type = CompressionType::Gorilla;
        header.flags = ChunkFlags::sealed();

        // Serialize to bytes
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 64);

        // Verify magic number
        assert_eq!(&bytes[0..4], &CHUNK_MAGIC.to_le_bytes());

        // Deserialize back
        let decoded = ChunkHeader::from_bytes(&bytes).unwrap();

        // Verify all fields match
        assert_eq!(decoded.magic, header.magic);
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.series_id, header.series_id);
        assert_eq!(decoded.start_timestamp, header.start_timestamp);
        assert_eq!(decoded.end_timestamp, header.end_timestamp);
        assert_eq!(decoded.point_count, header.point_count);
        assert_eq!(decoded.compressed_size, header.compressed_size);
        assert_eq!(decoded.uncompressed_size, header.uncompressed_size);
        assert_eq!(decoded.checksum, header.checksum);
        assert_eq!(decoded.compression_type, header.compression_type);
        assert_eq!(decoded.flags.0, header.flags.0);
    }

    #[test]
    fn test_chunk_header_deserialization_invalid() {
        // Too short
        let short_bytes = vec![0u8; 32];
        assert!(ChunkHeader::from_bytes(&short_bytes).is_err());

        // Invalid compression type
        let mut bytes = [0u8; 64];
        bytes[0..4].copy_from_slice(&CHUNK_MAGIC.to_le_bytes());
        bytes[58] = 99; // Invalid compression type
        assert!(ChunkHeader::from_bytes(&bytes).is_err());
    }

    // === Chunk State Machine Tests ===

    #[test]
    fn test_chunk_new_active() {
        let chunk = Chunk::new_active(42, 1000);

        assert!(chunk.is_active());
        assert!(!chunk.is_sealed());
        assert!(!chunk.is_compressed());
        assert_eq!(chunk.state(), ChunkState::Active);
        assert_eq!(chunk.metadata.series_id, 42);
        assert_eq!(chunk.point_count(), 0);
        assert!(chunk.points().is_some());
    }

    #[test]
    fn test_chunk_append() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);

        // Append first point
        let point1 = DataPoint { series_id: 1, timestamp: 1000, value: 42.0 };
        assert!(chunk.append(point1).is_ok());
        assert_eq!(chunk.point_count(), 1);
        assert_eq!(chunk.metadata.start_timestamp, 1000);
        assert_eq!(chunk.metadata.end_timestamp, 1000);

        // Append second point
        let point2 = DataPoint { series_id: 1, timestamp: 2000, value: 43.0 };
        assert!(chunk.append(point2).is_ok());
        assert_eq!(chunk.point_count(), 2);
        assert_eq!(chunk.metadata.start_timestamp, 1000);
        assert_eq!(chunk.metadata.end_timestamp, 2000);

        // Verify points
        let points = chunk.points().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].value, 42.0);
        assert_eq!(points[1].value, 43.0);
    }

    #[test]
    fn test_chunk_append_wrong_series() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        let point = DataPoint { series_id: 2, timestamp: 1000, value: 42.0 };

        let result = chunk.append(point);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("doesn't match"));
    }

    #[test]
    fn test_chunk_should_seal_point_count() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        let config = SealConfig {
            max_points: 5,
            max_duration_ms: 10_000,
            max_size_bytes: 10_000,
        };

        // Not full yet
        for i in 0..4 {
            chunk.append(DataPoint {
                series_id: 1,
                timestamp: i * 1000,
                value: 42.0
            }).unwrap();
        }
        assert!(!chunk.should_seal(&config));

        // Now it should seal
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: 5000,
            value: 42.0
        }).unwrap();
        assert!(chunk.should_seal(&config));
    }

    #[test]
    fn test_chunk_should_seal_duration() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        let config = SealConfig {
            max_points: 1000,
            max_duration_ms: 5000,  // 5 seconds
            max_size_bytes: 100_000,
        };

        // Add points within duration
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0
        }).unwrap();
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: 5000,
            value: 43.0
        }).unwrap();
        assert!(!chunk.should_seal(&config));

        // Add point exceeding duration
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: 7000,
            value: 44.0
        }).unwrap();
        assert!(chunk.should_seal(&config));
    }

    #[tokio::test]
    async fn test_chunk_seal() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);

        // Add some points
        chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 }).unwrap();
        chunk.append(DataPoint { series_id: 1, timestamp: 2000, value: 43.0 }).unwrap();

        // Seal the chunk
        let path = PathBuf::from("/tmp/test_chunk.gor");
        let result = chunk.seal(path.clone()).await;
        assert!(result.is_ok());

        // Verify state changed
        assert!(chunk.is_sealed());
        assert!(!chunk.is_active());
        assert_eq!(chunk.state(), ChunkState::Sealed);
        assert_eq!(chunk.metadata.path, path);
        assert_eq!(chunk.metadata.compression, CompressionType::Gorilla);

        // Cannot access points after sealing
        assert!(chunk.points().is_none());
    }

    #[tokio::test]
    async fn test_chunk_seal_empty() {
        let mut chunk = Chunk::new_active(1, 100);

        let result = chunk.seal(PathBuf::from("/tmp/test.gor")).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[tokio::test]
    async fn test_chunk_append_after_seal() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 }).unwrap();

        // Seal the chunk
        chunk.seal(PathBuf::from("/tmp/test.gor")).await.unwrap();

        // Try to append after sealing
        let point = DataPoint { series_id: 1, timestamp: 2000, value: 43.0 };
        let result = chunk.append(point);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Sealed"));
    }

    #[test]
    fn test_seal_config_default() {
        let config = SealConfig::default();

        assert_eq!(config.max_points, 10_000);
        assert_eq!(config.max_duration_ms, 3_600_000);  // 1 hour
        assert_eq!(config.max_size_bytes, 1_048_576);    // 1MB
    }
}
