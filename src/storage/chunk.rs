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

/// Magic number identifying Kuba chunk format: "Kuba" in hex
pub const CHUNK_MAGIC: u32 = 0x474F5249; // "GORI" (first 4 bytes of "Kuba")

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
            compression_type: CompressionType::Kuba,
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
            return Err(format!(
                "Invalid chunk header: point_count is 0. Chunks must contain at least one data point. \
                 Series ID: {}",
                self.series_id
            ));
        }
        if self.start_timestamp > self.end_timestamp {
            return Err(format!(
                "Invalid chunk header: start_timestamp ({}) > end_timestamp ({}). \
                 Time range is inverted. Series ID: {}",
                self.start_timestamp, self.end_timestamp, self.series_id
            ));
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
            return Err(format!(
                "Invalid header size: {} bytes (expected 64)",
                bytes.len()
            ));
        }

        // Parse magic number
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Parse version
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);

        // Parse series ID (16 bytes)
        let series_id = u128::from_le_bytes([
            bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
        ]);

        // Parse timestamps
        let start_timestamp = i64::from_le_bytes([
            bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29],
        ]);
        let end_timestamp = i64::from_le_bytes([
            bytes[30], bytes[31], bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37],
        ]);

        // Parse counts and sizes
        let point_count = u32::from_le_bytes([bytes[38], bytes[39], bytes[40], bytes[41]]);
        let compressed_size = u32::from_le_bytes([bytes[42], bytes[43], bytes[44], bytes[45]]);
        let uncompressed_size = u32::from_le_bytes([bytes[46], bytes[47], bytes[48], bytes[49]]);

        // Parse checksum
        let checksum = u64::from_le_bytes([
            bytes[50], bytes[51], bytes[52], bytes[53], bytes[54], bytes[55], bytes[56], bytes[57],
        ]);

        // Parse compression type
        let compression_type = match bytes[58] {
            0 => CompressionType::None,
            1 => CompressionType::Kuba,
            2 => CompressionType::Snappy,
            3 => CompressionType::KubaSnappy,
            4 => CompressionType::Ahpac,
            5 => CompressionType::AhpacSnappy,
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
    /// Kuba compression (default)
    Kuba = 1,
    /// Snappy compression (for cold storage)
    Snappy = 2,
    /// Kuba + Snappy (maximum compression)
    KubaSnappy = 3,
    /// AHPAC adaptive compression
    Ahpac = 4,
    /// AHPAC + Snappy (adaptive with secondary compression)
    AhpacSnappy = 5,
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

    /// Size on disk (compressed)
    pub size_bytes: u64,

    /// Original uncompressed size in bytes
    /// Used to calculate compression ratio
    #[serde(default)]
    pub uncompressed_size: u64,

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
/// - Sealed: Immutable, written to disk with Kuba compression
/// - Compressed: Sealed + additional Snappy compression layer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// Chunk is active and accepting writes in memory
    Active,
    /// Chunk is sealed and stored on disk (Kuba compressed)
    Sealed,
    /// Chunk is compressed with Snappy for cold storage
    Compressed,
}

/// Internal data storage for chunks
#[derive(Debug)]
enum ChunkData {
    /// Points stored in memory (Active state), sorted by timestamp
    /// Uses BTreeMap for automatic ordering and efficient out-of-order inserts
    InMemory(std::collections::BTreeMap<i64, crate::types::DataPoint>),
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
/// use kuba_tsdb::storage::chunk::{Chunk, SealConfig};
/// use kuba_tsdb::types::DataPoint;
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
///     chunk.seal("/tmp/chunk.kub".into()).await?;
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
    /// Capacity for active chunks used to enforce point limits
    capacity: usize,
    /// P2.2: Cached header to avoid re-reading from disk on decompress
    cached_header: Option<ChunkHeader>,
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

impl SealConfig {
    /// Minimum allowed points before sealing (must have at least 1 point)
    const MIN_POINTS: usize = 1;
    /// Maximum allowed points before sealing (10 million)
    const MAX_POINTS: usize = 10_000_000;
    /// Minimum duration (1 second)
    const MIN_DURATION_MS: i64 = 1_000;
    /// Maximum duration (7 days)
    const MAX_DURATION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
    /// Minimum size (1KB)
    const MIN_SIZE_BYTES: usize = 1_024;
    /// Maximum size (1GB)
    const MAX_SIZE_BYTES: usize = 1_024 * 1_024 * 1_024;

    /// Validate the seal configuration
    ///
    /// # Validation Rules
    ///
    /// - `max_points` must be between 1 and 10,000,000
    /// - `max_duration_ms` must be between 1,000 (1 second) and 604,800,000 (7 days)
    /// - `max_size_bytes` must be between 1,024 (1KB) and 1,073,741,824 (1GB)
    ///
    /// # Example
    ///
    /// ```
    /// use kuba_tsdb::storage::chunk::SealConfig;
    ///
    /// let config = SealConfig::default();
    /// assert!(config.validate().is_ok());
    ///
    /// let invalid = SealConfig {
    ///     max_points: 0,
    ///     max_duration_ms: 1000,
    ///     max_size_bytes: 1024,
    /// };
    /// assert!(invalid.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        // VAL-004: Add validation for SealConfig
        if self.max_points < Self::MIN_POINTS {
            return Err(format!(
                "max_points {} is below minimum {}",
                self.max_points,
                Self::MIN_POINTS
            ));
        }
        if self.max_points > Self::MAX_POINTS {
            return Err(format!(
                "max_points {} exceeds maximum {}",
                self.max_points,
                Self::MAX_POINTS
            ));
        }
        if self.max_duration_ms < Self::MIN_DURATION_MS {
            return Err(format!(
                "max_duration_ms {} is below minimum {}",
                self.max_duration_ms,
                Self::MIN_DURATION_MS
            ));
        }
        if self.max_duration_ms > Self::MAX_DURATION_MS {
            return Err(format!(
                "max_duration_ms {} exceeds maximum {}",
                self.max_duration_ms,
                Self::MAX_DURATION_MS
            ));
        }
        if self.max_size_bytes < Self::MIN_SIZE_BYTES {
            return Err(format!(
                "max_size_bytes {} is below minimum {}",
                self.max_size_bytes,
                Self::MIN_SIZE_BYTES
            ));
        }
        if self.max_size_bytes > Self::MAX_SIZE_BYTES {
            return Err(format!(
                "max_size_bytes {} exceeds maximum {}",
                self.max_size_bytes,
                Self::MAX_SIZE_BYTES
            ));
        }
        Ok(())
    }
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            max_points: 10_000,         // 10K points
            max_duration_ms: 3_600_000, // 1 hour
            max_size_bytes: 1_048_576,  // 1MB
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
    /// use kuba_tsdb::storage::chunk::Chunk;
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
                path: PathBuf::new(),      // Will be set when sealed
                start_timestamp: i64::MAX, // Will be updated to min on first append
                end_timestamp: i64::MIN,   // Will be updated to max on first append
                point_count: 0,
                size_bytes: 0,
                uncompressed_size: 0, // Will be updated when sealed
                compression: CompressionType::None,
                created_at: now,
                last_accessed: now,
            },
            state: ChunkState::Active,
            data: ChunkData::InMemory(std::collections::BTreeMap::new()),
            capacity,
            cached_header: None,
        }
    }

    /// Create chunk directly from BTreeMap (zero-copy from ActiveChunk)
    ///
    /// This constructor is used internally by ActiveChunk::seal() to avoid
    /// copying data when transitioning from active to sealed state.
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier
    /// * `points` - BTreeMap of points (already sorted by timestamp)
    ///
    /// # Returns
    ///
    /// Active chunk ready for sealing, or error if points is empty.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut points = BTreeMap::new();
    /// points.insert(1000, DataPoint { series_id: 1, timestamp: 1000, value: 42.0 });
    /// let chunk = Chunk::from_btreemap(1, points).unwrap();
    /// ```
    pub fn from_btreemap(
        series_id: SeriesId,
        points: std::collections::BTreeMap<i64, crate::types::DataPoint>,
    ) -> Result<Self, String> {
        if points.is_empty() {
            return Err("Cannot create chunk from empty data".to_string());
        }

        // P1.7: Validate all points belong to the same series
        // This prevents data corruption from mixed series in a single chunk
        for (timestamp, point) in points.iter() {
            if point.series_id != series_id {
                return Err(format!(
                    "Data integrity violation: Point at timestamp {} has series_id {} \
                     but chunk expects series_id {}. All points in a chunk must belong \
                     to the same series.",
                    timestamp, point.series_id, series_id
                ));
            }
        }

        let point_count = points.len() as u32;
        let start_timestamp = *points.keys().next().unwrap();
        let end_timestamp = *points.keys().next_back().unwrap();

        let now = chrono::Utc::now().timestamp_millis();

        Ok(Self {
            metadata: ChunkMetadata {
                chunk_id: ChunkId::new(),
                series_id,
                path: PathBuf::new(),
                start_timestamp,
                end_timestamp,
                point_count,
                size_bytes: 0,
                uncompressed_size: 0, // Will be updated when sealed
                compression: CompressionType::None,
                created_at: now,
                last_accessed: now,
            },
            state: ChunkState::Active,
            data: ChunkData::InMemory(points),
            capacity: 0,
            cached_header: None,
        })
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
    /// use kuba_tsdb::storage::chunk::Chunk;
    /// use kuba_tsdb::types::DataPoint;
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
            // Check configured capacity limit first
            if points.len() >= self.capacity {
                return Err(format!(
                    "Chunk has reached capacity of {} points. \
                     Call should_seal() and seal before appending more data.",
                    self.capacity
                ));
            }

            // CRITICAL: Hard limit to prevent unbounded memory growth
            const MAX_CHUNK_POINTS: usize = 10_000_000; // 10 million points
            if points.len() >= MAX_CHUNK_POINTS {
                return Err(format!(
                    "Chunk has reached maximum size of {} points. \
                     Call should_seal() and seal before appending more data.",
                    MAX_CHUNK_POINTS
                ));
            }

            // P0.2: Check for duplicate timestamp BEFORE inserting
            if points.contains_key(&point.timestamp) {
                return Err(format!(
                    "Duplicate timestamp {}: refusing to overwrite. \
                     Existing value: {}, new value: {}",
                    point.timestamp,
                    points.get(&point.timestamp).unwrap().value,
                    point.value
                ));
            }

            // P0.1: Insert into BTreeMap (automatically maintains sort order)
            points.insert(point.timestamp, point);

            // P0.1: Update metadata with correct min/max (handles out-of-order correctly)
            self.metadata.point_count = points.len() as u32;

            // BTreeMap keeps keys sorted, so first/last are min/max
            if let Some((&min_ts, _)) = points.iter().next() {
                self.metadata.start_timestamp = min_ts;
            }
            if let Some((&max_ts, _)) = points.iter().next_back() {
                self.metadata.end_timestamp = max_ts;
            }

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
    /// use kuba_tsdb::storage::chunk::{Chunk, SealConfig};
    /// use kuba_tsdb::types::DataPoint;
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

        // Empty chunks should never seal
        if self.metadata.point_count == 0 {
            return false;
        }

        // Check point count threshold
        if self.metadata.point_count >= config.max_points as u32 {
            return true;
        }

        // Check duration threshold (use checked_sub to prevent overflow)
        match self
            .metadata
            .end_timestamp
            .checked_sub(self.metadata.start_timestamp)
        {
            Some(duration) => {
                if duration >= config.max_duration_ms {
                    return true;
                }
            }
            None => {
                // Overflow means extreme time range spanning i64 limits
                // Definitely should seal in this case
                return true;
            }
        }

        // Check memory size threshold
        // Account for BTreeMap node overhead (~64 bytes per entry)
        const BTREEMAP_NODE_OVERHEAD: usize = 64;
        let size_estimate = self.metadata.point_count as usize
            * (std::mem::size_of::<crate::types::DataPoint>() + BTREEMAP_NODE_OVERHEAD);
        if size_estimate >= config.max_size_bytes {
            return true;
        }

        false
    }

    /// Seal the chunk: transition from Active to Sealed
    ///
    /// This operation:
    /// 1. Compresses points using Kuba algorithm
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
    /// - Parent directory doesn't exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kuba_tsdb::storage::chunk::Chunk;
    /// use kuba_tsdb::types::DataPoint;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut chunk = Chunk::new_active(1, 1000);
    /// chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 })?;
    ///
    /// chunk.seal("/tmp/chunk.kub".into()).await?;
    /// assert!(chunk.is_sealed());
    /// # Ok(())
    /// # }
    /// ```
    /// Seal chunk using the default compression algorithm (AHPAC)
    ///
    /// For backward compatibility or specific use cases, use `seal_with_compression()`
    /// to specify a different compression algorithm.
    pub async fn seal(&mut self, path: PathBuf) -> Result<(), String> {
        // Default to AHPAC for better compression on most data types
        self.seal_with_compression(path, CompressionType::Ahpac)
            .await
    }

    /// Seal chunk with a specific compression algorithm
    ///
    /// # Arguments
    ///
    /// * `path` - File path to write the sealed chunk
    /// * `compression_type` - Compression algorithm to use
    ///
    /// # Supported Compression Types
    ///
    /// * `CompressionType::Ahpac` - Adaptive compression, best for most data (default)
    /// * `CompressionType::Kuba` - Gorilla-based, good for random/high-entropy data
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kuba_tsdb::storage::chunk::{Chunk, CompressionType};
    /// # async fn example() -> Result<(), String> {
    /// let mut chunk = Chunk::new_active(1, 100);
    /// // ... append data ...
    /// chunk.seal_with_compression("/tmp/chunk.kub".into(), CompressionType::Kuba).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn seal_with_compression(
        &mut self,
        path: PathBuf,
        compression_type: CompressionType,
    ) -> Result<(), String> {
        use crate::compression::{AhpacCompressor, KubaCompressor};
        use crate::engine::traits::Compressor;
        use tokio::fs;
        use tokio::io::AsyncWriteExt;

        if !self.is_active() {
            return Err(format!(
                "Cannot seal chunk in {:?} state. Series ID: {}, path: {:?}. \
                 Only Active chunks can be sealed. Sealed chunks are already on disk.",
                self.state, self.metadata.series_id, self.metadata.path
            ));
        }

        // P1.6: Clone data for compression, keep original for retry on failure
        // This prevents data loss if seal fails (e.g., disk full, compression error)
        // We only clear the original data after successful seal
        let points = if let ChunkData::InMemory(ref points_map) = self.data {
            if points_map.is_empty() {
                return Err(format!(
                    "Cannot seal empty chunk. Series ID: {}, chunk ID: {}. \
                     Append at least one data point before sealing.",
                    self.metadata.series_id, self.metadata.chunk_id
                ));
            }

            // Clone to Vec for compression
            // Note: This is a temporary copy that will be freed after compression
            points_map.values().cloned().collect::<Vec<_>>()
        } else {
            return Err(format!(
                "Invalid state: Active chunk without in-memory data. Series ID: {}, chunk ID: {}. \
                 This indicates internal corruption - the chunk state is Active but data is not in memory.",
                self.metadata.series_id, self.metadata.chunk_id
            ));
        };

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                format!(
                    "Failed to create directory for series {}, chunk path {:?}: {}",
                    self.metadata.series_id, path, e
                )
            })?;
        }

        // P1.2: Compress data on blocking thread pool
        // This prevents CPU-intensive compression from blocking async runtime
        let comp_type = compression_type;
        let compressed = tokio::task::spawn_blocking(move || {
            match comp_type {
                CompressionType::Ahpac | CompressionType::AhpacSnappy => {
                    // Use AHPAC adaptive compression
                    let compressor = AhpacCompressor::new();
                    futures::executor::block_on(compressor.compress(&points))
                }
                CompressionType::Kuba | CompressionType::KubaSnappy | CompressionType::None => {
                    // Use Kuba (Gorilla-based) compression
                    let compressor = KubaCompressor::new();
                    futures::executor::block_on(compressor.compress(&points))
                }
                CompressionType::Snappy => {
                    // Snappy-only not supported for time-series, fall back to Kuba
                    let compressor = KubaCompressor::new();
                    futures::executor::block_on(compressor.compress(&points))
                }
            }
        })
        .await
        .map_err(|e| format!("Compression task panicked: {}", e))?
        .map_err(|e| format!("Compression failed: {}", e))?;

        // Build chunk header
        let mut header = ChunkHeader::new(self.metadata.series_id);
        header.start_timestamp = self.metadata.start_timestamp;
        header.end_timestamp = self.metadata.end_timestamp;
        header.point_count = self.metadata.point_count;
        header.compressed_size = compressed.compressed_size as u32;
        header.uncompressed_size = compressed.original_size as u32;
        header.checksum = compressed.checksum;
        header.compression_type = compression_type;
        header.flags = ChunkFlags::sealed();

        // Validate header
        header
            .validate()
            .map_err(|e| format!("Invalid header: {}", e))?;

        // Write to disk: header + compressed data
        let mut file = fs::File::create(&path).await.map_err(|e| {
            format!(
                "Failed to create chunk file for series {}, path {:?}: {}",
                self.metadata.series_id, path, e
            )
        })?;

        // Write 64-byte header
        file.write_all(&header.to_bytes()).await.map_err(|e| {
            format!(
                "Failed to write header for series {}, chunk {:?}: {}",
                self.metadata.series_id, path, e
            )
        })?;

        // Write compressed data
        file.write_all(&compressed.data).await.map_err(|e| {
            format!(
                "Failed to write data for series {}, chunk {:?}: {}",
                self.metadata.series_id, path, e
            )
        })?;

        // CRITICAL: Sync to disk before marking sealed
        // flush() only writes to OS buffers, sync_all() ensures disk persistence
        file.sync_all()
            .await
            .map_err(|e| format!("Failed to sync file to disk: {}", e))?;

        // Update chunk state (only after data is safely on disk)
        self.metadata.path = path.clone();
        self.metadata.compression = CompressionType::Kuba;
        self.metadata.size_bytes = (64 + compressed.compressed_size) as u64;
        self.state = ChunkState::Sealed;

        // P1.6: Now that seal succeeded, clear in-memory data and update to OnDisk
        // If we had failed earlier, the data would still be in memory for retry
        if let ChunkData::InMemory(ref mut points_map) = self.data {
            points_map.clear(); // Free memory now that data is safely on disk
        }
        self.data = ChunkData::OnDisk(path);

        Ok(())
    }

    /// Load a sealed chunk from disk
    ///
    /// Reads the chunk header, validates it, and creates a Chunk in Sealed state.
    /// The actual point data remains on disk until decompressed.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chunk file
    ///
    /// # Returns
    ///
    /// A `Chunk` in Sealed state with metadata populated from the file header.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - File doesn't exist
    /// - File is too small (< 64 bytes)
    /// - Header validation fails
    /// - Checksum doesn't match
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kuba_tsdb::storage::chunk::Chunk;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let chunk = Chunk::read("/tmp/chunk.kub".into()).await?;
    /// assert!(chunk.is_sealed());
    /// println!("Loaded chunk with {} points", chunk.point_count());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(path: PathBuf) -> Result<Self, String> {
        use tokio::fs;
        use tokio::io::AsyncReadExt;

        // Open and read header
        let mut file = fs::File::open(&path)
            .await
            .map_err(|e| format!("Failed to open file: {}", e))?;

        let mut header_bytes = [0u8; 64];
        file.read_exact(&mut header_bytes)
            .await
            .map_err(|e| format!("Failed to read header: {}", e))?;

        // Parse and validate header
        let header = ChunkHeader::from_bytes(&header_bytes)
            .map_err(|e| format!("Failed to parse header: {}", e))?;
        header
            .validate()
            .map_err(|e| format!("Invalid header: {}", e))?;

        // Read compressed data for checksum verification
        let mut compressed_data = vec![0u8; header.compressed_size as usize];
        file.read_exact(&mut compressed_data)
            .await
            .map_err(|e| format!("Failed to read data: {}", e))?;

        // Verify checksum
        use crate::compression::kuba::KubaCompressor;
        let calculated_checksum = KubaCompressor::calculate_checksum(&compressed_data);
        if calculated_checksum != header.checksum {
            return Err(format!(
                "Checksum mismatch: expected {}, got {}",
                header.checksum, calculated_checksum
            ));
        }

        // Create chunk in Sealed state
        let now = chrono::Utc::now().timestamp_millis();
        // Extract chunk ID from filename, falling back to a default UUID if parsing fails
        let chunk_id_str = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("chunk_"))
            .unwrap_or("00000000-0000-0000-0000-000000000000");
        // Use unchecked since this is internal trusted storage
        let chunk_id = ChunkId::from_string_unchecked(chunk_id_str);

        Ok(Self {
            metadata: ChunkMetadata {
                chunk_id,
                series_id: header.series_id,
                path: path.clone(),
                start_timestamp: header.start_timestamp,
                end_timestamp: header.end_timestamp,
                point_count: header.point_count,
                size_bytes: (64 + header.compressed_size) as u64,
                uncompressed_size: header.uncompressed_size as u64,
                compression: header.compression_type,
                created_at: now,
                last_accessed: now,
            },
            state: ChunkState::Sealed,
            data: ChunkData::OnDisk(path),
            capacity: 0,
            cached_header: Some(header), // P2.2: Cache header for decompress()
        })
    }

    /// Decompress and extract points from a sealed chunk
    ///
    /// Reads the compressed data from disk and decompresses it using the
    /// appropriate algorithm (based on header).
    ///
    /// # Returns
    ///
    /// Vector of decompressed data points in chronological order.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk is not Sealed
    /// - File read fails
    /// - Decompression fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kuba_tsdb::storage::chunk::Chunk;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let chunk = Chunk::read("/tmp/chunk.kub".into()).await?;
    /// let points = chunk.decompress().await?;
    /// println!("Decompressed {} points", points.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn decompress(&self) -> Result<Vec<crate::types::DataPoint>, String> {
        use crate::compression::{AhpacCompressor, KubaCompressor};
        use crate::engine::traits::{CompressedBlock, Compressor};
        use bytes::Bytes;
        use tokio::fs;
        use tokio::io::AsyncReadExt;

        if !self.is_sealed() {
            return Err(format!(
                "Cannot decompress chunk in {:?} state. Series ID: {}, chunk ID: {}. \
                 Only Sealed chunks can be decompressed. Active chunks have data in memory already.",
                self.state, self.metadata.series_id, self.metadata.chunk_id
            ));
        }

        let path = if let ChunkData::OnDisk(ref path) = self.data {
            path
        } else {
            return Err(format!(
                "Invalid state: Sealed chunk without disk path. Series ID: {}, chunk ID: {}. \
                 This indicates internal corruption - the chunk is marked Sealed but has no file path.",
                self.metadata.series_id, self.metadata.chunk_id
            ));
        };

        // P2.2: Use cached header if available, otherwise read from disk
        let (header, compressed_data) = if let Some(ref cached) = self.cached_header {
            // Fast path: header already cached, only read compressed data
            let mut file = fs::File::open(path)
                .await
                .map_err(|e| format!("Failed to open file: {}", e))?;

            // Seek past header to compressed data at offset 64
            use tokio::io::AsyncSeekExt;
            file.seek(std::io::SeekFrom::Start(64))
                .await
                .map_err(|e| format!("Failed to seek: {}", e))?;

            // Read compressed data
            let mut compressed_data = vec![0u8; cached.compressed_size as usize];
            file.read_exact(&mut compressed_data)
                .await
                .map_err(|e| format!("Failed to read data: {}", e))?;

            (cached.clone(), compressed_data)
        } else {
            // Slow path: read and parse header from disk
            let mut file = fs::File::open(path)
                .await
                .map_err(|e| format!("Failed to open file: {}", e))?;

            let mut header_bytes = [0u8; 64];
            file.read_exact(&mut header_bytes)
                .await
                .map_err(|e| format!("Failed to read header: {}", e))?;

            let header = ChunkHeader::from_bytes(&header_bytes)
                .map_err(|e| format!("Failed to parse header: {}", e))?;

            // Read compressed data
            let mut compressed_data = vec![0u8; header.compressed_size as usize];
            file.read_exact(&mut compressed_data)
                .await
                .map_err(|e| format!("Failed to read data: {}", e))?;

            (header, compressed_data)
        };

        // Determine algorithm ID from compression type
        let algorithm_id = match header.compression_type {
            CompressionType::Ahpac | CompressionType::AhpacSnappy => "ahpac",
            _ => "kuba",
        };

        // Create CompressedBlock for decompressor
        let compressed_block = CompressedBlock {
            algorithm_id: algorithm_id.to_string(),
            original_size: header.uncompressed_size as usize,
            compressed_size: header.compressed_size as usize,
            checksum: header.checksum,
            data: Bytes::from(compressed_data),
            metadata: crate::engine::traits::BlockMetadata {
                start_timestamp: header.start_timestamp,
                end_timestamp: header.end_timestamp,
                point_count: header.point_count as usize,
                series_id: header.series_id,
            },
        };

        // Decompress using the appropriate compressor based on header's compression_type
        match header.compression_type {
            CompressionType::Ahpac | CompressionType::AhpacSnappy => {
                let compressor = AhpacCompressor::new();
                compressor
                    .decompress(&compressed_block)
                    .await
                    .map_err(|e| format!("AHPAC decompression failed: {}", e))
            }
            _ => {
                // Kuba, KubaSnappy, Snappy, None - all use Kuba decompressor
                let compressor = KubaCompressor::new();
                compressor
                    .decompress(&compressed_block)
                    .await
                    .map_err(|e| format!("Kuba decompression failed: {}", e))
            }
        }
    }

    /// Get current point count
    pub fn point_count(&self) -> u32 {
        self.metadata.point_count
    }

    /// Get points (for Active chunks only)
    ///
    /// Returns Vec of points in sorted order for Active chunks.
    /// Returns None for Sealed/Compressed chunks (use decompress() instead).
    ///
    /// Note: This creates a Vec from the BTreeMap, which involves a copy.
    /// For large chunks, consider sealing and using decompress() instead.
    pub fn points(&self) -> Option<Vec<crate::types::DataPoint>> {
        if let ChunkData::InMemory(ref points) = self.data {
            // BTreeMap is already sorted by timestamp, collect values
            Some(points.values().cloned().collect())
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

    /// Get series ID
    pub fn series_id(&self) -> SeriesId {
        self.metadata.series_id
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
            uncompressed_size: 0,
            compression: CompressionType::Kuba,
            created_at: 0,
            last_accessed: 0,
        };

        // Overlapping ranges
        assert!(metadata.overlaps(500, 1500)); // Partial overlap (start)
        assert!(metadata.overlaps(1500, 2500)); // Partial overlap (end)
        assert!(metadata.overlaps(1200, 1800)); // Fully contained
        assert!(metadata.overlaps(500, 2500)); // Fully contains chunk

        // Non-overlapping ranges
        assert!(!metadata.overlaps(0, 999)); // Before
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
        header.compression_type = CompressionType::Kuba;
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
        let point1 = DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0,
        };
        assert!(chunk.append(point1).is_ok());
        assert_eq!(chunk.point_count(), 1);
        assert_eq!(chunk.metadata.start_timestamp, 1000);
        assert_eq!(chunk.metadata.end_timestamp, 1000);

        // Append second point
        let point2 = DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 43.0,
        };
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
        let point = DataPoint {
            series_id: 2,
            timestamp: 1000,
            value: 42.0,
        };

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
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i * 1000,
                    value: 42.0,
                })
                .unwrap();
        }
        assert!(!chunk.should_seal(&config));

        // Now it should seal
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 5000,
                value: 42.0,
            })
            .unwrap();
        assert!(chunk.should_seal(&config));
    }

    #[test]
    fn test_chunk_should_seal_duration() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        let config = SealConfig {
            max_points: 1000,
            max_duration_ms: 5000, // 5 seconds
            max_size_bytes: 100_000,
        };

        // Add points within duration
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .unwrap();
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 5000,
                value: 43.0,
            })
            .unwrap();
        assert!(!chunk.should_seal(&config));

        // Add point exceeding duration
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 7000,
                value: 44.0,
            })
            .unwrap();
        assert!(chunk.should_seal(&config));
    }

    #[tokio::test]
    async fn test_chunk_seal() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);

        // Add some points
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .unwrap();
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 43.0,
            })
            .unwrap();

        // Seal the chunk
        let path = PathBuf::from("/tmp/test_chunk.kub");
        let result = chunk.seal(path.clone()).await;
        assert!(result.is_ok());

        // Verify state changed
        assert!(chunk.is_sealed());
        assert!(!chunk.is_active());
        assert_eq!(chunk.state(), ChunkState::Sealed);
        assert_eq!(chunk.metadata.path, path);
        assert_eq!(chunk.metadata.compression, CompressionType::Kuba);

        // Cannot access points after sealing
        assert!(chunk.points().is_none());
    }

    #[tokio::test]
    async fn test_chunk_seal_empty() {
        let mut chunk = Chunk::new_active(1, 100);

        let result = chunk.seal(PathBuf::from("/tmp/test.kub")).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[tokio::test]
    async fn test_chunk_append_after_seal() {
        use crate::types::DataPoint;

        let mut chunk = Chunk::new_active(1, 100);
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .unwrap();

        // Seal the chunk
        chunk.seal(PathBuf::from("/tmp/test.kub")).await.unwrap();

        // Try to append after sealing
        let point = DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 43.0,
        };
        let result = chunk.append(point);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Sealed"));
    }

    #[test]
    fn test_seal_config_default() {
        let config = SealConfig::default();

        assert_eq!(config.max_points, 10_000);
        assert_eq!(config.max_duration_ms, 3_600_000); // 1 hour
        assert_eq!(config.max_size_bytes, 1_048_576); // 1MB
    }

    #[tokio::test]
    async fn test_chunk_seal_read_decompress_roundtrip() {
        use crate::types::DataPoint;

        // Create active chunk with test data
        let mut chunk = Chunk::new_active(42, 100);
        let original_points = vec![
            DataPoint {
                series_id: 42,
                timestamp: 1000,
                value: 10.5,
            },
            DataPoint {
                series_id: 42,
                timestamp: 2000,
                value: 20.75,
            },
            DataPoint {
                series_id: 42,
                timestamp: 3000,
                value: 30.25,
            },
            DataPoint {
                series_id: 42,
                timestamp: 4000,
                value: 40.0,
            },
        ];

        // Add points to chunk
        for point in &original_points {
            chunk.append(*point).expect("Failed to append point");
        }

        assert_eq!(chunk.point_count(), 4);
        assert!(chunk.is_active());

        // Seal to disk
        let test_path = PathBuf::from("/tmp/test_chunk_roundtrip.kub");
        chunk
            .seal(test_path.clone())
            .await
            .expect("Failed to seal chunk");

        assert!(chunk.is_sealed());
        assert_eq!(chunk.metadata.point_count, 4);

        // Read chunk from disk
        let loaded_chunk = Chunk::read(test_path.clone())
            .await
            .expect("Failed to read chunk");

        assert!(loaded_chunk.is_sealed());
        assert_eq!(loaded_chunk.series_id(), 42);
        assert_eq!(loaded_chunk.point_count(), 4);
        assert_eq!(loaded_chunk.metadata.start_timestamp, 1000);
        assert_eq!(loaded_chunk.metadata.end_timestamp, 4000);

        // Decompress and verify points
        let decompressed_points = loaded_chunk
            .decompress()
            .await
            .expect("Failed to decompress");

        assert_eq!(decompressed_points.len(), 4);
        assert_eq!(decompressed_points[0].timestamp, 1000);
        assert_eq!(decompressed_points[0].value, 10.5);
        assert_eq!(decompressed_points[1].timestamp, 2000);
        assert_eq!(decompressed_points[1].value, 20.75);
        assert_eq!(decompressed_points[2].timestamp, 3000);
        assert_eq!(decompressed_points[2].value, 30.25);
        assert_eq!(decompressed_points[3].timestamp, 4000);
        assert_eq!(decompressed_points[3].value, 40.0);

        // Cleanup
        let _ = tokio::fs::remove_file(test_path).await;
    }
}
