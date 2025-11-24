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
}
