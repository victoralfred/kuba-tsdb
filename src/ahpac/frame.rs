//! Chunk framing for AHPAC compressed data
//!
//! This module handles serialization and deserialization of compressed chunks
//! with proper headers, metadata, and checksums for data integrity.

use super::codecs::CodecId;
use super::profile::ChunkProfile;
use super::AhpacError;

/// A compressed chunk with metadata
///
/// This struct contains the compressed data along with all metadata
/// needed for decompression and verification.
#[derive(Debug)]
pub struct CompressedChunk {
    /// Codec used for compression
    pub codec: CodecId,
    /// Number of data points in the chunk
    pub point_count: u32,
    /// First timestamp in the chunk
    pub start_timestamp: i64,
    /// Last timestamp in the chunk
    pub end_timestamp: i64,
    /// Compressed data bytes
    pub data: Vec<u8>,
    /// Optional profile used for codec selection (not serialized)
    pub profile: Option<ChunkProfile>,
}

impl CompressedChunk {
    /// Calculate bits per sample achieved by this compression
    ///
    /// This is a key metric for comparing codec efficiency.
    pub fn bits_per_sample(&self) -> f64 {
        if self.point_count == 0 {
            return 0.0;
        }
        (self.data.len() * 8) as f64 / self.point_count as f64
    }

    /// Get the compression ratio compared to raw storage
    ///
    /// Raw storage is assumed to be 16 bytes per point (8 bytes timestamp + 8 bytes value).
    pub fn compression_ratio(&self) -> f64 {
        if self.data.is_empty() || self.point_count == 0 {
            return 1.0;
        }
        let raw_size = self.point_count as f64 * 16.0;
        raw_size / self.data.len() as f64
    }

    /// Serialize the compressed chunk to bytes for storage
    ///
    /// Format:
    /// ```text
    /// [Magic: 4 bytes] [Version: 1 byte] [Codec: 1 byte]
    /// [Point Count: 4 bytes] [Start TS: 8 bytes] [End TS: 8 bytes]
    /// [Data Length: 4 bytes] [Data: N bytes]
    /// [CRC32: 4 bytes]
    /// ```
    ///
    /// Total header overhead: 34 bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(34 + self.data.len());

        // Magic number: "AHPC" (AHPAC Chunk)
        buf.extend_from_slice(b"AHPC");

        // Version (1 byte)
        buf.push(1);

        // Codec ID (1 byte)
        buf.push(self.codec as u8);

        // Point count (4 bytes, little-endian)
        buf.extend_from_slice(&self.point_count.to_le_bytes());

        // Start timestamp (8 bytes, little-endian)
        buf.extend_from_slice(&self.start_timestamp.to_le_bytes());

        // End timestamp (8 bytes, little-endian)
        buf.extend_from_slice(&self.end_timestamp.to_le_bytes());

        // Data length (4 bytes, little-endian)
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());

        // Compressed data
        buf.extend_from_slice(&self.data);

        // CRC32 of everything before this point
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a compressed chunk from bytes
    ///
    /// Validates the magic number, version, and CRC32 checksum.
    pub fn from_bytes(data: &[u8]) -> Result<Self, AhpacError> {
        // Minimum size: header (30 bytes) + CRC (4 bytes) = 34 bytes
        if data.len() < 34 {
            return Err(AhpacError::InvalidFormat(format!(
                "Data too short: {} bytes, need at least 34",
                data.len()
            )));
        }

        // Verify magic number
        if &data[0..4] != b"AHPC" {
            return Err(AhpacError::InvalidFormat(format!(
                "Invalid magic number: expected 'AHPC', got {:?}",
                &data[0..4]
            )));
        }

        // Check version
        let version = data[4];
        if version != 1 {
            return Err(AhpacError::InvalidFormat(format!(
                "Unsupported version: {}",
                version
            )));
        }

        // Parse codec ID
        let codec = CodecId::from_byte(data[5])
            .ok_or_else(|| AhpacError::InvalidFormat(format!("Invalid codec ID: {}", data[5])))?;

        // Parse point count
        let point_count = u32::from_le_bytes(data[6..10].try_into().unwrap());

        // Parse timestamps
        let start_timestamp = i64::from_le_bytes(data[10..18].try_into().unwrap());
        let end_timestamp = i64::from_le_bytes(data[18..26].try_into().unwrap());

        // Parse data length
        let data_len = u32::from_le_bytes(data[26..30].try_into().unwrap()) as usize;

        // Verify total length
        let expected_len = 30 + data_len + 4; // header + data + CRC
        if data.len() != expected_len {
            return Err(AhpacError::InvalidFormat(format!(
                "Length mismatch: expected {}, got {}",
                expected_len,
                data.len()
            )));
        }

        // Extract compressed data
        let compressed_data = data[30..30 + data_len].to_vec();

        // Verify CRC32
        let stored_crc = u32::from_le_bytes(data[30 + data_len..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..30 + data_len]);

        if stored_crc != computed_crc {
            return Err(AhpacError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        Ok(Self {
            codec,
            point_count,
            start_timestamp,
            end_timestamp,
            data: compressed_data,
            profile: None,
        })
    }

    /// Get the total serialized size in bytes
    pub fn serialized_size(&self) -> usize {
        34 + self.data.len()
    }

    /// Check if this chunk's time range contains the given timestamp
    pub fn contains_timestamp(&self, timestamp: i64) -> bool {
        timestamp >= self.start_timestamp && timestamp <= self.end_timestamp
    }

    /// Get the duration of this chunk in milliseconds
    pub fn duration_ms(&self) -> i64 {
        self.end_timestamp.saturating_sub(self.start_timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_chunk() -> CompressedChunk {
        CompressedChunk {
            codec: CodecId::Chimp,
            point_count: 100,
            start_timestamp: 1000000,
            end_timestamp: 1099000,
            data: vec![0x01, 0x02, 0x03, 0x04, 0x05],
            profile: None,
        }
    }

    #[test]
    fn test_bits_per_sample() {
        let chunk = create_test_chunk();
        let bps = chunk.bits_per_sample();
        // 5 bytes = 40 bits / 100 points = 0.4 bits/sample
        assert!((bps - 0.4).abs() < 0.001);
    }

    #[test]
    fn test_bits_per_sample_empty() {
        let chunk = CompressedChunk {
            codec: CodecId::Kuba,
            point_count: 0,
            start_timestamp: 0,
            end_timestamp: 0,
            data: vec![],
            profile: None,
        };
        assert_eq!(chunk.bits_per_sample(), 0.0);
    }

    #[test]
    fn test_compression_ratio() {
        let chunk = create_test_chunk();
        let ratio = chunk.compression_ratio();
        // 100 points * 16 bytes = 1600 bytes raw / 5 bytes compressed = 320x
        assert!((ratio - 320.0).abs() < 0.001);
    }

    #[test]
    fn test_serialize_deserialize() {
        let original = create_test_chunk();
        let bytes = original.to_bytes();
        let restored = CompressedChunk::from_bytes(&bytes).unwrap();

        assert_eq!(restored.codec, original.codec);
        assert_eq!(restored.point_count, original.point_count);
        assert_eq!(restored.start_timestamp, original.start_timestamp);
        assert_eq!(restored.end_timestamp, original.end_timestamp);
        assert_eq!(restored.data, original.data);
    }

    #[test]
    fn test_serialize_size() {
        let chunk = create_test_chunk();
        let bytes = chunk.to_bytes();
        assert_eq!(bytes.len(), chunk.serialized_size());
        assert_eq!(bytes.len(), 34 + 5); // header + data + CRC
    }

    #[test]
    fn test_deserialize_invalid_magic() {
        let mut bytes = create_test_chunk().to_bytes();
        bytes[0] = b'X'; // Corrupt magic
        let result = CompressedChunk::from_bytes(&bytes);
        assert!(matches!(result, Err(AhpacError::InvalidFormat(_))));
    }

    #[test]
    fn test_deserialize_invalid_version() {
        let mut bytes = create_test_chunk().to_bytes();
        bytes[4] = 99; // Invalid version
                       // Recalculate CRC
        let len = bytes.len();
        let crc = crc32fast::hash(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
        let result = CompressedChunk::from_bytes(&bytes);
        assert!(matches!(result, Err(AhpacError::InvalidFormat(_))));
    }

    #[test]
    fn test_deserialize_crc_mismatch() {
        let mut bytes = create_test_chunk().to_bytes();
        // Corrupt data without updating CRC
        bytes[32] ^= 0xFF;
        let result = CompressedChunk::from_bytes(&bytes);
        assert!(matches!(result, Err(AhpacError::CrcMismatch { .. })));
    }

    #[test]
    fn test_deserialize_truncated() {
        let bytes = vec![0u8; 20]; // Too short
        let result = CompressedChunk::from_bytes(&bytes);
        assert!(matches!(result, Err(AhpacError::InvalidFormat(_))));
    }

    #[test]
    fn test_contains_timestamp() {
        let chunk = create_test_chunk();
        assert!(chunk.contains_timestamp(1000000)); // Start
        assert!(chunk.contains_timestamp(1050000)); // Middle
        assert!(chunk.contains_timestamp(1099000)); // End
        assert!(!chunk.contains_timestamp(999999)); // Before
        assert!(!chunk.contains_timestamp(1100000)); // After
    }

    #[test]
    fn test_duration_ms() {
        let chunk = create_test_chunk();
        assert_eq!(chunk.duration_ms(), 99000);
    }

    #[test]
    fn test_all_codecs() {
        for codec in [
            CodecId::Kuba,
            CodecId::Chimp,
            CodecId::Alp,
            CodecId::DeltaLz4,
            CodecId::Raw,
        ] {
            let chunk = CompressedChunk {
                codec,
                point_count: 50,
                start_timestamp: 1000,
                end_timestamp: 2000,
                data: vec![0x42; 10],
                profile: None,
            };

            let bytes = chunk.to_bytes();
            let restored = CompressedChunk::from_bytes(&bytes).unwrap();
            assert_eq!(restored.codec, codec);
        }
    }
}
