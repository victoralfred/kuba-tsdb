//! WAL record format
//!
//! Defines the binary format for WAL records including serialization,
//! deserialization, and CRC32 checksum computation.
//!
//! # Record Format
//!
//! ```text
//! ┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
//! │    Length    │     Type     │    CRC32     │   Payload    │   Padding    │
//! │   (4 bytes)  │   (1 byte)   │   (4 bytes)  │  (N bytes)   │  (0-7 bytes) │
//! └──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
//! ```
//!
//! - **Length**: Total length of payload (not including header)
//! - **Type**: Record type (full, first, middle, last)
//! - **CRC32**: Checksum of type + payload
//! - **Payload**: Serialized data
//! - **Padding**: Zero bytes to align to 8-byte boundary

use std::io::{self, Read, Write};

use crate::types::DataPoint;

/// Record header size in bytes (length + type + crc)
pub const HEADER_SIZE: usize = 9;

/// Maximum payload size per record (64KB - header)
pub const MAX_PAYLOAD_SIZE: usize = 65536 - HEADER_SIZE;

/// Record type indicating how the record relates to larger entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Complete record in a single entry
    Full = 0,
    /// First fragment of a multi-part record
    First = 1,
    /// Middle fragment of a multi-part record
    Middle = 2,
    /// Last fragment of a multi-part record
    Last = 3,
}

impl RecordType {
    /// Convert from byte value
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(RecordType::Full),
            1 => Some(RecordType::First),
            2 => Some(RecordType::Middle),
            3 => Some(RecordType::Last),
            _ => None,
        }
    }
}

/// A WAL record containing serialized data
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Record type
    pub record_type: RecordType,
    /// CRC32 checksum
    pub checksum: u32,
    /// Serialized payload
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a new full record from data points
    pub fn from_points(points: &[DataPoint]) -> Self {
        let payload = Self::serialize_points(points);
        let checksum = Self::compute_checksum(RecordType::Full, &payload);

        Self {
            record_type: RecordType::Full,
            checksum,
            payload,
        }
    }

    /// Create a record with specified type
    pub fn new(record_type: RecordType, payload: Vec<u8>) -> Self {
        let checksum = Self::compute_checksum(record_type, &payload);
        Self {
            record_type,
            checksum,
            payload,
        }
    }

    /// Serialize data points to bytes
    ///
    /// Format per point:
    /// - series_id: u128 (16 bytes)
    /// - timestamp: i64 (8 bytes)
    /// - value: f64 (8 bytes)
    pub fn serialize_points(points: &[DataPoint]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(points.len() * 32 + 4);

        // Number of points (u32)
        buf.extend_from_slice(&(points.len() as u32).to_le_bytes());

        for point in points {
            buf.extend_from_slice(&point.series_id.to_le_bytes());
            buf.extend_from_slice(&point.timestamp.to_le_bytes());
            buf.extend_from_slice(&point.value.to_le_bytes());
        }

        buf
    }

    /// Deserialize data points from bytes
    pub fn deserialize_points(data: &[u8]) -> io::Result<Vec<DataPoint>> {
        if data.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload too short for point count",
            ));
        }

        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let expected_len = 4 + count * 32; // 16 bytes series_id + 8 bytes timestamp + 8 bytes value

        if data.len() < expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "payload too short: expected {} bytes, got {}",
                    expected_len,
                    data.len()
                ),
            ));
        }

        let mut points = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            let series_id = u128::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
                data[offset + 11],
                data[offset + 12],
                data[offset + 13],
                data[offset + 14],
                data[offset + 15],
            ]);
            offset += 16;

            let timestamp = i64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            let value = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            points.push(DataPoint::new(series_id, timestamp, value));
        }

        Ok(points)
    }

    /// Compute CRC32 checksum for record
    pub fn compute_checksum(record_type: RecordType, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type as u8]);
        hasher.update(payload);
        hasher.finalize()
    }

    /// Verify the record checksum
    pub fn verify_checksum(&self) -> bool {
        let computed = Self::compute_checksum(self.record_type, &self.payload);
        computed == self.checksum
    }

    /// Write record to a writer
    ///
    /// Returns the number of bytes written (including padding).
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let payload_len = self.payload.len() as u32;

        // Write header
        writer.write_all(&payload_len.to_le_bytes())?; // 4 bytes
        writer.write_all(&[self.record_type as u8])?; // 1 byte
        writer.write_all(&self.checksum.to_le_bytes())?; // 4 bytes

        // Write payload
        writer.write_all(&self.payload)?;

        // Calculate and write padding for 8-byte alignment
        let total = HEADER_SIZE + self.payload.len();
        let padding = (8 - (total % 8)) % 8;
        if padding > 0 {
            writer.write_all(&vec![0u8; padding])?;
        }

        Ok(total + padding)
    }

    /// Read record from a reader
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)?;

        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let record_type = RecordType::from_byte(header[4])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid record type"))?;
        let checksum = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);

        // Read payload
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        // Skip padding
        let total = HEADER_SIZE + payload_len;
        let padding = (8 - (total % 8)) % 8;
        if padding > 0 {
            let mut pad_buf = vec![0u8; padding];
            reader.read_exact(&mut pad_buf)?;
        }

        Ok(Self {
            record_type,
            checksum,
            payload,
        })
    }

    /// Get total size of record on disk (including padding)
    pub fn disk_size(&self) -> usize {
        let total = HEADER_SIZE + self.payload.len();
        let padding = (8 - (total % 8)) % 8;
        total + padding
    }

    /// Get the points from this record
    pub fn get_points(&self) -> io::Result<Vec<DataPoint>> {
        Self::deserialize_points(&self.payload)
    }
}

/// Builder for creating multi-part records
pub struct RecordBuilder {
    buffer: Vec<u8>,
}

impl RecordBuilder {
    /// Create a new record builder
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Add points to the builder
    pub fn add_points(&mut self, points: &[DataPoint]) {
        let serialized = WalRecord::serialize_points(points);
        self.buffer.extend_from_slice(&serialized);
    }

    /// Build records from accumulated data
    ///
    /// Splits large payloads into multiple records if needed.
    pub fn build(self) -> Vec<WalRecord> {
        if self.buffer.is_empty() {
            return Vec::new();
        }

        if self.buffer.len() <= MAX_PAYLOAD_SIZE {
            // Single record
            return vec![WalRecord::new(RecordType::Full, self.buffer)];
        }

        // Split into multiple records
        let mut records = Vec::new();
        let mut chunks = self.buffer.chunks(MAX_PAYLOAD_SIZE).peekable();
        let mut is_first = true;

        while let Some(chunk) = chunks.next() {
            let is_last = chunks.peek().is_none();
            let record_type = match (is_first, is_last) {
                (true, true) => RecordType::Full, // Shouldn't happen due to early return
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            records.push(WalRecord::new(record_type, chunk.to_vec()));
            is_first = false;
        }

        records
    }
}

impl Default for RecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_points() -> Vec<DataPoint> {
        vec![
            DataPoint::new(1, 1000, 42.5),
            DataPoint::new(2, 1001, 43.5),
            DataPoint::new(3, 1002, 44.5),
        ]
    }

    #[test]
    fn test_serialize_deserialize_points() {
        let points = sample_points();
        let serialized = WalRecord::serialize_points(&points);
        let deserialized = WalRecord::deserialize_points(&serialized).unwrap();

        assert_eq!(points.len(), deserialized.len());
        for (orig, recovered) in points.iter().zip(deserialized.iter()) {
            assert_eq!(orig.series_id, recovered.series_id);
            assert_eq!(orig.timestamp, recovered.timestamp);
            assert!((orig.value - recovered.value).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_record_write_read() {
        let points = sample_points();
        let record = WalRecord::from_points(&points);

        let mut buf = Vec::new();
        let written = record.write_to(&mut buf).unwrap();

        assert!(written % 8 == 0, "Written size should be 8-byte aligned");

        let mut cursor = std::io::Cursor::new(buf);
        let recovered = WalRecord::read_from(&mut cursor).unwrap();

        assert_eq!(record.record_type, recovered.record_type);
        assert_eq!(record.checksum, recovered.checksum);
        assert_eq!(record.payload, recovered.payload);
    }

    #[test]
    fn test_checksum_verification() {
        let points = sample_points();
        let record = WalRecord::from_points(&points);

        assert!(record.verify_checksum());

        // Corrupt the payload
        let mut corrupted = record.clone();
        if !corrupted.payload.is_empty() {
            corrupted.payload[0] ^= 0xFF;
        }
        assert!(!corrupted.verify_checksum());
    }

    #[test]
    fn test_record_type_conversion() {
        assert_eq!(RecordType::from_byte(0), Some(RecordType::Full));
        assert_eq!(RecordType::from_byte(1), Some(RecordType::First));
        assert_eq!(RecordType::from_byte(2), Some(RecordType::Middle));
        assert_eq!(RecordType::from_byte(3), Some(RecordType::Last));
        assert_eq!(RecordType::from_byte(4), None);
    }

    #[test]
    fn test_disk_size_alignment() {
        let points = sample_points();
        let record = WalRecord::from_points(&points);

        assert!(record.disk_size() % 8 == 0);
    }

    #[test]
    fn test_record_builder_single() {
        let mut builder = RecordBuilder::new();
        builder.add_points(&sample_points());
        let records = builder.build();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, RecordType::Full);
    }

    #[test]
    fn test_empty_builder() {
        let builder = RecordBuilder::new();
        let records = builder.build();

        assert!(records.is_empty());
    }

    #[test]
    fn test_get_points() {
        let points = sample_points();
        let record = WalRecord::from_points(&points);
        let recovered = record.get_points().unwrap();

        assert_eq!(points.len(), recovered.len());
    }
}
