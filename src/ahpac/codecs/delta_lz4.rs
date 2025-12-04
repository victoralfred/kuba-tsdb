//! Delta + LZ4 codec for AHPAC
//!
//! This codec applies delta encoding followed by LZ4 compression.
//! It works well for smooth, highly autocorrelated data where the
//! XOR values have repetitive patterns that LZ4 can exploit.
//!
//! # Algorithm
//!
//! 1. Delta encode timestamps (varint)
//! 2. XOR encode values (byte-aligned)
//! 3. LZ4 compress the combined buffer
//!
//! # When to Use
//!
//! Delta+LZ4 is effective for:
//! - Smooth, slowly varying data with high autocorrelation
//! - Data where XOR patterns repeat frequently
//! - Cold storage archival (prioritizes ratio over speed)

use super::{Codec, CodecError, CodecId};
use crate::ahpac::profile::ChunkProfile;
use crate::types::DataPoint;

/// Delta + LZ4 codec
///
/// Combines delta/XOR preprocessing with LZ4 compression for
/// archival-quality compression of smooth time-series data.
pub struct DeltaLz4Codec {
    /// Compression level (0-9, higher = better compression but slower)
    /// Reserved for future use with lz4_flex compression levels
    #[allow(dead_code)]
    compression_level: u32,
}

impl DeltaLz4Codec {
    /// Create a new Delta+LZ4 codec with default settings
    pub fn new() -> Self {
        Self {
            compression_level: 4, // Balanced default
        }
    }

    /// Create with a specific compression level
    ///
    /// Level ranges from 0 (fastest) to 9 (best compression).
    pub fn with_level(level: u32) -> Self {
        Self {
            compression_level: level.min(9),
        }
    }

    /// Write a varint (variable-length integer) to the buffer
    fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // More bytes follow
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// Read a varint from a slice, returning the value and bytes consumed
    fn read_varint(data: &[u8]) -> Result<(u64, usize), CodecError> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut bytes_read = 0;

        for &byte in data {
            bytes_read += 1;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok((result, bytes_read));
            }
            shift += 7;
            if shift > 63 {
                return Err(CodecError::DecompressionFailed(
                    "Varint overflow".to_string(),
                ));
            }
        }

        Err(CodecError::DecompressionFailed(
            "Truncated varint".to_string(),
        ))
    }

    /// Encode a signed integer using zigzag encoding
    fn zigzag_encode(n: i64) -> u64 {
        ((n << 1) ^ (n >> 63)) as u64
    }

    /// Decode a zigzag-encoded unsigned integer to signed
    fn zigzag_decode(n: u64) -> i64 {
        ((n >> 1) as i64) ^ (-((n & 1) as i64))
    }

    /// Prepare raw buffer with delta-encoded timestamps and XOR-encoded values
    fn prepare_raw_buffer(points: &[DataPoint]) -> Vec<u8> {
        if points.is_empty() {
            return Vec::new();
        }

        // Estimate buffer size: ~16 bytes per point raw, but often much less after delta
        let mut buf = Vec::with_capacity(points.len() * 8);

        // Write point count
        Self::write_varint(&mut buf, points.len() as u64);

        // First timestamp (full 64 bits)
        buf.extend_from_slice(&points[0].timestamp.to_le_bytes());

        // Delta encode remaining timestamps
        let mut prev_ts = points[0].timestamp;
        for point in &points[1..] {
            let delta = point.timestamp - prev_ts;
            // Use signed zigzag encoding for deltas (in case of out-of-order correction)
            Self::write_varint(&mut buf, Self::zigzag_encode(delta));
            prev_ts = point.timestamp;
        }

        // First value (full 64 bits)
        buf.extend_from_slice(&points[0].value.to_bits().to_le_bytes());

        // XOR encode remaining values (byte-aligned)
        let mut prev_val = points[0].value.to_bits();
        for point in &points[1..] {
            let val = point.value.to_bits();
            let xor = prev_val ^ val;
            buf.extend_from_slice(&xor.to_le_bytes());
            prev_val = val;
        }

        buf
    }

    /// Decode raw buffer back to points
    fn decode_raw_buffer(data: &[u8]) -> Result<Vec<DataPoint>, CodecError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut pos = 0;

        // Read point count
        let (count, bytes) = Self::read_varint(&data[pos..])?;
        pos += bytes;
        let count = count as usize;

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut points = Vec::with_capacity(count);

        // Read first timestamp
        if pos + 8 > data.len() {
            return Err(CodecError::DecompressionFailed(
                "Buffer too short for first timestamp".to_string(),
            ));
        }
        let first_ts = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mut timestamps = vec![first_ts];

        // Read delta-encoded timestamps
        let mut prev_ts = first_ts;
        for _ in 1..count {
            let (zigzag_delta, bytes) = Self::read_varint(&data[pos..])?;
            let delta = Self::zigzag_decode(zigzag_delta);
            prev_ts += delta;
            timestamps.push(prev_ts);
            pos += bytes;
        }

        // Read first value
        if pos + 8 > data.len() {
            return Err(CodecError::DecompressionFailed(
                "Buffer too short for first value".to_string(),
            ));
        }
        let first_val_bits = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mut values = vec![f64::from_bits(first_val_bits)];

        // Read XOR-encoded values
        let mut prev_val = first_val_bits;
        for _ in 1..count {
            if pos + 8 > data.len() {
                return Err(CodecError::DecompressionFailed(
                    "Buffer too short for value".to_string(),
                ));
            }
            let xor = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let val_bits = prev_val ^ xor;
            values.push(f64::from_bits(val_bits));
            prev_val = val_bits;
            pos += 8;
        }

        // Combine timestamps and values
        for (ts, val) in timestamps.into_iter().zip(values) {
            points.push(DataPoint::new(0, ts, val));
        }

        Ok(points)
    }
}

impl Default for DeltaLz4Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for DeltaLz4Codec {
    fn id(&self) -> CodecId {
        CodecId::DeltaLz4
    }

    fn compress(&self, points: &[DataPoint]) -> Result<Vec<u8>, CodecError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare raw delta-encoded buffer
        let raw = Self::prepare_raw_buffer(points);

        // LZ4 compress with prepended size
        let compressed = lz4_flex::compress_prepend_size(&raw);

        Ok(compressed)
    }

    fn decompress(&self, data: &[u8], _count: usize) -> Result<Vec<DataPoint>, CodecError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // LZ4 decompress
        let raw = lz4_flex::decompress_size_prepended(data).map_err(|e| {
            CodecError::DecompressionFailed(format!("LZ4 decompression failed: {}", e))
        })?;

        // Decode raw buffer
        Self::decode_raw_buffer(&raw)
    }

    fn estimate_bits(&self, profile: &ChunkProfile, _sample: &[DataPoint]) -> f64 {
        // LZ4 estimation based on profile characteristics
        //
        // LZ4 works best on data with repetitive patterns

        // Base raw size: ~128 bits per point (8 bytes ts + 8 bytes val)
        let raw_bits = 128.0;

        // Estimate compression ratio based on characteristics
        let estimated_ratio = if profile.xor_zero_ratio > 0.7 {
            // Many identical values -> great compression
            8.0
        } else if profile.xor_zero_ratio > 0.5 {
            // Good amount of repetition
            5.0
        } else if profile.is_smooth() {
            // Smooth data has repetitive XOR patterns
            4.0
        } else if profile.monotonic != crate::ahpac::profile::Monotonicity::NonMonotonic {
            // Monotonic data compresses reasonably
            3.5
        } else {
            // Random data: minimal compression
            2.0
        };

        // Timestamp overhead (varint encoded deltas are small)
        let ts_bits = if profile.autocorr[0] > 0.95 {
            8.0
        } else {
            16.0
        };

        (raw_bits / estimated_ratio) + (ts_bits / estimated_ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new(
                    0,
                    1000000 + i as i64 * 1000,
                    100.0 + (i as f64 * 0.1).sin() * 10.0,
                )
            })
            .collect()
    }

    fn create_constant_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, 42.0))
            .collect()
    }

    fn create_smooth_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, 100.0 + i as f64 * 0.001))
            .collect()
    }

    #[test]
    fn test_varint_roundtrip() {
        let test_values = [0u64, 1, 127, 128, 16383, 16384, u64::MAX / 2];

        for &val in &test_values {
            let mut buf = Vec::new();
            DeltaLz4Codec::write_varint(&mut buf, val);

            let (decoded, bytes_read) = DeltaLz4Codec::read_varint(&buf).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(bytes_read, buf.len());
        }
    }

    #[test]
    fn test_zigzag_roundtrip() {
        for n in [-1000i64, -1, 0, 1, 1000, i64::MIN / 2, i64::MAX / 2] {
            let encoded = DeltaLz4Codec::zigzag_encode(n);
            let decoded = DeltaLz4Codec::zigzag_decode(encoded);
            assert_eq!(decoded, n);
        }
    }

    #[test]
    fn test_compress_decompress_basic() {
        let codec = DeltaLz4Codec::new();
        let points = create_test_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_compress_decompress_constant() {
        let codec = DeltaLz4Codec::new();
        let points = create_constant_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert_eq!(orig.value, dec.value);
        }

        // Constant data should compress very well with LZ4
        let bits_per_sample = (compressed.len() * 8) as f64 / points.len() as f64;
        println!("Constant data with LZ4: {:.2} bits/sample", bits_per_sample);
    }

    #[test]
    fn test_compress_decompress_smooth() {
        let codec = DeltaLz4Codec::new();
        let points = create_smooth_points(1000);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }

        let bits_per_sample = (compressed.len() * 8) as f64 / points.len() as f64;
        println!(
            "Smooth data (1000 points) with LZ4: {:.2} bits/sample",
            bits_per_sample
        );
    }

    #[test]
    fn test_compress_empty() {
        let codec = DeltaLz4Codec::new();
        let compressed = codec.compress(&[]).unwrap();
        assert!(compressed.is_empty());
    }

    #[test]
    fn test_compress_single_point() {
        let codec = DeltaLz4Codec::new();
        let points = vec![DataPoint::new(0, 1000000, 42.0)];

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, 1).unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000000);
        assert_eq!(decompressed[0].value, 42.0);
    }

    #[test]
    fn test_codec_id() {
        let codec = DeltaLz4Codec::new();
        assert_eq!(codec.id(), CodecId::DeltaLz4);
        assert_eq!(codec.name(), "delta_lz4");
    }

    #[test]
    fn test_estimate_bits() {
        let codec = DeltaLz4Codec::new();
        let points = create_constant_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let estimate = codec.estimate_bits(&profile, &points);
        assert!(estimate > 0.0);
        assert!(estimate < 100.0);
    }

    #[test]
    fn test_with_level() {
        let codec = DeltaLz4Codec::with_level(9);
        let points = create_test_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
    }
}
