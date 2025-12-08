//! Kuba codec wrapper for AHPAC
//!
//! This module wraps the existing KubaCompressor to implement the AHPAC Codec trait.
//! The Kuba algorithm (based on Facebook's Gorilla) uses delta-of-delta encoding for
//! timestamps and XOR encoding for values.

use super::{Codec, CodecError, CodecId};
use crate::ahpac::profile::ChunkProfile;
use crate::compression::bit_stream::{BitReader, BitWriter};
use crate::types::DataPoint;

/// Kuba codec implementation for AHPAC
///
/// Kuba uses delta-of-delta encoding for timestamps and XOR-based encoding
/// for values. It excels when:
/// - Timestamps have regular intervals
/// - Values change slowly or repeat frequently
pub struct KubaCodec;

impl KubaCodec {
    /// Create a new Kuba codec instance
    pub fn new() -> Self {
        Self
    }

    /// Compress timestamps using delta-of-delta encoding
    ///
    /// The encoding scheme:
    /// - dod = 0: Write '0' (1 bit)
    /// - dod in [-63, 64): Write '10' + 7 bits (9 bits)
    /// - dod in [-255, 256): Write '110' + 9 bits (12 bits)
    /// - dod in [-2047, 2048): Write '1110' + 12 bits (16 bits)
    /// - Otherwise: Write '1111' + 32 bits (36 bits)
    fn compress_timestamps(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // First timestamp: full 64 bits
        writer.write_bits(points[0].timestamp as u64, 64);

        if points.len() == 1 {
            return;
        }

        // Second timestamp: delta from first (64 bits)
        let first_delta = points[1].timestamp - points[0].timestamp;
        writer.write_bits(first_delta as u64, 64);

        if points.len() == 2 {
            return;
        }

        // Remaining timestamps: delta-of-delta with variable encoding
        let mut prev_ts = points[1].timestamp;
        let mut prev_delta = first_delta;

        for point in &points[2..] {
            let delta = point.timestamp - prev_ts;
            let dod = delta - prev_delta;

            Self::encode_dod(writer, dod);

            prev_ts = point.timestamp;
            prev_delta = delta;
        }
    }

    /// Encode delta-of-delta with variable bit packing
    fn encode_dod(writer: &mut BitWriter, dod: i64) {
        if dod == 0 {
            // Single 0 bit
            writer.write_bit(false);
        } else if (-63..64).contains(&dod) {
            // '10' + 7 bits
            writer.write_bit(true);
            writer.write_bit(false);
            // Convert to unsigned with bias
            writer.write_bits((dod + 63) as u64, 7);
        } else if (-255..256).contains(&dod) {
            // '110' + 9 bits
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits((dod + 255) as u64, 9);
        } else if (-2047..2048).contains(&dod) {
            // '1110' + 12 bits
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits((dod + 2047) as u64, 12);
        } else {
            // '1111' + 32 bits
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bits(dod as u64, 32);
        }
    }

    /// Decompress timestamps
    fn decompress_timestamps(reader: &mut BitReader, count: usize) -> Result<Vec<i64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut timestamps = Vec::with_capacity(count);

        // First timestamp
        let first_ts = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first timestamp: {}", e))
        })? as i64;
        timestamps.push(first_ts);

        if count == 1 {
            return Ok(timestamps);
        }

        // Second timestamp (delta)
        let first_delta = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first delta: {}", e))
        })? as i64;
        timestamps.push(first_ts + first_delta);

        if count == 2 {
            return Ok(timestamps);
        }

        // Remaining timestamps
        let mut prev_ts = timestamps[1];
        let mut prev_delta = first_delta;

        for _ in 2..count {
            let dod = Self::decode_dod(reader)?;
            let delta = prev_delta + dod;
            let ts = prev_ts + delta;
            timestamps.push(ts);
            prev_ts = ts;
            prev_delta = delta;
        }

        Ok(timestamps)
    }

    /// Decode delta-of-delta
    fn decode_dod(reader: &mut BitReader) -> Result<i64, CodecError> {
        let first_bit = reader.read_bit().map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read dod bit: {}", e))
        })?;

        if !first_bit {
            // dod = 0
            return Ok(0);
        }

        let second_bit = reader.read_bit().map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read dod bit: {}", e))
        })?;

        if !second_bit {
            // 7-bit value
            let val = reader.read_bits(7).map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read 7-bit dod: {}", e))
            })?;
            return Ok(val as i64 - 63);
        }

        let third_bit = reader.read_bit().map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read dod bit: {}", e))
        })?;

        if !third_bit {
            // 9-bit value
            let val = reader.read_bits(9).map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read 9-bit dod: {}", e))
            })?;
            return Ok(val as i64 - 255);
        }

        let fourth_bit = reader.read_bit().map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read dod bit: {}", e))
        })?;

        if !fourth_bit {
            // 12-bit value
            let val = reader.read_bits(12).map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read 12-bit dod: {}", e))
            })?;
            return Ok(val as i64 - 2047);
        }

        // 32-bit value (signed)
        let val = reader.read_bits(32).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read 32-bit dod: {}", e))
        })?;
        // Sign-extend from 32 bits
        Ok(val as i32 as i64)
    }

    /// Compress values using XOR encoding
    ///
    /// Uses the Gorilla algorithm with fixes for edge cases:
    /// - Leading zeros clamped to 31 (5-bit limit)
    /// - Meaningful bits stored as (value - 1) to fit 64 in 6 bits
    fn compress_values(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // First value: full 64 bits
        writer.write_bits(points[0].value.to_bits(), 64);

        let mut prev_value = points[0].value.to_bits();
        let mut prev_leading = 255u8; // Invalid initial value to force first new window
        let mut prev_trailing = 0u8;

        for point in &points[1..] {
            let value_bits = point.value.to_bits();
            let xor = prev_value ^ value_bits;

            if xor == 0 {
                // Same as previous: single 0 bit
                writer.write_bit(false);
            } else {
                writer.write_bit(true);

                let actual_leading = xor.leading_zeros() as u8;
                let trailing = xor.trailing_zeros() as u8;

                // Clamp leading to 31 (5-bit max) - this expands the "meaningful" region
                // to include some extra leading zeros, but decoding still works correctly
                let leading = actual_leading.min(31);

                // Meaningful bits: 1-64 range (since XOR is non-zero, at least 1 bit is set)
                // With clamped leading: meaningful = 64 - clamped_leading - trailing
                // Use saturating_sub to prevent underflow
                let meaningful_bits = 64u8.saturating_sub(leading).saturating_sub(trailing).max(1);

                // Check if we can reuse previous leading/trailing info
                // Use actual_leading for comparison to ensure the XOR fits in the window
                if actual_leading >= prev_leading && trailing >= prev_trailing {
                    // Control bit '0': reuse previous window
                    writer.write_bit(false);
                    let prev_meaningful = 64 - prev_leading - prev_trailing;
                    writer.write_bits(xor >> prev_trailing, prev_meaningful);
                } else {
                    // Control bit '1': new window
                    writer.write_bit(true);
                    writer.write_bits(leading as u64, 5);
                    // Store (meaningful_bits - 1) so that 64 fits in 6 bits (stored as 63)
                    // meaningful_bits is always >= 1 for non-zero XOR
                    writer.write_bits((meaningful_bits - 1) as u64, 6);
                    writer.write_bits(xor >> trailing, meaningful_bits);
                    prev_leading = leading;
                    prev_trailing = trailing;
                }
            }

            prev_value = value_bits;
        }
    }

    /// Decompress values
    fn decompress_values(reader: &mut BitReader, count: usize) -> Result<Vec<f64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut values = Vec::with_capacity(count);

        // First value
        let first_bits = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first value: {}", e))
        })?;
        values.push(f64::from_bits(first_bits));

        let mut prev_value = first_bits;
        let mut prev_leading = 0u8;
        let mut prev_meaningful = 64u8;

        for _ in 1..count {
            let first_bit = reader.read_bit().map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read value bit: {}", e))
            })?;

            if !first_bit {
                // Same as previous
                values.push(f64::from_bits(prev_value));
                continue;
            }

            let control_bit = reader.read_bit().map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read control bit: {}", e))
            })?;

            let xor = if !control_bit {
                // Reuse previous window
                let meaningful_data = reader.read_bits(prev_meaningful).map_err(|e| {
                    CodecError::DecompressionFailed(format!(
                        "Failed to read meaningful bits: {}",
                        e
                    ))
                })?;
                let trailing = 64 - prev_leading - prev_meaningful;
                meaningful_data << trailing
            } else {
                // New window
                let leading = reader.read_bits(5).map_err(|e| {
                    CodecError::DecompressionFailed(format!("Failed to read leading: {}", e))
                })? as u8;
                // Encoder stores (meaningful - 1) so that 64 fits in 6 bits as 63
                // We read and add 1 to recover the original meaningful value (1-64)
                let meaningful_minus_one = reader.read_bits(6).map_err(|e| {
                    CodecError::DecompressionFailed(format!("Failed to read meaningful: {}", e))
                })? as u8;
                let meaningful = meaningful_minus_one + 1;
                let meaningful_data = reader.read_bits(meaningful).map_err(|e| {
                    CodecError::DecompressionFailed(format!("Failed to read value data: {}", e))
                })?;
                // Use saturating subtraction to handle edge case where meaningful = 64
                let trailing = 64_u8.saturating_sub(leading).saturating_sub(meaningful);
                prev_leading = leading;
                prev_meaningful = meaningful;
                meaningful_data << trailing
            };

            let value_bits = prev_value ^ xor;
            values.push(f64::from_bits(value_bits));
            prev_value = value_bits;
        }

        Ok(values)
    }
}

impl Default for KubaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for KubaCodec {
    fn id(&self) -> CodecId {
        CodecId::Kuba
    }

    fn compress(&self, points: &[DataPoint]) -> Result<Vec<u8>, CodecError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let mut writer = BitWriter::new();

        // Compress timestamps and values
        Self::compress_timestamps(points, &mut writer);
        Self::compress_values(points, &mut writer);

        writer
            .finish()
            .map_err(|e| CodecError::CompressionFailed(e.to_string()))
    }

    fn decompress(&self, data: &[u8], count: usize) -> Result<Vec<DataPoint>, CodecError> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(data);

        // Decompress timestamps and values
        let timestamps = Self::decompress_timestamps(&mut reader, count)?;
        let values = Self::decompress_values(&mut reader, count)?;

        // Combine into data points (series_id = 0 for decompressed data)
        let points = timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(0, ts, val))
            .collect();

        Ok(points)
    }

    fn estimate_bits(&self, profile: &ChunkProfile, _sample: &[DataPoint]) -> f64 {
        // Kuba estimation based on profile characteristics
        //
        // Timestamp cost: ~2 bits for regular intervals (dod=0)
        // Value cost depends on XOR pattern

        // Base timestamp cost
        let ts_bits = if profile.autocorr[0] > 0.95 {
            1.5 // Very regular timestamps
        } else {
            4.0 // Less regular
        };

        // Value cost based on XOR zero ratio
        let val_bits = if profile.xor_zero_ratio > 0.5 {
            // Many identical values: ~1 bit per sample
            1.0 + (1.0 - profile.xor_zero_ratio) * 20.0
        } else {
            // Mixed values: ~15-30 bits per non-zero XOR
            1.0 + (1.0 - profile.xor_zero_ratio) * 25.0
        };

        ts_bits + val_bits
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

    #[test]
    fn test_compress_decompress_basic() {
        let codec = KubaCodec::new();
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
        let codec = KubaCodec::new();
        let points = create_constant_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert_eq!(orig.value, dec.value);
        }

        // Constant values should compress very well
        // ~64 bits for first value, ~1 bit per remaining value
        let bits_per_sample = (compressed.len() * 8) as f64 / points.len() as f64;
        assert!(
            bits_per_sample < 5.0,
            "Expected < 5 bits/sample for constant data, got {}",
            bits_per_sample
        );
    }

    #[test]
    fn test_compress_empty() {
        let codec = KubaCodec::new();
        let compressed = codec.compress(&[]).unwrap();
        assert!(compressed.is_empty());
    }

    #[test]
    fn test_compress_single_point() {
        let codec = KubaCodec::new();
        let points = vec![DataPoint::new(0, 1000000, 42.0)];

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, 1).unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000000);
        assert_eq!(decompressed[0].value, 42.0);
    }

    #[test]
    fn test_compress_two_points() {
        let codec = KubaCodec::new();
        let points = vec![
            DataPoint::new(0, 1000000, 10.0),
            DataPoint::new(0, 1001000, 20.0),
        ];

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, 2).unwrap();

        assert_eq!(decompressed.len(), 2);
        assert_eq!(decompressed[0], points[0]);
        assert_eq!(decompressed[1], points[1]);
    }

    #[test]
    fn test_estimate_bits() {
        let codec = KubaCodec::new();
        let points = create_constant_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let estimate = codec.estimate_bits(&profile, &points);
        assert!(estimate > 0.0);
        assert!(estimate < 50.0); // Should be reasonable
    }

    #[test]
    fn test_codec_id() {
        let codec = KubaCodec::new();
        assert_eq!(codec.id(), CodecId::Kuba);
        assert_eq!(codec.name(), "kuba");
    }

    /// Test with random-like data that produces XOR values with many meaningful bits
    /// This tests the edge case where meaningful_bits = 64 (no leading or trailing zeros)
    #[test]
    fn test_compress_decompress_random_data() {
        let codec = KubaCodec::new();

        // Generate random-like data that will produce high-entropy XOR values
        let points: Vec<DataPoint> = (0..100)
            .map(|i| {
                let x = i as f64;
                let value = (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0;
                DataPoint::new(0, 1000000 + i as i64 * 1000, value)
            })
            .collect();

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len(), "Point count mismatch");
        for (i, (orig, dec)) in points.iter().zip(decompressed.iter()).enumerate() {
            assert_eq!(
                orig.timestamp, dec.timestamp,
                "Timestamp mismatch at index {}: {} != {}",
                i, orig.timestamp, dec.timestamp
            );
            assert!(
                (orig.value - dec.value).abs() < 1e-10,
                "Value mismatch at index {}: {} != {}",
                i,
                orig.value,
                dec.value
            );
        }
    }

    /// Test with extreme values that produce XOR with all 64 bits set
    #[test]
    fn test_compress_decompress_extreme_xor() {
        let codec = KubaCodec::new();

        // Values that maximize XOR differences (bit patterns with no common bits)
        let points = vec![
            DataPoint::new(0, 1000000, f64::from_bits(0xAAAAAAAAAAAAAAAA)),
            DataPoint::new(0, 1001000, f64::from_bits(0x5555555555555555)),
            DataPoint::new(0, 1002000, f64::from_bits(0xFFFFFFFFFFFFFFFF)),
            DataPoint::new(0, 1003000, f64::from_bits(0x0000000000000000)),
            DataPoint::new(0, 1004000, f64::from_bits(0x123456789ABCDEF0)),
        ];

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (i, (orig, dec)) in points.iter().zip(decompressed.iter()).enumerate() {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert_eq!(
                orig.value.to_bits(),
                dec.value.to_bits(),
                "Value bits mismatch at index {}: {:016x} != {:016x}",
                i,
                orig.value.to_bits(),
                dec.value.to_bits()
            );
        }
    }
}
