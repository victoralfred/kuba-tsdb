//! Chimp codec implementation for AHPAC
//!
//! Chimp is an improved XOR-based compression algorithm that builds on Gorilla/Kuba.
//! The key improvement is better handling of leading zeros by using prediction
//! and a more compact encoding scheme.
//!
//! # Algorithm
//!
//! Chimp improves on Kuba in several ways:
//! 1. Uses previous leading zeros count to predict current count
//! 2. More efficient encoding of trailing zeros
//! 3. Better handling of slowly drifting values
//!
//! # Reference
//!
//! Panagiotis Liakos, Katia Papakonstantinopoulou, and Yannis Kotidis.
//! "Chimp: Efficient Lossless Floating Point Compression for Time Series Databases."
//! VLDB 2022.

use super::{Codec, CodecError, CodecId};
use crate::ahpac::profile::ChunkProfile;
use crate::compression::bit_stream::{BitReader, BitWriter};
use crate::types::DataPoint;

/// Chimp codec with improved XOR encoding
///
/// Chimp provides better compression than Kuba for many real-world datasets
/// by exploiting patterns in how floating point values change over time.
pub struct ChimpCodec {
    /// Size of leading zeros lookup table (default: 128)
    /// Reserved for future optimization
    #[allow(dead_code)]
    leading_zeros_table_size: usize,
}

impl ChimpCodec {
    /// Create a new Chimp codec with default settings
    pub fn new() -> Self {
        Self {
            leading_zeros_table_size: 128,
        }
    }

    /// Compress timestamps using delta-of-delta encoding (same as Kuba)
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
            writer.write_bit(false);
        } else if (-63..64).contains(&dod) {
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits((dod + 63) as u64, 7);
        } else if (-255..256).contains(&dod) {
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits((dod + 255) as u64, 9);
        } else if (-2047..2048).contains(&dod) {
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits((dod + 2047) as u64, 12);
        } else {
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

        let first_ts = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first timestamp: {}", e))
        })? as i64;
        timestamps.push(first_ts);

        if count == 1 {
            return Ok(timestamps);
        }

        let first_delta = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first delta: {}", e))
        })? as i64;
        timestamps.push(first_ts + first_delta);

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
        if !reader
            .read_bit()
            .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
        {
            return Ok(0);
        }

        if !reader
            .read_bit()
            .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
        {
            let val = reader
                .read_bits(7)
                .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
            return Ok(val as i64 - 63);
        }

        if !reader
            .read_bit()
            .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
        {
            let val = reader
                .read_bits(9)
                .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
            return Ok(val as i64 - 255);
        }

        if !reader
            .read_bit()
            .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
        {
            let val = reader
                .read_bits(12)
                .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
            return Ok(val as i64 - 2047);
        }

        let val = reader
            .read_bits(32)
            .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
        Ok(val as i32 as i64)
    }

    /// Compress values using Chimp's improved XOR encoding
    ///
    /// Chimp encoding scheme:
    /// - If XOR = 0: Write '0' (1 bit)
    /// - If leading zeros match prediction: Write '10' + trailing info + data
    /// - Otherwise: Write '11' + leading (6 bits) + trailing (6 bits) + data
    fn compress_values(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // First value: full 64 bits
        writer.write_bits(points[0].value.to_bits(), 64);

        let mut prev_value = points[0].value.to_bits();
        let mut prev_leading = 64u8; // Start with max (no prediction)

        for point in &points[1..] {
            let value_bits = point.value.to_bits();
            let xor = prev_value ^ value_bits;

            if xor == 0 {
                // Same as previous: single 0 bit
                writer.write_bit(false);
            } else {
                writer.write_bit(true);

                let leading = xor.leading_zeros() as u8;
                let trailing = xor.trailing_zeros() as u8;
                let meaningful_bits = 64 - leading - trailing;

                // Chimp optimization: predict leading zeros from previous
                if leading == prev_leading {
                    // Prediction matched: '0' + trailing (6 bits) + data
                    writer.write_bit(false);
                    writer.write_bits(trailing as u64, 6);
                    writer.write_bits(xor >> trailing, meaningful_bits);
                } else {
                    // Prediction failed: '1' + leading (6 bits) + trailing (6 bits) + data
                    writer.write_bit(true);
                    writer.write_bits(leading as u64, 6);
                    writer.write_bits(trailing as u64, 6);
                    writer.write_bits(xor >> trailing, meaningful_bits);
                }

                prev_leading = leading;
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

        let first_bits = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first value: {}", e))
        })?;
        values.push(f64::from_bits(first_bits));

        let mut prev_value = first_bits;
        let mut prev_leading = 64u8;

        for _ in 1..count {
            if !reader
                .read_bit()
                .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
            {
                // Same as previous
                values.push(f64::from_bits(prev_value));
                continue;
            }

            let xor = if !reader
                .read_bit()
                .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
            {
                // Prediction matched: read trailing + data
                let trailing = reader
                    .read_bits(6)
                    .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
                    as u8;
                let meaningful = 64 - prev_leading - trailing;
                let data = reader
                    .read_bits(meaningful)
                    .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
                data << trailing
            } else {
                // Prediction failed: read leading + trailing + data
                let leading = reader
                    .read_bits(6)
                    .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
                    as u8;
                let trailing = reader
                    .read_bits(6)
                    .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?
                    as u8;
                let meaningful = 64 - leading - trailing;
                let data = reader
                    .read_bits(meaningful)
                    .map_err(|e| CodecError::DecompressionFailed(e.to_string()))?;
                prev_leading = leading;
                data << trailing
            };

            let value_bits = prev_value ^ xor;
            values.push(f64::from_bits(value_bits));
            prev_value = value_bits;
        }

        Ok(values)
    }
}

impl Default for ChimpCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for ChimpCodec {
    fn id(&self) -> CodecId {
        CodecId::Chimp
    }

    fn compress(&self, points: &[DataPoint]) -> Result<Vec<u8>, CodecError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let mut writer = BitWriter::new();

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

        let timestamps = Self::decompress_timestamps(&mut reader, count)?;
        let values = Self::decompress_values(&mut reader, count)?;

        let points = timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(0, ts, val))
            .collect();

        Ok(points)
    }

    fn estimate_bits(&self, profile: &ChunkProfile, _sample: &[DataPoint]) -> f64 {
        // Chimp estimation
        // Generally better than Kuba for slowly varying data

        // Timestamp cost (same as Kuba)
        let ts_bits = if profile.autocorr[0] > 0.95 { 1.5 } else { 4.0 };

        // Value cost: Chimp tends to be ~10-20% better than Kuba
        let val_bits = if profile.xor_zero_ratio > 0.5 {
            // Many identical values
            1.0 + (1.0 - profile.xor_zero_ratio) * 16.0
        } else if profile.is_smooth() {
            // Smooth data: leading zeros prediction works well
            1.0 + (1.0 - profile.xor_zero_ratio) * 18.0
        } else {
            // Mixed data
            1.0 + (1.0 - profile.xor_zero_ratio) * 22.0
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

    fn create_slowly_changing_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, 100.0 + i as f64 * 0.001))
            .collect()
    }

    #[test]
    fn test_compress_decompress_basic() {
        let codec = ChimpCodec::new();
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
        let codec = ChimpCodec::new();
        let points = create_constant_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert_eq!(orig.value, dec.value);
        }

        // Constant values should compress very well
        let bits_per_sample = (compressed.len() * 8) as f64 / points.len() as f64;
        assert!(
            bits_per_sample < 5.0,
            "Expected < 5 bits/sample for constant data, got {}",
            bits_per_sample
        );
    }

    #[test]
    fn test_compress_slowly_changing() {
        let codec = ChimpCodec::new();
        let points = create_slowly_changing_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_compress_empty() {
        let codec = ChimpCodec::new();
        let compressed = codec.compress(&[]).unwrap();
        assert!(compressed.is_empty());
    }

    #[test]
    fn test_compress_single_point() {
        let codec = ChimpCodec::new();
        let points = vec![DataPoint::new(0, 1000000, 42.0)];

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, 1).unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000000);
        assert_eq!(decompressed[0].value, 42.0);
    }

    #[test]
    fn test_estimate_bits() {
        let codec = ChimpCodec::new();
        let points = create_constant_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let estimate = codec.estimate_bits(&profile, &points);
        assert!(estimate > 0.0);
        assert!(estimate < 50.0);
    }

    #[test]
    fn test_codec_id() {
        let codec = ChimpCodec::new();
        assert_eq!(codec.id(), CodecId::Chimp);
        assert_eq!(codec.name(), "chimp");
    }

    #[test]
    fn test_chimp_vs_kuba_slowly_changing() {
        // Chimp should generally be better or equal to Kuba for slowly changing data
        use crate::ahpac::codecs::kuba::KubaCodec;

        let chimp = ChimpCodec::new();
        let kuba = KubaCodec::new();
        let points = create_slowly_changing_points(1000);

        let chimp_compressed = chimp.compress(&points).unwrap();
        let kuba_compressed = kuba.compress(&points).unwrap();

        let chimp_bps = (chimp_compressed.len() * 8) as f64 / points.len() as f64;
        let kuba_bps = (kuba_compressed.len() * 8) as f64 / points.len() as f64;

        println!("Slowly changing data (1000 points):");
        println!("  Chimp: {:.2} bits/sample", chimp_bps);
        println!("  Kuba:  {:.2} bits/sample", kuba_bps);

        // Chimp should be within 20% of Kuba (could be better or slightly worse)
        assert!(
            chimp_bps < kuba_bps * 1.2,
            "Chimp should not be significantly worse than Kuba"
        );
    }
}
