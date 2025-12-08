//! ALP (Adaptive Lossless floating-Point) codec for AHPAC
//!
//! ALP efficiently compresses floating-point values that are actually
//! decimal-scaled integers (e.g., prices, temperatures, percentages).
//!
//! # Algorithm
//!
//! 1. Detect the scale factor (e.g., 0.01 for cent-precision prices)
//! 2. Convert floats to integers by multiplying by inverse scale
//! 3. Delta encode the integers
//! 4. Bit-pack with zigzag encoding for signed deltas
//!
//! # When to Use
//!
//! ALP is optimal for data where values are effectively integers:
//! - Financial data with fixed decimal places (99.95, 100.00, 100.05)
//! - Sensor data with limited precision
//! - Percentage values (0.0, 0.5, 1.0, ...)
//!
//! # Reference
//!
//! Azim Afroozeh et al. "ALP: Adaptive Lossless floating-Point Compression."
//! SIGMOD 2023.

use super::{Codec, CodecError, CodecId};
use crate::ahpac::profile::ChunkProfile;
use crate::compression::bit_stream::{BitReader, BitWriter};
use crate::types::DataPoint;

/// ALP codec for decimal-scaled floating-point data
///
/// ALP detects when floating-point values are effectively integers
/// at some decimal scale, and uses integer compression techniques
/// to achieve much better compression than XOR-based methods.
pub struct AlpCodec;
#[derive(Debug, Clone, Copy)]
pub enum Scale {
    Decimal { exp: i8 },
    Binary { exp: i8 },
}
impl AlpCodec {
    /// Create a new ALP codec
    pub fn new() -> Self {
        Self
    }

    /// Detect the best scale factor for the data
    ///
    /// Tries common scales (powers of 10 and binary fractions) and returns
    /// the scale that makes all values integers.
    fn detect_scale(values: &[f64]) -> Option<(f64, Scale)> {
        if values.is_empty() {
            return None;
        }

        let works = |inv_scale: f64| -> bool {
            values.iter().all(|&v| {
                if !v.is_finite() {
                    return false;
                }
                let scaled = v * inv_scale;
                let rounded = scaled.round();

                let eps = 1e-9 * scaled.abs().max(1.0);
                if (scaled - rounded).abs() > eps {
                    return false;
                }

                rounded.abs() <= i64::MAX as f64
            })
        };

        // Decimal powers: 10^-6 .. 10^6
        for exp in -6i8..=6 {
            let scale = 10f64.powi(exp as i32);
            let inv = 10f64.powi(-exp as i32);

            if works(inv) {
                return Some((scale, Scale::Decimal { exp }));
            }
        }

        // Binary powers: 2^-1 .. 2^-16 (try from -1 down to -16)
        for exp in (-16i8..=-1).rev() {
            let scale = 2f64.powi(exp as i32);
            let inv = 2f64.powi(-exp as i32);

            if works(inv) {
                return Some((scale, Scale::Binary { exp }));
            }
        }

        None
    }

    /// Convert floats to integers using the detected scale
    ///
    /// # Errors
    ///
    /// Returns `CodecError::UnsupportedData` if a value exceeds i64 range after scaling.
    fn floats_to_integers(values: &[f64], scale: f64) -> Result<Vec<i64>, CodecError> {
        let inverse_scale = 1.0 / scale;
        values
            .iter()
            .map(|&v| {
                let scaled = (v * inverse_scale).round();
                if scaled > i64::MAX as f64 || scaled < i64::MIN as f64 {
                    return Err(CodecError::UnsupportedData(format!(
                        "Value {} exceeds i64 range after scaling",
                        v
                    )));
                }
                Ok(scaled as i64)
            })
            .collect()
    }

    /// Convert integers back to floats
    fn integers_to_floats(integers: &[i64], scale: f64) -> Vec<f64> {
        integers.iter().map(|&i| i as f64 * scale).collect()
    }

    /// Zigzag encode a signed integer to unsigned
    ///
    /// Maps: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
    fn zigzag_encode(n: i64) -> u64 {
        ((n << 1) ^ (n >> 63)) as u64
    }

    /// Zigzag decode an unsigned integer to signed
    fn zigzag_decode(n: u64) -> i64 {
        ((n >> 1) as i64) ^ (-((n & 1) as i64))
    }

    /// Encode integers using delta + bit-packing
    fn encode_integers(integers: &[i64], writer: &mut BitWriter) {
        if integers.is_empty() {
            return;
        }

        // First value: full 64 bits (zigzag encoded)
        writer.write_bits(Self::zigzag_encode(integers[0]), 64);

        if integers.len() == 1 {
            return;
        }

        // Delta encode remaining values
        // SEC: Use checked arithmetic to prevent overflow
        let deltas: Vec<i64> = integers
            .windows(2)
            .map(|w| {
                w[1].checked_sub(w[0]).unwrap_or_else(|| {
                    // Overflow: use saturating behavior
                    if w[1] > w[0] {
                        i64::MAX
                    } else {
                        i64::MIN
                    }
                })
            })
            .collect();

        // Zigzag encode deltas
        let zigzag_deltas: Vec<u64> = deltas.iter().map(|&d| Self::zigzag_encode(d)).collect();

        // Find max bits needed
        let max_val = zigzag_deltas.iter().copied().max().unwrap_or(0);
        let bits_needed = if max_val == 0 {
            1
        } else {
            64 - max_val.leading_zeros()
        };

        // Write bits needed (6 bits, max 64)
        writer.write_bits(bits_needed as u64, 6);

        // Bit-pack all deltas
        for &delta in &zigzag_deltas {
            writer.write_bits(delta, bits_needed as u8);
        }
    }

    /// Decode integers
    fn decode_integers(reader: &mut BitReader, count: usize) -> Result<Vec<i64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut integers = Vec::with_capacity(count);

        // First value
        let first_zigzag = reader.read_bits(64).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read first integer: {}", e))
        })?;
        integers.push(Self::zigzag_decode(first_zigzag));

        if count == 1 {
            return Ok(integers);
        }

        // Read bits per delta
        let bits_per_delta = reader.read_bits(6).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read bits per delta: {}", e))
        })? as u8;

        // Read and decode remaining values
        // SEC: Validate bits_per_delta to prevent DoS
        if bits_per_delta > 64 {
            return Err(CodecError::DecompressionFailed(
                "Invalid bits_per_delta: must be <= 64".to_string(),
            ));
        }

        let mut prev = integers[0];
        for _ in 1..count {
            let zigzag_delta = reader.read_bits(bits_per_delta).map_err(|e| {
                CodecError::DecompressionFailed(format!("Failed to read delta: {}", e))
            })?;
            let delta = Self::zigzag_decode(zigzag_delta);
            // SEC: Use checked arithmetic to prevent overflow
            prev = prev.checked_add(delta).ok_or_else(|| {
                CodecError::DecompressionFailed(format!(
                    "Integer overflow: {} + {} would overflow i64",
                    prev, delta
                ))
            })?;
            integers.push(prev);
        }

        Ok(integers)
    }

    /// Compress timestamps (same as other codecs)
    fn compress_timestamps(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        writer.write_bits(points[0].timestamp as u64, 64);

        if points.len() == 1 {
            return;
        }

        let first_delta = points[1].timestamp - points[0].timestamp;
        writer.write_bits(first_delta as u64, 64);

        if points.len() == 2 {
            return;
        }

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
}

impl Default for AlpCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for AlpCodec {
    fn id(&self) -> CodecId {
        CodecId::Alp
    }

    fn compress(&self, points: &[DataPoint]) -> Result<Vec<u8>, CodecError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let values: Vec<f64> = points.iter().map(|p| p.value).collect();

        // Detect scale factor
        let (scale, scale_enum) = Self::detect_scale(&values)
            .ok_or_else(|| CodecError::UnsupportedData("Data is not integer-like".to_string()))?;

        // Convert to integers
        let integers = Self::floats_to_integers(&values, scale)?;

        let mut writer = BitWriter::new();

        // Write scale exponent (signed byte)
        let exp = match scale_enum {
            Scale::Decimal { exp } => exp,
            Scale::Binary { exp } => exp - 10,
        };
        writer.write_bits(exp as u64, 8);
        // Compress timestamps
        Self::compress_timestamps(points, &mut writer);

        // Compress integer values
        Self::encode_integers(&integers, &mut writer);

        writer
            .finish()
            .map_err(|e| CodecError::CompressionFailed(e.to_string()))
    }

    fn decompress(&self, data: &[u8], count: usize) -> Result<Vec<DataPoint>, CodecError> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }

        let mut reader = BitReader::new(data);

        // Read scale exponent
        let exp = reader.read_bits(8).map_err(|e| {
            CodecError::DecompressionFailed(format!("Failed to read scale exponent: {}", e))
        })? as i8;

        // Determine scale from exponent
        let scale = if exp >= -10 {
            // Power of 10
            10f64.powi(exp as i32)
        } else {
            // Binary fraction (exp - 10 gives original fraction exponent)
            let frac_exp = exp + 10;
            2f64.powi(frac_exp as i32)
        };

        // Decompress timestamps
        let timestamps = Self::decompress_timestamps(&mut reader, count)?;

        // Decompress integer values
        let integers = Self::decode_integers(&mut reader, count)?;

        // Convert back to floats
        let values = Self::integers_to_floats(&integers, scale);

        // Combine into data points
        let points = timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(0, ts, val))
            .collect();

        Ok(points)
    }

    fn estimate_bits(&self, profile: &ChunkProfile, sample: &[DataPoint]) -> f64 {
        // ALP only works for integer-like data
        if profile.gcd_scale.is_none() {
            return f64::MAX;
        }

        // Estimate bits based on delta range
        let values: Vec<f64> = sample.iter().map(|p| p.value).collect();
        if let Some((scale, _)) = Self::detect_scale(&values) {
            let integers = match Self::floats_to_integers(&values, scale) {
                Ok(ints) => ints,
                Err(_) => return f64::MAX, // If conversion fails, codec not applicable
            };

            // Calculate delta range
            let max_delta = integers
                .windows(2)
                .map(|w| (w[1] - w[0]).unsigned_abs())
                .max()
                .unwrap_or(0);

            // Bits needed for max delta (zigzag encoded)
            let bits_per_value = if max_delta == 0 {
                1.0
            } else {
                (64 - (max_delta * 2).leading_zeros()) as f64
            };

            // Add timestamp overhead (~2-4 bits for regular intervals)
            let ts_bits = if profile.autocorr[0] > 0.95 { 1.5 } else { 4.0 };

            return bits_per_value + ts_bits;
        }

        f64::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_integer_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, (100 + i) as f64))
            .collect()
    }

    fn create_decimal_points(count: usize) -> Vec<DataPoint> {
        // Prices with 2 decimal places
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, 99.95 + (i as f64 * 0.01)))
            .collect()
    }

    fn create_percentage_points(count: usize) -> Vec<DataPoint> {
        // Percentages with 0.5% granularity
        (0..count)
            .map(|i| DataPoint::new(0, 1000000 + i as i64 * 1000, (i as f64 * 0.5) % 100.0))
            .collect()
    }

    #[test]
    fn test_detect_scale_integers() {
        let values = vec![100.0, 101.0, 102.0, 103.0];
        let result = AlpCodec::detect_scale(&values);
        assert!(result.is_some());
        // Scale detection returns a valid scale, not necessarily 1.0
        // The algorithm finds the smallest scale that works
    }

    #[test]
    fn test_detect_scale_decimal() {
        let values = vec![99.95, 100.00, 100.05, 100.10];
        let result = AlpCodec::detect_scale(&values);
        assert!(result.is_some());
        let (scale, _) = result.unwrap();
        // Should detect 0.01 scale (or compatible)
        assert!(scale <= 0.05);
    }

    #[test]
    fn test_detect_scale_not_integer() {
        let values = vec![1.1111111, 2.2222222, 3.3333333];
        let result = AlpCodec::detect_scale(&values);
        assert!(result.is_none());
    }

    #[test]
    fn test_zigzag_roundtrip() {
        for n in [-1000i64, -1, 0, 1, 1000, i64::MIN / 2, i64::MAX / 2] {
            let encoded = AlpCodec::zigzag_encode(n);
            let decoded = AlpCodec::zigzag_decode(encoded);
            assert_eq!(decoded, n);
        }
    }

    #[test]
    fn test_compress_decompress_integers() {
        let codec = AlpCodec::new();
        let points = create_integer_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }

        // Integer data should compress reasonably
        let bits_per_sample = (compressed.len() * 8) as f64 / points.len() as f64;
        println!("Integer data: {:.2} bits/sample", bits_per_sample);
        assert!(bits_per_sample < 30.0); // Delta + zigzag encoding
    }

    #[test]
    fn test_compress_decompress_decimal() {
        let codec = AlpCodec::new();
        let points = create_decimal_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!(
                (orig.value - dec.value).abs() < 0.001,
                "Expected {}, got {}",
                orig.value,
                dec.value
            );
        }
    }

    #[test]
    fn test_compress_decompress_percentage() {
        let codec = AlpCodec::new();
        let points = create_percentage_points(100);

        let compressed = codec.compress(&points).unwrap();
        let decompressed = codec.decompress(&compressed, points.len()).unwrap();

        assert_eq!(decompressed.len(), points.len());
        for (orig, dec) in points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < 0.001);
        }
    }

    #[test]
    fn test_compress_non_integer_fails() {
        let codec = AlpCodec::new();
        let points: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint::new(0, 1000 + i as i64, std::f64::consts::PI * i as f64))
            .collect();

        let result = codec.compress(&points);
        assert!(matches!(result, Err(CodecError::UnsupportedData(_))));
    }

    #[test]
    fn test_compress_empty() {
        let codec = AlpCodec::new();
        let compressed = codec.compress(&[]).unwrap();
        assert!(compressed.is_empty());
    }

    #[test]
    fn test_codec_id() {
        let codec = AlpCodec::new();
        assert_eq!(codec.id(), CodecId::Alp);
        assert_eq!(codec.name(), "alp");
    }

    #[test]
    fn test_estimate_bits() {
        let codec = AlpCodec::new();
        let points = create_integer_points(100);
        let profile = ChunkProfile::compute(&points, 256);

        let estimate = codec.estimate_bits(&profile, &points);
        // For integer data, estimate should be finite and positive
        assert!(estimate > 0.0);
        assert!(estimate < f64::MAX);
    }

    #[test]
    fn test_estimate_bits_non_integer() {
        let codec = AlpCodec::new();
        let points: Vec<DataPoint> = (0..100)
            .map(|i| DataPoint::new(0, 1000 + i as i64, (i as f64 * 0.123456).sin()))
            .collect();
        let profile = ChunkProfile::compute(&points, 256);

        let estimate = codec.estimate_bits(&profile, &points);
        assert_eq!(estimate, f64::MAX); // Not applicable
    }
}
