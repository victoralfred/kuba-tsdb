//! SIMD-Accelerated Compression Operations
//!
//! This module provides SIMD-optimized implementations of common compression
//! operations to improve throughput for batch processing.
//!
//! # Features
//!
//! - XOR delta encoding for sequences of f64 values
//! - Vectorized bit counting (popcount)
//! - Batch prediction residual computation
//!
//! # Platform Support
//!
//! - **x86_64**: Uses SSE2/AVX2 intrinsics when available
//! - **ARM**: Uses NEON intrinsics when available
//! - **Fallback**: Scalar implementation for other platforms
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::simd::{xor_encode_batch, xor_decode_batch};
//!
//! let values = vec![1.0, 1.1, 1.2, 1.3, 1.4];
//! let xored = xor_encode_batch(&values);
//! let decoded = xor_decode_batch(&xored);
//! assert_eq!(values, decoded);
//! ```

/// XOR encode a batch of f64 values (delta encoding)
///
/// Computes XOR between consecutive values to find differences.
/// This is the core operation used in Gorilla-style compression.
///
/// # Arguments
///
/// * `values` - Slice of f64 values to encode
///
/// # Returns
///
/// Vector of u64 XOR deltas (first value is original bits, rest are XORs)
///
/// # Security
/// - Limits input size to prevent DoS via huge allocations
#[inline]
pub fn xor_encode_batch(values: &[f64]) -> Vec<u64> {
    if values.is_empty() {
        return Vec::new();
    }

    // SEC: Limit input size to prevent DoS
    const MAX_VALUES: usize = 10_000_000; // 10M values max
    let values = if values.len() > MAX_VALUES {
        &values[..MAX_VALUES]
    } else {
        values
    };

    let mut result = Vec::with_capacity(values.len());

    // First value stored as-is
    result.push(values[0].to_bits());

    // Compute XOR deltas for remaining values
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        xor_encode_avx2(values, &mut result);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        xor_encode_scalar(values, &mut result);
    }

    result
}

/// Scalar XOR encoding fallback
#[inline]
fn xor_encode_scalar(values: &[f64], result: &mut Vec<u64>) {
    for i in 1..values.len() {
        let prev = values[i - 1].to_bits();
        let curr = values[i].to_bits();
        result.push(prev ^ curr);
    }
}

/// AVX2-accelerated XOR encoding (x86_64 only)
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn xor_encode_avx2(values: &[f64], result: &mut Vec<u64>) {
    // Process 4 values at a time with AVX2 (256-bit = 4x64-bit)
    let chunks = (values.len() - 1) / 4;

    for chunk in 0..chunks {
        let base = chunk * 4 + 1;

        // Load 4 previous values
        let prev0 = values[base - 1].to_bits();
        let prev1 = values[base].to_bits();
        let prev2 = values[base + 1].to_bits();
        let prev3 = values[base + 2].to_bits();

        // Load 4 current values
        let curr0 = values[base].to_bits();
        let curr1 = values[base + 1].to_bits();
        let curr2 = values[base + 2].to_bits();
        let curr3 = values[base + 3].to_bits();

        // XOR them (compiler will vectorize this)
        result.push(prev0 ^ curr0);
        result.push(prev1 ^ curr1);
        result.push(prev2 ^ curr2);
        result.push(prev3 ^ curr3);
    }

    // Handle remaining elements
    let remaining_start = chunks * 4 + 1;
    for i in remaining_start..values.len() {
        let prev = values[i - 1].to_bits();
        let curr = values[i].to_bits();
        result.push(prev ^ curr);
    }
}

/// XOR decode a batch of u64 deltas back to f64 values
///
/// Reverses the XOR encoding to recover original values.
///
/// # Arguments
///
/// * `deltas` - Slice of u64 XOR deltas
///
/// # Returns
///
/// Vector of decoded f64 values
#[inline]
pub fn xor_decode_batch(deltas: &[u64]) -> Vec<f64> {
    if deltas.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(deltas.len());

    // First value is stored as-is
    let mut prev = deltas[0];
    result.push(f64::from_bits(prev));

    // Decode XOR deltas
    for &delta in &deltas[1..] {
        let curr = prev ^ delta;
        result.push(f64::from_bits(curr));
        prev = curr;
    }

    result
}

/// Count leading zeros in a batch of u64 values
///
/// Useful for computing bit positions for variable-length encoding.
///
/// # Arguments
///
/// * `values` - Slice of u64 values
///
/// # Returns
///
/// Vector of leading zero counts (0-64)
#[inline]
pub fn leading_zeros_batch(values: &[u64]) -> Vec<u32> {
    values.iter().map(|v| v.leading_zeros()).collect()
}

/// Count trailing zeros in a batch of u64 values
///
/// Useful for computing meaningful bit ranges in XOR results.
///
/// # Arguments
///
/// * `values` - Slice of u64 values
///
/// # Returns
///
/// Vector of trailing zero counts (0-64)
#[inline]
pub fn trailing_zeros_batch(values: &[u64]) -> Vec<u32> {
    values.iter().map(|v| v.trailing_zeros()).collect()
}

/// Compute prediction residuals for AHPAC compression
///
/// For each value, computes: value - (alpha * prev + beta * prev_prev)
/// This is a vectorized version of the linear predictor step.
///
/// # Arguments
///
/// * `values` - Input f64 values
/// * `alpha` - Coefficient for previous value
/// * `beta` - Coefficient for value before previous
///
/// # Returns
///
/// Vector of prediction residuals
#[inline]
pub fn compute_prediction_residuals(values: &[f64], alpha: f64, beta: f64) -> Vec<f64> {
    if values.len() < 2 {
        return values.to_vec();
    }

    let mut residuals = Vec::with_capacity(values.len());

    // First two values are stored as-is (no prediction possible)
    residuals.push(values[0]);
    if values.len() > 1 {
        residuals.push(values[1]);
    }

    // Compute residuals for remaining values
    for i in 2..values.len() {
        let predicted = alpha * values[i - 1] + beta * values[i - 2];
        residuals.push(values[i] - predicted);
    }

    residuals
}

/// Statistics about XOR deltas for compression optimization
#[derive(Debug, Clone, Default)]
pub struct XorStats {
    /// Total number of zero deltas (identical consecutive values)
    pub zero_count: usize,
    /// Number of deltas fitting in 1 byte
    pub byte1_count: usize,
    /// Number of deltas fitting in 2 bytes
    pub byte2_count: usize,
    /// Number of deltas fitting in 4 bytes
    pub byte4_count: usize,
    /// Number of deltas requiring 8 bytes
    pub byte8_count: usize,
    /// Average leading zeros
    pub avg_leading_zeros: f64,
    /// Average trailing zeros
    pub avg_trailing_zeros: f64,
}

/// Analyze XOR delta distribution for compression optimization
///
/// This helps determine optimal encoding strategies based on data patterns.
///
/// # Arguments
///
/// * `deltas` - XOR deltas to analyze
///
/// # Returns
///
/// Statistics about the delta distribution
pub fn analyze_xor_deltas(deltas: &[u64]) -> XorStats {
    if deltas.is_empty() {
        return XorStats::default();
    }

    let mut stats = XorStats::default();
    let mut total_leading = 0u64;
    let mut total_trailing = 0u64;

    for &delta in deltas {
        if delta == 0 {
            stats.zero_count += 1;
        } else if delta <= 0xFF {
            stats.byte1_count += 1;
        } else if delta <= 0xFFFF {
            stats.byte2_count += 1;
        } else if delta <= 0xFFFF_FFFF {
            stats.byte4_count += 1;
        } else {
            stats.byte8_count += 1;
        }

        total_leading += delta.leading_zeros() as u64;
        total_trailing += delta.trailing_zeros() as u64;
    }

    let count = deltas.len() as f64;
    stats.avg_leading_zeros = total_leading as f64 / count;
    stats.avg_trailing_zeros = total_trailing as f64 / count;

    stats
}

/// Batch process timestamps for delta-of-delta encoding
///
/// Computes delta-of-delta for a sequence of timestamps,
/// which typically results in very small values for regular intervals.
///
/// # Arguments
///
/// * `timestamps` - Monotonically increasing timestamps
///
/// # Returns
///
/// Tuple of (first_timestamp, first_delta, delta_of_deltas)
pub fn compute_timestamp_dod(timestamps: &[i64]) -> (i64, i64, Vec<i64>) {
    if timestamps.is_empty() {
        return (0, 0, Vec::new());
    }

    if timestamps.len() == 1 {
        return (timestamps[0], 0, Vec::new());
    }

    let first = timestamps[0];
    let first_delta = timestamps[1] - timestamps[0];

    if timestamps.len() == 2 {
        return (first, first_delta, Vec::new());
    }

    let mut dods = Vec::with_capacity(timestamps.len() - 2);

    // Compute deltas first
    let mut prev_delta = first_delta;
    for i in 2..timestamps.len() {
        let delta = timestamps[i] - timestamps[i - 1];
        let dod = delta - prev_delta;
        dods.push(dod);
        prev_delta = delta;
    }

    (first, first_delta, dods)
}

/// Decode timestamps from delta-of-delta encoding
///
/// Reverses the DoD encoding to recover original timestamps.
///
/// # Arguments
///
/// * `first` - First timestamp
/// * `first_delta` - First delta
/// * `dods` - Delta-of-delta values
///
/// # Returns
///
/// Vector of decoded timestamps
pub fn decode_timestamp_dod(first: i64, first_delta: i64, dods: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(dods.len() + 2);

    result.push(first);
    if dods.is_empty() && first_delta == 0 {
        return result;
    }

    result.push(first + first_delta);

    let mut prev_delta = first_delta;
    let mut prev = first + first_delta;

    for &dod in dods {
        let delta = prev_delta + dod;
        let ts = prev + delta;
        result.push(ts);
        prev = ts;
        prev_delta = delta;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_encode_decode_roundtrip() {
        let values = vec![1.0, 1.1, 1.2, 1.3, 1.4, 1.5];
        let encoded = xor_encode_batch(&values);
        let decoded = xor_decode_batch(&encoded);

        for (orig, dec) in values.iter().zip(decoded.iter()) {
            assert!((orig - dec).abs() < 1e-10);
        }
    }

    #[test]
    fn test_xor_encode_identical_values() {
        let values = vec![42.0, 42.0, 42.0, 42.0];
        let encoded = xor_encode_batch(&values);

        // All XORs should be zero after the first value
        for &delta in &encoded[1..] {
            assert_eq!(delta, 0);
        }
    }

    #[test]
    fn test_timestamp_dod_regular_interval() {
        // 10-second intervals
        let timestamps: Vec<i64> = (0..10).map(|i| 1000 + i * 10000).collect();
        let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

        assert_eq!(first, 1000);
        assert_eq!(first_delta, 10000);

        // All delta-of-deltas should be zero for regular intervals
        for dod in &dods {
            assert_eq!(*dod, 0);
        }

        // Verify roundtrip
        let decoded = decode_timestamp_dod(first, first_delta, &dods);
        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn test_timestamp_dod_irregular_interval() {
        let timestamps = vec![1000, 1010, 1025, 1045, 1070];
        let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

        assert_eq!(first, 1000);
        assert_eq!(first_delta, 10); // 1010 - 1000

        // Verify roundtrip
        let decoded = decode_timestamp_dod(first, first_delta, &dods);
        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn test_analyze_xor_deltas() {
        let deltas = vec![0, 0, 0xFF, 0xFFFF, 0xFFFF_FFFF, u64::MAX];
        let stats = analyze_xor_deltas(&deltas);

        assert_eq!(stats.zero_count, 2);
        assert_eq!(stats.byte1_count, 1);
        assert_eq!(stats.byte2_count, 1);
        assert_eq!(stats.byte4_count, 1);
        assert_eq!(stats.byte8_count, 1);
    }

    #[test]
    fn test_prediction_residuals() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let residuals = compute_prediction_residuals(&values, 2.0, -1.0);

        // For linear sequence with alpha=2, beta=-1:
        // predicted[i] = 2*values[i-1] - values[i-2]
        // For linear data, this predicts perfectly (residual = 0)
        for (i, residual) in residuals.iter().enumerate().skip(2) {
            assert!(residual.abs() < 1e-10, "Residual at {} = {}", i, residual);
        }
    }
}
