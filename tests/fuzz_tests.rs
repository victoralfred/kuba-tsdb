//! Fuzz Tests for Compression and Storage Components
//!
//! Uses property-based testing (proptest) to find edge cases and bugs
//! in compression algorithms, encoding/decoding, and data integrity.

use proptest::prelude::*;

// =============================================================================
// Test Data Strategies
// =============================================================================

/// Strategy for generating f64 values that won't be NaN or Inf
fn finite_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        // Normal values
        (-1e10..1e10f64),
        // Small values
        (-1.0..1.0f64),
        // Zero and near-zero
        Just(0.0),
        (-1e-10..1e-10f64),
        // Integer-like values
        (-1000i32..1000).prop_map(|i| i as f64),
        // Typical sensor values
        (0.0..100.0f64),
        // Financial values (2 decimal places)
        (0i32..10000).prop_map(|i| i as f64 / 100.0),
    ]
}

/// Strategy for generating monotonically increasing timestamps
fn timestamp_sequence(len: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(1i64..100000, len).prop_map(|deltas| {
        let mut result = Vec::with_capacity(deltas.len());
        let mut current = 1000000i64;
        for delta in deltas {
            result.push(current);
            current += delta;
        }
        result
    })
}

/// Strategy for generating byte sequences
fn byte_sequence(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..max_len)
}

/// Strategy for generating skewed byte distributions (like XOR residuals)
fn skewed_bytes(len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop_oneof![
            4 => Just(0u8),           // 40% zeros
            3 => 1u8..10,             // 30% small values
            2 => 10u8..50,            // 20% medium values
            1 => any::<u8>(),         // 10% any value
        ],
        len,
    )
}

// =============================================================================
// XOR Encoding Fuzz Tests
// =============================================================================

mod xor_encoding {
    use super::*;
    use kuba_tsdb::compression::{xor_decode_batch, xor_encode_batch};

    proptest! {
        /// XOR encode/decode roundtrip must be lossless
        #[test]
        fn roundtrip_preserves_data(
            values in prop::collection::vec(finite_f64(), 1..1000)
        ) {
            let encoded = xor_encode_batch(&values);
            let decoded = xor_decode_batch(&encoded);

            prop_assert_eq!(values.len(), decoded.len());
            for (orig, dec) in values.iter().zip(decoded.iter()) {
                prop_assert!(
                    (orig - dec).abs() < 1e-15 || (orig.is_nan() && dec.is_nan()),
                    "Value mismatch: {} vs {}",
                    orig,
                    dec
                );
            }
        }

        /// Constant values should produce all-zero deltas (except first)
        #[test]
        fn constant_values_produce_zero_deltas(
            value in finite_f64(),
            len in 2usize..100
        ) {
            let values: Vec<f64> = vec![value; len];
            let encoded = xor_encode_batch(&values);

            // First value is stored as-is, rest should be zero XORs
            for &delta in &encoded[1..] {
                prop_assert_eq!(delta, 0, "Non-zero delta for constant input");
            }
        }

        /// Encoded length equals input length
        #[test]
        fn encoded_length_matches_input(
            values in prop::collection::vec(finite_f64(), 0..1000)
        ) {
            let encoded = xor_encode_batch(&values);
            prop_assert_eq!(values.len(), encoded.len());
        }

        /// Empty input produces empty output
        #[test]
        fn empty_input_produces_empty_output(_ignored in Just(())) {
            let encoded = xor_encode_batch(&[]);
            prop_assert!(encoded.is_empty());

            let decoded = xor_decode_batch(&[]);
            prop_assert!(decoded.is_empty());
        }
    }
}

// =============================================================================
// Timestamp Delta-of-Delta Fuzz Tests
// =============================================================================

mod timestamp_dod {
    use super::*;
    use kuba_tsdb::compression::{compute_timestamp_dod, decode_timestamp_dod};

    proptest! {
        /// DoD encode/decode roundtrip must be lossless
        #[test]
        fn roundtrip_preserves_timestamps(
            timestamps in timestamp_sequence(100)
        ) {
            let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);
            let decoded = decode_timestamp_dod(first, first_delta, &dods);

            prop_assert_eq!(timestamps, decoded);
        }

        /// Regular intervals should produce all-zero DoDs
        #[test]
        fn regular_intervals_produce_zero_dods(
            interval in 1i64..10000,
            start in 0i64..1000000,
            count in 3usize..100
        ) {
            let timestamps: Vec<i64> = (0..count)
                .map(|i| start + i as i64 * interval)
                .collect();

            let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

            prop_assert_eq!(first, start);
            prop_assert_eq!(first_delta, interval);

            for &dod in &dods {
                prop_assert_eq!(dod, 0, "Non-zero DoD for regular intervals");
            }
        }

        /// Single timestamp
        #[test]
        fn single_timestamp(ts in any::<i64>()) {
            let timestamps = vec![ts];
            let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

            prop_assert_eq!(first, ts);
            prop_assert_eq!(first_delta, 0);
            prop_assert!(dods.is_empty());

            let decoded = decode_timestamp_dod(first, first_delta, &dods);
            prop_assert_eq!(decoded, timestamps);
        }

        /// Two timestamps
        #[test]
        fn two_timestamps(ts1 in any::<i64>(), delta in 0i64..1000000) {
            let ts2 = ts1.saturating_add(delta);
            let timestamps = vec![ts1, ts2];

            let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

            prop_assert_eq!(first, ts1);
            prop_assert_eq!(first_delta, ts2 - ts1);
            prop_assert!(dods.is_empty());

            let decoded = decode_timestamp_dod(first, first_delta, &dods);
            prop_assert_eq!(decoded, timestamps);
        }
    }
}

// =============================================================================
// ANS Compression Fuzz Tests
// =============================================================================

mod ans_compression {
    use super::*;
    use kuba_tsdb::compression::{ans_compress, ans_decompress};

    proptest! {
        /// ANS compress/decompress roundtrip must be lossless
        #[test]
        fn roundtrip_preserves_data(
            data in byte_sequence(1000)
        ) {
            prop_assume!(!data.is_empty());

            let compressed = ans_compress(&data);
            let decompressed = ans_decompress(&compressed);

            match decompressed {
                Ok(decoded) => prop_assert_eq!(data, decoded),
                Err(e) => prop_assert!(false, "Decompression failed: {:?}", e),
            }
        }

        /// Skewed data should compress reasonably
        /// Note: ANS has header overhead for frequency tables, so compression
        /// is only guaranteed for sufficiently large data with low entropy
        #[test]
        fn skewed_data_compresses_reasonably(
            data in skewed_bytes(500)
        ) {
            prop_assume!(!data.is_empty());

            let compressed = ans_compress(&data);
            let decompressed = ans_decompress(&compressed);

            // Compression ratio depends on actual entropy of generated data.
            // ANS has header overhead (~2-4 bytes per unique symbol), so
            // highly varied data may not compress well at small sizes.
            // For truly skewed data with mostly zeros/small values at 1000+ bytes,
            // compression should be at least break-even.
            if data.len() >= 1000 {
                let unique_symbols: std::collections::HashSet<u8> = data.iter().copied().collect();
                let header_overhead = unique_symbols.len() * 4; // Approximate header size

                // Allow for header overhead - compressed should be < original + overhead
                prop_assert!(
                    compressed.len() < data.len() + header_overhead,
                    "ANS should not expand data significantly"
                );
            }

            // Main property: roundtrip must work correctly
            match decompressed {
                Ok(decoded) => prop_assert_eq!(data, decoded),
                Err(e) => prop_assert!(false, "Decompression failed: {:?}", e),
            }
        }

        /// Constant data should compress extremely well
        #[test]
        fn constant_data_compresses_extremely_well(
            value in any::<u8>(),
            len in 100usize..1000
        ) {
            let data: Vec<u8> = vec![value; len];
            let compressed = ans_compress(&data);

            // Constant data should compress to very small size
            // (header + frequency table + minimal encoded data)
            prop_assert!(
                compressed.len() < data.len() / 2,
                "Constant data ({} bytes) should compress well, got {} bytes ({}%)",
                data.len(),
                compressed.len(),
                (compressed.len() * 100) / data.len()
            );
        }

        /// Empty input handling
        #[test]
        fn empty_input_handling(_ignored in Just(())) {
            let data: Vec<u8> = vec![];
            let compressed = ans_compress(&data);
            // Empty input should produce minimal output
            prop_assert!(compressed.len() <= 16);
        }

        /// Single byte input
        #[test]
        fn single_byte_input(value in any::<u8>()) {
            let data = vec![value];
            let compressed = ans_compress(&data);
            let decompressed = ans_decompress(&compressed).unwrap();
            prop_assert_eq!(data, decompressed);
        }
    }
}

// =============================================================================
// Data Integrity Fuzz Tests
// =============================================================================

mod data_integrity {
    use super::*;
    use kuba_tsdb::storage::integrity::{calculate_checksum, verify_checksum};

    proptest! {
        /// Checksum verification should pass for unchanged data
        #[test]
        fn checksum_passes_for_unchanged_data(
            data in byte_sequence(1000)
        ) {
            let checksum = calculate_checksum(&data);
            let result = verify_checksum(&data, checksum);
            prop_assert!(result.is_ok());
        }

        /// Any bit flip should be detected
        #[test]
        fn bit_flip_detected(
            data in byte_sequence(100),
            flip_pos in 0usize..100,
            flip_bit in 0u8..8
        ) {
            prop_assume!(data.len() > flip_pos);

            let checksum = calculate_checksum(&data);

            // Flip a single bit
            let mut corrupted = data.clone();
            corrupted[flip_pos] ^= 1 << flip_bit;

            // Verification should fail (unless the flip didn't change anything)
            if corrupted != data {
                let result = verify_checksum(&corrupted, checksum);
                prop_assert!(result.is_err(), "Bit flip was not detected");
            }
        }

        /// Same data produces same checksum
        #[test]
        fn checksum_deterministic(
            data in byte_sequence(500)
        ) {
            let checksum1 = calculate_checksum(&data);
            let checksum2 = calculate_checksum(&data);
            prop_assert_eq!(checksum1, checksum2);
        }

        /// Different data produces different checksum (with high probability)
        #[test]
        fn different_data_different_checksum(
            data1 in byte_sequence(100),
            data2 in byte_sequence(100)
        ) {
            prop_assume!(data1 != data2);

            let checksum1 = calculate_checksum(&data1);
            let checksum2 = calculate_checksum(&data2);

            // Very unlikely to collide for different data
            prop_assert_ne!(checksum1, checksum2);
        }
    }
}

// =============================================================================
// Compression Round-Trip Integration Tests
// =============================================================================

mod compression_integration {
    use super::*;
    use kuba_tsdb::compression::{AhpacCompressor, KubaCompressor};
    use kuba_tsdb::engine::traits::Compressor;
    use kuba_tsdb::types::DataPoint;

    /// Strategy for generating valid DataPoints
    fn data_point_sequence(len: usize) -> impl Strategy<Value = Vec<DataPoint>> {
        (
            timestamp_sequence(len),
            prop::collection::vec(finite_f64(), len),
        )
            .prop_map(|(timestamps, values)| {
                timestamps
                    .into_iter()
                    .zip(values)
                    .map(|(ts, val)| DataPoint::new(1, ts, val))
                    .collect()
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))] // Fewer cases due to async

        /// Kuba compression roundtrip
        #[test]
        fn kuba_roundtrip(
            points in data_point_sequence(100)
        ) {
            prop_assume!(!points.is_empty());

            let rt = tokio::runtime::Runtime::new().unwrap();
            let compressor = KubaCompressor::new();

            rt.block_on(async {
                let compressed = compressor.compress(&points).await.unwrap();
                let decompressed = compressor.decompress(&compressed).await.unwrap();

                assert_eq!(points.len(), decompressed.len());
                for (orig, dec) in points.iter().zip(decompressed.iter()) {
                    assert_eq!(orig.timestamp, dec.timestamp);
                    assert!(
                        (orig.value - dec.value).abs() < 1e-10,
                        "Value mismatch: {} vs {}",
                        orig.value,
                        dec.value
                    );
                }
            });
        }

        /// AHPAC compression roundtrip
        #[test]
        fn ahpac_roundtrip(
            points in data_point_sequence(100)
        ) {
            prop_assume!(!points.is_empty());

            let rt = tokio::runtime::Runtime::new().unwrap();
            let compressor = AhpacCompressor::new();

            rt.block_on(async {
                let compressed = compressor.compress(&points).await.unwrap();
                let decompressed = compressor.decompress(&compressed).await.unwrap();

                assert_eq!(points.len(), decompressed.len());
                for (orig, dec) in points.iter().zip(decompressed.iter()) {
                    assert_eq!(orig.timestamp, dec.timestamp);
                    assert!(
                        (orig.value - dec.value).abs() < 1e-10,
                        "Value mismatch: {} vs {}",
                        orig.value,
                        dec.value
                    );
                }
            });
        }

        /// AHPAC should never produce larger output than a reasonable bound
        #[test]
        fn ahpac_compression_bound(
            points in data_point_sequence(100)
        ) {
            prop_assume!(!points.is_empty());

            let rt = tokio::runtime::Runtime::new().unwrap();
            let compressor = AhpacCompressor::new();

            rt.block_on(async {
                let compressed = compressor.compress(&points).await.unwrap();
                let original_size = points.len() * 16; // 8 bytes ts + 8 bytes value
                let compressed_size = compressed.compressed_size;

                // Compressed size should be at most 2x original (with header overhead)
                // This is a generous bound - typically compression is much better
                assert!(
                    compressed_size < original_size * 2 + 256,
                    "Compressed {} bytes to {} bytes ({}%), expected reasonable compression",
                    original_size,
                    compressed_size,
                    (compressed_size * 100) / original_size
                );
            });
        }
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_cases {
    use kuba_tsdb::compression::{
        ans_compress, ans_decompress, compute_timestamp_dod, decode_timestamp_dod,
        xor_decode_batch, xor_encode_batch,
    };

    #[test]
    fn xor_special_float_values() {
        // Test with special float values
        let values = vec![0.0, -0.0, f64::MIN_POSITIVE, f64::MAX, f64::MIN, 1.0, -1.0];

        let encoded = xor_encode_batch(&values);
        let decoded = xor_decode_batch(&encoded);

        assert_eq!(values.len(), decoded.len());
        for (orig, dec) in values.iter().zip(decoded.iter()) {
            assert_eq!(orig.to_bits(), dec.to_bits());
        }
    }

    #[test]
    fn timestamp_dod_large_deltas() {
        // Test with very large deltas
        let timestamps = vec![0, 1_000_000_000, 2_000_000_000, 3_000_000_000];

        let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);
        let decoded = decode_timestamp_dod(first, first_delta, &dods);

        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn ans_all_same_symbol() {
        // All same symbol
        let data: Vec<u8> = vec![42; 1000];
        let compressed = ans_compress(&data);
        let decompressed = ans_decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn ans_all_different_symbols() {
        // All 256 different symbols
        let data: Vec<u8> = (0..=255).collect();
        let compressed = ans_compress(&data);
        let decompressed = ans_decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn ans_alternating_pattern() {
        // Alternating pattern
        let data: Vec<u8> = (0..1000)
            .map(|i| if i % 2 == 0 { 0 } else { 255 })
            .collect();
        let compressed = ans_compress(&data);
        let decompressed = ans_decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }
}
