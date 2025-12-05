/// Simplified tests to investigate claimed bugs in bug.result
/// Tests are focused on verifying round-trip correctness and edge cases
use kuba_tsdb::compression::kuba::KubaCompressor;
use kuba_tsdb::engine::traits::Compressor;
use kuba_tsdb::types::DataPoint;

/// ISSUE 1.1: Range Bounds - Test boundary values
#[tokio::test]
async fn test_range_boundary_values() {
    let compressor = KubaCompressor::new();

    // Test exact boundary values for each encoding range
    // Note: Timestamps must be monotonic, so we test negative dod by using
    // a decreasing delta (timestamps still increasing)
    let test_cases = vec![
        (vec![0, 10, 73], "dod = 63 (7-bit boundary)"),
        (vec![0, 10, 74], "dod = 64 (claimed missing)"),
        (
            vec![0, 100, 137],
            "dod = -63 (7-bit boundary, delta decreases from 100 to 37)",
        ),
        (vec![0, 10, 265], "dod = 255 (9-bit boundary)"),
        (vec![0, 10, 266], "dod = 256 (claimed missing)"),
        (vec![0, 10, 2057], "dod = 2047 (12-bit boundary)"),
        (vec![0, 10, 2058], "dod = 2048 (claimed missing)"),
    ];

    for (timestamps, desc) in test_cases {
        let points: Vec<DataPoint> = timestamps
            .into_iter()
            .map(|t| DataPoint {
                series_id: 1,
                timestamp: t,
                value: 1.0,
            })
            .collect();

        let compressed = compressor
            .compress(&points)
            .await
            .unwrap_or_else(|_| panic!("Compression failed for {}", desc));
        let decompressed = compressor
            .decompress(&compressed)
            .await
            .unwrap_or_else(|_| panic!("Decompression failed for {}", desc));

        assert_eq!(decompressed, points, "Round-trip failed for {}", desc);
    }
}

/// ISSUE 1.2: Large Delta Overflow - Test deltas exceeding i32
#[tokio::test]
async fn test_large_delta_overflow() {
    let compressor = KubaCompressor::new();

    // Delta that exceeds i32::MAX
    let large_ts = (i32::MAX as i64) + 10000;
    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10,
            value: 2.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: large_ts,
            value: 3.0,
        },
    ];

    println!(
        "Delta: {} (exceeds i32::MAX: {})",
        large_ts - 10,
        large_ts - 10 > i32::MAX as i64
    );

    match compressor.compress(&points).await {
        Ok(compressed) => match compressor.decompress(&compressed).await {
            Ok(decompressed) => {
                assert_eq!(decompressed, points, "Large delta should round-trip");
            },
            Err(e) => {
                println!("⚠️  BUG CONFIRMED: Decompression failed: {:?}", e);
                panic!("Large delta causes corruption");
            },
        },
        Err(e) => println!("Compression rejected large delta: {:?}", e),
    }
}

/// ISSUE 1.3: Wrapping Overflow - Test with extreme timestamps
#[tokio::test]
async fn test_wrapping_overflow() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 100,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 50,
            value: 2.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 10,
            value: 3.0,
        },
    ];

    let compressed = compressor
        .compress(&points)
        .await
        .expect("Should handle large timestamps");
    let decompressed = compressor
        .decompress(&compressed)
        .await
        .expect("Should decompress large timestamps");

    assert_eq!(decompressed, points);
}

/// ISSUE 2.1: Truncated Bitstream - Test robustness
#[tokio::test]
async fn test_truncated_bitstream() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10,
            value: 2.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 20,
            value: 3.0,
        },
    ];

    let mut compressed = compressor.compress(&points).await.unwrap();

    // Truncate the data
    if compressed.data.len() > 10 {
        compressed.data = compressed.data.slice(0..10);

        let result = compressor.decompress(&compressed).await;
        assert!(
            result.is_err(),
            "Truncated data should cause error, not panic"
        );
        println!(
            "✓ Truncated data properly rejected: {:?}",
            result.unwrap_err()
        );
    }
}

/// ISSUE 3: XOR Value Compression - Test various float patterns
#[tokio::test]
async fn test_xor_value_patterns() {
    let compressor = KubaCompressor::new();

    let test_cases = vec![
        (1.0, 1.0, "identical"),
        (1.0, 1.000000001, "tiny change"),
        (1.0, 2.0, "significant"),
        (f64::MAX, f64::MAX / 2.0, "large values"),
    ];

    for (val1, val2, desc) in test_cases {
        let points = vec![
            DataPoint {
                series_id: 1,
                timestamp: 0,
                value: val1,
            },
            DataPoint {
                series_id: 1,
                timestamp: 10,
                value: val2,
            },
        ];

        let compressed = compressor
            .compress(&points)
            .await
            .unwrap_or_else(|_| panic!("Compression failed for {}", desc));
        let decompressed = compressor
            .decompress(&compressed)
            .await
            .unwrap_or_else(|_| panic!("Decompression failed for {}", desc));

        assert_eq!(decompressed, points, "XOR pattern {} failed", desc);
    }
}

/// ISSUE 3.2: Leading Zeros - Test denormalized numbers
#[tokio::test]
async fn test_leading_zeros_extreme() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10,
            value: f64::MIN_POSITIVE,
        },
        DataPoint {
            series_id: 1,
            timestamp: 20,
            value: f64::MIN_POSITIVE * 2.0,
        },
    ];

    let v1 = points[0].value.to_bits();
    let v2 = points[1].value.to_bits();
    let xor = v1 ^ v2;
    println!("XOR leading zeros: {}", xor.leading_zeros());

    let compressed = compressor
        .compress(&points)
        .await
        .expect("Should handle denormalized numbers");
    let decompressed = compressor
        .decompress(&compressed)
        .await
        .expect("Should decompress denormalized numbers");

    assert_eq!(decompressed, points);
}

/// ISSUE 5: Security - Test malicious inputs
#[tokio::test]
async fn test_malicious_huge_count() {
    let compressor = KubaCompressor::new();
    use bytes::Bytes;

    // Create block claiming 15 million points (exceeds 10M limit)
    let malicious_data = vec![0xFFu8; 1000];

    let compressed = kuba_tsdb::engine::traits::CompressedBlock {
        algorithm_id: "Kuba".to_string(),
        original_size: 15_000_000 * 24, // Pretend huge data
        compressed_size: 1000,
        checksum: 0,
        data: Bytes::from(malicious_data),
        metadata: kuba_tsdb::engine::traits::BlockMetadata {
            start_timestamp: 0,
            end_timestamp: 1000,
            point_count: 15_000_000, // 15 million > 10M limit
            series_id: 1,
        },
    };

    let result = compressor.decompress(&compressed).await;
    println!("Malicious count result: {:?}", result);

    assert!(result.is_err(), "Should reject blocks > 10M points");
}

/// ISSUE 6.1: Original Size Calculation Bug
#[tokio::test]
async fn test_original_size_bug() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10,
            value: 2.0,
        },
    ];

    let compressed = compressor.compress(&points).await.unwrap();

    let expected_size = points.len() * std::mem::size_of::<DataPoint>();
    let actual_size = compressed.original_size;

    println!("Points: {}", points.len());
    println!("Expected original_size: {} bytes", expected_size);
    println!("Actual original_size: {} bytes", actual_size);
    println!(
        "size_of::<DataPoint>(): {}",
        std::mem::size_of::<DataPoint>()
    );

    if actual_size != expected_size {
        println!(
            "⚠️  BUG CONFIRMED: original_size is {} but should be {}",
            actual_size, expected_size
        );
        println!("    Likely using size_of_val(points) which returns slice metadata (16 bytes)");

        // This is definitely a bug
        assert_eq!(actual_size, 16, "Confirms using size_of_val() incorrectly");
    } else {
        println!("✓ Original size calculated correctly");
    }
}

/// TEST COVERAGE: Special Float Values
#[tokio::test]
async fn test_special_floats() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: f64::NAN,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10,
            value: f64::INFINITY,
        },
        DataPoint {
            series_id: 1,
            timestamp: 20,
            value: f64::NEG_INFINITY,
        },
        DataPoint {
            series_id: 1,
            timestamp: 30,
            value: 0.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 40,
            value: -0.0,
        },
    ];

    let compressed = compressor
        .compress(&points)
        .await
        .expect("Should handle special floats");
    let decompressed = compressor
        .decompress(&compressed)
        .await
        .expect("Should decompress special floats");

    assert_eq!(decompressed.len(), points.len());
    assert!(decompressed[0].value.is_nan());
    assert_eq!(decompressed[1].value, f64::INFINITY);
    assert_eq!(decompressed[2].value, f64::NEG_INFINITY);
}

/// TEST COVERAGE: Irregular Timestamps
#[tokio::test]
async fn test_irregular_timestamps() {
    let compressor = KubaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 5,
            value: 2.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 100,
            value: 3.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 105,
            value: 4.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 10000,
            value: 5.0,
        },
    ];

    let compressed = compressor
        .compress(&points)
        .await
        .expect("Should handle irregular timestamps");
    let decompressed = compressor
        .decompress(&compressed)
        .await
        .expect("Should decompress irregular timestamps");

    assert_eq!(decompressed, points);
}

/// TEST COVERAGE: Random Data (Worst Case)
#[tokio::test]
async fn test_random_data() {
    use rand::Rng;

    let compressor = KubaCompressor::new();
    let mut rng = rand::rng();

    let mut points: Vec<DataPoint> = (0..100)
        .map(|_| DataPoint {
            series_id: 1,
            timestamp: rng.random_range(0..1_000_000),
            value: rng.random(),
        })
        .collect();

    // Sort by timestamp (required for Kuba)
    points.sort_by_key(|p| p.timestamp);

    let compressed = compressor
        .compress(&points)
        .await
        .expect("Should handle random data");
    let decompressed = compressor
        .decompress(&compressed)
        .await
        .expect("Should decompress random data");

    let ratio = compressed.original_size as f64 / compressed.compressed_size as f64;
    println!("Random data ratio: {:.2}", ratio);

    assert_eq!(decompressed, points);
}
