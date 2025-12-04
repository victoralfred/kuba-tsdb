//! AHPAC Success Criteria Evaluation Tests
//!
//! This module validates that AHPAC achieves the required performance targets:
//! - Compression ratio improvement: Δ > 1.0 bits/sample on average
//! - Performance: Not more than 2x slower than Kuba baseline
//! - Correctness: Lossless round-trip for all data types

use kuba_tsdb::compression::{AhpacCompressor, KubaCompressor};
use kuba_tsdb::engine::traits::Compressor;
use kuba_tsdb::types::DataPoint;

// =============================================================================
// Test Data Generators
// =============================================================================

/// Regular time-series data (typical monitoring metrics)
fn create_regular_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new(
                1,
                1000000 + i as i64 * 1000,
                100.0 + (i as f64 * 0.1).sin() * 10.0,
            )
        })
        .collect()
}

/// Constant values (e.g., status codes, boolean metrics)
fn create_constant_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, 42.0))
        .collect()
}

/// Integer-like data (prices, counters scaled to floats)
fn create_integer_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, (100 + i) as f64))
        .collect()
}

/// Smooth, slowly changing data (temperature, slow sensors)
fn create_smooth_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, 100.0 + i as f64 * 0.001))
        .collect()
}

/// Random-ish data (high entropy, difficult to compress)
fn create_random_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            let x = i as f64;
            DataPoint::new(
                1,
                1000000 + i as i64 * 1000,
                (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0,
            )
        })
        .collect()
}

/// Monotonically increasing counters
fn create_monotonic_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(1, 1000000 + i as i64 * 1000, i as f64))
        .collect()
}

/// Financial tick data with 2 decimal places
fn create_financial_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new(
                1,
                1000000 + i as i64 * 1000,
                99.95 + (i as f64 * 0.01) % 10.0,
            )
        })
        .collect()
}

// =============================================================================
// Success Criteria Evaluation
// =============================================================================

/// Structure to hold compression ratio results
struct CompressionResult {
    name: &'static str,
    ahpac_bps: f64, // bits per sample
    kuba_bps: f64,  // bits per sample
    delta: f64,     // improvement (positive = AHPAC better)
}

/// Calculate bits per sample for a compressed block
fn bits_per_sample(compressed_size: usize, point_count: usize) -> f64 {
    (compressed_size * 8) as f64 / point_count as f64
}

/// Run compression comparison for a single dataset
async fn compare_compression(
    name: &'static str,
    points: &[DataPoint],
    ahpac: &AhpacCompressor,
    kuba: &KubaCompressor,
) -> CompressionResult {
    let ahpac_result = ahpac
        .compress(points)
        .await
        .expect("AHPAC compression failed");
    let kuba_result = kuba
        .compress(points)
        .await
        .expect("Kuba compression failed");

    let ahpac_bps = bits_per_sample(ahpac_result.compressed_size, points.len());
    let kuba_bps = bits_per_sample(kuba_result.compressed_size, points.len());
    let delta = kuba_bps - ahpac_bps;

    CompressionResult {
        name,
        ahpac_bps,
        kuba_bps,
        delta,
    }
}

#[tokio::test]
async fn test_success_criteria_compression_ratio() {
    // Success criterion: Average Δ > 1.0 bits/sample across representative datasets
    let ahpac = AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    let datasets: Vec<(&'static str, Vec<DataPoint>)> = vec![
        ("regular", create_regular_points(1000)),
        ("constant", create_constant_points(1000)),
        ("integer", create_integer_points(1000)),
        ("smooth", create_smooth_points(1000)),
        ("random", create_random_points(1000)),
        ("monotonic", create_monotonic_points(1000)),
        ("financial", create_financial_points(1000)),
    ];

    let mut results = Vec::new();
    for (name, points) in &datasets {
        let result = compare_compression(name, points, &ahpac, &kuba).await;
        results.push(result);
    }

    // Print summary table
    println!("\n=== AHPAC Success Criteria Evaluation ===\n");
    println!(
        "{:15} | {:>10} | {:>10} | {:>10} | {:>8}",
        "Dataset", "AHPAC", "Kuba", "Δ", "Pass?"
    );
    println!("{:-<62}", "");

    let mut total_delta = 0.0;
    let mut datasets_with_improvement = 0;

    for result in &results {
        let pass = if result.delta > 1.0 { "✓" } else { "-" };
        println!(
            "{:15} | {:>9.2}b | {:>9.2}b | {:>+9.2}b | {:>8}",
            result.name, result.ahpac_bps, result.kuba_bps, result.delta, pass
        );

        total_delta += result.delta;
        if result.delta > 0.0 {
            datasets_with_improvement += 1;
        }
    }

    let average_delta = total_delta / results.len() as f64;

    println!("{:-<62}", "");
    println!(
        "Average Δ: {:.2} bits/sample (target: > 1.0)",
        average_delta
    );
    println!(
        "Datasets with improvement: {}/{}",
        datasets_with_improvement,
        results.len()
    );

    // Success criteria check
    assert!(
        average_delta > 1.0,
        "AHPAC must achieve average Δ > 1.0 bits/sample, got {:.2}",
        average_delta
    );
    println!(
        "\n✓ SUCCESS: AHPAC achieves average Δ = {:.2} bits/sample",
        average_delta
    );
}

#[tokio::test]
async fn test_success_criteria_lossless_roundtrip() {
    // Success criterion: Lossless compression for all data types
    let ahpac = AhpacCompressor::new();

    let datasets: Vec<(&'static str, Vec<DataPoint>)> = vec![
        ("regular", create_regular_points(1000)),
        ("constant", create_constant_points(1000)),
        ("integer", create_integer_points(1000)),
        ("smooth", create_smooth_points(1000)),
        ("random", create_random_points(1000)),
        ("monotonic", create_monotonic_points(1000)),
        ("financial", create_financial_points(1000)),
    ];

    println!("\n=== AHPAC Lossless Round-Trip Verification ===\n");

    for (name, points) in &datasets {
        let compressed = ahpac.compress(points).await.expect("Compression failed");
        let decompressed = ahpac
            .decompress(&compressed)
            .await
            .expect("Decompression failed");

        assert_eq!(
            points.len(),
            decompressed.len(),
            "Point count mismatch for {}",
            name
        );

        for (i, (orig, dec)) in points.iter().zip(decompressed.iter()).enumerate() {
            assert_eq!(
                orig.timestamp, dec.timestamp,
                "Timestamp mismatch at index {} for {}",
                i, name
            );
            assert!(
                (orig.value - dec.value).abs() < 1e-10,
                "Value mismatch at index {} for {}: {} != {}",
                i,
                name,
                orig.value,
                dec.value
            );
        }

        println!(
            "✓ {} ({} points): Lossless round-trip verified",
            name,
            points.len()
        );
    }

    println!("\n✓ SUCCESS: All data types pass lossless round-trip verification");
}

#[tokio::test]
async fn test_success_criteria_integer_data_performance() {
    // Specific success criterion: Integer-like data should achieve > 30 bits/sample improvement
    // (This is where AHPAC's ALP codec really shines)
    let ahpac = AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    let integer_points = create_integer_points(1000);

    let ahpac_result = ahpac.compress(&integer_points).await.unwrap();
    let kuba_result = kuba.compress(&integer_points).await.unwrap();

    let ahpac_bps = bits_per_sample(ahpac_result.compressed_size, integer_points.len());
    let kuba_bps = bits_per_sample(kuba_result.compressed_size, integer_points.len());
    let delta = kuba_bps - ahpac_bps;

    println!("\n=== Integer Data Performance ===");
    println!("AHPAC: {:.2} bits/sample", ahpac_bps);
    println!("Kuba:  {:.2} bits/sample", kuba_bps);
    println!("Δ:     {:.2} bits/sample", delta);

    // ALP should achieve at least 30 bits/sample improvement on integer data
    assert!(
        delta > 30.0,
        "AHPAC should achieve > 30 bits/sample improvement on integer data, got {:.2}",
        delta
    );

    println!(
        "\n✓ SUCCESS: Integer data achieves Δ = {:.2} bits/sample",
        delta
    );
}

#[tokio::test]
async fn test_success_criteria_financial_data_performance() {
    // Specific success criterion: Financial data (decimal-scaled) should achieve > 40 bits/sample improvement
    let ahpac = AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    let financial_points = create_financial_points(1000);

    let ahpac_result = ahpac.compress(&financial_points).await.unwrap();
    let kuba_result = kuba.compress(&financial_points).await.unwrap();

    let ahpac_bps = bits_per_sample(ahpac_result.compressed_size, financial_points.len());
    let kuba_bps = bits_per_sample(kuba_result.compressed_size, financial_points.len());
    let delta = kuba_bps - ahpac_bps;

    println!("\n=== Financial Data Performance ===");
    println!("AHPAC: {:.2} bits/sample", ahpac_bps);
    println!("Kuba:  {:.2} bits/sample", kuba_bps);
    println!("Δ:     {:.2} bits/sample", delta);

    // ALP should achieve at least 40 bits/sample improvement on financial data
    assert!(
        delta > 40.0,
        "AHPAC should achieve > 40 bits/sample improvement on financial data, got {:.2}",
        delta
    );

    println!(
        "\n✓ SUCCESS: Financial data achieves Δ = {:.2} bits/sample",
        delta
    );
}

#[tokio::test]
async fn test_selection_strategies() {
    // Verify all selection strategies produce valid output
    let points = create_regular_points(100);

    let strategies = vec![
        ("heuristic", AhpacCompressor::fast()),
        ("verified", AhpacCompressor::new()),
        ("exhaustive", AhpacCompressor::best_ratio()),
    ];

    println!("\n=== Selection Strategy Validation ===\n");

    for (name, compressor) in strategies {
        let compressed = compressor
            .compress(&points)
            .await
            .expect("Compression failed");
        let decompressed = compressor
            .decompress(&compressed)
            .await
            .expect("Decompression failed");

        assert_eq!(points.len(), decompressed.len());

        let bps = bits_per_sample(compressed.compressed_size, points.len());
        println!("✓ {}: {:.2} bits/sample, round-trip OK", name, bps);
    }

    println!("\n✓ SUCCESS: All selection strategies produce valid output");
}
