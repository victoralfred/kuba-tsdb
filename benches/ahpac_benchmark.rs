//! AHPAC Compression Benchmarks
//!
//! Compares AHPAC adaptive compression against fixed Kuba compression
//! across different data patterns and selection strategies.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kuba_tsdb::compression::KubaCompressor;
use kuba_tsdb::engine::traits::Compressor;
use kuba_tsdb::types::DataPoint;
use std::hint::black_box;

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
// Compression Benchmarks
// =============================================================================

/// Benchmark AHPAC selection strategies
fn bench_ahpac_strategies(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("ahpac_strategies");
    group.throughput(Throughput::Elements(1000));

    let points = create_regular_points(1000);

    // Heuristic strategy (fastest)
    let ahpac_heuristic = kuba_tsdb::compression::AhpacCompressor::fast();
    group.bench_function("heuristic", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(ahpac_heuristic.compress(&points).await.unwrap()) })
        });
    });

    // Verified strategy (balanced)
    let ahpac_verified = kuba_tsdb::compression::AhpacCompressor::new();
    group.bench_function("verified", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(ahpac_verified.compress(&points).await.unwrap()) })
        });
    });

    // Exhaustive strategy (best ratio)
    let ahpac_exhaustive = kuba_tsdb::compression::AhpacCompressor::best_ratio();
    group.bench_function("exhaustive", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(ahpac_exhaustive.compress(&points).await.unwrap()) })
        });
    });

    // Baseline: Kuba
    let kuba = KubaCompressor::new();
    group.bench_function("kuba_baseline", |b| {
        b.iter(|| rt.block_on(async { black_box(kuba.compress(&points).await.unwrap()) }));
    });

    group.finish();
}

/// Benchmark AHPAC vs Kuba across different data patterns
fn bench_data_patterns(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("data_patterns");
    group.throughput(Throughput::Elements(1000));

    let datasets: Vec<(&str, Vec<DataPoint>)> = vec![
        ("regular", create_regular_points(1000)),
        ("constant", create_constant_points(1000)),
        ("integer", create_integer_points(1000)),
        ("smooth", create_smooth_points(1000)),
        ("random", create_random_points(1000)),
        ("monotonic", create_monotonic_points(1000)),
        ("financial", create_financial_points(1000)),
    ];

    let ahpac = kuba_tsdb::compression::AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    for (name, points) in &datasets {
        group.bench_with_input(BenchmarkId::new("ahpac", name), points, |b, points| {
            b.iter(|| rt.block_on(async { black_box(ahpac.compress(points).await.unwrap()) }));
        });

        group.bench_with_input(BenchmarkId::new("kuba", name), points, |b, points| {
            b.iter(|| rt.block_on(async { black_box(kuba.compress(points).await.unwrap()) }));
        });
    }

    group.finish();
}

/// Benchmark compression at different sizes
fn bench_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("chunk_sizes");

    let ahpac = kuba_tsdb::compression::AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    for size in [100, 500, 1000, 5000, 10000] {
        let points = create_regular_points(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("ahpac", size), &points, |b, points| {
            b.iter(|| rt.block_on(async { black_box(ahpac.compress(points).await.unwrap()) }));
        });

        group.bench_with_input(BenchmarkId::new("kuba", size), &points, |b, points| {
            b.iter(|| rt.block_on(async { black_box(kuba.compress(points).await.unwrap()) }));
        });
    }

    group.finish();
}

/// Benchmark decompression
fn bench_decompression(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("decompression");
    group.throughput(Throughput::Elements(1000));

    let points = create_regular_points(1000);

    let ahpac = kuba_tsdb::compression::AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    let ahpac_compressed = rt.block_on(ahpac.compress(&points)).unwrap();
    let kuba_compressed = rt.block_on(kuba.compress(&points)).unwrap();

    group.bench_function("ahpac", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(ahpac.decompress(&ahpac_compressed).await.unwrap()) })
        });
    });

    group.bench_function("kuba", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(kuba.decompress(&kuba_compressed).await.unwrap()) })
        });
    });

    group.finish();
}

// =============================================================================
// Compression Ratio Analysis (not timed, just measures ratios)
// =============================================================================

fn bench_compression_ratios(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("compression_ratios");

    let datasets: Vec<(&str, Vec<DataPoint>)> = vec![
        ("regular", create_regular_points(1000)),
        ("constant", create_constant_points(1000)),
        ("integer", create_integer_points(1000)),
        ("smooth", create_smooth_points(1000)),
        ("random", create_random_points(1000)),
        ("monotonic", create_monotonic_points(1000)),
        ("financial", create_financial_points(1000)),
    ];

    let ahpac = kuba_tsdb::compression::AhpacCompressor::new();
    let kuba = KubaCompressor::new();

    // Print compression ratio table
    println!("\n=== Compression Ratio Analysis ===");
    println!(
        "{:15} | {:>10} | {:>10} | {:>10} | {:>8}",
        "Dataset", "Original", "AHPAC", "Kuba", "AHPAC Δ"
    );
    println!("{:-<62}", "");

    for (name, points) in &datasets {
        let original_size = points.len() * 16;

        let ahpac_compressed = rt.block_on(ahpac.compress(points)).unwrap();
        let kuba_compressed = rt.block_on(kuba.compress(points)).unwrap();

        let ahpac_bps = (ahpac_compressed.compressed_size * 8) as f64 / points.len() as f64;
        let kuba_bps = (kuba_compressed.compressed_size * 8) as f64 / points.len() as f64;
        let delta = kuba_bps - ahpac_bps;

        println!(
            "{:15} | {:>10} | {:>9.2}b | {:>9.2}b | {:>+7.2}b",
            name, original_size, ahpac_bps, kuba_bps, delta
        );

        // Benchmark to get timing (the actual measurement)
        group.bench_with_input(
            BenchmarkId::new("ratio_check", name),
            points,
            |b, points| {
                b.iter(|| rt.block_on(async { black_box(ahpac.compress(points).await.unwrap()) }));
            },
        );
    }

    println!("{:-<62}", "");
    println!("(b = bits per sample, Δ = improvement of AHPAC over Kuba)\n");

    group.finish();
}

criterion_group!(
    benches,
    bench_ahpac_strategies,
    bench_data_patterns,
    bench_sizes,
    bench_decompression,
    bench_compression_ratios,
);
criterion_main!(benches);
