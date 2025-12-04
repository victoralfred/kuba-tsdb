//! Parallel Compression Benchmarks
//!
//! Benchmarks for multi-threaded compression of multiple series,
//! measuring throughput, parallelism efficiency, and scalability.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kuba_tsdb::compression::{ParallelCompressor, ParallelConfig};
use kuba_tsdb::types::DataPoint;
use std::hint::black_box;

// =============================================================================
// Test Data Generators
// =============================================================================

use kuba_tsdb::types::SeriesId;

/// Create test points for a given series
fn create_series_points(series_id: SeriesId, count: usize, base_ts: i64) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new(
                series_id,
                base_ts + i as i64 * 1000,
                100.0 + (i as f64 * 0.1).sin() * 10.0 + series_id as f64,
            )
        })
        .collect()
}

/// Create multiple series with the same point count
fn create_multi_series(
    series_count: usize,
    points_per_series: usize,
) -> Vec<(SeriesId, Vec<DataPoint>)> {
    (1..=series_count)
        .map(|series_id| {
            let series_id = series_id as SeriesId;
            let points = create_series_points(
                series_id,
                points_per_series,
                1000000 + series_id as i64 * 1000000,
            );
            (series_id, points)
        })
        .collect()
}

/// Create series with varying point counts
fn create_varied_series(series_count: usize) -> Vec<(SeriesId, Vec<DataPoint>)> {
    (1..=series_count)
        .map(|series_id| {
            let series_id = series_id as SeriesId;
            // Vary size: 100 to 1000 points
            let point_count = 100 + (series_id as usize % 10) * 100;
            let points = create_series_points(series_id, point_count, 1000000);
            (series_id, points)
        })
        .collect()
}

// =============================================================================
// Parallel vs Sequential Benchmarks
// =============================================================================

fn bench_parallel_vs_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("parallel_vs_sequential");

    // Test with different series counts
    for series_count in [4, 8, 16, 32] {
        let batches = create_multi_series(series_count, 500);
        let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();
        group.throughput(Throughput::Elements(total_points as u64));

        // Parallel with default concurrency
        let parallel = ParallelCompressor::default();

        group.bench_with_input(
            BenchmarkId::new("parallel", series_count),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async { black_box(parallel.compress_batch(batches.clone()).await) })
                });
            },
        );

        // Sequential (min_parallel_batch_size very high forces sequential)
        let sequential = ParallelCompressor::new(ParallelConfig {
            max_concurrent_tasks: 1,
            min_parallel_batch_size: 1000000,
            use_ahpac: true,
            collect_stats: false,
        });

        group.bench_with_input(
            BenchmarkId::new("sequential", series_count),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(sequential.compress_batch(batches.clone()).await)
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Concurrency Level Benchmarks
// =============================================================================

fn bench_concurrency_levels(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrency_levels");

    let batches = create_multi_series(16, 500);
    let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();
    group.throughput(Throughput::Elements(total_points as u64));

    for max_tasks in [1, 2, 4, 8, 16] {
        let compressor = ParallelCompressor::new(ParallelConfig {
            max_concurrent_tasks: max_tasks,
            min_parallel_batch_size: 100,
            use_ahpac: true,
            collect_stats: false,
        });

        group.bench_with_input(
            BenchmarkId::new("threads", max_tasks),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(compressor.compress_batch(batches.clone()).await)
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Points Per Series Scaling
// =============================================================================

fn bench_points_per_series(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("points_per_series");

    let compressor = ParallelCompressor::default();
    let series_count = 8;

    for points_per_series in [100, 500, 1000, 2000, 5000] {
        let batches = create_multi_series(series_count, points_per_series);
        let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();
        group.throughput(Throughput::Elements(total_points as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(points_per_series),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(compressor.compress_batch(batches.clone()).await)
                    })
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Configuration Presets
// =============================================================================

fn bench_config_presets(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("config_presets");

    let batches = create_multi_series(16, 500);
    let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();
    group.throughput(Throughput::Elements(total_points as u64));

    // Default configuration
    let default_compressor = ParallelCompressor::default();
    group.bench_with_input(
        BenchmarkId::new("preset", "default"),
        &batches,
        |b, batches| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(default_compressor.compress_batch(batches.clone()).await)
                })
            });
        },
    );

    // High throughput configuration
    let high_throughput = ParallelCompressor::new(ParallelConfig::high_throughput());
    group.bench_with_input(
        BenchmarkId::new("preset", "high_throughput"),
        &batches,
        |b, batches| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(high_throughput.compress_batch(batches.clone()).await)
                })
            });
        },
    );

    // Low latency configuration
    let low_latency = ParallelCompressor::new(ParallelConfig::low_latency());
    group.bench_with_input(
        BenchmarkId::new("preset", "low_latency"),
        &batches,
        |b, batches| {
            b.iter(|| {
                rt.block_on(async { black_box(low_latency.compress_batch(batches.clone()).await) })
            });
        },
    );

    group.finish();
}

// =============================================================================
// Parallelism Efficiency Analysis
// =============================================================================

fn bench_parallelism_efficiency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("parallelism_efficiency");

    // Print efficiency analysis
    println!("\n=== Parallelism Efficiency Analysis ===");

    for series_count in [8, 16, 32] {
        let batches = create_multi_series(series_count, 500);
        let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();

        let compressor = ParallelCompressor::default();
        let (_, stats) = rt.block_on(compressor.compress_batch_with_stats(batches.clone()));

        println!(
            "Series: {:2} | Points: {:6} | Wall: {:6}µs | Total CPU: {:8}µs | Efficiency: {:.1}% | Throughput: {:.1} MB/s",
            series_count,
            total_points,
            stats.wall_time_us,
            stats.total_compression_time_us,
            stats.parallelism_efficiency * 100.0,
            stats.throughput_mbps()
        );

        group.throughput(Throughput::Elements(total_points as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(series_count),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(compressor.compress_batch_with_stats(batches.clone()).await)
                    })
                });
            },
        );
    }

    println!();
    group.finish();
}

// =============================================================================
// Varied Series Size Benchmarks
// =============================================================================

fn bench_varied_series_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("varied_series_sizes");

    let compressor = ParallelCompressor::default();

    for series_count in [8, 16, 32] {
        let batches = create_varied_series(series_count);
        let total_points: usize = batches.iter().map(|(_, pts)| pts.len()).sum();
        group.throughput(Throughput::Elements(total_points as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(series_count),
            &batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(compressor.compress_batch(batches.clone()).await)
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parallel_vs_sequential,
    bench_concurrency_levels,
    bench_points_per_series,
    bench_config_presets,
    bench_parallelism_efficiency,
    bench_varied_series_sizes,
);
criterion_main!(benches);
