//! SIMD Operations Benchmarks
//!
//! Benchmarks for SIMD-accelerated compression operations including
//! XOR delta encoding, timestamp delta-of-delta, and batch processing.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kuba_tsdb::compression::{
    analyze_xor_deltas, compute_timestamp_dod, decode_timestamp_dod, xor_decode_batch,
    xor_encode_batch,
};
use std::hint::black_box;

// =============================================================================
// Test Data Generators
// =============================================================================

/// Create regular floating point values (typical sensor data)
fn create_regular_values(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| 100.0 + (i as f64 * 0.1).sin() * 10.0)
        .collect()
}

/// Create constant values (best case for XOR encoding)
fn create_constant_values(count: usize) -> Vec<f64> {
    vec![42.5; count]
}

/// Create monotonically increasing values
fn create_monotonic_values(count: usize) -> Vec<f64> {
    (0..count).map(|i| i as f64).collect()
}

/// Create high-entropy random-like values (worst case)
fn create_random_values(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| {
            let x = i as f64;
            (x * 1.23456).sin() * 100.0 + (x * 7.89).cos() * 50.0
        })
        .collect()
}

/// Create regular timestamps (10-second intervals)
fn create_regular_timestamps(count: usize) -> Vec<i64> {
    (0..count).map(|i| 1000000 + i as i64 * 10000).collect()
}

/// Create irregular timestamps (varying intervals)
fn create_irregular_timestamps(count: usize) -> Vec<i64> {
    let mut ts = Vec::with_capacity(count);
    let mut current = 1000000i64;
    for i in 0..count {
        ts.push(current);
        // Vary interval between 5000 and 15000
        current += 5000 + (i as i64 % 10) * 1000;
    }
    ts
}

// =============================================================================
// XOR Encoding Benchmarks
// =============================================================================

fn bench_xor_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("xor_encode");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(size as u64));

        let regular = create_regular_values(size);
        let constant = create_constant_values(size);
        let monotonic = create_monotonic_values(size);
        let random = create_random_values(size);

        group.bench_with_input(BenchmarkId::new("regular", size), &regular, |b, values| {
            b.iter(|| black_box(xor_encode_batch(values)));
        });

        group.bench_with_input(
            BenchmarkId::new("constant", size),
            &constant,
            |b, values| {
                b.iter(|| black_box(xor_encode_batch(values)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("monotonic", size),
            &monotonic,
            |b, values| {
                b.iter(|| black_box(xor_encode_batch(values)));
            },
        );

        group.bench_with_input(BenchmarkId::new("random", size), &random, |b, values| {
            b.iter(|| black_box(xor_encode_batch(values)));
        });
    }

    group.finish();
}

fn bench_xor_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("xor_decode");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(size as u64));

        let values = create_regular_values(size);
        let encoded = xor_encode_batch(&values);

        group.bench_with_input(BenchmarkId::from_parameter(size), &encoded, |b, deltas| {
            b.iter(|| black_box(xor_decode_batch(deltas)));
        });
    }

    group.finish();
}

fn bench_xor_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("xor_roundtrip");

    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        let values = create_regular_values(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &values, |b, values| {
            b.iter(|| {
                let encoded = xor_encode_batch(values);
                black_box(xor_decode_batch(&encoded))
            });
        });
    }

    group.finish();
}

// =============================================================================
// Timestamp Delta-of-Delta Benchmarks
// =============================================================================

fn bench_timestamp_dod_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_dod_encode");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(size as u64));

        let regular = create_regular_timestamps(size);
        let irregular = create_irregular_timestamps(size);

        group.bench_with_input(
            BenchmarkId::new("regular_interval", size),
            &regular,
            |b, timestamps| {
                b.iter(|| black_box(compute_timestamp_dod(timestamps)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("irregular_interval", size),
            &irregular,
            |b, timestamps| {
                b.iter(|| black_box(compute_timestamp_dod(timestamps)));
            },
        );
    }

    group.finish();
}

fn bench_timestamp_dod_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_dod_decode");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(size as u64));

        let timestamps = create_regular_timestamps(size);
        let (first, first_delta, dods) = compute_timestamp_dod(&timestamps);

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &(first, first_delta, &dods),
            |b, (first, first_delta, dods)| {
                b.iter(|| black_box(decode_timestamp_dod(*first, *first_delta, dods)));
            },
        );
    }

    group.finish();
}

// =============================================================================
// XOR Delta Analysis Benchmarks
// =============================================================================

fn bench_analyze_deltas(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyze_xor_deltas");

    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        let values = create_regular_values(size);
        let encoded = xor_encode_batch(&values);

        group.bench_with_input(BenchmarkId::from_parameter(size), &encoded, |b, deltas| {
            b.iter(|| black_box(analyze_xor_deltas(deltas)));
        });
    }

    group.finish();
}

// =============================================================================
// Combined Benchmarks
// =============================================================================

fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_full_pipeline");

    for size in [1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        let values = create_regular_values(size);
        let timestamps = create_regular_timestamps(size);

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &(values.clone(), timestamps.clone()),
            |b, (values, timestamps)| {
                b.iter(|| {
                    // Encode values using XOR
                    let value_deltas = xor_encode_batch(values);
                    // Encode timestamps using DoD
                    let (first, first_delta, dods) = compute_timestamp_dod(timestamps);
                    // Analyze delta distribution
                    let _stats = analyze_xor_deltas(&value_deltas);
                    black_box((value_deltas, first, first_delta, dods))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_xor_encode,
    bench_xor_decode,
    bench_xor_roundtrip,
    bench_timestamp_dod_encode,
    bench_timestamp_dod_decode,
    bench_analyze_deltas,
    bench_full_pipeline,
);
criterion_main!(benches);
