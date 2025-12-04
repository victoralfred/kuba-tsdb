//! ANS (Asymmetric Numeral Systems) Compression Benchmarks
//!
//! Benchmarks for rANS entropy coding performance across different
//! data distributions and sizes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kuba_tsdb::compression::{ans_compress, ans_decompress, AnsEncoder};
use std::hint::black_box;

// =============================================================================
// Test Data Generators
// =============================================================================

/// Create uniform random bytes (high entropy - hard to compress)
fn create_uniform_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 17 + 31) % 256) as u8).collect()
}

/// Create skewed distribution (low entropy - easy to compress)
fn create_skewed_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| {
            // 70% zeros, 20% ones, 10% other
            let r = i % 100;
            if r < 70 {
                0
            } else if r < 90 {
                1
            } else {
                (r % 10) as u8
            }
        })
        .collect()
}

/// Create run-length friendly data (many repeated values)
fn create_rle_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut value = 0u8;
    let mut run_length = 10;
    while data.len() < size {
        for _ in 0..run_length.min(size - data.len()) {
            data.push(value);
        }
        value = value.wrapping_add(1);
        run_length = ((run_length + 3) % 20) + 5; // Vary run lengths
    }
    data
}

/// Create realistic XOR delta residuals (what AHPAC would produce)
fn create_xor_residuals(size: usize) -> Vec<u8> {
    // XOR residuals typically have many zeros and small values
    (0..size)
        .map(|i| {
            let r = i % 100;
            if r < 50 {
                0
            } else if r < 75 {
                (i % 3) as u8
            } else if r < 90 {
                (i % 16) as u8
            } else {
                (i % 256) as u8
            }
        })
        .collect()
}

/// Create all zeros (best case)
fn create_constant_data(size: usize) -> Vec<u8> {
    vec![0; size]
}

/// Create all unique values (worst case for small data)
fn create_unique_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

// =============================================================================
// ANS Compression Benchmarks
// =============================================================================

fn bench_ans_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_compress");

    for size in [256, 1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Bytes(size as u64));

        let uniform = create_uniform_data(size);
        let skewed = create_skewed_data(size);
        let rle = create_rle_data(size);
        let xor_residuals = create_xor_residuals(size);
        let constant = create_constant_data(size);

        group.bench_with_input(BenchmarkId::new("uniform", size), &uniform, |b, data| {
            b.iter(|| black_box(ans_compress(data)));
        });

        group.bench_with_input(BenchmarkId::new("skewed", size), &skewed, |b, data| {
            b.iter(|| black_box(ans_compress(data)));
        });

        group.bench_with_input(BenchmarkId::new("rle_friendly", size), &rle, |b, data| {
            b.iter(|| black_box(ans_compress(data)));
        });

        group.bench_with_input(
            BenchmarkId::new("xor_residuals", size),
            &xor_residuals,
            |b, data| {
                b.iter(|| black_box(ans_compress(data)));
            },
        );

        group.bench_with_input(BenchmarkId::new("constant", size), &constant, |b, data| {
            b.iter(|| black_box(ans_compress(data)));
        });
    }

    group.finish();
}

fn bench_ans_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_decompress");

    for size in [256, 1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Bytes(size as u64));

        let skewed = create_skewed_data(size);
        let compressed = ans_compress(&skewed);

        group.bench_with_input(BenchmarkId::from_parameter(size), &compressed, |b, data| {
            b.iter(|| black_box(ans_decompress(data).unwrap()));
        });
    }

    group.finish();
}

fn bench_ans_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_roundtrip");

    for size in [1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));

        let data = create_xor_residuals(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let compressed = ans_compress(data);
                black_box(ans_decompress(&compressed).unwrap())
            });
        });
    }

    group.finish();
}

// =============================================================================
// ANS Encoder API Benchmarks
// =============================================================================

fn bench_ans_encoder_api(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_encoder_api");

    for size in [1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));

        let data = create_xor_residuals(size);

        group.bench_with_input(BenchmarkId::new("full_encode", size), &data, |b, data| {
            b.iter(|| {
                let mut encoder = AnsEncoder::new();
                encoder.build_frequencies(data);
                encoder.build_tables();
                black_box(encoder.encode(data))
            });
        });

        // Benchmark just frequency counting
        group.bench_with_input(
            BenchmarkId::new("build_frequencies", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut encoder = AnsEncoder::new();
                    encoder.build_frequencies(data);
                    encoder.build_tables();
                    black_box(&encoder);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Compression Ratio Analysis
// =============================================================================

fn bench_compression_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_compression_ratios");

    let size = 4096;
    let datasets: Vec<(&str, Vec<u8>)> = vec![
        ("uniform", create_uniform_data(size)),
        ("skewed", create_skewed_data(size)),
        ("rle_friendly", create_rle_data(size)),
        ("xor_residuals", create_xor_residuals(size)),
        ("constant", create_constant_data(size)),
        ("unique", create_unique_data(size)),
    ];

    println!("\n=== ANS Compression Ratio Analysis ===");
    println!(
        "{:15} | {:>10} | {:>10} | {:>8}",
        "Dataset", "Original", "Compressed", "Ratio"
    );
    println!("{:-<52}", "");

    for (name, data) in &datasets {
        let compressed = ans_compress(data);
        let ratio = data.len() as f64 / compressed.len() as f64;

        println!(
            "{:15} | {:>10} | {:>10} | {:>7.2}x",
            name,
            data.len(),
            compressed.len(),
            ratio
        );

        // Run benchmark for timing
        group.bench_with_input(BenchmarkId::new("ratio_test", name), data, |b, data| {
            b.iter(|| black_box(ans_compress(data)));
        });
    }

    println!("{:-<52}", "");
    println!();

    group.finish();
}

// =============================================================================
// Entropy Estimation vs Actual Compression
// =============================================================================

fn bench_entropy_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("ans_entropy_efficiency");

    let size = 4096;
    let data = create_skewed_data(size);

    // Calculate theoretical entropy
    let mut freq = [0u32; 256];
    for &byte in &data {
        freq[byte as usize] += 1;
    }

    let total = data.len() as f64;
    let mut entropy = 0.0f64;
    for &f in &freq {
        if f > 0 {
            let p = f as f64 / total;
            entropy -= p * p.log2();
        }
    }
    let theoretical_bits = entropy * data.len() as f64;
    let theoretical_bytes = (theoretical_bits / 8.0).ceil() as usize;

    let compressed = ans_compress(&data);
    let actual_bytes = compressed.len();

    println!("\n=== Entropy Efficiency Analysis ===");
    println!("Data size: {} bytes", data.len());
    println!("Entropy: {:.3} bits/symbol", entropy);
    println!("Theoretical minimum: {} bytes", theoretical_bytes);
    println!("Actual compressed: {} bytes", actual_bytes);
    println!(
        "Efficiency: {:.1}%",
        (theoretical_bytes as f64 / actual_bytes as f64) * 100.0
    );
    println!();

    group.bench_function("entropy_test", |b| {
        b.iter(|| black_box(ans_compress(&data)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ans_compress,
    bench_ans_decompress,
    bench_ans_roundtrip,
    bench_ans_encoder_api,
    bench_compression_ratios,
    bench_entropy_efficiency,
);
criterion_main!(benches);
