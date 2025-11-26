//! Performance benchmark tests for storage layer
//!
//! These tests measure and validate performance characteristics:
//! - Write throughput (points/second)
//! - Read throughput (points/second)
//! - Compression ratios
//! - Memory efficiency
//! - I/O latencies
//!
//! Run with: cargo test --release --test performance_benchmarks -- --ignored --nocapture

use gorilla_tsdb::compression::gorilla::GorillaCompressor;
use gorilla_tsdb::engine::traits::Compressor;
use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use gorilla_tsdb::storage::chunk::Chunk;
use gorilla_tsdb::storage::mmap::MmapChunk;
use gorilla_tsdb::storage::reader::{ChunkReader, QueryOptions};
use gorilla_tsdb::types::DataPoint;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// =============================================================================
// WRITE PERFORMANCE BENCHMARKS
// =============================================================================

/// Benchmark: Single point write latency
#[tokio::test]
#[ignore]
async fn benchmark_single_point_write_latency() {
    println!("\n=== Single Point Write Latency Benchmark ===\n");

    let config = SealConfig {
        max_points: 1_000_000,
        max_duration_ms: 3600_000,
        max_size_bytes: 100 * 1024 * 1024,
    };

    let chunk = Arc::new(ActiveChunk::new(1, 1_000_000, config));

    // Warmup
    for i in 0..1000 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    // Measure latencies
    let mut latencies = Vec::with_capacity(10000);

    for i in 1000..11000i64 {
        let start = Instant::now();
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
        latencies.push(start.elapsed());
    }

    latencies.sort();

    let avg = latencies.iter().map(|d| d.as_nanos()).sum::<u128>() / latencies.len() as u128;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min = latencies[0];
    let max = latencies[latencies.len() - 1];

    println!("Single Point Write Latency (10,000 samples):");
    println!("  Min:    {:>10?}", min);
    println!("  Avg:    {:>10} ns", avg);
    println!("  P50:    {:>10?}", p50);
    println!("  P95:    {:>10?}", p95);
    println!("  P99:    {:>10?}", p99);
    println!("  Max:    {:>10?}", max);
    println!("\nTarget: < 10 µs average");

    assert!(
        avg < 10_000,
        "Average latency {} ns exceeds 10 µs target",
        avg
    );
}

/// Benchmark: Batch write throughput
#[tokio::test]
#[ignore]
async fn benchmark_batch_write_throughput() {
    println!("\n=== Batch Write Throughput Benchmark ===\n");

    let batch_sizes = [100, 1000, 10000, 100000];

    for batch_size in batch_sizes {
        let config = SealConfig {
            max_points: batch_size + 1000,
            max_duration_ms: 3600_000,
            max_size_bytes: 1024 * 1024 * 1024,
        };

        let chunk = Arc::new(ActiveChunk::new(1, batch_size + 1000, config));

        // Create batch
        let batch: Vec<DataPoint> = (0..batch_size)
            .map(|i| DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: (i as f64).sin(),
            })
            .collect();

        // Measure batch write time
        let start = Instant::now();
        chunk.append_batch(batch).unwrap();
        let elapsed = start.elapsed();

        let throughput = batch_size as f64 / elapsed.as_secs_f64();

        println!(
            "Batch size {:>7}: {:>10.0} points/sec ({:>10?})",
            batch_size, throughput, elapsed
        );
    }

    println!("\nTarget: > 1M points/sec for batch operations");
}

/// Benchmark: Sustained write throughput
#[tokio::test]
#[ignore]
async fn benchmark_sustained_write_throughput() {
    println!("\n=== Sustained Write Throughput Benchmark ===\n");

    let duration = Duration::from_secs(10);
    let config = SealConfig {
        max_points: 100_000_000,
        max_duration_ms: 60_000_000,
        max_size_bytes: 10 * 1024 * 1024 * 1024,
    };

    let chunk = Arc::new(ActiveChunk::new(1, 100_000_000, config));
    let start = Instant::now();
    let mut points_written = 0u64;
    let mut timestamp = 0i64;

    while start.elapsed() < duration {
        for _ in 0..10000 {
            if chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp,
                    value: timestamp as f64,
                })
                .is_ok()
            {
                points_written += 1;
                timestamp += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = points_written as f64 / elapsed.as_secs_f64();

    println!(
        "Sustained Write Performance ({:.1}s):",
        elapsed.as_secs_f64()
    );
    println!("  Total points: {}", points_written);
    println!("  Throughput:   {:.0} points/sec", throughput);
    println!(
        "  Memory:       ~{} MB",
        (points_written * 24) / (1024 * 1024)
    );
    println!("\nTarget: > 1M points/sec sustained");

    assert!(
        throughput > 500_000.0,
        "Throughput {:.0} below minimum threshold",
        throughput
    );
}

// =============================================================================
// READ PERFORMANCE BENCHMARKS
// =============================================================================

/// Benchmark: Sequential read throughput
#[tokio::test]
#[ignore]
async fn benchmark_sequential_read_throughput() {
    println!("\n=== Sequential Read Throughput Benchmark ===\n");

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("read_bench.gor");

    // Create test data
    let point_counts = [1000, 10000, 100000];

    for point_count in point_counts {
        // Create and seal chunk
        let mut chunk = Chunk::new_active(1, point_count + 100);

        for i in 0..point_count {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: (i as f64).sin(),
                })
                .unwrap();
        }

        chunk.seal(chunk_path.clone()).await.unwrap();

        // Benchmark read
        let reader = ChunkReader::new();
        let iterations = 100;
        let mut total_points = 0usize;

        let start = Instant::now();
        for _ in 0..iterations {
            let points = reader
                .read_chunk(&chunk_path, QueryOptions::default())
                .await
                .unwrap();
            total_points += points.len();
        }
        let elapsed = start.elapsed();

        let reads_per_sec = iterations as f64 / elapsed.as_secs_f64();
        let points_per_sec = total_points as f64 / elapsed.as_secs_f64();

        println!(
            "Chunk size {:>7}: {:>8.0} reads/sec, {:>12.0} points/sec",
            point_count, reads_per_sec, points_per_sec
        );
    }

    println!("\nTarget: > 10M points/sec read throughput");
}

/// Benchmark: Memory-mapped read performance
#[tokio::test]
#[ignore]
async fn benchmark_mmap_read_performance() {
    println!("\n=== Memory-Mapped Read Performance ===\n");

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("mmap_bench.gor");

    // Create large test chunk
    let mut chunk = Chunk::new_active(1, 100_001);

    for i in 0..100_000 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: (i as f64).sin(),
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Benchmark mmap open time
    let open_iterations = 1000;
    let start = Instant::now();

    for _ in 0..open_iterations {
        let _mmap = MmapChunk::open(&chunk_path).unwrap();
    }

    let open_elapsed = start.elapsed();
    let open_latency = open_elapsed / open_iterations as u32;

    println!("MmapChunk::open() latency: {:?}", open_latency);

    // Benchmark decompress time
    let mmap = MmapChunk::open(&chunk_path).unwrap();
    let decompress_iterations = 100;

    let start = Instant::now();
    for _ in 0..decompress_iterations {
        let _points = mmap.decompress().await.unwrap();
    }
    let decompress_elapsed = start.elapsed();

    let decompress_latency = decompress_elapsed / decompress_iterations as u32;
    let points_per_sec =
        (100_000 * decompress_iterations) as f64 / decompress_elapsed.as_secs_f64();

    println!("Decompress latency (100k points): {:?}", decompress_latency);
    println!("Decompress throughput: {:.0} points/sec", points_per_sec);

    // Benchmark zero-copy header access
    let header_iterations = 1_000_000;
    let start = Instant::now();

    for _ in 0..header_iterations {
        let _ = mmap.header();
    }

    let header_elapsed = start.elapsed();
    let header_latency = header_elapsed.as_nanos() / header_iterations as u128;

    println!("Header access latency: {} ns", header_latency);

    println!("\nTargets:");
    println!("  - Open latency: < 100 µs");
    println!("  - Header access: < 100 ns");
    println!("  - Decompress: > 10M points/sec");
}

/// Benchmark: Parallel read scaling
#[tokio::test]
#[ignore]
async fn benchmark_parallel_read_scaling() {
    println!("\n=== Parallel Read Scaling Benchmark ===\n");

    let temp_dir = TempDir::new().unwrap();

    // Create 10 chunks
    let mut paths = vec![];
    for i in 0..10 {
        let chunk_path = temp_dir.path().join(format!("parallel_{}.gor", i));

        let mut chunk = Chunk::new_active(1, 10001);
        for j in 0..10000 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: (i * 10000 + j) as i64,
                    value: j as f64,
                })
                .unwrap();
        }

        chunk.seal(chunk_path.clone()).await.unwrap();
        paths.push(chunk_path);
    }

    let reader = ChunkReader::new();

    // Benchmark different parallelism levels
    for num_chunks in [1, 2, 5, 10] {
        let test_paths: Vec<_> = paths.iter().take(num_chunks).cloned().collect();
        let iterations = 50;

        let start = Instant::now();
        for _ in 0..iterations {
            let _points = reader
                .read_chunks_parallel(test_paths.clone(), QueryOptions::default())
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();

        let reads_per_sec = (iterations * num_chunks) as f64 / elapsed.as_secs_f64();
        let points_per_sec = (iterations * num_chunks * 10000) as f64 / elapsed.as_secs_f64();

        println!(
            "{:>2} chunks: {:>8.0} chunk-reads/sec, {:>12.0} points/sec",
            num_chunks, reads_per_sec, points_per_sec
        );
    }

    println!("\nTarget: Near-linear scaling with chunk count");
}

// =============================================================================
// COMPRESSION BENCHMARKS
// =============================================================================

/// Benchmark: Compression throughput
#[tokio::test]
#[ignore]
async fn benchmark_compression_throughput() {
    println!("\n=== Compression Throughput Benchmark ===\n");

    let compressor = GorillaCompressor::new();
    let point_counts = [100, 1000, 10000, 100000];

    for point_count in point_counts {
        // Create test data
        let points: Vec<DataPoint> = (0..point_count)
            .map(|i| DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: (i as f64).sin() * 100.0,
            })
            .collect();

        // Benchmark compression
        let iterations = if point_count < 10000 { 1000 } else { 100 };

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = compressor.compress(&points).await.unwrap();
        }
        let compress_elapsed = start.elapsed();

        let compress_throughput =
            (point_count * iterations) as f64 / compress_elapsed.as_secs_f64();

        // Benchmark decompression
        let compressed = compressor.compress(&points).await.unwrap();

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = compressor.decompress(&compressed).await.unwrap();
        }
        let decompress_elapsed = start.elapsed();

        let decompress_throughput =
            (point_count * iterations) as f64 / decompress_elapsed.as_secs_f64();

        // Calculate compression ratio
        let original_size = point_count * 24; // timestamp (8) + value (8) + series_id (8)
        let compressed_size = compressed.data.len();
        let ratio = original_size as f64 / compressed_size as f64;

        println!("{:>7} points:", point_count);
        println!(
            "  Compress:   {:>12.0} points/sec ({:>8.2?}/iter)",
            compress_throughput,
            compress_elapsed / iterations as u32
        );
        println!(
            "  Decompress: {:>12.0} points/sec ({:>8.2?}/iter)",
            decompress_throughput,
            decompress_elapsed / iterations as u32
        );
        println!(
            "  Ratio:      {:>12.2}x ({} -> {} bytes)",
            ratio, original_size, compressed_size
        );
        println!();
    }

    println!("Targets:");
    println!("  - Compression: > 1M points/sec");
    println!("  - Decompression: > 10M points/sec");
    println!("  - Compression ratio: > 5x for typical data");
}

/// Benchmark: Compression with different data patterns
#[tokio::test]
#[ignore]
async fn benchmark_compression_patterns() {
    println!("\n=== Compression Pattern Analysis ===\n");

    let compressor = GorillaCompressor::new();
    let point_count = 10000;

    let patterns: Vec<(&str, Vec<DataPoint>)> = vec![
        (
            "Constant",
            (0..point_count)
                .map(|i| DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: 42.0,
                })
                .collect(),
        ),
        (
            "Sequential",
            (0..point_count)
                .map(|i| DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: i as f64,
                })
                .collect(),
        ),
        (
            "Sine wave",
            (0..point_count)
                .map(|i| DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: (i as f64 * 0.1).sin() * 100.0,
                })
                .collect(),
        ),
        (
            "Random-ish",
            (0..point_count)
                .map(|i| DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: ((i * 31337) % 10000) as f64 / 100.0,
                })
                .collect(),
        ),
        (
            "High precision",
            (0..point_count)
                .map(|i| DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: (i as f64).sqrt() * 1e-10,
                })
                .collect(),
        ),
    ];

    println!(
        "{:<15} {:>10} {:>15} {:>15} {:>10}",
        "Pattern", "Orig Size", "Compressed", "Ratio", "Bits/pt"
    );
    println!("{}", "-".repeat(70));

    for (name, points) in patterns {
        let original_size = point_count * 24;
        let compressed = compressor.compress(&points).await.unwrap();
        let compressed_size = compressed.data.len();
        let ratio = original_size as f64 / compressed_size as f64;
        let bits_per_point = (compressed_size * 8) as f64 / point_count as f64;

        println!(
            "{:<15} {:>10} {:>15} {:>15.2}x {:>10.1}",
            name, original_size, compressed_size, ratio, bits_per_point
        );
    }

    println!("\nNote: Gorilla compression works best with slowly-changing values");
}

// =============================================================================
// SEAL PERFORMANCE BENCHMARKS
// =============================================================================

/// Benchmark: Seal operation latency
#[tokio::test]
#[ignore]
async fn benchmark_seal_latency() {
    println!("\n=== Seal Operation Latency Benchmark ===\n");

    let temp_dir = TempDir::new().unwrap();
    let point_counts = [100, 1000, 10000, 100000];

    for point_count in point_counts {
        let iterations = if point_count >= 100000 { 10 } else { 50 };
        let mut latencies = Vec::with_capacity(iterations);

        for iter in 0..iterations {
            let chunk_path = temp_dir
                .path()
                .join(format!("seal_bench_{}_{}.gor", point_count, iter));

            let mut chunk = Chunk::new_active(1, point_count + 100);

            for i in 0..point_count {
                chunk
                    .append(DataPoint {
                        series_id: 1,
                        timestamp: i as i64,
                        value: (i as f64).sin(),
                    })
                    .unwrap();
            }

            let start = Instant::now();
            chunk.seal(chunk_path).await.unwrap();
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().map(|d| d.as_micros()).sum::<u128>() / latencies.len() as u128;
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];

        println!(
            "{:>7} points: avg {:>8} µs, p50 {:>10?}, p99 {:>10?}",
            point_count, avg, p50, p99
        );
    }

    println!("\nNote: Seal includes compression + disk I/O");
}

// =============================================================================
// MEMORY BENCHMARKS
// =============================================================================

/// Benchmark: Memory efficiency
#[tokio::test]
#[ignore]
async fn benchmark_memory_efficiency() {
    println!("\n=== Memory Efficiency Benchmark ===\n");

    let config = SealConfig {
        max_points: 10_000_001,
        max_duration_ms: 3600_000,
        max_size_bytes: 1024 * 1024 * 1024,
    };

    let point_counts = [10000, 100000, 1000000, 10000000];

    for point_count in point_counts {
        let chunk = Arc::new(ActiveChunk::new(1, point_count + 100, config.clone()));

        // Fill chunk
        for i in 0..point_count {
            if chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp: i as i64,
                    value: (i as f64).sin(),
                })
                .is_err()
            {
                break;
            }
        }

        let actual_points = chunk.point_count() as usize;

        // Theoretical minimum: 8 (timestamp) + 8 (value) + 16 (series_id) = 32 bytes/point
        // Actual includes BTreeMap overhead, RwLock, etc.
        let theoretical_bytes = actual_points * 32;

        // Estimate actual memory (this is approximate)
        // BTreeMap typically uses ~50-60 bytes per entry due to tree structure
        let estimated_actual = actual_points * 56; // Rough estimate

        let overhead_ratio = estimated_actual as f64 / theoretical_bytes as f64;

        println!(
            "{:>10} points: ~{:>6} MB (theoretical: {:>6} MB, overhead: ~{:.1}x)",
            actual_points,
            estimated_actual / (1024 * 1024),
            theoretical_bytes / (1024 * 1024),
            overhead_ratio
        );
    }

    println!("\nNote: BTreeMap adds ~75% overhead for tree structure");
}

// =============================================================================
// SUMMARY BENCHMARK
// =============================================================================

/// Run all benchmarks and produce summary report
#[tokio::test]
#[ignore]
async fn benchmark_summary_report() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║           STORAGE LAYER PERFORMANCE BENCHMARK SUMMARY              ║");
    println!("╚════════════════════════════════════════════════════════════════════╝");
    println!();

    // Quick benchmark runs
    let temp_dir = TempDir::new().unwrap();

    // Write throughput
    let config = SealConfig {
        max_points: 1_000_001,
        max_duration_ms: 3600_000,
        max_size_bytes: 1024 * 1024 * 1024,
    };
    let chunk = Arc::new(ActiveChunk::new(1, 1_000_001, config));

    let start = Instant::now();
    for i in 0..1_000_000i64 {
        let _ = chunk.append(DataPoint {
            series_id: 1,
            timestamp: i,
            value: i as f64,
        });
    }
    let write_elapsed = start.elapsed();
    let write_throughput = 1_000_000.0 / write_elapsed.as_secs_f64();

    // Seal performance
    let chunk_path = temp_dir.path().join("summary_test.gor");
    let mut chunk = Chunk::new_active(1, 100_001);
    for i in 0..100_000 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    let start = Instant::now();
    chunk.seal(chunk_path.clone()).await.unwrap();
    let seal_elapsed = start.elapsed();

    // Read performance
    let start = Instant::now();
    let loaded = Chunk::read(chunk_path.clone()).await.unwrap();
    let _ = loaded.decompress().await.unwrap();
    let read_elapsed = start.elapsed();
    let read_throughput = 100_000.0 / read_elapsed.as_secs_f64();

    // Compression stats
    let compressor = GorillaCompressor::new();
    let points: Vec<DataPoint> = (0..10000)
        .map(|i| DataPoint {
            series_id: 1,
            timestamp: i as i64,
            value: (i as f64).sin() * 100.0,
        })
        .collect();
    let compressed = compressor.compress(&points).await.unwrap();
    let ratio = (10000.0 * 24.0) / compressed.data.len() as f64;

    println!("┌─────────────────────────────────────────────────────────────────────┐");
    println!("│ WRITE PERFORMANCE                                                   │");
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!(
        "│   Single-threaded throughput:  {:>15.0} points/sec         │",
        write_throughput
    );
    println!(
        "│   Target:                      {:>15} points/sec         │",
        "1,000,000"
    );
    println!(
        "│   Status:                      {:>15}                    │",
        if write_throughput > 1_000_000.0 {
            "✓ PASS"
        } else {
            "✗ FAIL"
        }
    );
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!("│ SEAL PERFORMANCE                                                    │");
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!(
        "│   100k points seal time:       {:>15?}                  │",
        seal_elapsed
    );
    println!(
        "│   Effective throughput:        {:>15.0} points/sec         │",
        100_000.0 / seal_elapsed.as_secs_f64()
    );
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!("│ READ PERFORMANCE                                                    │");
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!(
        "│   Read + decompress throughput: {:>14.0} points/sec         │",
        read_throughput
    );
    println!(
        "│   Target:                       {:>14} points/sec         │",
        "10,000,000"
    );
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!("│ COMPRESSION                                                         │");
    println!("├─────────────────────────────────────────────────────────────────────┤");
    println!(
        "│   Compression ratio:           {:>15.2}x                     │",
        ratio
    );
    println!(
        "│   Target:                      {:>15}x                     │",
        "> 5"
    );
    println!(
        "│   Status:                      {:>15}                    │",
        if ratio > 5.0 { "✓ PASS" } else { "✗ FAIL" }
    );
    println!("└─────────────────────────────────────────────────────────────────────┘");
    println!();

    println!("Run individual benchmarks with:");
    println!(
        "  cargo test --release --test performance_benchmarks -- --ignored --nocapture <name>"
    );
}
