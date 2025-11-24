//! Performance benchmarks for memory-mapped chunk access
//!
//! Compares traditional I/O vs memory-mapped reads to validate
//! the performance claims in the implementation.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use gorilla_tsdb::storage::chunk::Chunk;
use gorilla_tsdb::storage::mmap::MmapChunk;
use gorilla_tsdb::types::DataPoint;
use std::path::PathBuf;
use tempfile::TempDir;

/// Create a test chunk with specified number of points
async fn create_test_chunk(points: usize) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("bench_chunk.gor");

    let mut chunk = Chunk::new_active(1, points);
    for i in 0..points {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64 * 1000,
                value: (i as f64 * 1.5).sin(), // Realistic values
            })
            .unwrap();
    }

    chunk.seal(path.clone()).await.unwrap();
    (temp_dir, path)
}

/// Benchmark: Traditional I/O vs Memory-Mapped reads
fn bench_read_methods(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_comparison");

    for size in [100, 1000, 10000] {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_temp_dir, path) = rt.block_on(create_test_chunk(size));

        // Calculate throughput (points per second)
        group.throughput(Throughput::Elements(size as u64));

        // Benchmark traditional I/O (read header, read data, decompress)
        group.bench_with_input(BenchmarkId::new("traditional", size), &path, |b, path| {
            let path = path.clone();
            b.iter(|| {
                rt.block_on(async {
                    let chunk = Chunk::read(path.clone()).await.unwrap();
                    let points = chunk.decompress().await.unwrap();
                    black_box(points);
                })
            });
        });

        // Benchmark memory-mapped (open, decompress)
        group.bench_with_input(BenchmarkId::new("mmap", size), &path, |b, path| {
            let path = path.clone();
            b.iter(|| {
                rt.block_on(async {
                    let chunk = MmapChunk::open(&path).unwrap();
                    let points = chunk.decompress().await.unwrap();
                    black_box(points);
                })
            });
        });
    }

    group.finish();
}

/// Benchmark: Concurrent access scalability
fn bench_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_temp_dir, path) = rt.block_on(create_test_chunk(10000));

    for threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements((10000 * threads) as u64));

        group.bench_with_input(
            BenchmarkId::new("mmap_threads", threads),
            &threads,
            |b, &threads| {
                let path = path.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = vec![];
                        for _ in 0..threads {
                            let path = path.clone();
                            let handle = tokio::spawn(async move {
                                let chunk = MmapChunk::open(&path).unwrap();
                                let points = chunk.decompress().await.unwrap();
                                black_box(points);
                            });
                            handles.push(handle);
                        }
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Header access (cached vs re-parsing)
fn bench_header_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("header_access");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_temp_dir, path) = rt.block_on(create_test_chunk(10000));

    // Benchmark traditional (re-reads and re-parses header every time)
    group.bench_function("traditional_reread", |b| {
        let path = path.clone();
        b.iter(|| {
            rt.block_on(async {
                let chunk = Chunk::read(path.clone()).await.unwrap();
                let header = &chunk.metadata;
                black_box(header.point_count);
            })
        });
    });

    // Benchmark mmap (cached header, zero-cost access)
    group.bench_function("mmap_cached", |b| {
        let chunk = MmapChunk::open(&path).unwrap();
        b.iter(|| {
            let header = chunk.header();
            black_box(header.point_count);
        });
    });

    group.finish();
}

/// Benchmark: Zero-copy data access
fn bench_data_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_access");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_temp_dir, path) = rt.block_on(create_test_chunk(10000));
    let chunk = MmapChunk::open(&path).unwrap();

    group.throughput(Throughput::Bytes(chunk.size()));

    // Benchmark compressed data access (zero-copy)
    group.bench_function("compressed_data_zero_copy", |b| {
        b.iter(|| {
            let data = chunk.compressed_data();
            black_box(data.len());
        });
    });

    group.finish();
}

/// Benchmark: Prefetch and evict operations
fn bench_cache_management(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_management");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_temp_dir, path) = rt.block_on(create_test_chunk(10000));
    let chunk = MmapChunk::open(&path).unwrap();

    // Benchmark prefetch (madvise WILLNEED)
    group.bench_function("prefetch", |b| {
        b.iter(|| {
            chunk.prefetch().unwrap();
        });
    });

    // Benchmark evict (madvise DONTNEED)
    group.bench_function("evict", |b| {
        b.iter(|| {
            chunk.evict().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_read_methods,
    bench_concurrent_access,
    bench_header_access,
    bench_data_access,
    bench_cache_management
);
criterion_main!(benches);
