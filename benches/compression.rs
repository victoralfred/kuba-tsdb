use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use gorilla_tsdb::compression::GorillaCompressor;
use gorilla_tsdb::engine::traits::Compressor;
use gorilla_tsdb::types::DataPoint;

fn create_regular_points(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(
            1,
            1000 + (i as i64 * 10),
            100.0 + (i as f64 * 0.5),
        ))
        .collect()
}

fn bench_compression(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("compression");

    for size in [100, 1000, 10000].iter() {
        let points = create_regular_points(*size);
        let compressor = GorillaCompressor::new();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(compressor.compress(&points).await.unwrap())
                })
            });
        });
    }

    group.finish();
}

fn bench_decompression(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("decompression");

    for size in [100, 1000, 10000].iter() {
        let points = create_regular_points(*size);
        let compressor = GorillaCompressor::new();
        let compressed = rt.block_on(compressor.compress(&points)).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(compressor.decompress(&compressed).await.unwrap())
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_compression, bench_decompression);
criterion_main!(benches);
