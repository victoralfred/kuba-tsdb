use criterion::{black_box, criterion_group, criterion_main, Criterion};

// Placeholder for ingestion benchmarks
fn bench_ingestion(c: &mut Criterion) {
    c.bench_function("ingestion_placeholder", |b| b.iter(|| black_box(1 + 1)));
}

criterion_group!(benches, bench_ingestion);
criterion_main!(benches);
