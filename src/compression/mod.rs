//! Compression implementations
//!
//! This module provides compression algorithms for time-series data:
//!
//! - **Kuba**: The default XOR-based compression (Facebook Gorilla algorithm)
//! - **AHPAC**: Adaptive compression that selects the best codec per chunk
//! - **SIMD**: Vectorized operations for batch processing
//! - **Parallel**: Multi-threaded compression for high throughput
//! - **ANS**: Asymmetric Numeral Systems entropy coding for near-optimal compression
//! - **Metrics**: Compression performance monitoring and observability
//! - **Algorithm Selector**: Intelligent selection of compression algorithms

pub mod ahpac;
/// Algorithm selection based on data characteristics
pub mod algorithm_selector;
/// ANS (Asymmetric Numeral Systems) entropy coding
pub mod ans;
pub mod bit_stream;
pub mod kuba;
/// Compression metrics and observability
pub mod metrics;
/// Parallel compression utilities for multi-series batch processing
pub mod parallel;
/// SIMD-accelerated compression operations
pub mod simd;

pub use ahpac::AhpacCompressor;
pub use algorithm_selector::{
    AlgorithmSelector, AlgorithmStats, AlgorithmStatsSnapshot, BenchmarkResult,
    ChunkCharacteristics, CompressionBenchmark, SelectionConfig,
};
pub use ans::{
    compress as ans_compress, decompress as ans_decompress, AnsDecoder, AnsEncoder, AnsError,
    AnsStats,
};
pub use kuba::KubaCompressor;
pub use metrics::{
    global_metrics, global_sealing_metrics, CodecMetrics, CodecType, CompressionMetrics,
    CompressionMetricsSnapshot, CompressionTimer, Histogram, HistogramSnapshot, SealingMetrics,
    SealingMetricsSnapshot, COMPRESSION_RATIO_BUCKETS, SEAL_DURATION_BUCKETS,
};
pub use parallel::{ParallelCompressionResult, ParallelCompressor, ParallelConfig};
pub use simd::{
    analyze_xor_deltas, compute_timestamp_dod, decode_timestamp_dod, xor_decode_batch,
    xor_encode_batch, XorStats,
};
