[![Rust CI/CD](https://github.com/victoralfred/gorilla-tsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/victoralfred/gorilla-tsdb/actions/workflows/rust.yml)
# Gorilla TSDB

This project is a high-performance, production-ready time-series database written in Rust, featuring Facebook's Gorilla compression algorithm, pluggable engine architecture, Redis-based indexing, and async I/O for extreme throughput.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Performance](#performance)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Implementation Status](#implementation-status)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [References](#references)

## Overview

Gorilla TSDB is designed for monitoring, IoT, and observability workloads where high ingestion rates and efficient compression are critical. The database implements Facebook's Gorilla compression algorithm, achieving 10:1+ compression ratios on time-series data while maintaining sub-millisecond query latencies.

### Project Goals

1. **Extreme Performance**: Achieve >2 million points/second ingestion with sub-millisecond p99 query latency
2. **Efficient Storage**: Implement Gorilla compression for 10:1+ compression ratios on typical metrics data
3. **Production Ready**: Comprehensive error handling, monitoring, security hardening, and graceful shutdown
4. **Extensibility**: Pluggable architecture allowing custom compression, storage, and indexing backends
5. **Simplicity**: Clean, modular codebase following SOLID principles with extensive documentation

### Use Cases

- **Infrastructure Monitoring**: CPU, memory, disk, and network metrics from servers and containers
- **Application Metrics**: Request latencies, error rates, throughput measurements
- **IoT Data Collection**: Sensor readings from thousands of devices
- **Financial Data**: Stock prices, trading volumes, market indicators
- **Scientific Instrumentation**: Lab equipment measurements and experimental data

## Key Features

### Gorilla Compression

Implementation of Facebook's Gorilla algorithm optimized for time-series data:

- **Delta-of-Delta Timestamp Encoding**: Exploits the regular intervals in time-series data to compress timestamps to just a few bits
- **XOR-based Value Compression**: Uses XOR encoding for floating-point values, achieving high compression when consecutive values are similar
- **Compression Ratios**: Achieves 10:1 to 12:1 compression on real-world metrics data
- **Speed**: Compression at 27M points/second, decompression at 14.5M points/second

### Pluggable Engine Architecture

All major components are defined as traits with `Arc<dyn Trait + Send + Sync>` for maximum flexibility:

```rust
// Core traits for extensibility
pub trait Compressor: Send + Sync + 'static {
    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock>;
    async fn decompress(&self, block: &CompressedBlock) -> Result<Vec<DataPoint>>;
}

pub trait StorageEngine: Send + Sync + 'static {
    async fn write_chunk(&self, series_id: u128, chunk_id: ChunkId, data: &CompressedBlock) -> Result<ChunkLocation>;
    async fn read_chunk(&self, location: &ChunkLocation) -> Result<CompressedBlock>;
}

pub trait TimeIndex: Send + Sync + 'static {
    async fn add_chunk(&self, series_id: u128, chunk_id: ChunkId, time_range: TimeRange) -> Result<()>;
    async fn query_chunks(&self, series_id: u128, time_range: TimeRange) -> Result<Vec<ChunkReference>>;
}
```

**Available Implementations**:
- **Compressors**: GorillaCompressor (production), ParquetCompressor (planned)
- **Storage Engines**: LocalDiskEngine (production), S3Engine (planned)
- **Time Indexes**: RedisTimeIndex (production)

### Storage Layer

Chunk-based storage with advanced features:

- **64-Byte Binary Headers**: Magic number validation, versioning, checksums
- **Memory-Mapped Files**: Zero-copy reads with madvise hints for optimal performance
- **Active/Sealed Chunk Model**: In-memory writes with background sealing
- **Background Compression**: Automatic Snappy compression for cold data
- **Multi-Tier Cache**: Sharded LRU cache with watermark-based eviction
- **Crash Recovery**: Atomic writes with checksum verification

### Redis Integration

Full-featured Redis-based time-series indexing:

- **Sorted Set Time Index**: O(log n) time-range queries using ZRANGEBYSCORE
- **Connection Pooling**: Multiplexed connections with semaphore-based limiting
- **Lua Scripts**: Atomic multi-key operations for consistency
- **Write Buffer**: Redis-backed crash recovery buffer
- **Failover Support**: Automatic failover with local cache fallback
- **Query Optimization**: Result caching with TTL and LRU eviction

### Production Features

- **Prometheus Metrics**: 20+ metrics covering all critical operations
- **Security Hardening**: Path validation, rate limiting, input sanitization
- **TOML Configuration**: Environment variable overrides, hot-reload support
- **Graceful Shutdown**: In-flight operation completion, resource cleanup
- **Comprehensive Logging**: Structured logging with tracing

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Client Applications                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         TimeSeriesDB (Builder Pattern)                   │
│  ┌─────────────┐  ┌─────────────────┐  ┌──────────────────────────────┐ │
│  │ Compressor  │  │  StorageEngine  │  │         TimeIndex            │ │
│  │  (trait)    │  │    (trait)      │  │         (trait)              │ │
│  └─────────────┘  └─────────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
        │                    │                        │
        ▼                    ▼                        ▼
┌───────────────┐  ┌─────────────────────┐  ┌──────────────────────────┐
│   Gorilla     │  │   LocalDiskEngine   │  │     RedisTimeIndex       │
│  Compressor   │  │                     │  │                          │
│               │  │  ┌───────────────┐  │  │  ┌────────────────────┐  │
│ Delta-of-Delta│  │  │ Active Chunks │  │  │  │ Connection Pool    │  │
│ XOR Encoding  │  │  │ (In-Memory)   │  │  │  │ (Multiplexed)      │  │
│               │  │  └───────────────┘  │  │  └────────────────────┘  │
│               │  │          │          │  │           │              │
│               │  │          ▼          │  │           ▼              │
│               │  │  ┌───────────────┐  │  │  ┌────────────────────┐  │
│               │  │  │ Sealed Chunks │  │  │  │ Sorted Sets (ZSET) │  │
│               │  │  │ (Mmap Files)  │  │  │  │ ts:series:{id}:idx │  │
│               │  │  └───────────────┘  │  │  └────────────────────┘  │
│               │  │          │          │  │           │              │
│               │  │          ▼          │  │           ▼              │
│               │  │  ┌───────────────┐  │  │  ┌────────────────────┐  │
│               │  │  │ Compressed    │  │  │  │ Lua Scripts        │  │
│               │  │  │ (Snappy)      │  │  │  │ (Atomic Ops)       │  │
│               │  │  └───────────────┘  │  │  └────────────────────┘  │
└───────────────┘  └─────────────────────┘  └──────────────────────────┘
```

### Data Flow

1. **Write Path**:
   - Data points arrive via ingestion API
   - Points buffered in ActiveChunk (BTreeMap for ordering)
   - When thresholds hit, chunk sealed and compressed
   - Compressed data written to disk with checksum
   - Chunk metadata indexed in Redis

2. **Read Path**:
   - Query specifies time range and series
   - Redis returns relevant chunk references
   - Chunks loaded via memory-mapped files (zero-copy)
   - Data decompressed and filtered
   - Results merged and returned

### Chunk Lifecycle

```
  Active           Sealed           Compressed
┌─────────┐     ┌─────────┐      ┌─────────────┐
│In-Memory│ ──▶ │  .gor   │ ──▶  │.gor.snappy  │
│BTreeMap │     │  file   │      │   file      │
└─────────┘     └─────────┘      └─────────────┘
   Write          Read-Only        Archival
   Buffer         Mmap Access      Storage
```

## Performance

### Benchmarks

| Operation | Throughput | Latency (p99) |
|-----------|------------|---------------|
| Write (single point) | 3.1M pts/sec | <10 µs |
| Write (batched) | 1.67M pts/sec sustained | <100 µs |
| Read (mmap) | 14.5M pts/sec | <1 ms |
| Compression | 27M pts/sec | - |
| Decompression | 14.5M pts/sec | - |
| Concurrent writes (16 threads) | 3.09M pts/sec | - |
| Seal operations | 3,712/sec | - |

### Compression Ratios

| Data Pattern | Compression Ratio |
|--------------|-------------------|
| Regular intervals | 12:1 |
| Metrics data | 10:1 |
| Variable intervals | 8:1 |
| Random data | 2:1 |

### Resource Usage

- **Memory**: ~22 bytes per point in active chunks
- **CPU**: Near-linear scaling with concurrent threads
- **Disk**: Depends on compression ratio (typically 10x reduction)

## Installation

### Prerequisites

- Rust 1.70 or later
- Redis 6.0 or later (for index backend)
- Linux (recommended) or macOS

### Building from Source

```bash
# Clone the repository
git clone https://github.com/victoralfred/gorilla-tsdb.git
cd gorilla-tsdb/source

# Build in release mode
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

### Features

```toml
[dependencies]
gorilla-tsdb = { version = "0.1.0", features = ["redis-tls"] }
```

Available features:
- `default`: Core functionality
- `redis-tls`: TLS support for Redis connections using rustls

## Quick Start

### Basic Usage

```rust
use gorilla_tsdb::{TimeSeriesDBBuilder, DatabaseConfig, DataPoint};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure database
    let config = DatabaseConfig {
        data_dir: PathBuf::from("/var/lib/tsdb"),
        redis_url: Some("redis://localhost:6379".to_string()),
        ..Default::default()
    };

    // Build database with default engines
    let db = TimeSeriesDBBuilder::new()
        .with_config(config)
        .build()
        .await?;

    // Write data points
    let point = DataPoint {
        series_id: 12345,
        timestamp: 1700000000000, // milliseconds
        value: 42.5,
    };

    db.write(point).await?;

    // Query data
    let results = db.query(12345, TimeRange::new(
        1700000000000,
        1700000001000
    )).await?;

    Ok(())
}
```

### Custom Engine Configuration

```rust
use gorilla_tsdb::{
    TimeSeriesDBBuilder,
    compression::GorillaCompressor,
    storage::LocalDiskEngine,
    redis::RedisTimeIndex,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create custom engines
    let compressor = GorillaCompressor::default();
    let storage = LocalDiskEngine::new("/data/tsdb");
    let index = RedisTimeIndex::new("redis://localhost:6379").await?;

    // Build with custom engines
    let db = TimeSeriesDBBuilder::new()
        .with_compressor(compressor)
        .with_storage(storage)
        .with_index(index)
        .build()
        .await?;

    Ok(())
}
```

### Using the ChunkWriter

```rust
use gorilla_tsdb::storage::{ChunkWriter, ChunkWriterConfig};

// Create writer with custom config
let config = ChunkWriterConfig {
    max_points: 10_000,           // Seal after 10K points
    max_duration: Duration::from_secs(3600), // Or after 1 hour
    auto_seal: true,
    ..Default::default()
};

let writer = ChunkWriter::new(series_id, "/data/chunks", config);

// Write points
writer.write(point).await?;

// Or batch write for better performance
writer.write_batch(&points).await?;

// Get statistics
let stats = writer.stats().await;
println!("Points written: {}", stats.points_written);
```

### Reading with ChunkReader

```rust
use gorilla_tsdb::storage::{ChunkReader, QueryOptions};

let reader = ChunkReader::new();

// Query with filters
let options = QueryOptions {
    start_time: Some(1700000000000),
    end_time: Some(1700001000000),
    limit: Some(1000),
    use_mmap: true,
};

// Single chunk read
let points = reader.read_chunk("/data/chunk_001.gor", options).await?;

// Parallel multi-chunk read
let chunks = vec!["/data/chunk_001.gor", "/data/chunk_002.gor"];
let all_points = reader.read_chunks_parallel(chunks, options).await?;
```

## Configuration

### TOML Configuration File

```toml
[storage]
base_path = "/var/lib/tsdb"
chunk_size_limit = 1048576      # 1MB
chunk_time_limit = 3600         # 1 hour
chunk_point_limit = 10000
compression_enabled = true
compression_delay = 300         # 5 minutes
cache_size_mb = 512

[redis]
url = "redis://localhost:6379"
pool_size = 16
connection_timeout_secs = 5
command_timeout_secs = 1
health_check_interval_secs = 30
tls_enabled = false

[redis.retry]
max_retries = 3
base_delay_ms = 100
max_delay_ms = 10000

[redis.failover]
health_check_interval_ms = 5000
failure_threshold = 3
recovery_threshold = 2
local_cache_size_mb = 128
cache_ttl_secs = 300

[compression]
min_age_seconds = 3600          # Compress after 1 hour
worker_count = 4                # Auto-detect if 0
scan_interval_seconds = 300
max_chunks_per_scan = 100

[cache]
max_size_bytes = 268435456      # 256MB
num_shards = 16
low_watermark = 0.7
high_watermark = 0.9
eviction_interval_ms = 1000

[security]
rate_limit_points_per_sec = 10000000
max_series_id = 18446744073709551615
path_validation_enabled = true
```

### Environment Variables

All configuration options can be overridden via environment variables:

```bash
export TSDB_STORAGE_BASE_PATH=/data/tsdb
export TSDB_REDIS_URL=redis://redis-cluster:6379
export TSDB_CACHE_MAX_SIZE_BYTES=536870912  # 512MB
```

## API Reference

### Core Types

```rust
/// A single data point in a time series
pub struct DataPoint {
    pub series_id: SeriesId,    // u128 series identifier
    pub timestamp: i64,          // Milliseconds since epoch
    pub value: f64,              // The metric value
}

/// Time range for queries
pub struct TimeRange {
    pub start: i64,              // Inclusive start
    pub end: i64,                // Inclusive end
}

/// Compressed data block
pub struct CompressedBlock {
    pub algorithm_id: String,
    pub original_size: usize,
    pub compressed_size: usize,
    pub checksum: u64,
    pub data: Bytes,
    pub metadata: BlockMetadata,
}
```

### Storage Types

```rust
/// 64-byte chunk header
pub struct ChunkHeader {
    pub magic: u32,              // 0x474F5249 ("GORI")
    pub version: u16,            // Format version
    pub series_id: SeriesId,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub point_count: u32,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub checksum: u64,
    pub compression_type: CompressionType,
    pub flags: ChunkFlags,
}

/// Compression types
pub enum CompressionType {
    None = 0,
    Gorilla = 1,
    Snappy = 2,
    GorillaSnappy = 3,
}
```

## Project Structure

```
/home/voseghale/projects/db/
├── README.md                    # This file
├── CLAUDE.md                    # AI assistant instructions
├── docs/                        # Documentation
│   ├── 00-prototype-overview.md # Project overview
│   ├── ultrathink-plan.md       # Architecture deep dive
│   ├── completed/               # Completed phase documentation
│   │   ├── 01-gorilla-compression.md
│   │   ├── 02-storage-layer.md
│   │   └── 03-redis-integration.md
│   ├── 04-async-ingestion.md    # Future phases
│   ├── 05-query-engine.md
│   └── ...
└── source/                      # Git repository (Rust code)
    ├── Cargo.toml
    ├── src/
    │   ├── lib.rs               # Library entry point
    │   ├── types.rs             # Core data types
    │   ├── error.rs             # Error definitions
    │   ├── compression/         # Gorilla compression
    │   │   ├── mod.rs
    │   │   ├── bit_stream.rs    # Bit-level I/O
    │   │   └── gorilla.rs       # Gorilla algorithm
    │   ├── storage/             # Chunk storage
    │   │   ├── mod.rs
    │   │   ├── chunk.rs         # Chunk structures
    │   │   ├── local_disk.rs    # Disk storage engine
    │   │   ├── active_chunk.rs  # In-memory buffer
    │   │   ├── mmap.rs          # Memory-mapped files
    │   │   ├── writer.rs        # ChunkWriter
    │   │   ├── reader.rs        # ChunkReader
    │   │   ├── compressor.rs    # Background compression
    │   │   ├── cache.rs         # Multi-tier cache
    │   │   └── directory.rs     # Directory management
    │   ├── redis/               # Redis integration
    │   │   ├── mod.rs
    │   │   ├── connection.rs    # Connection pooling
    │   │   ├── index.rs         # RedisTimeIndex
    │   │   ├── scripts.rs       # Lua scripts
    │   │   ├── series.rs        # Series management
    │   │   ├── query.rs         # Query optimization
    │   │   ├── buffer.rs        # Write buffer
    │   │   ├── failover.rs      # High availability
    │   │   └── metrics.rs       # Redis metrics
    │   ├── engine/              # Pluggable engines
    │   │   ├── mod.rs
    │   │   ├── traits.rs        # Engine traits
    │   │   └── builder.rs       # Database builder
    │   ├── config.rs            # Configuration
    │   ├── metrics.rs           # Prometheus metrics
    │   └── security.rs          # Security hardening
    ├── tests/                   # Integration tests
    ├── benches/                 # Benchmarks
    └── examples/                # Usage examples
```

## Implementation Status

### Completed Phases

| Phase | Component | Status | Lines of Code |
|-------|-----------|--------|---------------|
| 1 | Gorilla Compression | Complete | ~1,200 |
| 1.5 | Bug Fixes & Hardening | Complete | +200 |
| 2 | Storage Layer | Complete | ~4,400 |
| 3 | Redis Integration | Complete | ~4,600 |

### Test Coverage

- **Total Tests**: 352+ tests
- **Pass Rate**: 100%
- **Categories**:
  - Unit tests: 140
  - Doc tests: 58
  - Integration tests: 100+
  - Stress tests: 7
  - Performance benchmarks: 11

### Remaining Phases (Planned)

| Phase | Component | Estimated Hours |
|-------|-----------|----------------|
| 4 | Async Ingestion Pipeline | ~60 |
| 5 | Query Engine | ~70 |
| 6 | Background Services | ~70 |
| 7 | Query Language | ~66 |
| 8 | Multi-dimensional Aggregation | ~90 |

## Testing

### Run All Tests

```bash
cd source

# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_gorilla_compression

# Run storage lifecycle tests
cargo test --test storage_lifecycle_tests
```

### Run Performance Benchmarks

```bash
# Run criterion benchmarks
cargo bench

# Run manual performance tests
cargo test --release --test performance_benchmarks -- --ignored --nocapture
```

### Run Stress Tests

```bash
# Run production validation stress tests
cargo test --release -- --ignored stress
```

### Code Quality

```bash
# Run clippy
cargo clippy -- -D warnings

# Format code
cargo fmt

# Generate documentation
cargo doc --open
```

## Contributing

1. Read the documentation in `/docs/` for implementation details
2. Follow Rust best practices and SOLID principles
3. Write tests for new features (aim for >80% coverage)
4. Ensure `cargo clippy` passes without warnings
5. Format code with `cargo fmt`
6. Update relevant documentation

### Development Principles

- **Clean Architecture**: Each component in its own module
- **Dependency Injection**: Interfaces between layers
- **Single Responsibility**: Focused, cohesive modules
- **Comprehensive Error Handling**: Use `thiserror` and `Result` types
- **Production Logging**: Structured logging with `tracing`

## License

MIT License

## References

- [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) - Facebook's original paper
- [Prometheus](https://prometheus.io/) - Inspiration for query language design
- [InfluxDB](https://www.influxdata.com/) - Reference implementation patterns
- [Redis Sorted Sets](https://redis.io/docs/data-types/sorted-sets/) - Time index implementation

---

**Current Version**: 0.1.0
**Build Status**: All 352+ tests passing
**Code Quality**: Clippy clean, fully documented
**Last Updated**: November 2024
