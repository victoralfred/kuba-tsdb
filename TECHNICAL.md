# Gorilla TSDB Technical Documentation

This document contains detailed technical information about Gorilla TSDB's architecture, internals, and implementation details. For installation and basic usage, see [README.md](README.md).

## Table of Contents

- [Architecture](#architecture)
- [Gorilla Compression](#gorilla-compression)
- [Pluggable Engine Architecture](#pluggable-engine-architecture)
- [Storage Layer](#storage-layer)
- [Redis Integration](#redis-integration)
- [Performance](#performance)
- [Configuration Reference](#configuration-reference)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Implementation Status](#implementation-status)

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

## Gorilla Compression

Implementation of Facebook's Gorilla algorithm optimized for time-series data:

- **Delta-of-Delta Timestamp Encoding**: Exploits the regular intervals in time-series data to compress timestamps to just a few bits
- **XOR-based Value Compression**: Uses XOR encoding for floating-point values, achieving high compression when consecutive values are similar
- **Compression Ratios**: Achieves 10:1 to 12:1 compression on real-world metrics data
- **Speed**: Compression at 27M points/second, decompression at 14.5M points/second

### Compression Ratios

| Data Pattern | Compression Ratio |
|--------------|-------------------|
| Regular intervals | 12:1 |
| Metrics data | 10:1 |
| Variable intervals | 8:1 |
| Random data | 2:1 |

## Pluggable Engine Architecture

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
- **Time Indexes**: RedisTimeIndex (production), InMemoryTimeIndex

## Storage Layer

Chunk-based storage with advanced features:

- **64-Byte Binary Headers**: Magic number validation, versioning, checksums
- **Memory-Mapped Files**: Zero-copy reads with madvise hints for optimal performance
- **Active/Sealed Chunk Model**: In-memory writes with background sealing
- **Background Compression**: Automatic Snappy compression for cold data
- **Multi-Tier Cache**: Sharded LRU cache with watermark-based eviction
- **Crash Recovery**: Atomic writes with checksum verification

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

## Redis Integration

Full-featured Redis-based time-series indexing:

- **Sorted Set Time Index**: O(log n) time-range queries using ZRANGEBYSCORE
- **Connection Pooling**: Multiplexed connections with semaphore-based limiting
- **Lua Scripts**: Atomic multi-key operations for consistency
- **Write Buffer**: Redis-backed crash recovery buffer
- **Failover Support**: Automatic failover with local cache fallback
- **Query Optimization**: Result caching with TTL and LRU eviction

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

### Resource Usage

- **Memory**: ~22 bytes per point in active chunks
- **CPU**: Near-linear scaling with concurrent threads
- **Disk**: Depends on compression ratio (typically 10x reduction)

## Configuration Reference

### TOML Configuration File

```toml
[server]
listen_addr = "0.0.0.0:8090"
data_dir = "/var/lib/tsdb/data"
max_chunk_size = 65536
retention_days = 30
cors_allowed_origins = []
log_level = "info"

[redis]
enabled = true
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
    end_time: Some(1700000001000),
    limit: Some(1000),
    use_mmap: true,
};

// Single chunk read
let points = reader.read_chunk("/data/chunk_001.gor", options).await?;

// Parallel multi-chunk read
let chunks = vec!["/data/chunk_001.gor", "/data/chunk_002.gor"];
let all_points = reader.read_chunks_parallel(chunks, options).await?;
```

## Project Structure

```
gorilla-tsdb/
├── Cargo.toml
├── README.md                    # Quick start and installation
├── TECHNICAL.md                 # This file
├── src/
│   ├── lib.rs                   # Library entry point
│   ├── types.rs                 # Core data types
│   ├── error.rs                 # Error definitions
│   ├── compression/             # Gorilla compression
│   │   ├── mod.rs
│   │   ├── bit_stream.rs        # Bit-level I/O
│   │   └── gorilla.rs           # Gorilla algorithm
│   ├── storage/                 # Chunk storage
│   │   ├── mod.rs
│   │   ├── chunk.rs             # Chunk structures
│   │   ├── local_disk.rs        # Disk storage engine
│   │   ├── active_chunk.rs      # In-memory buffer
│   │   ├── mmap.rs              # Memory-mapped files
│   │   ├── writer.rs            # ChunkWriter
│   │   ├── reader.rs            # ChunkReader
│   │   ├── compressor.rs        # Background compression
│   │   ├── cache.rs             # Multi-tier cache
│   │   └── directory.rs         # Directory management
│   ├── redis/                   # Redis integration
│   │   ├── mod.rs
│   │   ├── connection.rs        # Connection pooling
│   │   ├── index.rs             # RedisTimeIndex
│   │   ├── scripts.rs           # Lua scripts
│   │   ├── series.rs            # Series management
│   │   ├── query.rs             # Query optimization
│   │   ├── buffer.rs            # Write buffer
│   │   ├── failover.rs          # High availability
│   │   └── metrics.rs           # Redis metrics
│   ├── engine/                  # Pluggable engines
│   │   ├── mod.rs
│   │   ├── traits.rs            # Engine traits
│   │   └── builder.rs           # Database builder
│   ├── query/                   # Query engine
│   │   ├── mod.rs
│   │   ├── ast.rs               # Query AST
│   │   └── parser/              # SQL/PromQL parsers
│   ├── aggregation/             # Aggregation functions
│   ├── bin/server/              # HTTP server binary
│   ├── config.rs                # Configuration
│   ├── metrics.rs               # Prometheus metrics
│   └── security.rs              # Security hardening
├── tests/                       # Integration tests
├── benches/                     # Benchmarks
└── examples/                    # Usage examples
```

## Implementation Status

### Completed Phases

| Phase | Component | Status | Lines of Code |
|-------|-----------|--------|---------------|
| 1 | Gorilla Compression | Complete | ~1,200 |
| 1.5 | Bug Fixes & Hardening | Complete | +200 |
| 2 | Storage Layer | Complete | ~4,400 |
| 3 | Redis Integration | Complete | ~4,600 |
| 4 | Query Engine | Complete | ~3,500 |
| 5 | HTTP Server | Complete | ~2,000 |

### Test Coverage

- **Total Tests**: 1,100+ tests
- **Pass Rate**: 100%
- **Categories**:
  - Unit tests: 800+
  - Doc tests: 115
  - Integration tests: 100+
  - Stress tests: 17
  - Performance benchmarks: 24

### Production Features

- **Prometheus Metrics**: 20+ metrics covering all critical operations
- **Security Hardening**: Path validation, rate limiting, input sanitization
- **TOML Configuration**: Environment variable overrides, hot-reload support
- **Graceful Shutdown**: In-flight operation completion, resource cleanup
- **Comprehensive Logging**: Structured logging with tracing

---

For more information, see the [README.md](README.md) for installation and basic usage.
