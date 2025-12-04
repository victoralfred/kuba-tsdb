# Changelog

All notable changes to Kuba TSDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Parallel chunk compression for concurrent sealing operations
- Fix for write method to handle large batches exceeding chunk limit

## [0.1.0] - 2024

### Added

#### Core Database
- High-performance time-series database engine with pluggable architecture
- Kuba compression algorithm (Gorilla-style Delta-of-Delta + XOR encoding)
- 10:1+ compression ratios on real-world metrics data
- Sub-millisecond query latency (p99 < 1ms)
- 2M+ points/second ingestion throughput

#### AHPAC Adaptive Compression
- Adaptive Hierarchical Predictive Arithmetic Compression system
- Statistical data profiling for optimal codec selection
- Multiple codec implementations:
  - ALP (Adaptive Lossless Precision) for integer-like data
  - Chimp XOR codec for floating-point patterns
  - Delta + LZ4 for smooth time-series
  - Kuba codec as fallback
- ANS (Asymmetric Numeral Systems) entropy coding
- SIMD-accelerated batch compression operations
- Parallel multi-series compression

#### Storage Layer
- Chunk-based storage with automatic lifecycle management
- Active chunks (in-memory) with BTreeMap-backed write buffers
- Sealed chunks with Snappy secondary compression
- Memory-mapped file access for zero-copy reads
- 64-byte binary headers with CRC integrity checking
- Background sealing and compression services
- Data integrity verification and recovery tools

#### Redis Integration
- Optional Redis backend for distributed indexing (feature-gated)
- Sorted Set time index for O(log n) range queries
- Connection pooling with semaphore limiting
- Lua scripts for atomic multi-key operations
- Write buffer for crash recovery
- Failover support with local cache fallback
- Pub/Sub for cross-node cache invalidation
- Secondary indexes for tag-based queries

#### Query Engine
- SQL query support with time-series extensions
- PromQL (Prometheus Query Language) compatibility
- Aggregation functions: avg, min, max, sum, count, stddev, percentile, median
- Time-bucketed aggregations with auto-interval detection
- Downsampling operators
- Filter, sort, and limit operators
- Parallel multi-chunk query execution
- Query result caching

#### Ingestion Pipeline
- Async batch ingestion with Tokio runtime
- Network protocols: TCP, UDP, TLS
- Protocol auto-detection (JSON, Protobuf)
- Out-of-order handling with per-series write buffers
- Schema validation and tag filtering
- Write-ahead log (WAL) for durability
- Per-client rate limiting

#### HTTP Server
- RESTful API with Axum framework
- Endpoints: /api/v1/write, /api/v1/query, /api/v1/query/sql
- Series management: /api/v1/series, /api/v1/series/find
- Health checks: /health (liveness/readiness)
- Prometheus metrics: /metrics
- CORS support for frontend integration
- Graceful shutdown handling

#### Observability
- Prometheus metrics (20+ metrics)
- Compression metrics and observability
- Batch metrics testing scripts
- Debug and info logging with tracing

#### Configuration
- TOML configuration file support (application.toml)
- Environment variable overrides
- Configurable write buffering thresholds
- Advanced tuning options

#### Testing
- 350+ tests with 100% pass rate
- Unit tests, integration tests, doc tests
- Property-based fuzz tests for compression correctness
- Stress tests and performance benchmarks
- Benchmark suite for compression, ingestion, and AHPAC

### Changed
- Renamed project from Gorilla TSDB to Kuba TSDB
- Changed file extension from .gor to .kub

### Fixed
- Race condition in security tests with environment variables
- Redis key injection vulnerability in tag handling
- Queries now include data from active write buffers
- SQL parser time unit mismatch
- Redis series registration to rebuild secondary indexes on restart
- Local disk detection of compression type from algorithm_id
- Kuba codec bug in AHPAC integration
- CORS preflight handling for frontend requests
- Flaky tests and doctest examples

### Security
- Comprehensive security hardening
- Path validation and input sanitization
- Rate limiting with Governor
- Minimum timeout validation to prevent instant timeouts
- Removed test-only code from production builds

## Project History

The project began as "Gorilla TSDB" implementing Facebook's Gorilla compression paper, then evolved into "Kuba TSDB" with the addition of the AHPAC adaptive compression system and production-ready features.

[Unreleased]: https://github.com/victoralfred/kuba-tsdb/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/victoralfred/kuba-tsdb/releases/tag/v0.1.0
