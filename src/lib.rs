//! Gorilla TSDB - High-performance time-series database with pluggable engines
//!
//! This library provides a production-ready time-series database with:
//! - Gorilla compression for 10:1+ compression ratios
//! - Pluggable storage, compression, and indexing engines
//! - 2M+ points/second ingestion
//! - Sub-millisecond query latency
//! - Multi-dimensional aggregation

#![warn(missing_docs)]
#![warn(clippy::all)]
// Allow manual modulo checks since is_multiple_of is unstable on stable Rust (Docker builds)
#![allow(clippy::manual_is_multiple_of)]

pub mod compression;
pub mod engine;
pub mod error;
pub mod index;
pub mod storage;
pub mod types;

/// Prometheus metrics and telemetry
pub mod metrics;

/// Configuration management with TOML support
pub mod config;

/// Security hardening (path validation, rate limiting)
pub mod security;

/// Redis integration for time-series indexing
/// Provides Redis-based indexing using Sorted Sets for ultra-fast time-based lookups
///
/// This module is optional and enabled via the `redis-index` feature flag.
/// When disabled, use the local in-memory index from `engine::stubs::InMemoryIndex`.
#[cfg(feature = "redis-index")]
pub mod redis;

/// Async ingestion pipeline for high-throughput data ingestion
/// Provides batching, buffering, parallel writes, and backpressure management
pub mod ingestion;

/// Query engine for time-series data retrieval and analysis
/// Provides vectorized execution, parallel processing, aggregations, and downsampling
pub mod query;

/// Background services for chunk management, compaction, monitoring, and health checks
/// Provides service lifecycle management, graceful shutdown, and dependency ordering
pub mod services;

/// Multi-dimensional aggregation engine for space-time aggregation
/// Provides string interning, series registry, and tag-based lookups
pub mod aggregation;

/// Unified cache module for all cache-related functionality
/// Provides storage cache, query cache, local metadata cache, and unified statistics
pub mod cache;

// Re-export main types
pub use engine::{DatabaseConfig, TimeSeriesDB, TimeSeriesDBBuilder};
pub use error::{Error, Result};
pub use types::{ChunkId, ChunkIdError, DataPoint, SeriesId, TimeRange};

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic_sanity() {
        assert_eq!(2 + 2, 4);
    }
}
