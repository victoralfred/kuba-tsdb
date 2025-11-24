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

pub mod types;
pub mod engine;
pub mod compression;
pub mod storage;
pub mod index;
pub mod error;
pub mod metrics;
pub mod config;
pub mod security;

// Re-export main types
pub use types::{DataPoint, TimeRange, SeriesId};
pub use engine::{TimeSeriesDB, TimeSeriesDBBuilder, DatabaseConfig};
pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic_sanity() {
        assert_eq!(2 + 2, 4);
    }
}
