//! Pluggable engine architecture for compression, storage, and indexing
//!
//! This module provides the core traits and implementations for the
//! pluggable engine architecture:
//!
//! - **Compressor**: Compression algorithms (Gorilla, Parquet)
//! - **StorageEngine**: Storage backends (LocalDisk, S3)
//! - **TimeIndex**: Time-series indexing (Redis, InMemory)
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::engine::{TimeSeriesDBBuilder, DatabaseConfig};
//! use gorilla_tsdb::engine::stubs::{S3Engine, ParquetCompressor};
//!
//! let db = TimeSeriesDBBuilder::new()
//!     .with_compressor(ParquetCompressor::new(Default::default()))
//!     .with_storage(S3Engine::new(Default::default()))
//!     .build()
//!     .await?;
//! ```
//!
//! This module provides the core traits and implementations for the
//! pluggable engine architecture:
//!
//! - **Compressor**: Compression algorithms (Gorilla, Parquet)
//! - **StorageEngine**: Storage backends (LocalDisk, S3)
//! - **TimeIndex**: Time-series indexing (Redis, InMemory)
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::engine::{TimeSeriesDBBuilder, DatabaseConfig};
//! use gorilla_tsdb::engine::stubs::{S3Engine, ParquetCompressor};
//!
//! let db = TimeSeriesDBBuilder::new()
//!     .with_compressor(ParquetCompressor::new(Default::default()))
//!     .with_storage(S3Engine::new(Default::default()))
//!     .build()
//!     .await?;
//! ```

pub mod builder;
pub mod stubs;
pub mod traits;

pub use builder::{DatabaseConfig, DatabaseStats, TimeSeriesDB, TimeSeriesDBBuilder};
pub use stubs::{
    InMemoryTimeIndex, ParquetCompressor, ParquetConfig, S3Config, S3Engine, SecretString,
};
