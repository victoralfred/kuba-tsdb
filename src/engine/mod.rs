//! Pluggable engine architecture for compression, storage, and indexing

pub mod builder;
pub mod traits;

pub use builder::{DatabaseConfig, TimeSeriesDB, TimeSeriesDBBuilder};
