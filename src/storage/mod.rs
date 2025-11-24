//! Storage layer for persisting compressed time-series data
//!
//! This module provides the storage backend for the time-series database, implementing
//! chunk-based storage with memory-mapped files and background compression.
//!
//! # Architecture
//!
//! The storage layer uses a hierarchical chunk-based design:
//!
//! ```text
//! Storage Flow:
//! Write → Active Chunk → Seal → Disk File → Background Compression → Snappy
//!         (in memory)      (mmap)           (async)                  (cold)
//! ```
//!
//! # Key Components
//!
//! - **Chunk**: Basic storage unit containing compressed data blocks
//! - **Active Chunks**: In-memory write buffers for fast ingestion
//! - **Sealed Chunks**: Immutable chunks written to disk via memory-mapping
//! - **LocalDiskEngine**: Storage engine implementation for local filesystem
//! - **Directory Management**: Organizes chunks by series ID
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::storage::LocalDiskEngine;
//! use gorilla_tsdb::types::DataPoint;
//! use gorilla_tsdb::engine::traits::StorageEngine;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create storage engine
//! let storage = LocalDiskEngine::new("/data/tsdb".into())?;
//!
//! // Write data points
//! let points = vec![
//!     DataPoint::new(1, 1000, 42.5),
//!     DataPoint::new(1, 1010, 43.1),
//! ];
//! storage.write(1, &points).await?;
//!
//! // Read data back
//! let chunks = storage.list_chunks(1, 1000, 2000).await?;
//! # Ok(())
//! # }
//! ```

pub mod chunk;
pub mod local_disk;

pub use chunk::*;
pub use local_disk::LocalDiskEngine;
