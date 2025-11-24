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
//! ```rust,no_run
//! use gorilla_tsdb::storage::LocalDiskEngine;
//! use gorilla_tsdb::engine::traits::StorageEngine;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create storage engine
//! let storage = LocalDiskEngine::new("/tmp/tsdb".into())?;
//!
//! // Engine is ready for use
//! assert_eq!(storage.engine_id(), "local-disk-v1");
//! # Ok(())
//! # }
//! ```

/// Thread-safe active chunk implementation with concurrent write support
pub mod active_chunk;
/// Core chunk storage with lifecycle management
pub mod chunk;
/// Local disk storage engine implementation
pub mod local_disk;

pub use active_chunk::ActiveChunk;
pub use chunk::*;
pub use local_disk::LocalDiskEngine;
