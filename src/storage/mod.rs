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
//! use kuba_tsdb::storage::LocalDiskEngine;
//! use kuba_tsdb::engine::traits::StorageEngine;
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
/// Background compression service for sealed chunks
pub mod compressor;
/// Directory management utilities (metadata, locks, cleanup)
pub mod directory;
/// Data integrity verification and corruption detection
pub mod integrity;
/// Local disk storage engine implementation
pub mod local_disk;
/// Memory-mapped chunk implementation for zero-copy reads
pub mod mmap;
/// High-level chunk reader with query capabilities
pub mod reader;
/// High-level chunk writer with batching and auto-rotation
pub mod writer;

pub use active_chunk::ActiveChunk;
pub use chunk::*;
pub use compressor::{CompressionConfig, CompressionService, CompressionStats};
pub use directory::{DirectoryMaintenance, SeriesMetadata, WriteLock, WriteLockConfig};
pub use integrity::{
    attempt_chunk_repair, calculate_checksum, verify_checksum, CorruptedChunk, CorruptionSeverity,
    IntegrityChecker, IntegrityError, IntegrityReport, RecoveryManager, RecoveryReport,
    RepairResult,
};
pub use local_disk::LocalDiskEngine;
pub use mmap::MmapChunk;
pub use reader::{ChunkReader, QueryOptions};
pub use writer::{ChunkWriter, ChunkWriterConfig, WriteStats};

/// Security utilities for path validation
pub mod security {
    use std::path::Path;

    /// Validate that a path doesn't contain traversal attempts
    ///
    /// Checks for:
    /// - ".." components (parent directory traversal)
    /// - Absolute paths outside expected directory
    /// - Null bytes
    /// - Symlinks (optional check)
    pub fn validate_path_no_traversal(path: &Path, base_dir: &Path) -> Result<(), String> {
        // Check for null bytes in path string
        if let Some(path_str) = path.to_str() {
            if path_str.contains('\0') {
                return Err("Path contains null bytes".to_string());
            }

            // Windows-specific validation: Check for drive letter traversal
            #[cfg(windows)]
            {
                // Detect attempts to traverse to different drives (e.g., C:, D:)
                if path_str.len() >= 2 && path_str.chars().nth(1) == Some(':') {
                    // This is an absolute path with drive letter
                    let path_drive = path_str.chars().nth(0).unwrap().to_ascii_uppercase();

                    if let Some(base_str) = base_dir.to_str() {
                        if base_str.len() >= 2 && base_str.chars().nth(1) == Some(':') {
                            let base_drive = base_str.chars().nth(0).unwrap().to_ascii_uppercase();
                            if path_drive != base_drive {
                                return Err(format!(
                                    "Path attempts to traverse to different drive: {} vs {}",
                                    path_drive, base_drive
                                ));
                            }
                        }
                    }
                }

                // Also check for UNC paths (\\server\share)
                if path_str.starts_with("\\\\") || path_str.starts_with("//") {
                    return Err("UNC paths are not allowed".to_string());
                }
            }
        }

        // Normalize and check that path is within base_dir
        let canonical_base = base_dir
            .canonicalize()
            .map_err(|e| format!("Failed to canonicalize base dir: {}", e))?;

        // If path doesn't exist yet, check its parent
        let path_to_check = if path.exists() {
            path.canonicalize()
                .map_err(|e| format!("Failed to canonicalize path: {}", e))?
        } else {
            // For non-existent paths, validate parent and filename separately
            if let Some(parent) = path.parent() {
                if parent.exists() {
                    let canonical_parent = parent
                        .canonicalize()
                        .map_err(|e| format!("Failed to canonicalize parent: {}", e))?;

                    // Verify parent is within base
                    if !canonical_parent.starts_with(&canonical_base) {
                        return Err(format!(
                            "Path parent traverses outside base directory: {:?} not in {:?}",
                            canonical_parent, canonical_base
                        ));
                    }

                    // Check filename for traversal patterns
                    if let Some(filename) = path.file_name() {
                        let filename_str = filename.to_string_lossy();
                        if filename_str.contains("..") {
                            return Err("Filename contains '..' pattern".to_string());
                        }
                    }

                    return Ok(());
                }
            }

            // Parent doesn't exist or path has no parent - check components
            for component in path.components() {
                use std::path::Component;
                match component {
                    Component::ParentDir => {
                        return Err("Path contains '..' component".to_string());
                    }
                    Component::RootDir if !canonical_base.starts_with("/") => {
                        return Err("Absolute path on non-Unix system".to_string());
                    }
                    _ => {}
                }
            }

            return Ok(());
        };

        // Verify resolved path is within base directory
        if !path_to_check.starts_with(&canonical_base) {
            return Err(format!(
                "Path traverses outside base directory: {:?} not in {:?}",
                path_to_check, canonical_base
            ));
        }

        Ok(())
    }

    /// Check if a path is a symlink
    #[cfg(unix)]
    pub fn is_symlink(path: &Path) -> bool {
        path.symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }

    /// Check if a path is a symlink (Windows version)
    #[cfg(not(unix))]
    pub fn is_symlink(path: &Path) -> bool {
        path.symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }
}
