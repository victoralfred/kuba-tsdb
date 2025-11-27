//! Emergency spill-to-disk module
//!
//! Provides emergency overflow storage when memory buffers reach capacity.
//! Data is temporarily written to disk with LZ4 compression and recovered
//! when memory pressure subsides.
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────────┐
//! │                       Spill Manager                                │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
//! │  │  Threshold  │  │   Writer    │  │   Reader    │                │
//! │  │   Monitor   │→ │  (async)    │→ │  (recovery) │                │
//! │  └─────────────┘  └─────────────┘  └─────────────┘                │
//! │         │               │               │                          │
//! │         v               v               v                          │
//! │  ┌────────────────────────────────────────────────────────────┐   │
//! │  │                    Spill Files                              │   │
//! │  │  spill-001.lz4    spill-002.lz4    spill-003.lz4           │   │
//! │  └────────────────────────────────────────────────────────────┘   │
//! └───────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Memory threshold triggers**: Automatically spill when memory exceeds limit
//! - **LZ4 compression**: Fast compression for reduced disk usage
//! - **Parallel recovery**: Multi-threaded reading of spill files
//! - **Automatic cleanup**: Remove spill files after successful recovery
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::ingestion::spill::{SpillManager, SpillConfig};
//! use gorilla_tsdb::types::DataPoint;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = SpillConfig::default();
//! let spill_manager = SpillManager::new(config)?;
//!
//! // Spill data when memory pressure is high
//! let points = vec![DataPoint::new(1, 1000, 42.5)];
//! spill_manager.spill(points).await?;
//!
//! // Recover data when memory pressure subsides
//! let recovered = spill_manager.recover_all().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod file;
pub mod manager;
pub mod recovery;

pub use config::SpillConfig;
pub use error::{SpillError, SpillResult};
pub use file::{SpillFile, SpillFileId};
pub use manager::SpillManager;
pub use recovery::SpillRecovery;

use std::sync::atomic::{AtomicU64, Ordering};

/// Global spill file counter
static SPILL_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique spill file ID
pub fn next_spill_file_id() -> SpillFileId {
    SPILL_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spill_file_id_generation() {
        let id1 = next_spill_file_id();
        let id2 = next_spill_file_id();
        let id3 = next_spill_file_id();

        assert!(id2 > id1);
        assert!(id3 > id2);
    }
}
