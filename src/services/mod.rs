//! Background Services Module
//!
//! Provides background services for chunk management, compaction, monitoring,
//! and system maintenance to ensure optimal database performance and reliability.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
//! │  Chunk Manager  │────▶│ Compaction Svc   │────▶│   Storage   │
//! └─────────────────┘     └──────────────────┘     └─────────────┘
//!         │                       │                       │
//!         ▼                       ▼                       ▼
//! ┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
//! │ Monitor Service │◀────│ Metrics Collector│◀────│Health Check │
//! └─────────────────┘     └──────────────────┘     └─────────────┘
//! ```
//!
//! # Services
//!
//! - **ServiceManager**: Coordinates lifecycle of all background services
//! - **ChunkManager**: Monitors and manages chunk lifecycle (sealing, rotation)
//! - **CompactionService**: Merges and optimizes chunks for query performance
//! - **MonitorService**: Collects metrics and monitors system health
//! - **HealthChecker**: Performs health checks and alerts
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::services::{ServiceManager, ServiceConfig};
//!
//! // Create service manager with default config
//! let config = ServiceConfig::default();
//! let manager = ServiceManager::new(config);
//!
//! // Check service status
//! let status = manager.status();
//! assert_eq!(status.len(), 0); // No services registered yet
//! ```

pub mod chunk_manager;
pub mod compactor;
pub mod framework;
pub mod health;
pub mod monitor;

pub use chunk_manager::{ChunkManager, ChunkManagerConfig};
pub use compactor::{CompactionConfig, CompactionService, CompactionStrategy};
pub use framework::{Service, ServiceConfig, ServiceManager, ServiceStatus, SharedServiceManager};
pub use health::{HealthChecker, HealthConfig, HealthStatus};
pub use monitor::{MonitorConfig, MonitorService};
