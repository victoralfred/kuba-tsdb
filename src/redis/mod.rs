//! Redis integration for time-series indexing
//!
//! This module provides Redis-based indexing for the time-series database,
//! using Sorted Sets for ultra-fast time-based lookups.
//!
//! # Architecture
//!
//! ```text
//! Redis Schema:
//! ts:registry                    → SET of all series_ids
//! ts:series:{id}:index           → ZSET(timestamp → chunk_id)
//! ts:series:{id}:meta            → HASH {created_at, last_write, tags...}
//! ts:series:{id}:buffer          → LIST of pending points (write buffer)
//! ts:chunks:{chunk_id}           → HASH {series_id, path, timestamps...}
//! ```
//!
//! # Features
//!
//! - Connection pooling with health checks
//! - Atomic operations via Lua scripts
//! - Series management and discovery
//! - Time-range chunk queries
//! - Query optimization with caching
//! - Write buffer for crash recovery
//! - Failover with local cache fallback
//! - Comprehensive metrics and monitoring
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisTimeIndex};
//! use gorilla_tsdb::engine::traits::TimeIndex;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = RedisConfig::default();
//! let index = RedisTimeIndex::new(config).await?;
//!
//! // Index is ready for use
//! assert_eq!(index.index_id(), "redis-time-index-v1");
//! # Ok(())
//! # }
//! ```

// Core modules
pub mod connection;
pub mod index;
pub mod scripts;
pub mod series;

// Optimization modules
pub mod buffer;
pub mod query;

// High availability modules
pub mod failover;

// Monitoring modules
pub mod metrics;

// Re-export main types
pub use buffer::{BufferConfig, WriteBuffer};
pub use connection::{RedisConfig, RedisPool, RetryPolicy};
pub use failover::{FailoverConfig, FailoverManager};
pub use index::RedisTimeIndex;
pub use metrics::{MetricsConfig, RedisMetrics};
pub use query::{QueryConfig, QueryPlanner};
pub use scripts::LuaScripts;
pub use series::SeriesManager;
