//! Query Engine for time-series data retrieval and analysis
//!
//! This module provides a high-performance query engine with:
//! - Vectorized execution for CPU-efficient processing
//! - Parallel query execution using morsel-driven parallelism
//! - Multi-level caching for sub-millisecond latency
//! - Rich aggregation functions including percentiles (t-digest)
//! - Downsampling algorithms (LTTB, M4) for visualization
//!
//! # Architecture
//!
//! ```text
//! Query String
//!      │
//!      ▼
//! ┌─────────────┐
//! │   Parse     │  Native Query Language → AST
//! └─────────────┘
//!      │
//!      ▼
//! ┌─────────────┐
//! │  Validate   │  Type check, permission check
//! └─────────────┘
//!      │
//!      ▼
//! ┌─────────────┐
//! │  Optimize   │  Predicate pushdown, chunk pruning
//! └─────────────┘
//!      │
//!      ▼
//! ┌─────────────┐
//! │   Plan      │  Choose execution strategy
//! └─────────────┘
//!      │
//!      ▼
//! ┌─────────────┐
//! │  Execute    │  Vectorized, parallel
//! └─────────────┘
//!      │
//!      ▼
//! ┌─────────────┐
//! │  Format     │  JSON, CSV, Protobuf
//! └─────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::query::{QueryBuilder, AggregationFunction};
//! use gorilla_tsdb::types::TimeRange;
//! use std::time::Duration;
//!
//! // Build a simple select query
//! let query = QueryBuilder::new()
//!     .select_series(1)
//!     .time_range(TimeRange::new(0, 3600000).unwrap())
//!     .build()
//!     .unwrap();
//! ```

pub mod ast;
pub mod cache;
pub mod chunk_index;
pub mod error;
pub mod executor;
pub mod integration;
pub mod operators;
pub mod parser;
pub mod planner;
pub mod result;
pub mod subscription;

// Re-export main types
pub use ast::{
    Aggregation, AggregationFunction, DownsampleMethod, OrderDirection, Predicate, PredicateOp,
    Query, QueryBuilder, SeriesSelector, WindowSpec,
};
pub use cache::{CacheConfig, CacheKey, QueryCache, SharedQueryCache};
pub use chunk_index::{ChunkIndex, ChunkIndexConfig, IndexedChunk};
pub use error::{QueryError, QueryErrorKind};
pub use executor::{ExecutorConfig, QueryExecutor};
pub use integration::{QueryEngine, RedisQueryExt};
pub use operators::{
    AggregationOperator, DataBatch, DownsampleOperator, FilterOperator, Operator, ScanOperator,
    StorageQueryExt, StorageScanOperator,
};
pub use parser::{parse_promql, parse_query, parse_sql};
pub use planner::{QueryPlan, QueryPlanner};
pub use result::{CacheInfo, QueryResult, ResultFormat, ResultMetadata, ResultRow};
pub use subscription::{
    QuerySubscription, SharedSubscriptionManager, SubscriptionConfig, SubscriptionManager,
    SubscriptionUpdate,
};
