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
//! ```rust,ignore
//! use gorilla_tsdb::query::{QueryBuilder, QueryEngine};
//! use gorilla_tsdb::types::TimeRange;
//!
//! // Build a query
//! let query = QueryBuilder::new()
//!     .select_series(series_id)
//!     .time_range(TimeRange::last_hours(1))
//!     .aggregate(Aggregation::Avg)
//!     .window(Duration::from_secs(60))
//!     .build()?;
//!
//! // Execute
//! let engine = QueryEngine::new(config);
//! let results = engine.execute(query).await?;
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
pub use result::{QueryResult, ResultFormat, ResultRow};
pub use subscription::{
    QuerySubscription, SharedSubscriptionManager, SubscriptionConfig, SubscriptionManager,
    SubscriptionUpdate,
};
