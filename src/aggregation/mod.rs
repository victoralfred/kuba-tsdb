//! Multi-Dimensional Aggregation Engine
//!
//! This module provides sophisticated aggregation capabilities for time-series data
//! from multiple hosts with overlapping tags. It performs space-time aggregation
//! based on query functions before presenting unified time-series to clients.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │         Metric Definition           │
//! │  metric_name + tag_combinations     │
//! └─────────────────────────────────────┘
//!                  ↓
//! ┌─────────────────────────────────────┐
//! │         Series Instances            │
//! │  Unique metric+tag combinations     │
//! └─────────────────────────────────────┘
//!                  ↓
//! ┌─────────────────────────────────────┐
//! │      Space-Time Aggregator          │
//! │  Merges across spatial dimensions   │
//! └─────────────────────────────────────┘
//!                  ↓
//! ┌─────────────────────────────────────┐
//! │      Unified Time Series            │
//! │    Single series for UI display     │
//! └─────────────────────────────────────┘
//! ```
//!
//! # Storage Architecture (Hybrid)
//!
//! The metadata layer uses a hybrid approach:
//! - **Local (in-memory + optional sled)**: Fast reads, no network overhead
//! - **Redis**: Distributed coordination for multi-node deployments
//!
//! # Key Components
//!
//! - **String Interning**: Compact storage for tag keys/values via ID mapping
//!   - 80-90% memory reduction for repetitive tags
//!   - O(1) tag comparison via integer IDs
//! - **Series Registry**: Maps metric + tags → series_id
//! - **Tag Resolver**: Inverted index for efficient tag-based series lookup
//! - **Space-Time Aggregator**: Two-phase aggregation across space and time
//! - **Cardinality Controller**: Prevents explosion of high-cardinality tags
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::aggregation::{MetadataStore, SpaceTimeAggregator};
//!
//! // Create metadata store
//! let store = MetadataStore::in_memory();
//!
//! // Register series (strings are interned automatically)
//! let series_id = store.register_series(
//!     "cpu_usage",
//!     &[("host", "server1"), ("dc", "us-east"), ("env", "prod")]
//! )?;
//!
//! // Query: Average CPU across all hosts in datacenter
//! // avg(cpu_usage, {datacenter:us-east}, 1h-now, 5m)
//! let series = store.get_metric_series("cpu_usage");
//!
//! let aggregator = SpaceTimeAggregator::new(storage);
//! let result = aggregator.aggregate(
//!     series,
//!     time_range,
//!     AggregateFunction::Avg,
//!     Duration::from_secs(300), // 5 minute windows
//! ).await?;
//! ```

pub mod cardinality;
pub mod data_model;
pub mod functions;
pub mod index;
pub mod metadata;
pub mod query;
pub mod space_time;

// Re-export main types from data_model
pub use data_model::{
    AggregatedPoint, AggregationMetadata, AggregationStats, AlignedData, InternedTag,
    InternedTagSet, MergedTagSet, MetricFamily, MetricPoint, MultiSeriesData, SeriesIdentifier,
    SourceIdentifier, TagKeyId, TagValueId, TimestampData, UnifiedTimeSeries,
};

// Re-export main types from metadata
pub use metadata::{
    MetadataConfig, MetadataStats, MetadataStore, SeriesEntry, SeriesRegistry, StringInterner,
    TagDictionary, TagDictionaryStats, TagKeyInterner, TagValueInterner,
};

// Re-export main types from index
pub use index::{
    BitmapIndex, BitmapIndexStats, MatchMode, ResolvedSeries, TagBitmap, TagFilter, TagMatcher,
    TagResolver, TagResolverConfig, TagResolverStats, TagResolverStatsSnapshot,
};

// Re-export SeriesEntryResolved from metadata
pub use metadata::SeriesEntryResolved;

// Re-export main types from space_time
pub use space_time::{
    is_valid_timestamp, AggregateFunction, AggregateQuery, AggregateState, AggregatorStats,
    AggregatorStatsSnapshot, DataSource, InMemoryDataSource, SpaceTimeAggregator, WindowIterator,
    WindowIteratorError, MAX_VALID_TIMESTAMP_MS, MIN_VALID_TIMESTAMP_MS,
};

// Re-export main types from functions
pub use functions::{
    bottomk, clamp_max, clamp_min, clamp_values, delta, exponential_moving_avg, filter_above,
    filter_below, group_by_tag, histogram_quantile, idelta, irate, is_valid_label_name, label_join,
    label_replace, moving_avg, moving_max, moving_min, moving_sum, rate_with_resets, topk,
    HistogramBucket, LabelError, RankedSeries,
};

// Re-export main types from cardinality
pub use cardinality::{
    CardinalityConfig, CardinalityController, CardinalityError, CardinalityEstimator,
    CardinalityStats, MetricCardinalityTracker,
};

// Re-export main types from query
pub use query::{
    AggQuery, AggQueryBuilder, AggQueryPlanner, QueryCost, QueryPlan, QueryPlanError,
    QueryPlannerConfig, ResultPoint, TagFilterSpec, TagMatchMode, ToQueryResult,
};
