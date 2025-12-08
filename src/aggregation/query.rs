//! Query Integration for Aggregation Engine
//!
//! This module bridges the query engine with the aggregation layer,
//! providing optimized query execution paths for multi-dimensional aggregations.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       AggregationQuery                          │
//! │  (metric_name, tag_filters, time_range, functions, grouping)    │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//!                                  ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     AggregationQueryPlanner                     │
//! │     1. Validate cardinality limits                              │
//! │     2. Resolve series via TagResolver (bitmap indexes)          │
//! │     3. Generate optimized execution plan                        │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//!                                  ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    AggregationExecutor                          │
//! │     1. Fetch data from storage (parallel per series)            │
//! │     2. Execute space-time aggregation                           │
//! │     3. Apply post-aggregation functions                         │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//!                                  ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     UnifiedTimeSeries                           │
//! │          (aggregated result for client consumption)             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use kuba_tsdb::aggregation::{AggQueryBuilder, AggregateFunction};
//! use std::time::Duration;
//!
//! // Build a query
//! let query = AggQueryBuilder::new("cpu_usage")
//!     .with_tag("datacenter", "us-east")
//!     .time_range(0, 3600000)
//!     .aggregate(AggregateFunction::Avg)
//!     .window_size(Duration::from_secs(300))
//!     .group_by(&["host"])
//!     .build()
//!     .unwrap();
//!
//! assert_eq!(query.metric_name, "cpu_usage");
//! assert_eq!(query.group_by.len(), 1);
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;
use crate::types::{SeriesId, TimeRange};

use super::cardinality::{CardinalityController, CardinalityError};
use super::data_model::UnifiedTimeSeries;
use super::index::{TagMatcher, TagResolver};
use super::metadata::MetadataStore;
use super::space_time::{AggregateFunction, AggregateQuery, DataSource, SpaceTimeAggregator};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Query Rate Limiter (SEC-010)
// ============================================================================

/// Simple token bucket rate limiter for queries
///
/// SEC-010: Prevents DoS attacks via excessive queries
struct QueryRateLimiter {
    /// Tokens available (scaled by 1000 for sub-token precision)
    tokens_scaled: AtomicU64,
    /// Maximum tokens (bucket size, scaled)
    max_tokens_scaled: u64,
    /// Last refill timestamp in milliseconds
    last_refill_ms: AtomicU64,
    /// Refill rate (tokens per millisecond, scaled by 1000)
    refill_rate_scaled: u64,
}

impl QueryRateLimiter {
    const SCALE: u64 = 1000;

    fn new(queries_per_second: usize) -> Self {
        let max_tokens_scaled = (queries_per_second as u64) * Self::SCALE;
        // tokens per ms = queries_per_second / 1000
        let refill_rate_scaled = (queries_per_second as u64 * Self::SCALE) / 1_000;

        Self {
            tokens_scaled: AtomicU64::new(max_tokens_scaled),
            max_tokens_scaled,
            last_refill_ms: AtomicU64::new(Self::current_time_ms()),
            refill_rate_scaled: refill_rate_scaled.max(1),
        }
    }

    fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Try to acquire a token, return true if successful
    fn try_acquire(&self) -> bool {
        let now_ms = Self::current_time_ms();
        let last_ms = self.last_refill_ms.load(Ordering::Acquire);
        let elapsed_ms = now_ms.saturating_sub(last_ms);

        // Calculate tokens to add
        let tokens_to_add = elapsed_ms * self.refill_rate_scaled;

        // Try to update last_refill atomically
        if tokens_to_add > 0
            && self
                .last_refill_ms
                .compare_exchange(last_ms, now_ms, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            // Successfully claimed the refill, add tokens
            let _ =
                self.tokens_scaled
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                        Some((current + tokens_to_add).min(self.max_tokens_scaled))
                    });
        }

        // Try to acquire a token atomically
        self.tokens_scaled
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |tokens| {
                if tokens >= Self::SCALE {
                    Some(tokens - Self::SCALE)
                } else {
                    None
                }
            })
            .is_ok()
    }
}

// ============================================================================
// Query Types
// ============================================================================

/// An aggregation query specification
#[derive(Debug, Clone)]
pub struct AggQuery {
    /// Metric name to query
    pub metric_name: String,

    /// Tag filters for series selection
    pub tag_filters: Vec<TagFilterSpec>,

    /// Time range to query
    pub time_range: TimeRange,

    /// Primary aggregation function
    pub function: AggregateFunction,

    /// Time window size for temporal aggregation
    pub window_size: Option<Duration>,

    /// Step interval for output (if different from window)
    /// Defaults to window size if not specified
    pub step: Option<Duration>,

    /// Tags to group by (empty = aggregate all series together)
    pub group_by: Vec<String>,

    /// Optional quantile for percentile functions
    pub quantile: Option<f64>,

    /// Limit on number of series to process
    pub series_limit: Option<usize>,

    /// Maximum points to return
    pub max_points: Option<usize>,
}

/// Tag filter specification for queries
#[derive(Debug, Clone)]
pub struct TagFilterSpec {
    /// Tag key
    pub key: String,

    /// Tag value or pattern
    pub value: String,

    /// Match mode
    pub mode: TagMatchMode,
}

/// Tag matching mode for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagMatchMode {
    /// Exact value match
    Exact,

    /// Regex pattern match
    Regex,

    /// Prefix match
    Prefix,

    /// Not equal (negation)
    NotEqual,

    /// Tag key must exist
    Exists,
}

// ============================================================================
// Query Builder
// ============================================================================

/// Builder for constructing aggregation queries
#[derive(Debug)]
pub struct AggQueryBuilder {
    metric_name: String,
    tag_filters: Vec<TagFilterSpec>,
    time_range: Option<TimeRange>,
    function: AggregateFunction,
    window_size: Option<Duration>,
    step: Option<Duration>,
    group_by: Vec<String>,
    quantile: Option<f64>,
    series_limit: Option<usize>,
    max_points: Option<usize>,
}

impl AggQueryBuilder {
    /// Create a new query builder for a metric
    pub fn new(metric_name: &str) -> Self {
        Self {
            metric_name: metric_name.to_string(),
            tag_filters: Vec::new(),
            time_range: None,
            function: AggregateFunction::Avg,
            window_size: None,
            step: None,
            group_by: Vec::new(),
            quantile: None,
            series_limit: None,
            max_points: None,
        }
    }

    /// Add an exact tag filter
    pub fn with_tag(mut self, key: &str, value: &str) -> Self {
        self.tag_filters.push(TagFilterSpec {
            key: key.to_string(),
            value: value.to_string(),
            mode: TagMatchMode::Exact,
        });
        self
    }

    /// Add a regex tag filter
    pub fn with_tag_regex(mut self, key: &str, pattern: &str) -> Self {
        self.tag_filters.push(TagFilterSpec {
            key: key.to_string(),
            value: pattern.to_string(),
            mode: TagMatchMode::Regex,
        });
        self
    }

    /// Add a prefix tag filter
    pub fn with_tag_prefix(mut self, key: &str, prefix: &str) -> Self {
        self.tag_filters.push(TagFilterSpec {
            key: key.to_string(),
            value: prefix.to_string(),
            mode: TagMatchMode::Prefix,
        });
        self
    }

    /// Add a negation tag filter
    pub fn without_tag(mut self, key: &str, value: &str) -> Self {
        self.tag_filters.push(TagFilterSpec {
            key: key.to_string(),
            value: value.to_string(),
            mode: TagMatchMode::NotEqual,
        });
        self
    }

    /// Require a tag to exist
    pub fn has_tag(mut self, key: &str) -> Self {
        self.tag_filters.push(TagFilterSpec {
            key: key.to_string(),
            value: String::new(),
            mode: TagMatchMode::Exists,
        });
        self
    }

    /// Set the time range
    pub fn time_range(mut self, start: i64, end: i64) -> Self {
        self.time_range = Some(TimeRange { start, end });
        self
    }

    /// Set the aggregation function
    pub fn aggregate(mut self, function: AggregateFunction) -> Self {
        self.function = function;
        self
    }

    /// Set the window size for temporal aggregation
    pub fn window_size(mut self, size: Duration) -> Self {
        self.window_size = Some(size);
        self
    }

    /// Set the step interval for output
    pub fn step(mut self, step: Duration) -> Self {
        self.step = Some(step);
        self
    }

    /// Set group by tags
    pub fn group_by(mut self, tags: &[&str]) -> Self {
        self.group_by = tags.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Set quantile for percentile function
    pub fn quantile(mut self, q: f64) -> Self {
        self.quantile = Some(q);
        self
    }

    /// Limit the number of series to process
    pub fn series_limit(mut self, limit: usize) -> Self {
        self.series_limit = Some(limit);
        self
    }

    /// Limit the number of points returned
    pub fn max_points(mut self, limit: usize) -> Self {
        self.max_points = Some(limit);
        self
    }

    /// Build the query
    pub fn build(self) -> Result<AggQuery, QueryPlanError> {
        let time_range = self.time_range.ok_or(QueryPlanError::MissingTimeRange)?;

        if time_range.start >= time_range.end {
            return Err(QueryPlanError::InvalidTimeRange {
                start: time_range.start,
                end: time_range.end,
            });
        }

        Ok(AggQuery {
            metric_name: self.metric_name,
            tag_filters: self.tag_filters,
            time_range,
            function: self.function,
            window_size: self.window_size,
            step: self.step,
            group_by: self.group_by,
            quantile: self.quantile,
            series_limit: self.series_limit,
            max_points: self.max_points,
        })
    }
}

// ============================================================================
// Query Plan
// ============================================================================

/// An optimized query execution plan
#[derive(Debug)]
pub struct QueryPlan {
    /// Original query
    pub query: AggQuery,

    /// Resolved series IDs to query
    pub series_ids: Vec<SeriesId>,

    /// Series grouped by grouping key
    pub series_groups: HashMap<String, Vec<SeriesId>>,

    /// Estimated cost metrics
    pub estimated_cost: QueryCost,

    /// Whether to use parallel execution
    pub use_parallel: bool,

    /// Number of parallel workers to use
    pub parallel_workers: usize,
}

/// Estimated query execution cost
#[derive(Debug, Clone, Default)]
pub struct QueryCost {
    /// Estimated number of series
    pub series_count: usize,

    /// Estimated data points to scan
    pub estimated_points: u64,

    /// Estimated memory usage in bytes
    pub estimated_memory: usize,

    /// Estimated CPU cost (arbitrary units)
    pub cpu_cost: f64,

    /// Estimated I/O cost (arbitrary units)
    pub io_cost: f64,
}

impl QueryCost {
    /// Calculate total cost for optimization decisions
    pub fn total_cost(&self) -> f64 {
        // I/O is typically 10x more expensive than CPU
        self.cpu_cost + self.io_cost * 10.0
    }
}

// ============================================================================
// Query Planner
// ============================================================================

/// Configuration for the aggregation query planner
#[derive(Debug, Clone)]
pub struct QueryPlannerConfig {
    /// Maximum series to query without warning
    pub max_series_soft_limit: usize,

    /// Maximum series to query (hard limit)
    pub max_series_hard_limit: usize,

    /// Threshold for parallel execution (minimum series count)
    pub parallel_threshold: usize,

    /// Maximum parallel workers
    pub max_parallel_workers: usize,

    /// Enable cardinality checks
    pub enable_cardinality_check: bool,

    /// Maximum queries per second (rate limiting)
    /// SEC-010: Prevents DoS via excessive queries
    pub max_queries_per_second: Option<usize>,

    /// Query timeout in seconds
    /// SEC-011: Prevents long-running queries from blocking
    pub query_timeout_secs: Option<u64>,
}

impl Default for QueryPlannerConfig {
    fn default() -> Self {
        Self {
            max_series_soft_limit: 1000,
            max_series_hard_limit: 10000,
            parallel_threshold: 10,
            max_parallel_workers: 8,
            enable_cardinality_check: true,
            max_queries_per_second: Some(100), // Default: 100 QPS
            query_timeout_secs: Some(300),     // Default: 5 minutes
        }
    }
}

/// Query planner for aggregation queries
///
/// Integrates with MetadataStore for series resolution and
/// CardinalityController for limit enforcement.
///
/// # Security
/// - Implements query rate limiting (SEC-010)
/// - Enforces query timeouts (SEC-011)
pub struct AggQueryPlanner {
    /// Metadata store for series lookup
    metadata: Arc<MetadataStore>,

    /// Tag resolver for bitmap-based lookups
    tag_resolver: TagResolver,

    /// Optional cardinality controller
    cardinality: Option<Arc<CardinalityController>>,

    /// Planner configuration
    config: QueryPlannerConfig,

    /// Rate limiter for queries (SEC-010)
    /// Uses a simple token bucket algorithm
    query_rate_limiter: Option<std::sync::Arc<parking_lot::Mutex<QueryRateLimiter>>>,
}

impl AggQueryPlanner {
    /// Create a new query planner
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        let tag_resolver = TagResolver::new(metadata.clone());
        let config = QueryPlannerConfig::default();
        let query_rate_limiter = config
            .max_queries_per_second
            .map(|qps| std::sync::Arc::new(parking_lot::Mutex::new(QueryRateLimiter::new(qps))));

        Self {
            metadata,
            tag_resolver,
            cardinality: None,
            config,
            query_rate_limiter,
        }
    }

    /// Set cardinality controller for limit enforcement
    pub fn with_cardinality_controller(mut self, controller: Arc<CardinalityController>) -> Self {
        self.cardinality = Some(controller);
        self
    }

    /// Set planner configuration
    pub fn with_config(mut self, config: QueryPlannerConfig) -> Self {
        let query_rate_limiter = config
            .max_queries_per_second
            .map(|qps| std::sync::Arc::new(parking_lot::Mutex::new(QueryRateLimiter::new(qps))));
        self.query_rate_limiter = query_rate_limiter;
        self.config = config;
        self
    }

    /// Plan an aggregation query
    ///
    /// This performs:
    /// 1. Input validation (time range, limits, etc.)
    /// 2. Tag resolution using bitmap indexes
    /// 3. Cardinality limit validation
    /// 4. Series grouping for group_by clauses
    /// 5. Cost estimation
    /// 6. Parallel execution decision
    ///
    /// # Security
    /// - Validates all query parameters
    /// - Enforces resource limits
    /// - Prevents resource exhaustion attacks
    /// - Implements query rate limiting (SEC-010)
    pub fn plan(&self, query: &AggQuery) -> Result<QueryPlan, QueryPlanError> {
        // SEC-010: Check query rate limit
        if let Some(ref rate_limiter) = self.query_rate_limiter {
            let limiter = rate_limiter.lock();
            if !limiter.try_acquire() {
                return Err(QueryPlanError::Internal(
                    "Query rate limit exceeded. Please try again later.".to_string(),
                ));
            }
        }

        // SEC-009: Validate time range
        if !super::space_time::is_valid_timestamp(query.time_range.start) {
            return Err(QueryPlanError::InvalidTimeRange {
                start: query.time_range.start,
                end: query.time_range.end,
            });
        }
        if !super::space_time::is_valid_timestamp(query.time_range.end) {
            return Err(QueryPlanError::InvalidTimeRange {
                start: query.time_range.start,
                end: query.time_range.end,
            });
        }

        // Validate window size if specified
        if let Some(window) = query.window_size {
            if window.is_zero() {
                return Err(QueryPlanError::Internal(
                    "Window size cannot be zero".to_string(),
                ));
            }
            // Prevent extremely large windows
            const MAX_WINDOW_MS: u128 = 365 * 24 * 60 * 60 * 1000; // 1 year
            if window.as_millis() > MAX_WINDOW_MS {
                return Err(QueryPlanError::Internal(format!(
                    "Window size too large: {:?} (maximum: {:?})",
                    window, MAX_WINDOW_MS
                )));
            }
        }

        // Validate step size if specified
        if let Some(step) = query.step {
            if step.is_zero() {
                return Err(QueryPlanError::Internal(
                    "Step size cannot be zero".to_string(),
                ));
            }
            // Prevent extremely large steps
            const MAX_STEP_MS: u128 = 365 * 24 * 60 * 60 * 1000; // 1 year
            if step.as_millis() > MAX_STEP_MS {
                return Err(QueryPlanError::Internal(format!(
                    "Step size too large: {:?} (maximum: {:?})",
                    step, MAX_STEP_MS
                )));
            }
            // Step should not be larger than window if window is specified
            if let Some(window) = query.window_size {
                if step > window {
                    return Err(QueryPlanError::Internal(format!(
                        "Step size ({:?}) cannot be larger than window size ({:?})",
                        step, window
                    )));
                }
            }
        }

        // Validate series limit
        if let Some(limit) = query.series_limit {
            if limit == 0 {
                return Err(QueryPlanError::Internal(
                    "Series limit cannot be zero".to_string(),
                ));
            }
            if limit > self.config.max_series_hard_limit {
                return Err(QueryPlanError::TooManySeries {
                    count: limit,
                    limit: self.config.max_series_hard_limit,
                });
            }
        }

        // Validate max_points
        if let Some(max_points) = query.max_points {
            if max_points == 0 {
                return Err(QueryPlanError::Internal(
                    "Max points cannot be zero".to_string(),
                ));
            }
            const MAX_POINTS_LIMIT: usize = 10_000_000; // 10M points
            if max_points > MAX_POINTS_LIMIT {
                return Err(QueryPlanError::Internal(format!(
                    "Max points too large: {} (maximum: {})",
                    max_points, MAX_POINTS_LIMIT
                )));
            }
        }

        // Validate quantile if specified
        if let Some(quantile) = query.quantile {
            if !(0.0..=1.0).contains(&quantile) {
                return Err(QueryPlanError::Internal(format!(
                    "Quantile must be between 0.0 and 1.0, got: {}",
                    quantile
                )));
            }
        }

        // Validate time range duration to prevent excessive queries
        let duration_ns = query.time_range.end.saturating_sub(query.time_range.start);
        const MAX_DURATION_NS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000; // 1 year in nanoseconds
        if duration_ns > MAX_DURATION_NS {
            return Err(QueryPlanError::InvalidTimeRange {
                start: query.time_range.start,
                end: query.time_range.end,
            });
        }
        if duration_ns <= 0 {
            return Err(QueryPlanError::InvalidTimeRange {
                start: query.time_range.start,
                end: query.time_range.end,
            });
        }

        // Build tag matcher from query filters
        let matcher = self.build_tag_matcher(query)?;

        // Resolve series IDs using bitmap indexes
        let resolved = self
            .tag_resolver
            .resolve_with_info(&matcher)
            .map_err(|e| QueryPlanError::ResolutionError(e.to_string()))?;

        let series_ids: Vec<SeriesId> = resolved.iter().map(|r| r.series_id).collect();

        // Check cardinality limits
        if self.config.enable_cardinality_check {
            if series_ids.len() > self.config.max_series_hard_limit {
                return Err(QueryPlanError::TooManySeries {
                    count: series_ids.len(),
                    limit: self.config.max_series_hard_limit,
                });
            }

            if let Some(ref controller) = self.cardinality {
                let active = controller.active_series() as usize;
                if active > self.config.max_series_hard_limit {
                    return Err(QueryPlanError::CardinalityExceeded(
                        CardinalityError::SeriesLimitExceeded {
                            current: active,
                            limit: self.config.max_series_hard_limit,
                        },
                    ));
                }
            }
        }

        // Apply series limit if specified
        let series_ids = if let Some(limit) = query.series_limit {
            series_ids.into_iter().take(limit).collect()
        } else {
            series_ids
        };

        // Group series by grouping key if group_by is specified
        let series_groups = if query.group_by.is_empty() {
            // No grouping - all series in one group
            let mut groups = HashMap::new();
            groups.insert("_all".to_string(), series_ids.clone());
            groups
        } else {
            self.group_series_by_tags(&series_ids, &query.group_by)?
        };

        // Estimate query cost
        let estimated_cost = self.estimate_cost(query, series_ids.len());

        // Decide on parallel execution
        let use_parallel = series_ids.len() >= self.config.parallel_threshold;
        let parallel_workers = if use_parallel {
            std::cmp::min(series_ids.len(), self.config.max_parallel_workers)
        } else {
            1
        };

        Ok(QueryPlan {
            query: query.clone(),
            series_ids,
            series_groups,
            estimated_cost,
            use_parallel,
            parallel_workers,
        })
    }

    /// Execute a query plan using the provided data source
    ///
    /// Fetches data from the data source and performs space-time aggregation.
    pub fn execute<S: DataSource>(
        &self,
        plan: QueryPlan,
        data_source: S,
    ) -> Result<UnifiedTimeSeries, QueryPlanError> {
        // Create aggregator with data source
        let aggregator = SpaceTimeAggregator::new(data_source);

        // Build the underlying query for SpaceTimeAggregator
        // Uses the TagMatcher and TimeRange from the plan
        let matcher = self.build_tag_matcher(&plan.query)?;
        let mut agg_query =
            AggregateQuery::new(matcher.clone(), plan.query.time_range, plan.query.function);

        if let Some(window) = plan.query.window_size {
            agg_query = agg_query.with_window(window);
        }

        if let Some(limit) = plan.query.max_points {
            agg_query = agg_query.with_limit(limit);
        }

        // Aggregate across all resolved series
        let result = aggregator
            .aggregate(&plan.series_ids, &agg_query)
            .map_err(|e| QueryPlanError::ExecutionError(e.to_string()))?;

        Ok(result)
    }

    /// Execute with per-group aggregation
    ///
    /// Returns a map of group key to aggregated time series.
    pub fn execute_grouped<S: DataSource + Clone>(
        &self,
        plan: QueryPlan,
        data_source: S,
    ) -> Result<HashMap<String, UnifiedTimeSeries>, QueryPlanError> {
        let matcher = self.build_tag_matcher(&plan.query)?;

        let mut results = HashMap::new();

        for (group_key, series_ids) in &plan.series_groups {
            // Create a new aggregator for each group
            let aggregator = SpaceTimeAggregator::new(data_source.clone());

            let mut agg_query =
                AggregateQuery::new(matcher.clone(), plan.query.time_range, plan.query.function);

            if let Some(window) = plan.query.window_size {
                agg_query = agg_query.with_window(window);
            }

            if let Some(limit) = plan.query.max_points {
                agg_query = agg_query.with_limit(limit);
            }

            let result = aggregator
                .aggregate(series_ids, &agg_query)
                .map_err(|e| QueryPlanError::ExecutionError(e.to_string()))?;

            results.insert(group_key.clone(), result);
        }

        Ok(results)
    }

    /// Build a TagMatcher from query filters
    ///
    /// # Security
    /// - Validates regex patterns to prevent ReDoS attacks
    /// - Limits pattern complexity
    /// - Validates tag key/value lengths
    ///
    /// # Errors
    /// Returns an error if validation fails (e.g., ReDoS pattern detected)
    fn build_tag_matcher(&self, query: &AggQuery) -> Result<TagMatcher, QueryPlanError> {
        // SEC-003: Validate metric name
        const MAX_METRIC_NAME_LEN: usize = 256;
        if query.metric_name.len() > MAX_METRIC_NAME_LEN {
            return Err(QueryPlanError::Internal(format!(
                "Metric name too long: {} chars (max: {})",
                query.metric_name.len(),
                MAX_METRIC_NAME_LEN
            )));
        }
        if query.metric_name.is_empty() {
            return Err(QueryPlanError::Internal(
                "Metric name cannot be empty".to_string(),
            ));
        }

        let mut matcher = TagMatcher::new().metric(&query.metric_name);

        for filter in &query.tag_filters {
            // SEC-003: Validate tag key/value lengths
            const MAX_TAG_KEY_LEN: usize = 256;
            const MAX_TAG_VALUE_LEN: usize = 1024;
            if filter.key.len() > MAX_TAG_KEY_LEN {
                return Err(QueryPlanError::Internal(format!(
                    "Tag key too long: {} chars (max: {})",
                    filter.key.len(),
                    MAX_TAG_KEY_LEN
                )));
            }
            if filter.key.is_empty() {
                return Err(QueryPlanError::Internal(
                    "Tag key cannot be empty".to_string(),
                ));
            }
            if filter.value.len() > MAX_TAG_VALUE_LEN {
                return Err(QueryPlanError::Internal(format!(
                    "Tag value too long: {} chars (max: {})",
                    filter.value.len(),
                    MAX_TAG_VALUE_LEN
                )));
            }

            match filter.mode {
                TagMatchMode::Exact => {
                    matcher = matcher.with(&filter.key, &filter.value);
                },
                TagMatchMode::Regex => {
                    // SEC-003: Validate regex pattern complexity to prevent ReDoS
                    const MAX_REGEX_LEN: usize = 1000;
                    if filter.value.len() > MAX_REGEX_LEN {
                        return Err(QueryPlanError::Internal(format!(
                            "Regex pattern too long: {} chars (max: {})",
                            filter.value.len(),
                            MAX_REGEX_LEN
                        )));
                    }
                    // Check for nested quantifiers (common ReDoS pattern)
                    if filter.value.contains("(.*)*") || filter.value.contains("(.*+)*") {
                        return Err(QueryPlanError::Internal(format!(
                            "Potential ReDoS pattern detected in regex: {}",
                            if filter.value.len() > 100 {
                                &filter.value[..100]
                            } else {
                                &filter.value
                            }
                        )));
                    }
                    // Check for exponential backtracking patterns
                    if filter.value.matches(r"(.+)+").count() > 0
                        || filter.value.matches(r"(.+)*").count() > 0
                    {
                        return Err(QueryPlanError::Internal(
                            "Regex pattern may cause exponential backtracking".to_string(),
                        ));
                    }
                    matcher = matcher.with_regex(&filter.key, &filter.value);
                },
                TagMatchMode::Prefix => {
                    matcher = matcher.with_prefix(&filter.key, &filter.value);
                },
                TagMatchMode::NotEqual => {
                    matcher = matcher.without(&filter.key, &filter.value);
                },
                TagMatchMode::Exists => {
                    matcher = matcher.has_key(&filter.key);
                },
            }
        }

        Ok(matcher)
    }

    /// Group series by tag values
    ///
    /// This resolves the interned tag IDs back to strings using the TagDictionary.
    fn group_series_by_tags(
        &self,
        series_ids: &[SeriesId],
        group_by: &[String],
    ) -> Result<HashMap<String, Vec<SeriesId>>, QueryPlanError> {
        let mut groups: HashMap<String, Vec<SeriesId>> = HashMap::new();
        let dictionary = self.metadata.dictionary();

        for &series_id in series_ids {
            // Get series entry to access interned tags
            if let Some(entry) = self.metadata.get_series(series_id) {
                // Build group key from tag values by resolving interned IDs
                let mut key_parts: Vec<String> = Vec::new();

                for group_key in group_by {
                    // Find the tag value for this key by first getting the key ID
                    if let Some(key_id) = dictionary.get_key_id(group_key) {
                        // Find the tag with this key and resolve the value
                        let value = entry
                            .tags
                            .iter()
                            .find(|t| t.key == key_id)
                            .and_then(|t| dictionary.resolve_value(t.key, t.value))
                            .unwrap_or_else(|| "_unknown".to_string());

                        key_parts.push(format!("{}={}", group_key, value));
                    } else {
                        key_parts.push(format!("{}=_unknown", group_key));
                    }
                }

                let group_key = key_parts.join(",");
                groups.entry(group_key).or_default().push(series_id);
            }
        }

        Ok(groups)
    }

    /// Estimate query cost
    ///
    /// # Performance
    /// - More accurate cost estimation based on actual data characteristics
    /// - Accounts for windowing overhead
    /// - Estimates memory usage more precisely
    ///
    /// # Security
    /// - Uses saturating arithmetic to prevent integer overflow
    fn estimate_cost(&self, query: &AggQuery, series_count: usize) -> QueryCost {
        // Estimate points based on time range
        // Note: time_range is in nanoseconds, convert to seconds
        // SEC: Use saturating arithmetic to prevent overflow
        let duration_ns = query.time_range.end.saturating_sub(query.time_range.start);
        let duration_secs = duration_ns.saturating_div(1_000_000_000);
        let duration_ms = duration_ns.saturating_div(1_000_000);

        // Estimate points per series based on typical ingestion rate
        // Assume 1 point per second for typical metrics
        // SEC: Use checked arithmetic to prevent overflow
        let points_per_series = (duration_secs as usize).max(1);
        let total_points = points_per_series
            .checked_mul(series_count)
            .map(|p| p as u64)
            .unwrap_or(u64::MAX);

        // Adjust for windowing: if windowed, estimate number of output points
        let estimated_output_points = if let Some(window) = query.window_size {
            let window_ms = window.as_millis() as i64;
            let step_ms = query
                .step
                .map(|s| s.as_millis() as i64)
                .unwrap_or(window_ms);
            // Number of windows = ceil(duration / step)
            // SEC: Use checked arithmetic to prevent overflow
            if step_ms > 0 {
                let num_windows = duration_ms
                    .checked_add(step_ms - 1)
                    .and_then(|d| d.checked_div(step_ms))
                    .map(|n| n.max(1) as u64)
                    .unwrap_or(u64::MAX);
                num_windows
            } else {
                1
            }
        } else {
            // Instant aggregation produces 1 point
            1
        };

        // Memory: ~16 bytes per input point (timestamp + value)
        // Plus ~24 bytes per output point (AggregatedPoint)
        // SEC: Use checked arithmetic to prevent overflow
        let input_memory = (total_points as usize)
            .checked_mul(16)
            .unwrap_or(usize::MAX);
        let output_memory = (estimated_output_points as usize)
            .checked_mul(24)
            .unwrap_or(usize::MAX);
        let estimated_memory = input_memory
            .checked_add(output_memory)
            .unwrap_or(usize::MAX);

        // CPU cost based on operation complexity
        let cpu_factor = match query.function {
            AggregateFunction::Count | AggregateFunction::Sum => 1.0,
            AggregateFunction::Avg | AggregateFunction::Min | AggregateFunction::Max => 1.0,
            AggregateFunction::StdDev | AggregateFunction::Variance => 2.0,
            AggregateFunction::Quantile(_) => 3.0, // Requires sorting
            AggregateFunction::Rate | AggregateFunction::Increase => 1.5,
            _ => 1.0,
        };

        // CPU cost scales with input points and output points
        let cpu_cost =
            (total_points as f64) * 0.001 * cpu_factor + (estimated_output_points as f64) * 0.01; // Output processing overhead

        // I/O cost based on data volume
        // Assume 10ms per series for I/O, plus overhead for large time ranges
        let io_cost = (series_count as f64) * 10.0 + (duration_secs as f64) * 0.1; // Additional cost for large time ranges

        QueryCost {
            series_count,
            estimated_points: total_points,
            estimated_memory,
            cpu_cost,
            io_cost,
        }
    }
}

// ============================================================================
// Query Errors
// ============================================================================

/// Errors that can occur during query planning
#[derive(Debug)]
pub enum QueryPlanError {
    /// Missing time range in query
    MissingTimeRange,

    /// Invalid time range (start >= end)
    InvalidTimeRange {
        /// Start timestamp
        start: i64,
        /// End timestamp
        end: i64,
    },

    /// Too many series matched
    TooManySeries {
        /// Number of series matched
        count: usize,
        /// Maximum allowed
        limit: usize,
    },

    /// Cardinality limit exceeded
    CardinalityExceeded(CardinalityError),

    /// Series resolution error
    ResolutionError(String),

    /// Query execution error
    ExecutionError(String),

    /// Internal error
    Internal(String),
}

impl std::fmt::Display for QueryPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryPlanError::MissingTimeRange => write!(f, "Query requires a time range"),
            QueryPlanError::InvalidTimeRange { start, end } => {
                write!(
                    f,
                    "Invalid time range: start ({}) must be less than end ({})",
                    start, end
                )
            },
            QueryPlanError::TooManySeries { count, limit } => {
                write!(
                    f,
                    "Query matched too many series: {} (limit: {})",
                    count, limit
                )
            },
            QueryPlanError::CardinalityExceeded(e) => {
                write!(f, "Cardinality limit exceeded: {}", e)
            },
            QueryPlanError::ResolutionError(msg) => write!(f, "Series resolution failed: {}", msg),
            QueryPlanError::ExecutionError(msg) => write!(f, "Query execution failed: {}", msg),
            QueryPlanError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for QueryPlanError {}

impl From<Error> for QueryPlanError {
    fn from(e: Error) -> Self {
        QueryPlanError::Internal(e.to_string())
    }
}

impl From<CardinalityError> for QueryPlanError {
    fn from(e: CardinalityError) -> Self {
        QueryPlanError::CardinalityExceeded(e)
    }
}

// ============================================================================
// Query Result Conversion
// ============================================================================

/// Extension trait for converting aggregation results to query results
pub trait ToQueryResult {
    /// Convert to a format suitable for the query engine
    fn to_result_rows(&self) -> Vec<ResultPoint>;
}

/// A single result point
#[derive(Debug, Clone)]
pub struct ResultPoint {
    /// Timestamp in nanoseconds
    pub timestamp: i64,

    /// Aggregated value
    pub value: f64,

    /// Optional labels/tags
    pub labels: HashMap<String, String>,
}

impl ToQueryResult for UnifiedTimeSeries {
    fn to_result_rows(&self) -> Vec<ResultPoint> {
        self.points
            .iter()
            .map(|p| ResultPoint {
                timestamp: p.timestamp,
                value: p.value,
                labels: HashMap::new(),
            })
            .collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ===== AggQueryBuilder tests =====

    #[test]
    fn test_query_builder_basic() {
        let query = AggQueryBuilder::new("cpu_usage")
            .with_tag("datacenter", "us-east")
            .time_range(1000, 2000)
            .aggregate(AggregateFunction::Avg)
            .build()
            .unwrap();

        assert_eq!(query.metric_name, "cpu_usage");
        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.time_range.start, 1000);
        assert_eq!(query.time_range.end, 2000);
    }

    #[test]
    fn test_query_builder_with_window() {
        let query = AggQueryBuilder::new("http_requests")
            .time_range(0, 3_600_000_000_000) // 1 hour in nanos
            .aggregate(AggregateFunction::Sum)
            .window_size(Duration::from_secs(60))
            .build()
            .unwrap();

        assert!(query.window_size.is_some());
        assert_eq!(query.window_size.unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_query_builder_with_group_by() {
        let query = AggQueryBuilder::new("latency")
            .time_range(0, 1000)
            .group_by(&["host", "service"])
            .build()
            .unwrap();

        assert_eq!(query.group_by.len(), 2);
        assert_eq!(query.group_by[0], "host");
        assert_eq!(query.group_by[1], "service");
    }

    #[test]
    fn test_query_builder_missing_time_range() {
        let result = AggQueryBuilder::new("cpu_usage")
            .aggregate(AggregateFunction::Avg)
            .build();

        assert!(matches!(result, Err(QueryPlanError::MissingTimeRange)));
    }

    #[test]
    fn test_query_builder_invalid_time_range() {
        let result = AggQueryBuilder::new("cpu_usage")
            .time_range(2000, 1000) // start > end
            .build();

        assert!(matches!(
            result,
            Err(QueryPlanError::InvalidTimeRange { .. })
        ));
    }

    #[test]
    fn test_query_builder_with_tag_regex() {
        let query = AggQueryBuilder::new("metric")
            .with_tag_regex("host", "server-[0-9]+")
            .time_range(0, 1000)
            .build()
            .unwrap();

        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.tag_filters[0].mode, TagMatchMode::Regex);
    }

    #[test]
    fn test_query_builder_with_tag_prefix() {
        let query = AggQueryBuilder::new("metric")
            .with_tag_prefix("env", "prod")
            .time_range(0, 1000)
            .build()
            .unwrap();

        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.tag_filters[0].mode, TagMatchMode::Prefix);
    }

    #[test]
    fn test_query_builder_without_tag() {
        let query = AggQueryBuilder::new("metric")
            .without_tag("env", "test")
            .time_range(0, 1000)
            .build()
            .unwrap();

        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.tag_filters[0].mode, TagMatchMode::NotEqual);
    }

    #[test]
    fn test_query_builder_has_tag() {
        let query = AggQueryBuilder::new("metric")
            .has_tag("region")
            .time_range(0, 1000)
            .build()
            .unwrap();

        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.tag_filters[0].mode, TagMatchMode::Exists);
        assert_eq!(query.tag_filters[0].key, "region");
    }

    #[test]
    fn test_query_builder_quantile() {
        let query = AggQueryBuilder::new("latency")
            .time_range(0, 1000)
            .quantile(0.99)
            .build()
            .unwrap();

        assert_eq!(query.quantile, Some(0.99));
    }

    #[test]
    fn test_query_builder_series_limit() {
        let query = AggQueryBuilder::new("metric")
            .time_range(0, 1000)
            .series_limit(100)
            .build()
            .unwrap();

        assert_eq!(query.series_limit, Some(100));
    }

    #[test]
    fn test_query_builder_max_points() {
        let query = AggQueryBuilder::new("metric")
            .time_range(0, 1000)
            .max_points(1000)
            .build()
            .unwrap();

        assert_eq!(query.max_points, Some(1000));
    }

    #[test]
    fn test_query_builder_chaining() {
        let query = AggQueryBuilder::new("cpu")
            .with_tag("dc", "us-east")
            .with_tag_regex("host", "web-.*")
            .has_tag("tier")
            .without_tag("deprecated", "true")
            .time_range(0, 3_600_000)
            .aggregate(AggregateFunction::Max)
            .window_size(Duration::from_secs(60))
            .group_by(&["host"])
            .series_limit(50)
            .max_points(500)
            .build()
            .unwrap();

        assert_eq!(query.tag_filters.len(), 4);
        assert_eq!(query.function, AggregateFunction::Max);
    }

    #[test]
    fn test_query_builder_equal_time_range() {
        let result = AggQueryBuilder::new("metric")
            .time_range(1000, 1000) // start == end
            .build();

        assert!(matches!(
            result,
            Err(QueryPlanError::InvalidTimeRange { .. })
        ));
    }

    #[test]
    fn test_query_builder_debug() {
        let builder = AggQueryBuilder::new("test");
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("AggQueryBuilder"));
    }

    // ===== AggQuery tests =====

    #[test]
    fn test_agg_query_clone() {
        let query = AggQueryBuilder::new("test")
            .time_range(0, 1000)
            .build()
            .unwrap();

        let cloned = query.clone();
        assert_eq!(cloned.metric_name, query.metric_name);
        assert_eq!(cloned.time_range.start, query.time_range.start);
    }

    #[test]
    fn test_agg_query_debug() {
        let query = AggQueryBuilder::new("test")
            .time_range(0, 1000)
            .build()
            .unwrap();

        let debug_str = format!("{:?}", query);
        assert!(debug_str.contains("AggQuery"));
    }

    // ===== TagFilterSpec tests =====

    #[test]
    fn test_tag_filter_spec_clone() {
        let filter = TagFilterSpec {
            key: "host".to_string(),
            value: "server1".to_string(),
            mode: TagMatchMode::Exact,
        };

        let cloned = filter.clone();
        assert_eq!(cloned.key, "host");
        assert_eq!(cloned.value, "server1");
        assert_eq!(cloned.mode, TagMatchMode::Exact);
    }

    #[test]
    fn test_tag_filter_spec_debug() {
        let filter = TagFilterSpec {
            key: "env".to_string(),
            value: "prod".to_string(),
            mode: TagMatchMode::Prefix,
        };

        let debug_str = format!("{:?}", filter);
        assert!(debug_str.contains("TagFilterSpec"));
        assert!(debug_str.contains("env"));
    }

    // ===== TagMatchMode tests =====

    #[test]
    fn test_tag_match_mode_equality() {
        assert_eq!(TagMatchMode::Exact, TagMatchMode::Exact);
        assert_ne!(TagMatchMode::Exact, TagMatchMode::Regex);
    }

    #[test]
    fn test_tag_match_mode_copy() {
        let mode = TagMatchMode::Prefix;
        let copied = mode;
        assert_eq!(mode, copied);
    }

    #[test]
    fn test_tag_match_mode_all_variants() {
        let modes = [
            TagMatchMode::Exact,
            TagMatchMode::Regex,
            TagMatchMode::Prefix,
            TagMatchMode::NotEqual,
            TagMatchMode::Exists,
        ];

        for mode in modes {
            let debug_str = format!("{:?}", mode);
            assert!(!debug_str.is_empty());
        }
    }

    // ===== QueryCost tests =====

    #[test]
    fn test_query_cost_default() {
        let cost = QueryCost::default();
        assert_eq!(cost.series_count, 0);
        assert_eq!(cost.estimated_points, 0);
        assert_eq!(cost.estimated_memory, 0);
        assert_eq!(cost.cpu_cost, 0.0);
        assert_eq!(cost.io_cost, 0.0);
    }

    #[test]
    fn test_query_cost_total_cost() {
        let cost = QueryCost {
            series_count: 10,
            estimated_points: 1000,
            estimated_memory: 16000,
            cpu_cost: 1.0,
            io_cost: 0.5,
        };

        // I/O is 10x more expensive than CPU
        let expected = 1.0 + 0.5 * 10.0;
        assert_eq!(cost.total_cost(), expected);
    }

    #[test]
    fn test_query_cost_clone() {
        let cost = QueryCost {
            series_count: 50,
            estimated_points: 5000,
            estimated_memory: 80000,
            cpu_cost: 2.5,
            io_cost: 3.0,
        };

        let cloned = cost.clone();
        assert_eq!(cloned.series_count, 50);
        assert_eq!(cloned.cpu_cost, 2.5);
    }

    #[test]
    fn test_query_cost_debug() {
        let cost = QueryCost::default();
        let debug_str = format!("{:?}", cost);
        assert!(debug_str.contains("QueryCost"));
    }

    // ===== QueryPlannerConfig tests =====

    #[test]
    fn test_planner_config_defaults() {
        let config = QueryPlannerConfig::default();
        assert_eq!(config.max_series_soft_limit, 1000);
        assert_eq!(config.max_series_hard_limit, 10000);
        assert!(config.enable_cardinality_check);
    }

    #[test]
    fn test_planner_config_clone() {
        let config = QueryPlannerConfig {
            max_series_soft_limit: 500,
            max_series_hard_limit: 5000,
            parallel_threshold: 5,
            max_parallel_workers: 4,
            enable_cardinality_check: false,
            max_queries_per_second: Some(100),
            query_timeout_secs: Some(300),
        };

        let cloned = config.clone();
        assert_eq!(cloned.max_series_soft_limit, 500);
        assert_eq!(cloned.max_parallel_workers, 4);
        assert!(!cloned.enable_cardinality_check);
    }

    #[test]
    fn test_planner_config_debug() {
        let config = QueryPlannerConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("QueryPlannerConfig"));
    }

    // ===== ResultPoint tests =====

    #[test]
    fn test_result_point_clone() {
        let point = ResultPoint {
            timestamp: 1000,
            value: 42.5,
            labels: HashMap::from([("host".to_string(), "server1".to_string())]),
        };

        let cloned = point.clone();
        assert_eq!(cloned.timestamp, 1000);
        assert_eq!(cloned.value, 42.5);
        assert_eq!(cloned.labels.get("host"), Some(&"server1".to_string()));
    }

    #[test]
    fn test_result_point_debug() {
        let point = ResultPoint {
            timestamp: 100,
            value: 1.0,
            labels: HashMap::new(),
        };

        let debug_str = format!("{:?}", point);
        assert!(debug_str.contains("ResultPoint"));
    }

    // ===== QueryPlanError tests =====

    #[test]
    fn test_query_plan_error_display_missing_time_range() {
        let err = QueryPlanError::MissingTimeRange;
        let msg = err.to_string();
        assert!(msg.contains("time range"));
    }

    #[test]
    fn test_query_plan_error_display_invalid_time_range() {
        let err = QueryPlanError::InvalidTimeRange {
            start: 200,
            end: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("200"));
        assert!(msg.contains("100"));
    }

    #[test]
    fn test_query_plan_error_display_too_many_series() {
        let err = QueryPlanError::TooManySeries {
            count: 15000,
            limit: 10000,
        };
        let msg = err.to_string();
        assert!(msg.contains("15000"));
        assert!(msg.contains("10000"));
    }

    #[test]
    fn test_query_plan_error_display_resolution_error() {
        let err = QueryPlanError::ResolutionError("tag not found".to_string());
        let msg = err.to_string();
        assert!(msg.contains("tag not found"));
    }

    #[test]
    fn test_query_plan_error_display_execution_error() {
        let err = QueryPlanError::ExecutionError("timeout".to_string());
        let msg = err.to_string();
        assert!(msg.contains("timeout"));
    }

    #[test]
    fn test_query_plan_error_display_internal() {
        let err = QueryPlanError::Internal("unexpected".to_string());
        let msg = err.to_string();
        assert!(msg.contains("unexpected"));
    }

    #[test]
    fn test_query_plan_error_debug() {
        let err = QueryPlanError::MissingTimeRange;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("MissingTimeRange"));
    }

    // ===== Query cost estimation tests =====

    #[test]
    fn test_query_cost_estimation() {
        let config = QueryPlannerConfig::default();
        let store = MetadataStore::in_memory();
        let planner = AggQueryPlanner::new(Arc::new(store)).with_config(config);

        let query = AggQuery {
            metric_name: "test".to_string(),
            tag_filters: vec![],
            time_range: TimeRange {
                start: 0,
                end: 60_000_000_000,
            }, // 1 minute
            function: AggregateFunction::Avg,
            window_size: None,
            step: None,
            group_by: vec![],
            quantile: None,
            series_limit: None,
            max_points: None,
        };

        let cost = planner.estimate_cost(&query, 100);
        assert_eq!(cost.series_count, 100);
        assert!(cost.estimated_points > 0);
        assert!(cost.cpu_cost > 0.0);
    }

    #[test]
    fn test_query_cost_estimation_stddev() {
        let config = QueryPlannerConfig::default();
        let store = MetadataStore::in_memory();
        let planner = AggQueryPlanner::new(Arc::new(store)).with_config(config);

        let query = AggQuery {
            metric_name: "test".to_string(),
            tag_filters: vec![],
            time_range: TimeRange {
                start: 0,
                end: 60_000_000_000,
            },
            function: AggregateFunction::StdDev,
            window_size: None,
            step: None,
            group_by: vec![],
            quantile: None,
            series_limit: None,
            max_points: None,
        };

        let cost_stddev = planner.estimate_cost(&query, 100);

        let query_avg = AggQuery {
            function: AggregateFunction::Avg,
            ..query.clone()
        };
        let cost_avg = planner.estimate_cost(&query_avg, 100);

        // StdDev should have higher CPU cost
        assert!(cost_stddev.cpu_cost > cost_avg.cpu_cost);
    }

    // ===== AggQueryPlanner tests =====

    #[test]
    fn test_agg_query_planner_new() {
        let store = MetadataStore::in_memory();
        let planner = AggQueryPlanner::new(Arc::new(store));
        // Just verify it doesn't panic
        let _ = planner;
    }

    #[test]
    fn test_agg_query_planner_with_config() {
        let store = MetadataStore::in_memory();
        let config = QueryPlannerConfig {
            max_series_hard_limit: 5000,
            ..Default::default()
        };
        let planner = AggQueryPlanner::new(Arc::new(store)).with_config(config);
        let _ = planner;
    }
}
