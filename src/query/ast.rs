//! Query Abstract Syntax Tree (AST)
//!
//! Defines the query language structure for time-series queries.
//! Supports range queries, aggregations, downsampling, and streaming.
//!
//! # Query Types
//!
//! - **Select**: Basic time-range queries with optional filtering
//! - **Aggregate**: Grouped aggregations with windowing
//! - **Downsample**: Visual downsampling for charting (LTTB, M4)
//! - **Latest**: Get most recent value(s) for series
//! - **Stream**: Continuous queries for real-time data
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::query::ast::{QueryBuilder, Aggregation, AggregationFunction};
//! use gorilla_tsdb::types::TimeRange;
//! use std::time::Duration;
//!
//! // Build a windowed average query
//! let query = QueryBuilder::new()
//!     .select_measurement("cpu")
//!     .unwrap()
//!     .with_tag("host", "server01")
//!     .time_range(TimeRange { start: 0, end: 1000 })
//!     .aggregate(AggregationFunction::Avg)
//!     .window(Duration::from_secs(60))
//!     .build()
//!     .unwrap();
//! ```

use crate::types::{SeriesId, TagFilter, TimeRange};
use regex::Regex;
use std::collections::HashMap;
use std::time::Duration;

use super::error::{QueryError, QueryResult};

// ============================================================================
// Core Query Types
// ============================================================================

/// Root query type representing all supported query operations
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    /// Basic range query: SELECT points WHERE time BETWEEN start AND end
    Select(SelectQuery),

    /// Aggregation query: SELECT agg(value) GROUP BY time_bucket
    Aggregate(AggregateQuery),

    /// Downsampling query: SELECT downsample(points, n) for visualization
    Downsample(DownsampleQuery),

    /// Latest query: SELECT last point(s) for each series
    Latest(LatestQuery),

    /// Streaming query: Continuous subscription to new data
    Stream(StreamQuery),

    /// Explain query: Return execution plan without executing
    Explain(Box<Query>),
}

impl Query {
    /// Check if this query modifies data (always false for queries)
    pub fn is_read_only(&self) -> bool {
        true
    }

    /// Get the series selector for this query
    pub fn series_selector(&self) -> &SeriesSelector {
        match self {
            Query::Select(q) => &q.selector,
            Query::Aggregate(q) => &q.selector,
            Query::Downsample(q) => &q.selector,
            Query::Latest(q) => &q.selector,
            Query::Stream(q) => &q.selector,
            Query::Explain(q) => q.series_selector(),
        }
    }

    /// Get the time range for this query (if applicable)
    pub fn time_range(&self) -> Option<&TimeRange> {
        match self {
            Query::Select(q) => Some(&q.time_range),
            Query::Aggregate(q) => Some(&q.time_range),
            Query::Downsample(q) => Some(&q.time_range),
            Query::Latest(_) => None,
            Query::Stream(_) => None,
            Query::Explain(q) => q.time_range(),
        }
    }
}

// ============================================================================
// Select Query
// ============================================================================

/// Basic range query for retrieving raw data points
#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    /// Series to query
    pub selector: SeriesSelector,

    /// Time range for the query
    pub time_range: TimeRange,

    /// Optional value filter predicates
    pub predicates: Vec<Predicate>,

    /// Fields to return (empty = all fields)
    pub projections: Vec<String>,

    /// Result ordering
    pub order_by: Option<OrderBy>,

    /// Maximum number of results
    pub limit: Option<usize>,

    /// Number of results to skip
    pub offset: Option<usize>,
}

impl SelectQuery {
    /// Create a new select query for a series
    pub fn new(selector: SeriesSelector, time_range: TimeRange) -> Self {
        Self {
            selector,
            time_range,
            predicates: Vec::new(),
            projections: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Add a value predicate filter
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set result ordering
    pub fn with_order(mut self, order: OrderBy) -> Self {
        self.order_by = Some(order);
        self
    }
}

// ============================================================================
// Aggregate Query
// ============================================================================

/// Aggregation query with grouping and windowing
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateQuery {
    /// Series to aggregate
    pub selector: SeriesSelector,

    /// Time range for the query
    pub time_range: TimeRange,

    /// Aggregation specification
    pub aggregation: Aggregation,

    /// Optional value filter predicates (applied before aggregation)
    pub predicates: Vec<Predicate>,

    /// Result ordering
    pub order_by: Option<OrderBy>,

    /// Maximum number of results
    pub limit: Option<usize>,
}

impl AggregateQuery {
    /// Create a new aggregate query
    pub fn new(selector: SeriesSelector, time_range: TimeRange, aggregation: Aggregation) -> Self {
        Self {
            selector,
            time_range,
            aggregation,
            predicates: Vec::new(),
            order_by: None,
            limit: None,
        }
    }
}

/// Aggregation specification including function, window, and grouping
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregation {
    /// Aggregation function to apply
    pub function: AggregationFunction,

    /// Time window for grouping (None = aggregate all)
    pub window: Option<WindowSpec>,

    /// Additional grouping dimensions (tag keys)
    pub group_by: Vec<String>,

    /// Fill strategy for empty windows
    pub fill: FillStrategy,
}

impl Aggregation {
    /// Create a simple aggregation without windowing
    pub fn simple(function: AggregationFunction) -> Self {
        Self {
            function,
            window: None,
            group_by: Vec::new(),
            fill: FillStrategy::None,
        }
    }

    /// Create a windowed aggregation
    pub fn windowed(function: AggregationFunction, window: WindowSpec) -> Self {
        Self {
            function,
            window: Some(window),
            group_by: Vec::new(),
            fill: FillStrategy::None,
        }
    }

    /// Add group by dimensions
    pub fn with_group_by(mut self, tags: Vec<String>) -> Self {
        self.group_by = tags;
        self
    }

    /// Set fill strategy for empty windows
    pub fn with_fill(mut self, fill: FillStrategy) -> Self {
        self.fill = fill;
        self
    }
}

/// Supported aggregation functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationFunction {
    // Basic aggregations
    /// Count of values
    Count,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Arithmetic mean
    Avg,

    // Statistical aggregations
    /// Standard deviation (sample)
    StdDev,
    /// Variance (sample)
    Variance,

    // Percentile aggregations (using t-digest)
    /// Median (50th percentile)
    Median,
    /// Specific percentile (0-100)
    Percentile(u8),

    // Positional aggregations
    /// First value in window
    First,
    /// Last value in window
    Last,

    // Time-series specific
    /// Rate of change (derivative)
    Rate,
    /// Cumulative increase (handles counter resets)
    Increase,
    /// Difference from previous value
    Delta,

    // Approximate aggregations
    /// Approximate count of distinct values
    CountDistinct,
}

impl AggregationFunction {
    /// Check if this function requires sorted input
    pub fn requires_sorted(&self) -> bool {
        matches!(
            self,
            AggregationFunction::First
                | AggregationFunction::Last
                | AggregationFunction::Rate
                | AggregationFunction::Increase
                | AggregationFunction::Delta
        )
    }

    /// Check if this function can be computed incrementally
    pub fn is_incremental(&self) -> bool {
        matches!(
            self,
            AggregationFunction::Count
                | AggregationFunction::Sum
                | AggregationFunction::Min
                | AggregationFunction::Max
        )
    }

    /// Check if this function produces approximate results
    pub fn is_approximate(&self) -> bool {
        matches!(
            self,
            AggregationFunction::Percentile(_)
                | AggregationFunction::Median
                | AggregationFunction::CountDistinct
        )
    }
}

/// Time window specification for aggregation
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    /// Window duration
    pub duration: Duration,

    /// Window type (tumbling, sliding, etc.)
    pub window_type: WindowType,

    /// Alignment offset from epoch
    pub offset: Option<Duration>,
}

impl WindowSpec {
    /// Create a tumbling window (non-overlapping)
    pub fn tumbling(duration: Duration) -> Self {
        Self {
            duration,
            window_type: WindowType::Tumbling,
            offset: None,
        }
    }

    /// Create a sliding window with specified slide interval
    pub fn sliding(duration: Duration, slide: Duration) -> Self {
        Self {
            duration,
            window_type: WindowType::Sliding { slide },
            offset: None,
        }
    }

    /// Set window alignment offset
    pub fn with_offset(mut self, offset: Duration) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Window type for aggregation
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Non-overlapping fixed windows
    Tumbling,
    /// Overlapping windows with slide interval
    Sliding {
        /// Slide interval between window starts
        slide: Duration,
    },
    /// Session windows based on activity gaps
    Session {
        /// Maximum gap between events in same session
        gap: Duration,
    },
}

/// Strategy for filling empty windows
#[derive(Debug, Clone, PartialEq)]
pub enum FillStrategy {
    /// No fill - omit empty windows
    None,
    /// Fill with null/NaN
    Null,
    /// Fill with zero
    Zero,
    /// Fill with previous value
    Previous,
    /// Linear interpolation
    Linear,
    /// Fill with specific value
    Value(f64),
}

// ============================================================================
// Downsample Query
// ============================================================================

/// Downsampling query for reducing data points while preserving visual fidelity
#[derive(Debug, Clone, PartialEq)]
pub struct DownsampleQuery {
    /// Series to downsample
    pub selector: SeriesSelector,

    /// Time range for the query
    pub time_range: TimeRange,

    /// Target number of output points
    pub target_points: usize,

    /// Downsampling method
    pub method: DownsampleMethod,
}

impl DownsampleQuery {
    /// Create a new downsample query
    pub fn new(
        selector: SeriesSelector,
        time_range: TimeRange,
        target_points: usize,
        method: DownsampleMethod,
    ) -> Self {
        Self {
            selector,
            time_range,
            target_points,
            method,
        }
    }
}

/// Downsampling method for visual data reduction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownsampleMethod {
    /// Largest Triangle Three Buckets - best for general visualization
    Lttb,
    /// M4 algorithm - preserves min, max, first, last per bucket
    M4,
    /// Simple average per bucket
    Average,
    /// Min/Max per bucket (good for showing range)
    MinMax,
    /// First value per bucket
    First,
    /// Last value per bucket
    Last,
}

// ============================================================================
// Latest Query
// ============================================================================

/// Query for retrieving the most recent value(s)
#[derive(Debug, Clone, PartialEq)]
pub struct LatestQuery {
    /// Series to query
    pub selector: SeriesSelector,

    /// Number of latest points to return (default: 1)
    pub count: usize,
}

impl LatestQuery {
    /// Create a query for the single latest value
    pub fn new(selector: SeriesSelector) -> Self {
        Self { selector, count: 1 }
    }

    /// Create a query for the N latest values
    pub fn with_count(selector: SeriesSelector, count: usize) -> Self {
        Self { selector, count }
    }
}

// ============================================================================
// Stream Query
// ============================================================================

/// Continuous query for streaming new data
#[derive(Debug, Clone, PartialEq)]
pub struct StreamQuery {
    /// Series to stream
    pub selector: SeriesSelector,

    /// Optional aggregation for streaming results
    pub aggregation: Option<Aggregation>,

    /// Stream mode
    pub mode: StreamMode,
}

impl StreamQuery {
    /// Create a raw data stream (tail mode)
    pub fn tail(selector: SeriesSelector) -> Self {
        Self {
            selector,
            aggregation: None,
            mode: StreamMode::Tail,
        }
    }

    /// Create a windowed aggregation stream
    pub fn windowed(selector: SeriesSelector, aggregation: Aggregation) -> Self {
        Self {
            selector,
            aggregation: Some(aggregation),
            mode: StreamMode::Window,
        }
    }
}

/// Streaming mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// Stream raw data points as they arrive
    Tail,
    /// Stream aggregated windows
    Window,
    /// Stream changes (delta mode)
    Changes,
}

// ============================================================================
// Series Selector
// ============================================================================

/// Selector for matching one or more time series
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesSelector {
    /// Match by explicit series ID
    pub series_id: Option<SeriesId>,

    /// Match by measurement name (supports wildcards)
    pub measurement: Option<String>,

    /// Match by tag filters
    pub tag_filters: Vec<TagMatcher>,
}

impl SeriesSelector {
    /// Create a selector matching a specific series ID
    pub fn by_id(series_id: SeriesId) -> Self {
        Self {
            series_id: Some(series_id),
            measurement: None,
            tag_filters: Vec::new(),
        }
    }

    /// Create a selector matching a measurement name
    ///
    /// Measurement names are validated to prevent injection attacks (SEC-006)
    pub fn by_measurement(measurement: impl Into<String>) -> QueryResult<Self> {
        let name = measurement.into();

        // Validate measurement name (SEC-006)
        Self::validate_identifier(&name, "Measurement name")?;

        Ok(Self {
            series_id: None,
            measurement: Some(name),
            tag_filters: Vec::new(),
        })
    }

    /// Validate an identifier (measurement name, tag key, etc.)
    ///
    /// Allows: alphanumeric, underscore, dot, hyphen
    /// Disallows: spaces, special characters, path traversal patterns
    fn validate_identifier(name: &str, field_name: &str) -> QueryResult<()> {
        if name.is_empty() {
            return Err(QueryError::validation(format!("{} cannot be empty", field_name)));
        }

        if name.len() > 256 {
            return Err(QueryError::validation(format!(
                "{} exceeds maximum length of 256 characters",
                field_name
            )));
        }

        // Check for path traversal attempts
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            return Err(QueryError::validation(format!(
                "{} contains invalid path characters",
                field_name
            )));
        }

        // Only allow safe characters: alphanumeric, underscore, dot, hyphen
        // First character must be alphanumeric or underscore
        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphanumeric() && first_char != '_' {
            return Err(QueryError::validation(format!(
                "{} must start with an alphanumeric character or underscore",
                field_name
            )));
        }

        for c in name.chars() {
            if !c.is_alphanumeric() && c != '_' && c != '.' && c != '-' {
                return Err(QueryError::validation(format!(
                    "{} contains invalid character: '{}'",
                    field_name, c
                )));
            }
        }

        Ok(())
    }

    /// Add a tag filter
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_filters.push(TagMatcher::Equals {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add a tag regex filter
    ///
    /// Returns an error if the regex pattern is invalid (SEC-003: validates regex to prevent ReDoS)
    pub fn with_tag_regex(
        mut self,
        key: impl Into<String>,
        pattern: impl Into<String>,
    ) -> QueryResult<Self> {
        let pattern_str = pattern.into();

        // Validate regex pattern at construction time (SEC-003)
        // This prevents ReDoS attacks by catching invalid patterns early
        if let Err(e) = Regex::new(&pattern_str) {
            return Err(QueryError::validation(format!(
                "Invalid regex pattern '{}': {}",
                pattern_str, e
            )));
        }

        // Limit pattern length to prevent overly complex patterns
        const MAX_PATTERN_LEN: usize = 1000;
        if pattern_str.len() > MAX_PATTERN_LEN {
            return Err(QueryError::validation(format!(
                "Regex pattern exceeds maximum length of {} characters",
                MAX_PATTERN_LEN
            )));
        }

        self.tag_filters.push(TagMatcher::Regex {
            key: key.into(),
            pattern: pattern_str,
        });
        Ok(self)
    }

    /// Check if this selector can match multiple series
    pub fn is_multi_series(&self) -> bool {
        self.series_id.is_none()
            && self
                .tag_filters
                .iter()
                .any(|f| matches!(f, TagMatcher::Regex { .. } | TagMatcher::NotEquals { .. }))
    }

    /// Convert to TagFilter for index lookup
    pub fn to_tag_filter(&self) -> TagFilter {
        let mut filters = HashMap::new();
        for matcher in &self.tag_filters {
            if let TagMatcher::Equals { key, value } = matcher {
                filters.insert(key.clone(), value.clone());
            }
        }
        TagFilter::Exact(filters)
    }
}

/// Tag matching operators
#[derive(Debug, Clone, PartialEq)]
pub enum TagMatcher {
    /// Exact match: tag = value
    Equals {
        /// Tag key to match
        key: String,
        /// Expected tag value
        value: String,
    },
    /// Not equal: tag != value
    NotEquals {
        /// Tag key to match
        key: String,
        /// Value that should not match
        value: String,
    },
    /// Regex match: tag =~ /pattern/
    Regex {
        /// Tag key to match
        key: String,
        /// Regex pattern to match against
        pattern: String,
    },
    /// Not regex: tag !~ /pattern/
    NotRegex {
        /// Tag key to match
        key: String,
        /// Regex pattern that should not match
        pattern: String,
    },
    /// Tag exists
    Exists {
        /// Tag key that must exist
        key: String,
    },
    /// Tag does not exist
    NotExists {
        /// Tag key that must not exist
        key: String,
    },
}

// ============================================================================
// Predicates (Value Filters)
// ============================================================================

/// Value filter predicate for WHERE clauses
#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    /// Field to filter on (typically "value" for single-field series)
    pub field: String,
    /// Comparison operator
    pub op: PredicateOp,
    /// Value to compare against
    pub value: PredicateValue,
}

impl Predicate {
    /// Create a new predicate
    pub fn new(field: impl Into<String>, op: PredicateOp, value: PredicateValue) -> Self {
        Self {
            field: field.into(),
            op,
            value,
        }
    }

    /// Create field > value predicate
    pub fn gt(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Gt, PredicateValue::Float(value))
    }

    /// Create field >= value predicate
    pub fn gte(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Gte, PredicateValue::Float(value))
    }

    /// Create field < value predicate
    pub fn lt(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Lt, PredicateValue::Float(value))
    }

    /// Create field <= value predicate
    pub fn lte(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Lte, PredicateValue::Float(value))
    }

    /// Create field = value predicate
    pub fn eq(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Eq, PredicateValue::Float(value))
    }

    /// Create field != value predicate
    pub fn ne(field: impl Into<String>, value: f64) -> Self {
        Self::new(field, PredicateOp::Ne, PredicateValue::Float(value))
    }

    /// Create IS NULL predicate
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::new(field, PredicateOp::IsNull, PredicateValue::Null)
    }

    /// Create IS NOT NULL predicate
    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::new(field, PredicateOp::IsNotNull, PredicateValue::Null)
    }

    /// Evaluate predicate against a value
    pub fn evaluate(&self, value: f64) -> bool {
        match (&self.op, &self.value) {
            (PredicateOp::Gt, PredicateValue::Float(v)) => value > *v,
            (PredicateOp::Gte, PredicateValue::Float(v)) => value >= *v,
            (PredicateOp::Lt, PredicateValue::Float(v)) => value < *v,
            (PredicateOp::Lte, PredicateValue::Float(v)) => value <= *v,
            (PredicateOp::Eq, PredicateValue::Float(v)) => (value - *v).abs() < f64::EPSILON,
            (PredicateOp::Ne, PredicateValue::Float(v)) => (value - *v).abs() >= f64::EPSILON,
            (PredicateOp::IsNull, _) => value.is_nan(),
            (PredicateOp::IsNotNull, _) => !value.is_nan(),
            (PredicateOp::Between, PredicateValue::Range(lo, hi)) => value >= *lo && value <= *hi,
            _ => false,
        }
    }
}

/// Predicate comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Is null/NaN
    IsNull,
    /// Is not null/NaN
    IsNotNull,
    /// Between (inclusive)
    Between,
    /// In set of values
    In,
}

/// Predicate value types
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateValue {
    /// Floating point value
    Float(f64),
    /// Integer value
    Int(i64),
    /// String value
    String(String),
    /// Null/NaN
    Null,
    /// Range for BETWEEN
    Range(f64, f64),
    /// Set of values for IN
    Set(Vec<f64>),
}

// ============================================================================
// Order By
// ============================================================================

/// Result ordering specification
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    /// Field to order by
    pub field: OrderField,
    /// Sort direction
    pub direction: OrderDirection,
}

impl OrderBy {
    /// Create ascending order by timestamp
    pub fn time_asc() -> Self {
        Self {
            field: OrderField::Timestamp,
            direction: OrderDirection::Asc,
        }
    }

    /// Create descending order by timestamp
    pub fn time_desc() -> Self {
        Self {
            field: OrderField::Timestamp,
            direction: OrderDirection::Desc,
        }
    }

    /// Create ascending order by value
    pub fn value_asc() -> Self {
        Self {
            field: OrderField::Value,
            direction: OrderDirection::Asc,
        }
    }

    /// Create descending order by value
    pub fn value_desc() -> Self {
        Self {
            field: OrderField::Value,
            direction: OrderDirection::Desc,
        }
    }
}

/// Fields available for ordering
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderField {
    /// Order by timestamp
    Timestamp,
    /// Order by value
    Value,
    /// Order by custom field
    Field(String),
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

// ============================================================================
// Query Builder
// ============================================================================

/// Fluent builder for constructing queries
#[derive(Debug, Default)]
pub struct QueryBuilder {
    selector: Option<SeriesSelector>,
    time_range: Option<TimeRange>,
    aggregation: Option<Aggregation>,
    predicates: Vec<Predicate>,
    order_by: Option<OrderBy>,
    limit: Option<usize>,
    offset: Option<usize>,
    downsample: Option<(usize, DownsampleMethod)>,
    stream_mode: Option<StreamMode>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Select by series ID
    pub fn select_series(mut self, series_id: SeriesId) -> Self {
        self.selector = Some(SeriesSelector::by_id(series_id));
        self
    }

    /// Select by measurement name
    ///
    /// Returns an error if measurement name is invalid
    pub fn select_measurement(mut self, measurement: impl Into<String>) -> QueryResult<Self> {
        self.selector = Some(SeriesSelector::by_measurement(measurement)?);
        Ok(self)
    }

    /// Add tag filter to selector
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Some(ref mut selector) = self.selector {
            selector.tag_filters.push(TagMatcher::Equals {
                key: key.into(),
                value: value.into(),
            });
        }
        self
    }

    /// Set time range
    pub fn time_range(mut self, range: TimeRange) -> Self {
        self.time_range = Some(range);
        self
    }

    /// Set aggregation function
    pub fn aggregate(mut self, function: AggregationFunction) -> Self {
        self.aggregation = Some(Aggregation::simple(function));
        self
    }

    /// Set aggregation window
    pub fn window(mut self, duration: Duration) -> Self {
        if let Some(ref mut agg) = self.aggregation {
            agg.window = Some(WindowSpec::tumbling(duration));
        }
        self
    }

    /// Add group by dimension
    pub fn group_by(mut self, tag: impl Into<String>) -> Self {
        if let Some(ref mut agg) = self.aggregation {
            agg.group_by.push(tag.into());
        }
        self
    }

    /// Add value predicate
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Set result ordering
    pub fn order(mut self, order: OrderBy) -> Self {
        self.order_by = Some(order);
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set result offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Enable downsampling
    pub fn downsample(mut self, target_points: usize, method: DownsampleMethod) -> Self {
        self.downsample = Some((target_points, method));
        self
    }

    /// Enable streaming mode
    pub fn stream(mut self, mode: StreamMode) -> Self {
        self.stream_mode = Some(mode);
        self
    }

    /// Build the query
    pub fn build(self) -> QueryResult<Query> {
        let selector = self
            .selector
            .ok_or_else(|| QueryError::validation("series selector is required"))?;

        // Streaming query
        if let Some(mode) = self.stream_mode {
            return Ok(Query::Stream(StreamQuery {
                selector,
                aggregation: self.aggregation,
                mode,
            }));
        }

        // Downsample query
        if let Some((target_points, method)) = self.downsample {
            let time_range = self
                .time_range
                .ok_or_else(|| QueryError::validation("time_range is required for downsample"))?;
            return Ok(Query::Downsample(DownsampleQuery {
                selector,
                time_range,
                target_points,
                method,
            }));
        }

        // Aggregate query
        if let Some(aggregation) = self.aggregation {
            let time_range = self
                .time_range
                .ok_or_else(|| QueryError::validation("time_range is required for aggregation"))?;
            return Ok(Query::Aggregate(AggregateQuery {
                selector,
                time_range,
                aggregation,
                predicates: self.predicates,
                order_by: self.order_by,
                limit: self.limit,
            }));
        }

        // Select query (requires time range) or Latest query (no time range)
        match self.time_range {
            Some(time_range) => Ok(Query::Select(SelectQuery {
                selector,
                time_range,
                predicates: self.predicates,
                projections: Vec::new(),
                order_by: self.order_by,
                limit: self.limit,
                offset: self.offset,
            })),
            None => Ok(Query::Latest(LatestQuery {
                selector,
                count: self.limit.unwrap_or(1),
            })),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_query_builder() {
        let query = QueryBuilder::new()
            .select_series(42)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .limit(100)
            .build()
            .unwrap();

        match query {
            Query::Select(q) => {
                assert_eq!(q.selector.series_id, Some(42));
                assert_eq!(q.time_range.start, 0);
                assert_eq!(q.time_range.end, 1000);
                assert_eq!(q.limit, Some(100));
            }
            _ => panic!("expected Select query"),
        }
    }

    #[test]
    fn test_aggregate_query_builder() {
        let query = QueryBuilder::new()
            .select_measurement("cpu")
            .unwrap()
            .with_tag("host", "server01")
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .aggregate(AggregationFunction::Avg)
            .window(Duration::from_secs(60))
            .build()
            .unwrap();

        match query {
            Query::Aggregate(q) => {
                assert_eq!(q.selector.measurement, Some("cpu".to_string()));
                assert!(q.aggregation.window.is_some());
                assert_eq!(q.aggregation.function, AggregationFunction::Avg);
            }
            _ => panic!("expected Aggregate query"),
        }
    }

    #[test]
    fn test_downsample_query_builder() {
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .downsample(100, DownsampleMethod::Lttb)
            .build()
            .unwrap();

        match query {
            Query::Downsample(q) => {
                assert_eq!(q.target_points, 100);
                assert_eq!(q.method, DownsampleMethod::Lttb);
            }
            _ => panic!("expected Downsample query"),
        }
    }

    #[test]
    fn test_latest_query_builder() {
        let query = QueryBuilder::new()
            .select_series(1)
            .limit(5)
            .build()
            .unwrap();

        match query {
            Query::Latest(q) => {
                assert_eq!(q.count, 5);
            }
            _ => panic!("expected Latest query"),
        }
    }

    #[test]
    fn test_predicate_evaluation() {
        let pred = Predicate::gt("value", 10.0);
        assert!(pred.evaluate(15.0));
        assert!(!pred.evaluate(5.0));

        let pred = Predicate::lte("value", 10.0);
        assert!(pred.evaluate(10.0));
        assert!(pred.evaluate(5.0));
        assert!(!pred.evaluate(15.0));
    }

    #[test]
    fn test_series_selector_tag_filter() {
        let selector = SeriesSelector::by_measurement("cpu")
            .unwrap()
            .with_tag("host", "server01")
            .with_tag("region", "us-east");

        let filter = selector.to_tag_filter();
        match filter {
            TagFilter::Exact(tags) => {
                assert_eq!(tags.get("host"), Some(&"server01".to_string()));
                assert_eq!(tags.get("region"), Some(&"us-east".to_string()));
            }
            _ => panic!("Expected Exact filter"),
        }
    }

    #[test]
    fn test_aggregation_function_properties() {
        assert!(AggregationFunction::Sum.is_incremental());
        assert!(!AggregationFunction::StdDev.is_incremental());

        assert!(AggregationFunction::Percentile(95).is_approximate());
        assert!(!AggregationFunction::Avg.is_approximate());

        assert!(AggregationFunction::Rate.requires_sorted());
        assert!(!AggregationFunction::Count.requires_sorted());
    }

    #[test]
    fn test_missing_selector_error() {
        let result = QueryBuilder::new()
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .build();

        assert!(result.is_err());
    }
}
