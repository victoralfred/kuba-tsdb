//! Query Planner - Cost-based optimization and plan generation
//!
//! This module transforms parsed queries into optimized execution plans.
//! It performs:
//! - Cost estimation based on statistics
//! - Predicate pushdown to minimize data scanning
//! - Chunk pruning using zone maps
//! - Operator ordering optimization
//! - Parallel execution planning
//!
//! # Planning Phases
//!
//! ```text
//! Query AST
//!     │
//!     ▼
//! ┌─────────────────┐
//! │  Logical Plan   │  High-level operations (what to do)
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │   Optimize      │  Predicate pushdown, pruning decisions
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │  Physical Plan  │  Concrete operators (how to do it)
//! └─────────────────┘
//! ```
//!
//! # Zone Map Pruning
//!
//! Each chunk stores min/max statistics. The planner uses these to skip
//! chunks that cannot contain matching data, dramatically reducing I/O.

use crate::query::ast::{
    AggregateQuery, DownsampleQuery, LatestQuery, Predicate, PredicateOp, PredicateValue, Query,
    SelectQuery, SeriesSelector,
};
use crate::query::error::QueryError;
use crate::types::{SeriesId, TimeRange};
use std::collections::HashSet;
use std::fmt;

// ============================================================================
// Planner Configuration
// ============================================================================

/// Configuration for query planning
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Enable predicate pushdown optimization (default: true)
    pub enable_predicate_pushdown: bool,

    /// Enable zone map pruning (default: true)
    pub enable_zone_map_pruning: bool,

    /// Enable parallel plan generation (default: true)
    pub enable_parallel_plans: bool,

    /// Minimum chunk size to consider parallelization (default: 4)
    pub parallel_threshold_chunks: usize,

    /// Maximum plan complexity (prevents runaway optimization)
    pub max_plan_nodes: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            enable_predicate_pushdown: true,
            enable_zone_map_pruning: true,
            enable_parallel_plans: true,
            parallel_threshold_chunks: 4,
            max_plan_nodes: 1000,
        }
    }
}

// ============================================================================
// Query Planner
// ============================================================================

/// Query planner that generates optimized execution plans
pub struct QueryPlanner {
    /// Planner configuration
    config: PlannerConfig,
}

impl QueryPlanner {
    /// Create a new query planner with default configuration
    pub fn new() -> Self {
        Self::with_config(PlannerConfig::default())
    }

    /// Create a query planner with custom configuration
    pub fn with_config(config: PlannerConfig) -> Self {
        Self { config }
    }

    /// Generate an execution plan for a query
    ///
    /// # Arguments
    ///
    /// * `query` - The parsed query to plan
    ///
    /// # Returns
    ///
    /// * `Ok(QueryPlan)` - Optimized execution plan
    /// * `Err(QueryError)` - If planning fails
    pub fn plan(&self, query: &Query) -> Result<QueryPlan, QueryError> {
        // Generate logical plan based on query type
        let logical_plan = self.create_logical_plan(query)?;

        // Optimize the logical plan
        let optimized = self.optimize(logical_plan)?;

        // Convert to physical plan
        let physical_plan = self.create_physical_plan(optimized)?;

        Ok(physical_plan)
    }

    /// Create initial logical plan from query AST
    fn create_logical_plan(&self, query: &Query) -> Result<LogicalPlan, QueryError> {
        match query {
            Query::Select(select) => self.plan_select(select),
            Query::Aggregate(agg) => self.plan_aggregate(agg),
            Query::Downsample(ds) => self.plan_downsample(ds),
            Query::Latest(latest) => self.plan_latest(latest),
            Query::Stream(_) => Err(QueryError::planning(
                "Streaming queries have separate planning path",
            )),
            Query::Explain(inner) => {
                // For EXPLAIN, plan the inner query and wrap it
                let inner_plan = self.create_logical_plan(inner)?;
                Ok(LogicalPlan::Explain(Box::new(inner_plan)))
            }
        }
    }

    /// Plan a SELECT query
    fn plan_select(&self, query: &SelectQuery) -> Result<LogicalPlan, QueryError> {
        // Start with a scan of the data
        let mut plan = LogicalPlan::Scan {
            selector: query.selector.clone(),
            time_range: Some(query.time_range),
            columns: vec!["timestamp".into(), "value".into()],
        };

        // Add filter if predicates exist
        for predicate in &query.predicates {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
            };
        }

        // Add sort if ordering specified
        if let Some(ref order) = query.order_by {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: vec![("timestamp".into(), order.direction.clone())],
            };
        }

        // Add limit/offset if specified
        if query.limit.is_some() || query.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: query.limit,
                offset: query.offset,
            };
        }

        Ok(plan)
    }

    /// Plan an AGGREGATE query
    fn plan_aggregate(&self, query: &AggregateQuery) -> Result<LogicalPlan, QueryError> {
        // Start with base scan
        let scan = LogicalPlan::Scan {
            selector: query.selector.clone(),
            time_range: Some(query.time_range),
            columns: vec!["timestamp".into(), "value".into()],
        };

        // Apply predicates if they exist
        let mut plan = scan;
        for predicate in &query.predicates {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
            };
        }

        // Add aggregation
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            functions: vec![query.aggregation.clone()],
            window: query.aggregation.window.clone(),
            group_by: query.aggregation.group_by.clone(),
        };

        Ok(plan)
    }

    /// Plan a DOWNSAMPLE query
    fn plan_downsample(&self, query: &DownsampleQuery) -> Result<LogicalPlan, QueryError> {
        // Start with base scan
        let scan = LogicalPlan::Scan {
            selector: query.selector.clone(),
            time_range: Some(query.time_range),
            columns: vec!["timestamp".into(), "value".into()],
        };

        // Add downsample operation
        let plan = LogicalPlan::Downsample {
            input: Box::new(scan),
            method: query.method.clone(),
            target_points: query.target_points,
        };

        Ok(plan)
    }

    /// Plan a LATEST query
    fn plan_latest(&self, query: &LatestQuery) -> Result<LogicalPlan, QueryError> {
        // Latest is a special case - we scan backwards and take first N
        let plan = LogicalPlan::Latest {
            selector: query.selector.clone(),
            count: query.count,
        };

        Ok(plan)
    }

    /// Optimize a logical plan
    ///
    /// Applies optimization rules in order:
    /// 1. Predicate pushdown - move filters closer to scan
    /// 2. Column pruning - only read needed columns
    /// 3. Zone map analysis - determine which chunks to skip
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, QueryError> {
        let mut optimized = plan;

        // Apply predicate pushdown if enabled
        if self.config.enable_predicate_pushdown {
            optimized = self.pushdown_predicates(optimized);
        }

        // TODO: Add more optimization passes
        // - Column pruning
        // - Constant folding
        // - Join reordering (when we add joins)

        Ok(optimized)
    }

    /// Push predicates down closer to scan operators
    ///
    /// This reduces data flowing through the pipeline by filtering early
    fn pushdown_predicates(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // If we have a filter over a scan, combine them
            LogicalPlan::Filter { input, predicate } => {
                match *input {
                    LogicalPlan::Scan {
                        selector,
                        time_range,
                        columns,
                    } => {
                        // Push predicate into scan
                        LogicalPlan::ScanWithPredicate {
                            selector,
                            time_range,
                            columns,
                            predicate,
                        }
                    }
                    other => {
                        // Can't push down, keep structure
                        LogicalPlan::Filter {
                            input: Box::new(self.pushdown_predicates(other)),
                            predicate,
                        }
                    }
                }
            }
            // Recursively process other plan nodes
            LogicalPlan::Aggregate {
                input,
                functions,
                window,
                group_by,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.pushdown_predicates(*input)),
                functions,
                window,
                group_by,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.pushdown_predicates(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.pushdown_predicates(*input)),
                limit,
                offset,
            },
            LogicalPlan::Downsample {
                input,
                method,
                target_points,
            } => LogicalPlan::Downsample {
                input: Box::new(self.pushdown_predicates(*input)),
                method,
                target_points,
            },
            LogicalPlan::Explain(inner) => {
                LogicalPlan::Explain(Box::new(self.pushdown_predicates(*inner)))
            }
            // Leaf nodes pass through unchanged
            other => other,
        }
    }

    /// Convert logical plan to physical plan with concrete operators
    fn create_physical_plan(&self, logical: LogicalPlan) -> Result<QueryPlan, QueryError> {
        // Calculate cost estimate for the plan
        let estimated_cost = self.estimate_cost(&logical);

        // Determine if we should use parallel execution
        let use_parallel = self.config.enable_parallel_plans
            && estimated_cost.chunks_to_scan >= self.config.parallel_threshold_chunks;

        Ok(QueryPlan {
            logical_plan: logical,
            estimated_cost,
            use_parallel,
            chunk_assignments: Vec::new(), // Filled in during execution
        })
    }

    /// Estimate the cost of executing a logical plan
    fn estimate_cost(&self, plan: &LogicalPlan) -> CostEstimate {
        match plan {
            LogicalPlan::Scan { time_range, .. }
            | LogicalPlan::ScanWithPredicate { time_range, .. } => {
                // Estimate based on time range
                // TODO: Use actual statistics from catalog
                let duration_hours = time_range
                    .map(|tr| (tr.end - tr.start) as f64 / 3_600_000_000_000.0)
                    .unwrap_or(24.0);

                // Rough estimate: ~1 chunk per hour, ~10K rows per chunk
                let chunks = (duration_hours.ceil() as usize).max(1);
                let rows = chunks * 10_000;

                CostEstimate {
                    estimated_rows: rows,
                    estimated_bytes: rows * 16, // timestamp (8) + value (8)
                    chunks_to_scan: chunks,
                    cpu_cost: rows as f64 * 0.001, // 1ms per 1000 rows
                    io_cost: chunks as f64 * 10.0, // 10ms per chunk
                }
            }
            LogicalPlan::Filter { input, .. } => {
                // Filtering reduces output but scans same input
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows /= 2; // Assume 50% selectivity
                cost.cpu_cost *= 1.1; // Small overhead for predicate evaluation
                cost
            }
            LogicalPlan::Aggregate { input, .. } => {
                // Aggregation compresses output significantly
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows = (cost.estimated_rows / 100).max(1);
                cost.cpu_cost *= 1.5; // Aggregation is CPU intensive
                cost
            }
            LogicalPlan::Sort { input, .. } => {
                let mut cost = self.estimate_cost(input);
                // Sort is O(n log n)
                let n = cost.estimated_rows as f64;
                cost.cpu_cost += n * n.log2() * 0.0001;
                cost
            }
            LogicalPlan::Limit { input, limit, .. } => {
                let mut cost = self.estimate_cost(input);
                if let Some(limit) = limit {
                    cost.estimated_rows = cost.estimated_rows.min(*limit);
                }
                cost
            }
            LogicalPlan::Downsample {
                input,
                target_points,
                ..
            } => {
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows = *target_points;
                cost.cpu_cost *= 1.2; // Downsampling algorithm overhead
                cost
            }
            LogicalPlan::Latest { count, .. } => CostEstimate {
                estimated_rows: *count,
                estimated_bytes: count * 16,
                chunks_to_scan: 1, // Only need latest chunk
                cpu_cost: 1.0,
                io_cost: 10.0,
            },
            LogicalPlan::Explain(inner) => {
                // Explain doesn't execute, just plans
                let mut cost = self.estimate_cost(inner);
                cost.io_cost = 0.0; // No actual I/O
                cost.estimated_rows = 1; // Just returns plan description
                cost
            }
        }
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Logical Plan
// ============================================================================

/// Logical query plan - describes what operations to perform
///
/// This is an intermediate representation that gets optimized before
/// being converted to a physical plan with concrete operators.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan data from storage
    Scan {
        selector: SeriesSelector,
        time_range: Option<TimeRange>,
        columns: Vec<String>,
    },

    /// Scan with integrated predicate (after optimization)
    ScanWithPredicate {
        selector: SeriesSelector,
        time_range: Option<TimeRange>,
        columns: Vec<String>,
        predicate: Predicate,
    },

    /// Filter rows based on predicate
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },

    /// Aggregate values
    Aggregate {
        input: Box<LogicalPlan>,
        functions: Vec<crate::query::ast::Aggregation>,
        window: Option<crate::query::ast::WindowSpec>,
        group_by: Vec<String>,
    },

    /// Sort results
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<(String, crate::query::ast::OrderDirection)>,
    },

    /// Limit and offset results
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    /// Downsample for visualization
    Downsample {
        input: Box<LogicalPlan>,
        method: crate::query::ast::DownsampleMethod,
        target_points: usize,
    },

    /// Get latest values (optimized path)
    Latest {
        selector: SeriesSelector,
        count: usize,
    },

    /// Explain wrapper
    Explain(Box<LogicalPlan>),
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format_with_indent(f, 0)
    }
}

impl LogicalPlan {
    /// Format plan with indentation for nested structure
    fn format_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let prefix = "  ".repeat(indent);
        match self {
            LogicalPlan::Scan {
                selector,
                time_range,
                ..
            } => {
                write!(f, "{}Scan(", prefix)?;
                write!(f, "selector={:?}", selector)?;
                if let Some(tr) = time_range {
                    write!(f, ", time_range={}..{}", tr.start, tr.end)?;
                }
                writeln!(f, ")")
            }
            LogicalPlan::ScanWithPredicate {
                selector,
                time_range,
                predicate,
                ..
            } => {
                write!(f, "{}ScanWithPredicate(", prefix)?;
                write!(f, "selector={:?}", selector)?;
                if let Some(tr) = time_range {
                    write!(f, ", time_range={}..{}", tr.start, tr.end)?;
                }
                write!(f, ", predicate={:?}", predicate)?;
                writeln!(f, ")")
            }
            LogicalPlan::Filter { input, predicate } => {
                writeln!(f, "{}Filter(predicate={:?})", prefix, predicate)?;
                input.format_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                input,
                functions,
                window,
                group_by,
            } => {
                write!(f, "{}Aggregate(", prefix)?;
                write!(f, "functions={:?}", functions)?;
                if let Some(w) = window {
                    write!(f, ", window={:?}", w)?;
                }
                if !group_by.is_empty() {
                    write!(f, ", group_by={:?}", group_by)?;
                }
                writeln!(f, ")")?;
                input.format_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort { input, order_by } => {
                writeln!(f, "{}Sort(order_by={:?})", prefix, order_by)?;
                input.format_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                write!(f, "{}Limit(", prefix)?;
                if let Some(l) = limit {
                    write!(f, "limit={}", l)?;
                }
                if let Some(o) = offset {
                    write!(f, ", offset={}", o)?;
                }
                writeln!(f, ")")?;
                input.format_with_indent(f, indent + 1)
            }
            LogicalPlan::Downsample {
                input,
                method,
                target_points,
            } => {
                writeln!(
                    f,
                    "{}Downsample(method={:?}, target={})",
                    prefix, method, target_points
                )?;
                input.format_with_indent(f, indent + 1)
            }
            LogicalPlan::Latest { selector, count } => {
                writeln!(
                    f,
                    "{}Latest(selector={:?}, count={})",
                    prefix, selector, count
                )
            }
            LogicalPlan::Explain(inner) => {
                writeln!(f, "{}Explain", prefix)?;
                inner.format_with_indent(f, indent + 1)
            }
        }
    }
}

// ============================================================================
// Physical Plan / Query Plan
// ============================================================================

/// Physical query plan ready for execution
#[derive(Debug)]
pub struct QueryPlan {
    /// The optimized logical plan
    pub logical_plan: LogicalPlan,

    /// Estimated execution cost
    pub estimated_cost: CostEstimate,

    /// Whether to use parallel execution
    pub use_parallel: bool,

    /// Chunk assignments for parallel workers (filled during execution)
    pub chunk_assignments: Vec<ChunkAssignment>,
}

impl QueryPlan {
    /// Get human-readable plan description
    pub fn explain(&self) -> String {
        let mut output = String::new();
        output.push_str("Query Plan:\n");
        output.push_str(&format!("{}\n", self.logical_plan));
        output.push_str("\nCost Estimate:\n");
        output.push_str(&format!(
            "  Estimated rows: {}\n",
            self.estimated_cost.estimated_rows
        ));
        output.push_str(&format!(
            "  Estimated bytes: {}\n",
            self.estimated_cost.estimated_bytes
        ));
        output.push_str(&format!(
            "  Chunks to scan: {}\n",
            self.estimated_cost.chunks_to_scan
        ));
        output.push_str(&format!("  Parallel: {}\n", self.use_parallel));
        output
    }
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Estimated cost of executing a query plan
#[derive(Debug, Clone, Default)]
pub struct CostEstimate {
    /// Estimated number of output rows
    pub estimated_rows: usize,

    /// Estimated bytes to process
    pub estimated_bytes: usize,

    /// Number of chunks that need to be scanned
    pub chunks_to_scan: usize,

    /// Relative CPU cost (arbitrary units)
    pub cpu_cost: f64,

    /// Relative I/O cost (arbitrary units)
    pub io_cost: f64,
}

impl CostEstimate {
    /// Total estimated cost (weighted sum of CPU and I/O)
    pub fn total_cost(&self) -> f64 {
        // I/O is typically more expensive than CPU
        self.cpu_cost + self.io_cost * 10.0
    }
}

// ============================================================================
// Chunk Assignment
// ============================================================================

/// Assignment of chunks to worker threads for parallel execution
#[derive(Debug, Clone)]
pub struct ChunkAssignment {
    /// Worker thread ID
    pub worker_id: usize,

    /// Chunks assigned to this worker
    pub chunk_ids: Vec<u64>,

    /// Series IDs in these chunks
    pub series_ids: HashSet<SeriesId>,
}

// ============================================================================
// Zone Map
// ============================================================================

/// Zone map statistics for a chunk, used for pruning
#[derive(Debug, Clone)]
pub struct ZoneMap {
    /// Chunk identifier
    pub chunk_id: u64,

    /// Minimum timestamp in chunk
    pub min_timestamp: i64,

    /// Maximum timestamp in chunk
    pub max_timestamp: i64,

    /// Minimum value in chunk
    pub min_value: f64,

    /// Maximum value in chunk
    pub max_value: f64,

    /// Number of rows in chunk
    pub row_count: usize,

    /// Series IDs present in chunk
    pub series_ids: HashSet<SeriesId>,
}

impl ZoneMap {
    /// Check if chunk can be pruned based on time range
    pub fn can_prune_by_time(&self, time_range: &TimeRange) -> bool {
        // Chunk can be pruned if its time range doesn't overlap with query
        self.max_timestamp < time_range.start || self.min_timestamp > time_range.end
    }

    /// Check if chunk can be pruned based on value predicate
    pub fn can_prune_by_predicate(&self, predicate: &Predicate) -> bool {
        // Extract f64 value from predicate if possible
        let pred_value = match &predicate.value {
            PredicateValue::Float(v) => *v,
            PredicateValue::Int(v) => *v as f64,
            _ => return false, // Can't prune without numeric comparison
        };

        match predicate.op {
            PredicateOp::Eq => {
                // Can prune if value is outside min/max range
                pred_value < self.min_value || pred_value > self.max_value
            }
            PredicateOp::Ne => {
                // Can only prune if entire chunk has same value that equals predicate
                false // Conservative - don't prune
            }
            PredicateOp::Lt => {
                // Can prune if all values >= predicate
                self.min_value >= pred_value
            }
            PredicateOp::Lte => {
                // Can prune if all values > predicate
                self.min_value > pred_value
            }
            PredicateOp::Gt => {
                // Can prune if all values <= predicate
                self.max_value <= pred_value
            }
            PredicateOp::Gte => {
                // Can prune if all values < predicate
                self.max_value < pred_value
            }
            PredicateOp::Between => {
                // Can prune if chunk range doesn't overlap with between range
                match &predicate.value {
                    PredicateValue::Range(lo, hi) => self.max_value < *lo || self.min_value > *hi,
                    _ => false,
                }
            }
            PredicateOp::IsNull | PredicateOp::IsNotNull | PredicateOp::In => {
                // Would need null statistics in zone map
                false
            }
        }
    }

    /// Check if chunk can be pruned based on series selector
    pub fn can_prune_by_series(&self, series_ids: &HashSet<SeriesId>) -> bool {
        // Can prune if chunk has no matching series
        self.series_ids.is_disjoint(series_ids)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{AggregationFunction, QueryBuilder};

    #[test]
    fn test_planner_creation() {
        let planner = QueryPlanner::new();
        assert!(planner.config.enable_predicate_pushdown);
        assert!(planner.config.enable_zone_map_pruning);
    }

    #[test]
    fn test_plan_select_query() {
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .build()
            .unwrap();

        let plan = planner.plan(&query).unwrap();
        assert!(plan.estimated_cost.chunks_to_scan > 0);
    }

    #[test]
    fn test_plan_aggregate_query() {
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .aggregate(AggregationFunction::Avg)
            .build()
            .unwrap();

        let plan = planner.plan(&query).unwrap();
        // Aggregate should reduce estimated rows
        assert!(plan.estimated_cost.estimated_rows > 0);
    }

    #[test]
    fn test_predicate_pushdown() {
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .filter(Predicate::gt("value", 100.0))
            .build()
            .unwrap();

        let plan = planner.plan(&query).unwrap();

        // Verify predicate was pushed down to scan
        match &plan.logical_plan {
            LogicalPlan::ScanWithPredicate { .. } => {
                // Predicate was pushed down
            }
            _ => panic!("Expected ScanWithPredicate after pushdown"),
        }
    }

    #[test]
    fn test_cost_estimation() {
        let planner = QueryPlanner::new();

        // Larger time range should have higher cost
        let small_query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 3_600_000_000_000,
            }) // 1 hour
            .build()
            .unwrap();

        let large_query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 86_400_000_000_000,
            }) // 24 hours
            .build()
            .unwrap();

        let small_plan = planner.plan(&small_query).unwrap();
        let large_plan = planner.plan(&large_query).unwrap();

        assert!(large_plan.estimated_cost.total_cost() > small_plan.estimated_cost.total_cost());
    }

    #[test]
    fn test_zone_map_time_pruning() {
        let zone_map = ZoneMap {
            chunk_id: 1,
            min_timestamp: 1000,
            max_timestamp: 2000,
            min_value: 0.0,
            max_value: 100.0,
            row_count: 1000,
            series_ids: HashSet::from([1, 2, 3]),
        };

        // Query before chunk - can prune
        assert!(zone_map.can_prune_by_time(&TimeRange { start: 0, end: 500 }));

        // Query after chunk - can prune
        assert!(zone_map.can_prune_by_time(&TimeRange {
            start: 3000,
            end: 4000
        }));

        // Query overlaps chunk - cannot prune
        assert!(!zone_map.can_prune_by_time(&TimeRange {
            start: 1500,
            end: 2500
        }));
    }

    #[test]
    fn test_zone_map_value_pruning() {
        let zone_map = ZoneMap {
            chunk_id: 1,
            min_timestamp: 0,
            max_timestamp: 1000,
            min_value: 10.0,
            max_value: 50.0,
            row_count: 100,
            series_ids: HashSet::new(),
        };

        // Value > 50 when max is 50 - can prune
        let pred_gt = Predicate::gt("value", 50.0);
        assert!(zone_map.can_prune_by_predicate(&pred_gt));

        // Value < 10 when min is 10 - can prune
        let pred_lt = Predicate::lt("value", 10.0);
        assert!(zone_map.can_prune_by_predicate(&pred_lt));

        // Value = 30 (within range) - cannot prune
        let pred_eq = Predicate::eq("value", 30.0);
        assert!(!zone_map.can_prune_by_predicate(&pred_eq));
    }

    #[test]
    fn test_plan_explain() {
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .build()
            .unwrap();

        let plan = planner.plan(&query).unwrap();
        let explain = plan.explain();

        assert!(explain.contains("Query Plan:"));
        assert!(explain.contains("Estimated rows:"));
        assert!(explain.contains("Chunks to scan:"));
    }
}
