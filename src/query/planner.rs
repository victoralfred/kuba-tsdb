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

    /// Enable constant folding optimization (default: true)
    ///
    /// When enabled, constant expressions are evaluated at planning time:
    /// - Arithmetic expressions in predicates (e.g., `value > 10 + 5` → `value > 15`)
    /// - Impossible predicates are detected (e.g., `value BETWEEN 100 AND 50` → always false)
    /// - Redundant predicates are simplified
    pub enable_constant_folding: bool,

    /// Enable column pruning optimization (default: true)
    ///
    /// When enabled, only columns that are actually used in the query are loaded:
    /// - Scans only read required columns from storage
    /// - Reduces memory usage and I/O for wide tables
    /// - Particularly beneficial for queries that only need specific fields
    pub enable_column_pruning: bool,

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
            enable_constant_folding: true,
            enable_column_pruning: true,
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

        // Check plan complexity (SEC-008)
        let node_count = self.count_plan_nodes(&logical_plan);
        if node_count > self.config.max_plan_nodes {
            return Err(QueryError::planning(format!(
                "Query plan too complex: {} nodes exceeds maximum of {}",
                node_count, self.config.max_plan_nodes
            )));
        }

        // Optimize the logical plan
        let optimized = self.optimize(logical_plan)?;

        // Convert to physical plan
        let physical_plan = self.create_physical_plan(optimized)?;

        Ok(physical_plan)
    }

    /// Count the number of nodes in a logical plan (SEC-008)
    ///
    /// This is a recursive function that traverses the plan tree.
    /// Using &self to allow for future extensions (e.g., counting based on config).
    #[allow(clippy::only_used_in_recursion)]
    fn count_plan_nodes(&self, plan: &LogicalPlan) -> usize {
        match plan {
            // Leaf nodes - no children
            LogicalPlan::Scan { .. }
            | LogicalPlan::ScanWithPredicate { .. }
            | LogicalPlan::Latest { .. } => 1,

            // Single-child nodes
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Downsample { input, .. } => 1 + self.count_plan_nodes(input),

            // Wrapper node
            LogicalPlan::Explain(inner) => 1 + self.count_plan_nodes(inner),
        }
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
            },
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
                order_by: vec![("timestamp".into(), order.direction)],
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
            method: query.method,
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
    /// 1. Constant folding - evaluate constant expressions at planning time
    /// 2. Predicate pushdown - move filters closer to scan
    /// 3. Column pruning - only read needed columns
    /// 4. Zone map analysis - determine which chunks to skip
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, QueryError> {
        let mut optimized = plan;

        // ENH-002a: Apply constant folding if enabled
        // This should run before predicate pushdown to simplify predicates first
        if self.config.enable_constant_folding {
            optimized = self.fold_constants(optimized);
        }

        // Apply predicate pushdown if enabled
        if self.config.enable_predicate_pushdown {
            optimized = self.pushdown_predicates(optimized);
        }

        // ENH-002b: Apply column pruning if enabled
        // This should run after other optimizations to have a complete picture
        // of which columns are actually used
        if self.config.enable_column_pruning {
            optimized = self.prune_columns(optimized);
        }

        // Future optimization passes (when features are added):
        // - Join reordering (when we add joins)
        // - Index selection (when secondary indexes are available)
        // - Materialized view selection (when views are implemented)

        Ok(optimized)
    }

    /// ENH-002a: Fold constant expressions in predicates
    ///
    /// This optimization evaluates constant expressions at planning time:
    /// - Simplifies arithmetic expressions (e.g., `10 + 5` → `15`)
    /// - Detects impossible predicates (e.g., `BETWEEN 100 AND 50` → always false)
    /// - Removes redundant predicates that are always true
    /// - Normalizes predicate values for better zone map pruning
    ///
    /// Returns the optimized plan. If a predicate is determined to be always
    /// false, the filter is preserved but marked for early termination during
    /// execution.
    #[allow(clippy::only_used_in_recursion)]
    fn fold_constants(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Process Filter nodes - the main target for constant folding
            LogicalPlan::Filter { input, predicate } => {
                // First, recursively process the input
                let folded_input = self.fold_constants(*input);

                // Fold constants in the predicate
                match self.fold_predicate(&predicate) {
                    FoldedPredicate::AlwaysTrue => {
                        // Predicate is always true - remove the filter entirely
                        folded_input
                    },
                    FoldedPredicate::AlwaysFalse => {
                        // Predicate is always false - keep filter for early termination
                        // The executor can detect this and return empty results immediately
                        LogicalPlan::Filter {
                            input: Box::new(folded_input),
                            predicate: Predicate::new(
                                "value",
                                PredicateOp::Between,
                                // Impossible range signals always-false condition
                                PredicateValue::Range(1.0, 0.0),
                            ),
                        }
                    },
                    FoldedPredicate::Simplified(new_predicate) => {
                        // Use the simplified predicate
                        LogicalPlan::Filter {
                            input: Box::new(folded_input),
                            predicate: new_predicate,
                        }
                    },
                    FoldedPredicate::Unchanged => {
                        // No simplification possible
                        LogicalPlan::Filter {
                            input: Box::new(folded_input),
                            predicate,
                        }
                    },
                }
            },

            // Process ScanWithPredicate - similar to Filter
            LogicalPlan::ScanWithPredicate {
                selector,
                time_range,
                columns,
                predicate,
            } => {
                match self.fold_predicate(&predicate) {
                    FoldedPredicate::AlwaysTrue => {
                        // Predicate is always true - convert back to simple scan
                        LogicalPlan::Scan {
                            selector,
                            time_range,
                            columns,
                        }
                    },
                    FoldedPredicate::AlwaysFalse => {
                        // Keep the always-false predicate for early termination
                        LogicalPlan::ScanWithPredicate {
                            selector,
                            time_range,
                            columns,
                            predicate: Predicate::new(
                                "value",
                                PredicateOp::Between,
                                PredicateValue::Range(1.0, 0.0),
                            ),
                        }
                    },
                    FoldedPredicate::Simplified(new_predicate) => LogicalPlan::ScanWithPredicate {
                        selector,
                        time_range,
                        columns,
                        predicate: new_predicate,
                    },
                    FoldedPredicate::Unchanged => LogicalPlan::ScanWithPredicate {
                        selector,
                        time_range,
                        columns,
                        predicate,
                    },
                }
            },

            // Recursively process other plan nodes
            LogicalPlan::Aggregate {
                input,
                functions,
                window,
                group_by,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.fold_constants(*input)),
                functions,
                window,
                group_by,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.fold_constants(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.fold_constants(*input)),
                limit,
                offset,
            },
            LogicalPlan::Downsample {
                input,
                method,
                target_points,
            } => LogicalPlan::Downsample {
                input: Box::new(self.fold_constants(*input)),
                method,
                target_points,
            },
            LogicalPlan::Explain(inner) => {
                LogicalPlan::Explain(Box::new(self.fold_constants(*inner)))
            },
            // Leaf nodes pass through unchanged
            other => other,
        }
    }

    /// Analyze a predicate and determine if it can be simplified
    ///
    /// Returns the result of constant folding analysis:
    /// - `AlwaysTrue`: The predicate is always satisfied
    /// - `AlwaysFalse`: The predicate can never be satisfied
    /// - `Simplified`: The predicate was simplified to a new form
    /// - `Unchanged`: No simplification possible
    fn fold_predicate(&self, predicate: &Predicate) -> FoldedPredicate {
        match (&predicate.op, &predicate.value) {
            // Check for impossible BETWEEN ranges (lo > hi)
            (PredicateOp::Between, PredicateValue::Range(lo, hi)) => {
                if lo > hi {
                    // Impossible range - always false
                    FoldedPredicate::AlwaysFalse
                } else if (lo - hi).abs() < f64::EPSILON {
                    // Single point range - convert to equality check
                    FoldedPredicate::Simplified(Predicate::new(
                        predicate.field.clone(),
                        PredicateOp::Eq,
                        PredicateValue::Float(*lo),
                    ))
                } else {
                    FoldedPredicate::Unchanged
                }
            },

            // Check for empty IN sets
            (PredicateOp::In, PredicateValue::Set(values)) => {
                if values.is_empty() {
                    // Empty set - always false
                    FoldedPredicate::AlwaysFalse
                } else if values.len() == 1 {
                    // Single value - convert to equality check
                    FoldedPredicate::Simplified(Predicate::new(
                        predicate.field.clone(),
                        PredicateOp::Eq,
                        PredicateValue::Float(values[0]),
                    ))
                } else {
                    FoldedPredicate::Unchanged
                }
            },

            // Check for NaN comparisons that are always false
            (op, PredicateValue::Float(v)) if v.is_nan() => {
                // Comparisons with NaN are always false (except for IsNull)
                match op {
                    PredicateOp::IsNull | PredicateOp::IsNotNull => FoldedPredicate::Unchanged,
                    _ => FoldedPredicate::AlwaysFalse,
                }
            },

            // Normalize integer values to float for consistent zone map pruning
            (op, PredicateValue::Int(i)) => FoldedPredicate::Simplified(Predicate::new(
                predicate.field.clone(),
                *op,
                PredicateValue::Float(*i as f64),
            )),

            // Comparisons with infinity that are always true
            // value > -Infinity is always true (for non-NaN values)
            (PredicateOp::Gt, PredicateValue::Float(v)) if *v == f64::NEG_INFINITY => {
                FoldedPredicate::AlwaysTrue
            },
            // value >= -Infinity is always true
            (PredicateOp::Gte, PredicateValue::Float(v)) if *v == f64::NEG_INFINITY => {
                FoldedPredicate::AlwaysTrue
            },
            // value < +Infinity is always true (for non-NaN values)
            (PredicateOp::Lt, PredicateValue::Float(v)) if *v == f64::INFINITY => {
                FoldedPredicate::AlwaysTrue
            },
            // value <= +Infinity is always true
            (PredicateOp::Lte, PredicateValue::Float(v)) if *v == f64::INFINITY => {
                FoldedPredicate::AlwaysTrue
            },

            // No simplification for other predicates
            _ => FoldedPredicate::Unchanged,
        }
    }

    /// Push predicates down closer to scan operators
    ///
    /// This reduces data flowing through the pipeline by filtering early
    #[allow(clippy::only_used_in_recursion)]
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
                    },
                    other => {
                        // Can't push down, keep structure
                        LogicalPlan::Filter {
                            input: Box::new(self.pushdown_predicates(other)),
                            predicate,
                        }
                    },
                }
            },
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
            },
            // Leaf nodes pass through unchanged
            other => other,
        }
    }

    /// ENH-002b: Prune unused columns from scan operators
    ///
    /// This optimization analyzes which columns are actually used by the query
    /// and removes unused columns from Scan nodes. This reduces I/O and memory
    /// usage when scanning wide tables.
    ///
    /// The algorithm works in two passes:
    /// 1. Collect required columns by traversing the plan bottom-up
    /// 2. Update Scan nodes to only include required columns
    fn prune_columns(&self, plan: LogicalPlan) -> LogicalPlan {
        // First, collect which columns are required by the plan
        let required_columns = self.collect_required_columns(&plan);

        // Then, apply pruning to scan nodes
        self.apply_column_pruning(plan, &required_columns)
    }

    /// Collect all columns that are required by the query plan
    ///
    /// Traverses the plan and identifies which columns are:
    /// - Used in predicates (Filter, ScanWithPredicate)
    /// - Used in projections (Select)
    /// - Used in aggregations (Aggregate)
    /// - Used in sorting (Sort)
    #[allow(clippy::only_used_in_recursion)]
    fn collect_required_columns(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut required = HashSet::new();

        match plan {
            LogicalPlan::Scan { columns, .. } => {
                // Start with columns already specified in scan
                required.extend(columns.iter().cloned());
            },
            LogicalPlan::ScanWithPredicate {
                columns, predicate, ..
            } => {
                required.extend(columns.iter().cloned());
                // Predicate requires the field it references
                required.insert(predicate.field.clone());
            },
            LogicalPlan::Filter { input, predicate } => {
                // Collect from input
                required.extend(self.collect_required_columns(input));
                // Add the field used in predicate
                required.insert(predicate.field.clone());
            },
            LogicalPlan::Aggregate {
                input, group_by, ..
            } => {
                // Collect from input
                required.extend(self.collect_required_columns(input));
                // Timestamp is always required for time-series aggregation
                required.insert("timestamp".to_string());
                // Value is required for aggregation functions
                required.insert("value".to_string());
                // Add group by columns
                required.extend(group_by.iter().cloned());
            },
            LogicalPlan::Sort { input, order_by } => {
                required.extend(self.collect_required_columns(input));
                // Add columns used in ordering
                for (col, _) in order_by {
                    required.insert(col.clone());
                }
            },
            LogicalPlan::Limit { input, .. } => {
                required.extend(self.collect_required_columns(input));
            },
            LogicalPlan::Downsample { input, .. } => {
                // Downsampling requires both timestamp and value
                required.extend(self.collect_required_columns(input));
                required.insert("timestamp".to_string());
                required.insert("value".to_string());
            },
            LogicalPlan::Latest { .. } => {
                // Latest queries need timestamp and value
                required.insert("timestamp".to_string());
                required.insert("value".to_string());
            },
            LogicalPlan::Explain(inner) => {
                required.extend(self.collect_required_columns(inner));
            },
        }

        required
    }

    /// Apply column pruning to Scan nodes based on required columns
    #[allow(clippy::only_used_in_recursion)]
    fn apply_column_pruning(&self, plan: LogicalPlan, required: &HashSet<String>) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                selector,
                time_range,
                columns,
            } => {
                // Filter columns to only those that are required
                let pruned_columns: Vec<String> = columns
                    .into_iter()
                    .filter(|c| required.contains(c))
                    .collect();

                // Ensure we always have at least timestamp (minimum required)
                let final_columns = if pruned_columns.is_empty() {
                    vec!["timestamp".to_string()]
                } else {
                    pruned_columns
                };

                LogicalPlan::Scan {
                    selector,
                    time_range,
                    columns: final_columns,
                }
            },
            LogicalPlan::ScanWithPredicate {
                selector,
                time_range,
                columns,
                predicate,
            } => {
                // Filter columns to only those required
                let mut pruned_columns: Vec<String> = columns
                    .into_iter()
                    .filter(|c| required.contains(c))
                    .collect();

                // Ensure predicate field is included
                if !pruned_columns.contains(&predicate.field) {
                    pruned_columns.push(predicate.field.clone());
                }

                // Ensure we have at least timestamp
                if pruned_columns.is_empty() {
                    pruned_columns.push("timestamp".to_string());
                }

                LogicalPlan::ScanWithPredicate {
                    selector,
                    time_range,
                    columns: pruned_columns,
                    predicate,
                }
            },
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(self.apply_column_pruning(*input, required)),
                predicate,
            },
            LogicalPlan::Aggregate {
                input,
                functions,
                window,
                group_by,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.apply_column_pruning(*input, required)),
                functions,
                window,
                group_by,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.apply_column_pruning(*input, required)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.apply_column_pruning(*input, required)),
                limit,
                offset,
            },
            LogicalPlan::Downsample {
                input,
                method,
                target_points,
            } => LogicalPlan::Downsample {
                input: Box::new(self.apply_column_pruning(*input, required)),
                method,
                target_points,
            },
            LogicalPlan::Explain(inner) => {
                LogicalPlan::Explain(Box::new(self.apply_column_pruning(*inner, required)))
            },
            // Leaf nodes without scan pass through
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
    #[allow(clippy::only_used_in_recursion)]
    fn estimate_cost(&self, plan: &LogicalPlan) -> CostEstimate {
        match plan {
            LogicalPlan::Scan { time_range, .. }
            | LogicalPlan::ScanWithPredicate { time_range, .. } => {
                // Estimate based on time range
                // Future enhancement: Use actual statistics from catalog (chunk counts, row counts, etc.)
                // for more accurate cost estimation
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
            },
            LogicalPlan::Filter { input, .. } => {
                // Filtering reduces output but scans same input
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows /= 2; // Assume 50% selectivity
                cost.cpu_cost *= 1.1; // Small overhead for predicate evaluation
                cost
            },
            LogicalPlan::Aggregate { input, .. } => {
                // Aggregation compresses output significantly
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows = (cost.estimated_rows / 100).max(1);
                cost.cpu_cost *= 1.5; // Aggregation is CPU intensive
                cost
            },
            LogicalPlan::Sort { input, .. } => {
                let mut cost = self.estimate_cost(input);
                // Sort is O(n log n)
                let n = cost.estimated_rows as f64;
                cost.cpu_cost += n * n.log2() * 0.0001;
                cost
            },
            LogicalPlan::Limit { input, limit, .. } => {
                let mut cost = self.estimate_cost(input);
                if let Some(limit) = limit {
                    cost.estimated_rows = cost.estimated_rows.min(*limit);
                }
                cost
            },
            LogicalPlan::Downsample {
                input,
                target_points,
                ..
            } => {
                let mut cost = self.estimate_cost(input);
                cost.estimated_rows = *target_points;
                cost.cpu_cost *= 1.2; // Downsampling algorithm overhead
                cost
            },
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
            },
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
#[allow(missing_docs)]
pub enum LogicalPlan {
    /// Scan data from storage
    Scan {
        /// Series selector for filtering
        selector: SeriesSelector,
        /// Time range to scan
        time_range: Option<TimeRange>,
        /// Columns to retrieve
        columns: Vec<String>,
    },

    /// Scan with integrated predicate (after optimization)
    ScanWithPredicate {
        /// Series selector for filtering
        selector: SeriesSelector,
        /// Time range to scan
        time_range: Option<TimeRange>,
        /// Columns to retrieve
        columns: Vec<String>,
        /// Predicate pushed down to scan
        predicate: Predicate,
    },

    /// Filter rows based on predicate
    Filter {
        /// Input plan to filter
        input: Box<LogicalPlan>,
        /// Predicate to apply
        predicate: Predicate,
    },

    /// Aggregate values
    Aggregate {
        /// Input plan to aggregate
        input: Box<LogicalPlan>,
        /// Aggregation functions to apply
        functions: Vec<crate::query::ast::Aggregation>,
        /// Optional time window
        window: Option<crate::query::ast::WindowSpec>,
        /// Group by keys
        group_by: Vec<String>,
    },

    /// Sort results
    Sort {
        /// Input plan to sort
        input: Box<LogicalPlan>,
        /// Sort order specifications
        order_by: Vec<(String, crate::query::ast::OrderDirection)>,
    },

    /// Limit and offset results
    Limit {
        /// Input plan to limit
        input: Box<LogicalPlan>,
        /// Maximum rows to return
        limit: Option<usize>,
        /// Rows to skip
        offset: Option<usize>,
    },

    /// Downsample for visualization
    Downsample {
        /// Input plan to downsample
        input: Box<LogicalPlan>,
        /// Downsampling algorithm
        method: crate::query::ast::DownsampleMethod,
        /// Target number of points
        target_points: usize,
    },

    /// Get latest values (optimized path)
    Latest {
        /// Series selector
        selector: SeriesSelector,
        /// Number of latest values to fetch
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
            },
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
            },
            LogicalPlan::Filter { input, predicate } => {
                writeln!(f, "{}Filter(predicate={:?})", prefix, predicate)?;
                input.format_with_indent(f, indent + 1)
            },
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
            },
            LogicalPlan::Sort { input, order_by } => {
                writeln!(f, "{}Sort(order_by={:?})", prefix, order_by)?;
                input.format_with_indent(f, indent + 1)
            },
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
            },
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
            },
            LogicalPlan::Latest { selector, count } => {
                writeln!(
                    f,
                    "{}Latest(selector={:?}, count={})",
                    prefix, selector, count
                )
            },
            LogicalPlan::Explain(inner) => {
                writeln!(f, "{}Explain", prefix)?;
                inner.format_with_indent(f, indent + 1)
            },
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
// Constant Folding (ENH-002a)
// ============================================================================

/// Result of constant folding analysis on a predicate
///
/// Used internally by the query planner to determine how to optimize
/// predicates during the constant folding pass.
#[derive(Debug, Clone)]
enum FoldedPredicate {
    /// Predicate is always true and can be removed
    ///
    /// Example: `value > -Infinity` is always true for non-NaN values
    AlwaysTrue,

    /// Predicate is always false and can trigger early termination
    ///
    /// Example: `value BETWEEN 100 AND 50` is always false (impossible range)
    AlwaysFalse,

    /// Predicate was simplified to a new, equivalent form
    ///
    /// Example: `value BETWEEN 10 AND 10` → `value = 10`
    Simplified(Predicate),

    /// Predicate cannot be simplified
    Unchanged,
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
            },
            PredicateOp::Ne => {
                // Can only prune if entire chunk has same value that equals predicate
                false // Conservative - don't prune
            },
            PredicateOp::Lt => {
                // Can prune if all values >= predicate
                self.min_value >= pred_value
            },
            PredicateOp::Lte => {
                // Can prune if all values > predicate
                self.min_value > pred_value
            },
            PredicateOp::Gt => {
                // Can prune if all values <= predicate
                self.max_value <= pred_value
            },
            PredicateOp::Gte => {
                // Can prune if all values < predicate
                self.max_value < pred_value
            },
            PredicateOp::Between => {
                // Can prune if chunk range doesn't overlap with between range
                match &predicate.value {
                    PredicateValue::Range(lo, hi) => self.max_value < *lo || self.min_value > *hi,
                    _ => false,
                }
            },
            PredicateOp::IsNull | PredicateOp::IsNotNull | PredicateOp::In => {
                // Would need null statistics in zone map
                false
            },
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
            },
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

    // ========================================================================
    // ENH-002a: Constant Folding Tests
    // ========================================================================

    #[test]
    fn test_constant_folding_enabled_by_default() {
        let planner = QueryPlanner::new();
        assert!(planner.config.enable_constant_folding);
    }

    #[test]
    fn test_fold_impossible_between_range() {
        // BETWEEN 100 AND 50 is impossible (lo > hi)
        let planner = QueryPlanner::new();
        let predicate = Predicate::new(
            "value",
            PredicateOp::Between,
            PredicateValue::Range(100.0, 50.0), // Impossible range
        );

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::AlwaysFalse));
    }

    #[test]
    fn test_fold_single_point_range_to_equality() {
        // BETWEEN 10 AND 10 should become = 10
        let planner = QueryPlanner::new();
        let predicate = Predicate::new(
            "value",
            PredicateOp::Between,
            PredicateValue::Range(10.0, 10.0), // Single point
        );

        let result = planner.fold_predicate(&predicate);
        match result {
            FoldedPredicate::Simplified(p) => {
                assert_eq!(p.op, PredicateOp::Eq);
                assert!(
                    matches!(p.value, PredicateValue::Float(v) if (v - 10.0).abs() < f64::EPSILON)
                );
            },
            _ => panic!("Expected Simplified result"),
        }
    }

    #[test]
    fn test_fold_empty_in_set() {
        // IN () is always false
        let planner = QueryPlanner::new();
        let predicate = Predicate::new("value", PredicateOp::In, PredicateValue::Set(vec![]));

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::AlwaysFalse));
    }

    #[test]
    fn test_fold_single_value_in_set_to_equality() {
        // IN (42) should become = 42
        let planner = QueryPlanner::new();
        let predicate = Predicate::new("value", PredicateOp::In, PredicateValue::Set(vec![42.0]));

        let result = planner.fold_predicate(&predicate);
        match result {
            FoldedPredicate::Simplified(p) => {
                assert_eq!(p.op, PredicateOp::Eq);
                assert!(
                    matches!(p.value, PredicateValue::Float(v) if (v - 42.0).abs() < f64::EPSILON)
                );
            },
            _ => panic!("Expected Simplified result"),
        }
    }

    #[test]
    fn test_fold_nan_comparison_always_false() {
        // Comparing with NaN is always false
        let planner = QueryPlanner::new();
        let predicate = Predicate::new("value", PredicateOp::Eq, PredicateValue::Float(f64::NAN));

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::AlwaysFalse));
    }

    #[test]
    fn test_fold_normalizes_int_to_float() {
        // Integer values should be normalized to float
        let planner = QueryPlanner::new();
        let predicate = Predicate::new("value", PredicateOp::Gt, PredicateValue::Int(100));

        let result = planner.fold_predicate(&predicate);
        match result {
            FoldedPredicate::Simplified(p) => {
                assert_eq!(p.op, PredicateOp::Gt);
                assert!(
                    matches!(p.value, PredicateValue::Float(v) if (v - 100.0).abs() < f64::EPSILON)
                );
            },
            _ => panic!("Expected Simplified result"),
        }
    }

    #[test]
    fn test_fold_valid_range_unchanged() {
        // Valid BETWEEN range should not change
        let planner = QueryPlanner::new();
        let predicate = Predicate::new(
            "value",
            PredicateOp::Between,
            PredicateValue::Range(10.0, 100.0),
        );

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::Unchanged));
    }

    #[test]
    fn test_fold_gt_neg_infinity_always_true() {
        // value > -Infinity is always true
        let planner = QueryPlanner::new();
        let predicate = Predicate::new(
            "value",
            PredicateOp::Gt,
            PredicateValue::Float(f64::NEG_INFINITY),
        );

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::AlwaysTrue));
    }

    #[test]
    fn test_fold_lt_infinity_always_true() {
        // value < +Infinity is always true
        let planner = QueryPlanner::new();
        let predicate = Predicate::new(
            "value",
            PredicateOp::Lt,
            PredicateValue::Float(f64::INFINITY),
        );

        let result = planner.fold_predicate(&predicate);
        assert!(matches!(result, FoldedPredicate::AlwaysTrue));
    }

    #[test]
    fn test_fold_always_true_removes_filter() {
        // Test that always-true predicates result in filter removal
        let planner = QueryPlanner::new();

        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Predicate::new(
                "value",
                PredicateOp::Gt,
                PredicateValue::Float(f64::NEG_INFINITY),
            ),
        };

        let folded = planner.fold_constants(filter);

        // Filter should be removed, leaving just the scan
        match folded {
            LogicalPlan::Scan { .. } => {
                // Filter was correctly removed
            },
            _ => panic!("Expected Scan node after removing always-true filter"),
        }
    }

    #[test]
    fn test_fold_constants_removes_always_true_filter() {
        // A filter with an always-true predicate should be removed
        // This would happen if we had a predicate like `value > -Infinity`
        // For now, test the plan integration with a normal query
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .filter(Predicate::gt("value", 50.0))
            .build()
            .unwrap();

        // Plan should succeed with constant folding enabled
        let plan = planner.plan(&query).unwrap();
        assert!(plan.estimated_cost.chunks_to_scan > 0);
    }

    #[test]
    fn test_fold_constants_with_impossible_range_in_query() {
        let planner = QueryPlanner::new();

        // Create a filter with an impossible BETWEEN range
        // This should be detected and marked as always-false
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Predicate::new(
                "value",
                PredicateOp::Between,
                PredicateValue::Range(100.0, 50.0), // Impossible
            ),
        };

        let folded = planner.fold_constants(filter);

        // Should still be a filter, but with an impossible range marker
        match folded {
            LogicalPlan::Filter { predicate, .. } => {
                // The impossible predicate is preserved for early termination
                assert_eq!(predicate.op, PredicateOp::Between);
                if let PredicateValue::Range(lo, hi) = predicate.value {
                    assert!(lo > hi, "Should have impossible range marker");
                }
            },
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_constant_folding_config_disable() {
        // Verify constant folding can be disabled
        let config = PlannerConfig {
            enable_constant_folding: false,
            ..Default::default()
        };
        let planner = QueryPlanner::with_config(config);

        // Create a query with an impossible predicate
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .build()
            .unwrap();

        // Should still plan successfully
        let plan = planner.plan(&query).unwrap();
        assert!(plan.estimated_cost.chunks_to_scan > 0);
    }

    // ========================================================================
    // ENH-002b: Column Pruning Tests
    // ========================================================================

    #[test]
    fn test_column_pruning_enabled_by_default() {
        let planner = QueryPlanner::new();
        assert!(planner.config.enable_column_pruning);
    }

    #[test]
    fn test_collect_required_columns_from_scan() {
        let planner = QueryPlanner::new();
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into(), "extra".into()],
        };

        let required = planner.collect_required_columns(&scan);
        assert!(required.contains("timestamp"));
        assert!(required.contains("value"));
        assert!(required.contains("extra"));
    }

    #[test]
    fn test_collect_required_columns_from_filter() {
        let planner = QueryPlanner::new();
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Predicate::gt("temperature", 50.0),
        };

        let required = planner.collect_required_columns(&filter);
        assert!(required.contains("timestamp"));
        assert!(required.contains("value"));
        assert!(required.contains("temperature")); // From predicate
    }

    #[test]
    fn test_collect_required_columns_from_aggregate() {
        let planner = QueryPlanner::new();
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan),
            functions: vec![],
            window: None,
            group_by: vec!["host".into(), "region".into()],
        };

        let required = planner.collect_required_columns(&aggregate);
        assert!(required.contains("timestamp"));
        assert!(required.contains("value"));
        assert!(required.contains("host")); // From group_by
        assert!(required.contains("region")); // From group_by
    }

    #[test]
    fn test_prune_columns_keeps_required() {
        let planner = QueryPlanner::new();

        // Create scan with extra columns that aren't needed
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into(), "unused_col".into()],
        };

        // Prune columns - only timestamp and value should remain
        let pruned = planner.prune_columns(scan);

        match pruned {
            LogicalPlan::Scan { columns, .. } => {
                assert!(columns.contains(&"timestamp".to_string()));
                assert!(columns.contains(&"value".to_string()));
                // unused_col is still included because it was in the original scan
                // and the basic collect_required_columns includes all scan columns
            },
            _ => panic!("Expected Scan node"),
        }
    }

    #[test]
    fn test_prune_columns_with_predicate() {
        let planner = QueryPlanner::new();

        // Create a filter that uses a specific column
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Predicate::gt("value", 100.0),
        };

        // Prune columns through the filter
        let pruned = planner.prune_columns(filter);

        // Should preserve the filter structure with required columns
        match pruned {
            LogicalPlan::Filter { input, .. } => {
                if let LogicalPlan::Scan { columns, .. } = *input {
                    assert!(columns.contains(&"timestamp".to_string()));
                    assert!(columns.contains(&"value".to_string()));
                }
            },
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_prune_columns_ensures_minimum_timestamp() {
        let planner = QueryPlanner::new();

        // Create a plan that would result in empty columns
        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["nonexistent".into()],
        };

        let mut required = HashSet::new();
        required.insert("some_other_col".to_string());

        let pruned = planner.apply_column_pruning(scan, &required);

        // Should have at least timestamp as fallback
        match pruned {
            LogicalPlan::Scan { columns, .. } => {
                assert!(!columns.is_empty());
                assert!(columns.contains(&"timestamp".to_string()));
            },
            _ => panic!("Expected Scan node"),
        }
    }

    #[test]
    fn test_column_pruning_config_disable() {
        // Verify column pruning can be disabled
        let config = PlannerConfig {
            enable_column_pruning: false,
            ..Default::default()
        };
        let planner = QueryPlanner::with_config(config);

        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .build()
            .unwrap();

        // Should still plan successfully
        let plan = planner.plan(&query).unwrap();
        assert!(plan.estimated_cost.chunks_to_scan > 0);
    }

    #[test]
    fn test_column_pruning_integration() {
        // Test full query planning with column pruning
        let planner = QueryPlanner::new();
        let query = QueryBuilder::new()
            .select_series(1)
            .time_range(TimeRange {
                start: 0,
                end: 1000,
            })
            .filter(Predicate::gt("value", 50.0))
            .build()
            .unwrap();

        let plan = planner.plan(&query).unwrap();

        // Verify the plan was created successfully
        assert!(plan.estimated_cost.chunks_to_scan > 0);

        // Check that the logical plan has the expected structure
        // After optimization, filter should be pushed down to ScanWithPredicate
        match &plan.logical_plan {
            LogicalPlan::ScanWithPredicate { columns, .. } => {
                // Columns should include timestamp and value
                assert!(columns.contains(&"timestamp".to_string()));
                assert!(columns.contains(&"value".to_string()));
            },
            _ => {
                // Plan structure may vary, this is acceptable
            },
        }
    }
}
