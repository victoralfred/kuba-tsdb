//! Query Integration Module
//!
//! This module bridges the query engine with storage and index layers:
//! - Redis index for tag-based series discovery
//! - LocalDiskEngine for chunk data access
//! - Operator tree construction from logical plans
//!
//! # Architecture
//!
//! ```text
//! Query
//!   │
//!   ├─► Redis Index ─► Series IDs (by tags)
//!   │
//!   ├─► Planner ─► LogicalPlan
//!   │
//!   └─► Integration ─► Operator Tree ─► Results
//!           │
//!           └─► Storage Engine (chunks)
//! ```

use crate::query::ast::{AggregationFunction, Query, SeriesSelector};
use crate::query::error::QueryError;
use crate::query::executor::{ExecutionContext, ExecutorConfig};
use crate::query::operators::{
    AggregationOperator, DownsampleOperator, FilterOperator, Operator, ScanOperator,
    StorageScanOperator,
};
use crate::query::planner::{LogicalPlan, QueryPlanner};
use crate::query::result::{QueryResult, ResultRow};
use crate::redis::RedisTimeIndex;
use crate::storage::LocalDiskEngine;
use crate::types::{SeriesId, TagFilter};

use std::collections::HashMap;

use std::sync::Arc;

/// Query engine that integrates all components for end-to-end query execution
///
/// This is the main entry point for executing queries against the database.
/// It coordinates:
/// - Series discovery via Redis index (tag-based lookups)
/// - Query planning and optimization
/// - Operator tree construction
/// - Data retrieval from storage
pub struct QueryEngine {
    /// Redis index for series metadata and tag lookups
    redis_index: Option<Arc<RedisTimeIndex>>,

    /// Storage engine for chunk data
    storage: Arc<LocalDiskEngine>,

    /// Query planner
    planner: QueryPlanner,

    /// Executor configuration
    config: ExecutorConfig,
}

impl QueryEngine {
    /// Create a new query engine with storage only (no Redis)
    ///
    /// Use this for simple deployments without Redis index.
    /// Series must be queried by ID directly.
    pub fn new(storage: Arc<LocalDiskEngine>) -> Self {
        Self {
            redis_index: None,
            storage,
            planner: QueryPlanner::new(),
            config: ExecutorConfig::default(),
        }
    }

    /// Create a query engine with Redis index support
    ///
    /// Enables tag-based series discovery and metadata caching.
    pub fn with_redis(storage: Arc<LocalDiskEngine>, redis: Arc<RedisTimeIndex>) -> Self {
        Self {
            redis_index: Some(redis),
            storage,
            planner: QueryPlanner::new(),
            config: ExecutorConfig::default(),
        }
    }

    /// Set executor configuration
    pub fn with_config(mut self, config: ExecutorConfig) -> Self {
        self.config = config;
        self
    }

    /// Execute a query and return results
    ///
    /// This is the main entry point for query execution:
    /// 1. Plan the query (generate logical plan, optimize)
    /// 2. Resolve series (using Redis if available, or direct ID)
    /// 3. Build operator tree from plan
    /// 4. Execute operators and collect results
    pub async fn execute(&self, query: &Query) -> Result<QueryResult, QueryError> {
        // Generate optimized plan
        let plan = self.planner.plan(query)?;

        // Build operator tree from physical plan
        let mut operator = self.build_operator_tree(&plan.logical_plan).await?;

        // Create execution context
        let mut ctx = ExecutionContext::new(&self.config);

        // Execute and collect results
        let mut rows = Vec::new();
        let mut rows_scanned = 0u64;

        while let Some(batch) = operator.next_batch(&mut ctx)? {
            rows_scanned += batch.len() as u64;

            // Convert batch to result rows
            for i in 0..batch.len() {
                if rows.len() >= self.config.max_result_rows {
                    break;
                }

                rows.push(ResultRow {
                    timestamp: batch.timestamps[i],
                    value: batch.values[i],
                    series_id: batch.series_ids.as_ref().map(|ids| ids[i] as u128),
                    tags: HashMap::new(),
                });
            }

            // Check if we've hit the row limit
            if rows.len() >= self.config.max_result_rows {
                break;
            }
        }

        Ok(QueryResult::new(rows).with_rows_scanned(rows_scanned))
    }

    /// Build an operator tree from a logical plan
    ///
    /// Recursively constructs operators bottom-up, connecting them
    /// in a pull-based pipeline.
    fn build_operator_tree<'a>(
        &'a self,
        plan: &'a LogicalPlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Box<dyn Operator>, QueryError>> + Send + 'a>,
    > {
        Box::pin(self.build_operator_tree_impl(plan))
    }

    /// Implementation of operator tree building (separated for async recursion)
    async fn build_operator_tree_impl(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Box<dyn Operator>, QueryError> {
        match plan {
            // Scan operator - leaf node that reads from storage
            LogicalPlan::Scan {
                selector,
                time_range,
                columns: _,
            } => {
                let series_ids = self.resolve_series(selector).await?;

                if series_ids.is_empty() {
                    // No matching series - return empty scan
                    Ok(Box::new(
                        ScanOperator::new(selector.clone(), *time_range)
                            .with_batch_size(self.config.batch_size),
                    ))
                } else if series_ids.len() == 1 {
                    // Single series - use storage scan
                    let mut scan_selector = selector.clone();
                    scan_selector.series_id = Some(series_ids[0]);

                    Ok(Box::new(
                        StorageScanOperator::new(self.storage.clone(), scan_selector)
                            .with_optional_time_range(*time_range)
                            .with_batch_size(self.config.batch_size),
                    ))
                } else {
                    // Multiple series - for now, use first one
                    // TODO: Implement multi-series scan with union operator
                    let mut scan_selector = selector.clone();
                    scan_selector.series_id = Some(series_ids[0]);

                    Ok(Box::new(
                        StorageScanOperator::new(self.storage.clone(), scan_selector)
                            .with_optional_time_range(*time_range)
                            .with_batch_size(self.config.batch_size),
                    ))
                }
            }

            // Scan with predicate pushdown
            LogicalPlan::ScanWithPredicate {
                selector,
                time_range,
                predicate,
                columns: _,
            } => {
                let series_ids = self.resolve_series(selector).await?;

                if series_ids.is_empty() {
                    Ok(Box::new(
                        ScanOperator::new(selector.clone(), *time_range)
                            .with_predicate(predicate.clone())
                            .with_batch_size(self.config.batch_size),
                    ))
                } else {
                    let mut scan_selector = selector.clone();
                    scan_selector.series_id = Some(series_ids[0]);

                    Ok(Box::new(
                        StorageScanOperator::new(self.storage.clone(), scan_selector)
                            .with_optional_time_range(*time_range)
                            .with_predicate(predicate.clone())
                            .with_batch_size(self.config.batch_size),
                    ))
                }
            }

            // Filter operator
            LogicalPlan::Filter { input, predicate } => {
                let input_op = self.build_operator_tree(input).await?;
                Ok(Box::new(FilterOperator::new(input_op, predicate.clone())))
            }

            // Aggregation operator
            LogicalPlan::Aggregate {
                input,
                functions,
                window,
                group_by,
            } => {
                let input_op = self.build_operator_tree(input).await?;

                // Get first aggregation function (for now, single function support)
                let function = functions
                    .first()
                    .map(|a| a.function.clone())
                    .unwrap_or(AggregationFunction::Count);

                // Build aggregation operator with builder pattern
                let mut agg_op = AggregationOperator::new(input_op, function);

                // Apply window if present
                if let Some(w) = window {
                    agg_op = agg_op.with_window(w.clone());
                }

                // Apply group by series if specified
                if !group_by.is_empty() {
                    agg_op = agg_op.with_group_by_series();
                }

                Ok(Box::new(agg_op))
            }

            // Downsample operator
            LogicalPlan::Downsample {
                input,
                method,
                target_points,
            } => {
                let input_op = self.build_operator_tree(input).await?;
                Ok(Box::new(DownsampleOperator::new(
                    input_op,
                    method.clone(),
                    *target_points,
                )))
            }

            // Sort - for now, data is already sorted by timestamp
            LogicalPlan::Sort { input, order_by: _ } => {
                // TODO: Implement proper sort operator
                self.build_operator_tree(input).await
            }

            // Limit - apply after collecting
            LogicalPlan::Limit {
                input,
                limit: _,
                offset: _,
            } => {
                // TODO: Implement limit operator
                self.build_operator_tree(input).await
            }

            // Latest - special handling
            LogicalPlan::Latest { selector, count } => {
                // For latest, we scan backwards and take first N
                // For now, use regular scan - TODO: optimize
                let series_ids = self.resolve_series(selector).await?;

                if series_ids.is_empty() {
                    Ok(Box::new(
                        ScanOperator::new(selector.clone(), None).with_batch_size(*count),
                    ))
                } else {
                    let mut scan_selector = selector.clone();
                    scan_selector.series_id = Some(series_ids[0]);

                    Ok(Box::new(
                        StorageScanOperator::new(self.storage.clone(), scan_selector)
                            .with_batch_size(*count),
                    ))
                }
            }

            // Explain - doesn't execute, returns plan description
            LogicalPlan::Explain(_inner) => {
                // Return empty scan - actual explain is handled at higher level
                Ok(Box::new(ScanOperator::new(
                    SeriesSelector::by_measurement("*"),
                    None,
                )))
            }
        }
    }

    /// Resolve series IDs from a selector
    ///
    /// Uses Redis index if available for tag-based lookups,
    /// otherwise falls back to direct ID from selector.
    async fn resolve_series(&self, selector: &SeriesSelector) -> Result<Vec<SeriesId>, QueryError> {
        // If selector has a direct series ID, use it
        if let Some(id) = selector.series_id {
            return Ok(vec![id]);
        }

        // If we have Redis, use it for tag-based lookup
        if let Some(ref redis) = self.redis_index {
            // Convert TagMatchers to TagFilter for Redis
            let tag_filter = self.convert_tag_matchers_to_filter(&selector.tag_filters);

            // Get metric name from selector
            let metric_name = selector.measurement.as_deref().unwrap_or("*");

            // Query Redis for matching series
            let series_ids = redis
                .find_series_by_tags(metric_name, &tag_filter)
                .await
                .map_err(|e| QueryError::execution(format!("Redis lookup failed: {}", e)))?;

            return Ok(series_ids);
        }

        // No Redis and no direct ID - return empty
        Ok(Vec::new())
    }

    /// Convert query AST TagMatchers to types::TagFilter for Redis lookup
    fn convert_tag_matchers_to_filter(
        &self,
        matchers: &[crate::query::ast::TagMatcher],
    ) -> TagFilter {
        use crate::query::ast::TagMatcher;

        if matchers.is_empty() {
            return TagFilter::All;
        }

        // Extract exact matches (key=value pairs)
        let exact_tags: HashMap<String, String> = matchers
            .iter()
            .filter_map(|m| match m {
                TagMatcher::Equals { key, value } => Some((key.clone(), value.clone())),
                _ => None,
            })
            .collect();

        if !exact_tags.is_empty() {
            return TagFilter::Exact(exact_tags);
        }

        // Check for regex patterns
        let pattern = matchers.iter().find_map(|m| match m {
            TagMatcher::Regex { pattern, .. } => Some(pattern.clone()),
            _ => None,
        });

        if let Some(p) = pattern {
            return TagFilter::Pattern(p);
        }

        // Fallback to all
        TagFilter::All
    }
}

/// Extension trait for RedisTimeIndex to support query engine integration
#[async_trait::async_trait]
pub trait RedisQueryExt {
    /// Find series by metric name and tag filters
    async fn find_series_by_tags(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, crate::error::IndexError>;
}

#[async_trait::async_trait]
impl RedisQueryExt for RedisTimeIndex {
    async fn find_series_by_tags(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, crate::error::IndexError> {
        // Use the existing find_series method from TimeIndex trait
        use crate::engine::traits::TimeIndex;
        self.find_series(metric_name, tag_filter).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{Aggregation, DownsampleMethod, Predicate};
    use crate::types::TimeRange;
    use tempfile::TempDir;

    fn create_test_engine() -> (Arc<LocalDiskEngine>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let engine = Arc::new(
            LocalDiskEngine::new(temp_dir.path().to_path_buf()).expect("Failed to create engine"),
        );
        (engine, temp_dir)
    }

    #[test]
    fn test_query_engine_creation() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);
        assert!(engine.redis_index.is_none());
    }

    #[test]
    fn test_query_engine_with_config() {
        let (storage, _temp) = create_test_engine();
        let config = ExecutorConfig::default()
            .with_batch_size(1024)
            .with_max_result_rows(100);

        let engine = QueryEngine::new(storage).with_config(config);
        assert_eq!(engine.config.batch_size, 1024);
        assert_eq!(engine.config.max_result_rows, 100);
    }

    #[tokio::test]
    async fn test_resolve_series_by_id() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        let selector = SeriesSelector::by_id(42);
        let series = engine.resolve_series(&selector).await.unwrap();

        assert_eq!(series.len(), 1);
        assert_eq!(series[0], 42);
    }

    #[tokio::test]
    async fn test_resolve_series_no_match() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        // Selector without ID and no Redis
        let selector = SeriesSelector::by_measurement("cpu.usage");
        let series = engine.resolve_series(&selector).await.unwrap();

        assert!(series.is_empty());
    }

    #[tokio::test]
    async fn test_build_scan_operator() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        let plan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: Some(TimeRange {
                start: 0,
                end: 1000,
            }),
            columns: vec!["timestamp".into(), "value".into()],
        };

        let operator = engine.build_operator_tree(&plan).await.unwrap();
        assert_eq!(operator.name(), "StorageScan");
    }

    #[tokio::test]
    async fn test_build_filter_operator() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: None,
            columns: vec![],
        };

        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Predicate::gt("value", 50.0),
        };

        let operator = engine.build_operator_tree(&plan).await.unwrap();
        assert_eq!(operator.name(), "Filter");
    }

    #[tokio::test]
    async fn test_build_aggregation_operator() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: None,
            columns: vec![],
        };

        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan),
            functions: vec![Aggregation::simple(AggregationFunction::Avg)],
            window: None,
            group_by: vec![],
        };

        let operator = engine.build_operator_tree(&plan).await.unwrap();
        assert_eq!(operator.name(), "Aggregation");
    }

    #[tokio::test]
    async fn test_build_downsample_operator() {
        let (storage, _temp) = create_test_engine();
        let engine = QueryEngine::new(storage);

        let scan = LogicalPlan::Scan {
            selector: SeriesSelector::by_id(1),
            time_range: None,
            columns: vec![],
        };

        let plan = LogicalPlan::Downsample {
            input: Box::new(scan),
            method: DownsampleMethod::Lttb,
            target_points: 100,
        };

        let operator = engine.build_operator_tree(&plan).await.unwrap();
        assert_eq!(operator.name(), "Downsample");
    }
}
