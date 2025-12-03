//! Query Executor - Vectorized execution engine
//!
//! This module provides the core query execution infrastructure with:
//! - Vectorized batch processing for CPU efficiency
//! - Morsel-driven parallelism for multi-core scaling
//! - Streaming execution to minimize memory footprint
//! - Integration with storage layer for optimized data access
//!
//! # Architecture
//!
//! The executor follows a pull-based volcano model with vectorized operators:
//!
//! ```text
//! ┌─────────────────┐
//! │  Result Sink    │  ← Final results collected here
//! └────────┬────────┘
//!          │ pull batches
//! ┌────────▼────────┐
//! │  Aggregation    │  ← Optional: GROUP BY, window functions
//! └────────┬────────┘
//!          │ pull batches
//! ┌────────▼────────┐
//! │    Filter       │  ← Predicate evaluation (SIMD)
//! └────────┬────────┘
//!          │ pull batches
//! ┌────────▼────────┐
//! │  Chunk Scanner  │  ← Decompress, decode, vectorize
//! └─────────────────┘
//! ```
//!
//! # Morsel-Driven Parallelism
//!
//! Work is divided into "morsels" (small batches of ~1000-4000 rows) that
//! are distributed across worker threads. This provides:
//! - Better cache locality than full partitioning
//! - Dynamic load balancing across cores
//! - Minimal synchronization overhead

use crate::query::ast::Query;
use crate::query::error::QueryError;
use crate::query::operators::{DataBatch, Operator};
use crate::query::result::{QueryResult, ResultRow};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Executor Configuration
// ============================================================================

/// Configuration for query execution
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum number of parallel workers (default: num_cpus)
    pub max_parallelism: usize,

    /// Size of data batches for vectorized processing (default: 4096)
    /// Larger batches = better SIMD utilization, more memory
    pub batch_size: usize,

    /// Maximum memory budget for query execution in bytes (default: 256MB)
    pub memory_limit: usize,

    /// Query timeout duration (default: 30 seconds)
    pub timeout: Duration,

    /// Maximum number of rows in result (default: 1_000_000)
    pub max_result_rows: usize,

    /// Enable SIMD vectorization (default: true)
    pub enable_simd: bool,

    /// Enable parallel execution (default: true)
    pub enable_parallel: bool,

    /// Enable query result caching (default: true)
    pub enable_caching: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_parallelism: num_cpus::get(),
            batch_size: 4096,
            memory_limit: 256 * 1024 * 1024, // 256 MB
            timeout: Duration::from_secs(30),
            max_result_rows: 1_000_000,
            enable_simd: true,
            enable_parallel: true,
            enable_caching: true,
        }
    }
}

impl ExecutorConfig {
    /// Create a new executor config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum parallelism
    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.max_parallelism = workers.max(1);
        self
    }

    /// Set batch size for vectorized processing
    pub fn with_batch_size(mut self, size: usize) -> Self {
        // Batch size should be power of 2 for SIMD alignment
        self.batch_size = size.next_power_of_two();
        self
    }

    /// Set memory limit in bytes
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Set query timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set maximum result rows
    pub fn with_max_result_rows(mut self, rows: usize) -> Self {
        self.max_result_rows = rows;
        self
    }

    /// Disable SIMD vectorization
    pub fn without_simd(mut self) -> Self {
        self.enable_simd = false;
        self
    }

    /// Disable parallel execution (single-threaded mode)
    pub fn without_parallel(mut self) -> Self {
        self.enable_parallel = false;
        self
    }

    /// Disable result caching
    pub fn without_caching(mut self) -> Self {
        self.enable_caching = false;
        self
    }
}

// ============================================================================
// Query Executor
// ============================================================================

/// Main query executor that orchestrates query processing
///
/// The executor is responsible for:
/// 1. Receiving planned queries from the query planner
/// 2. Allocating resources and coordinating parallel execution
/// 3. Managing data flow through the operator pipeline
/// 4. Collecting and formatting results
pub struct QueryExecutor {
    /// Executor configuration
    config: ExecutorConfig,

    /// Execution statistics for monitoring
    stats: ExecutionStats,
}

impl QueryExecutor {
    /// Create a new query executor with default configuration
    pub fn new() -> Self {
        Self::with_config(ExecutorConfig::default())
    }

    /// Create a query executor with custom configuration
    pub fn with_config(config: ExecutorConfig) -> Self {
        Self {
            config,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute a query and return results
    ///
    /// This is the main entry point for query execution. The query goes through:
    /// 1. Validation - ensure query is well-formed
    /// 2. Planning - choose execution strategy (delegated to planner)
    /// 3. Execution - run the query plan
    /// 4. Formatting - convert results to requested format
    ///
    /// # Arguments
    ///
    /// * `query` - The parsed and validated query to execute
    ///
    /// # Returns
    ///
    /// * `Ok(QueryResult)` - Query results with metadata
    /// * `Err(QueryError)` - If execution fails
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::query::{QueryExecutor, QueryBuilder, ExecutorConfig};
    /// use gorilla_tsdb::types::TimeRange;
    ///
    /// // Create executor with config using with_config method
    /// let mut executor = QueryExecutor::with_config(ExecutorConfig::default());
    /// let query = QueryBuilder::new()
    ///     .select_series(1)
    ///     .time_range(TimeRange::new(0, 3600000).unwrap())
    ///     .build()
    ///     .unwrap();
    /// let result = executor.execute(query);
    /// ```
    pub fn execute(&mut self, query: Query) -> Result<QueryResult, QueryError> {
        let start = Instant::now();

        // Check for timeout before starting (EDGE-007)
        // Use is_zero() to catch sub-second timeouts like Duration::from_nanos(1)
        if self.config.timeout.is_zero() {
            return Err(QueryError::timeout("Query timeout is zero"));
        }

        // Execute based on query type
        let result = match &query {
            Query::Select(select) => self.execute_select(select),
            Query::Aggregate(agg) => self.execute_aggregate(agg),
            Query::Downsample(ds) => self.execute_downsample(ds),
            Query::Latest(latest) => self.execute_latest(latest),
            Query::Stream(_stream) => Err(QueryError::execution(
                "Streaming queries require async execution",
            )),
            Query::Explain(inner) => self.execute_explain(inner),
        };

        // Record execution time
        let duration = start.elapsed();
        self.stats.total_queries += 1;
        self.stats.total_execution_time += duration;

        // Add execution metadata to result
        result.map(|r| r.with_execution_time(duration))
    }

    /// Execute a SELECT query - fetch raw data points
    ///
    /// NOTE: This stub returns empty results. For production query execution,
    /// use `QueryRouter::execute_select()` in `bin/server/query_router.rs` which
    /// integrates with RedisTimeIndex and LocalDiskEngine for actual storage access.
    fn execute_select(
        &mut self,
        _query: &crate::query::ast::SelectQuery,
    ) -> Result<QueryResult, QueryError> {
        // Stub implementation - see bin/server/query_router.rs for production use
        Ok(QueryResult::empty())
    }

    /// Execute an AGGREGATE query - compute aggregations over time windows
    ///
    /// NOTE: This stub returns empty results. For production query execution,
    /// use `QueryRouter::execute_aggregate()` in `bin/server/query_router.rs` and
    /// the `aggregation` module which provide full aggregation support.
    fn execute_aggregate(
        &mut self,
        _query: &crate::query::ast::AggregateQuery,
    ) -> Result<QueryResult, QueryError> {
        // Stub implementation - see bin/server/query_router.rs for production use
        Ok(QueryResult::empty())
    }

    /// Execute a DOWNSAMPLE query - reduce data for visualization
    ///
    /// NOTE: This stub returns empty results. For production query execution,
    /// use `QueryRouter::execute_downsample()` in `bin/server/query_router.rs` and
    /// `query::operators::downsample` which implement LTTB, M4, and Average algorithms.
    fn execute_downsample(
        &mut self,
        _query: &crate::query::ast::DownsampleQuery,
    ) -> Result<QueryResult, QueryError> {
        // Stub implementation - see bin/server/query_router.rs for production use
        Ok(QueryResult::empty())
    }

    /// Execute a LATEST query - fetch most recent values
    ///
    /// NOTE: This stub returns empty results. For production query execution,
    /// use `QueryRouter::execute_latest()` in `bin/server/query_router.rs` which
    /// efficiently finds the most recent values for each series.
    fn execute_latest(
        &mut self,
        _query: &crate::query::ast::LatestQuery,
    ) -> Result<QueryResult, QueryError> {
        // Stub implementation - see bin/server/query_router.rs for production use
        Ok(QueryResult::empty())
    }

    /// Execute an EXPLAIN query - return query plan without executing
    ///
    /// Generates the query plan, formats it as human-readable output,
    /// and returns it as the query result. Includes cost estimates.
    fn execute_explain(
        &mut self,
        query: &crate::query::ast::Query,
    ) -> Result<QueryResult, QueryError> {
        use crate::query::planner::QueryPlanner;

        // Generate query plan
        let planner = QueryPlanner::new();
        let plan = planner.plan(query)?;

        // Format the plan as a string
        let plan_text = format!(
            "EXPLAIN\n\
             ========\n\n\
             Logical Plan:\n\
             {}\n\n\
             Cost Estimate:\n\
             - Estimated rows: {}\n\
             - Estimated bytes: {}\n\
             - Chunks to scan: {}\n",
            plan.logical_plan,
            plan.estimated_cost.estimated_rows,
            plan.estimated_cost.estimated_bytes,
            plan.estimated_cost.chunks_to_scan
        );

        // Return the plan as a special explain result
        Ok(QueryResult::explain(plan_text))
    }

    /// Get current execution statistics
    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Reset execution statistics
    pub fn reset_stats(&mut self) {
        self.stats = ExecutionStats::default();
    }

    /// Get executor configuration
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Execute an operator tree and collect results
    ///
    /// This is the core execution loop that pulls batches from operators
    /// and collects them into a QueryResult. It handles:
    /// - Memory tracking and limits
    /// - Timeout checking
    /// - Row limit enforcement
    /// - Statistics collection
    ///
    /// # Arguments
    ///
    /// * `operator` - Root of the operator tree to execute
    /// * `ctx` - Execution context with limits and state
    ///
    /// # Returns
    ///
    /// * `Ok(QueryResult)` - Results with metadata
    /// * `Err(QueryError)` - If execution fails or limits exceeded
    pub fn execute_operator(
        &mut self,
        mut operator: Box<dyn Operator>,
        ctx: &mut ExecutionContext,
    ) -> Result<QueryResult, QueryError> {
        let mut rows = Vec::new();
        let mut rows_scanned = 0u64;
        let mut was_truncated = false;

        // Pull batches from operator tree until exhausted or limit reached
        while let Some(batch) = operator.next_batch(ctx)? {
            // Track statistics
            rows_scanned += batch.len() as u64;
            self.stats.rows_scanned += batch.len() as u64;

            // Convert batch to result rows
            for i in 0..batch.len() {
                // Check row limit
                if rows.len() >= self.config.max_result_rows {
                    was_truncated = true;
                    break;
                }

                rows.push(ResultRow {
                    timestamp: batch.timestamps[i],
                    value: batch.values[i],
                    series_id: batch.series_ids.as_ref().map(|ids| ids[i]),
                    tags: HashMap::new(),
                });
            }

            // Check if we've hit the row limit
            if rows.len() >= self.config.max_result_rows {
                was_truncated = true;
                break;
            }

            // Check for timeout
            if ctx.is_timed_out() {
                return Err(QueryError::timeout("Query execution timed out"));
            }

            // Check memory limit
            if ctx.is_memory_exceeded() {
                return Err(QueryError::resource_limit("Memory limit exceeded"));
            }
        }

        // Update statistics
        self.stats.rows_returned += rows.len() as u64;

        // Build result with metadata
        let mut result = QueryResult::new(rows).with_rows_scanned(rows_scanned);

        if was_truncated {
            result.metadata.truncated = true;
            result.metadata.add_warning(format!(
                "Results truncated to {} rows",
                self.config.max_result_rows
            ));
        }

        Ok(result)
    }

    /// Execute a batch operation with a custom processor function
    ///
    /// This is useful for cases where you don't want to collect all results
    /// but instead process each batch as it arrives (streaming processing).
    ///
    /// # Arguments
    ///
    /// * `operator` - Root of the operator tree
    /// * `ctx` - Execution context
    /// * `processor` - Function to process each batch
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Processing completed
    /// * `Err(QueryError)` - If processing fails
    pub fn execute_streaming<F>(
        &mut self,
        mut operator: Box<dyn Operator>,
        ctx: &mut ExecutionContext,
        mut processor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(DataBatch) -> Result<(), QueryError>,
    {
        while let Some(batch) = operator.next_batch(ctx)? {
            self.stats.rows_scanned += batch.len() as u64;

            processor(batch)?;

            // Check limits
            if ctx.is_timed_out() {
                return Err(QueryError::timeout("Query execution timed out"));
            }
            if ctx.is_memory_exceeded() {
                return Err(QueryError::resource_limit("Memory limit exceeded"));
            }
        }

        Ok(())
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Execution Statistics
// ============================================================================

/// Statistics collected during query execution for monitoring and optimization
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Total number of queries executed
    pub total_queries: u64,

    /// Total execution time across all queries
    pub total_execution_time: Duration,

    /// Total rows scanned
    pub rows_scanned: u64,

    /// Total rows returned
    pub rows_returned: u64,

    /// Total bytes scanned from storage
    pub bytes_scanned: u64,

    /// Number of chunks read
    pub chunks_read: u64,

    /// Number of chunks skipped via zone maps
    pub chunks_pruned: u64,

    /// Cache hit count
    pub cache_hits: u64,

    /// Cache miss count
    pub cache_misses: u64,
}

impl ExecutionStats {
    /// Calculate cache hit ratio (0.0 to 1.0)
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Calculate average query latency
    pub fn avg_query_latency(&self) -> Duration {
        if self.total_queries == 0 {
            Duration::ZERO
        } else {
            // EDGE-005: Cap at u32::MAX to prevent truncation after 4 billion queries
            self.total_execution_time / self.total_queries.min(u32::MAX as u64) as u32
        }
    }

    /// Calculate selectivity (rows returned / rows scanned)
    pub fn selectivity(&self) -> f64 {
        if self.rows_scanned == 0 {
            1.0
        } else {
            self.rows_returned as f64 / self.rows_scanned as f64
        }
    }

    /// Calculate chunk pruning efficiency
    pub fn pruning_efficiency(&self) -> f64 {
        let total = self.chunks_read + self.chunks_pruned;
        if total == 0 {
            0.0
        } else {
            self.chunks_pruned as f64 / total as f64
        }
    }
}

// ============================================================================
// Execution Context
// ============================================================================

/// Context passed through the execution pipeline
///
/// Contains shared state and resources needed by operators during execution
#[derive(Debug)]
pub struct ExecutionContext {
    /// Query start time for timeout checking
    pub start_time: Instant,

    /// Timeout duration
    pub timeout: Duration,

    /// Current memory usage in bytes
    pub memory_used: usize,

    /// Memory limit in bytes
    pub memory_limit: usize,

    /// Rows produced so far
    pub rows_produced: usize,

    /// Maximum rows allowed
    pub max_rows: usize,

    /// Whether execution has been cancelled
    pub cancelled: bool,
}

impl ExecutionContext {
    /// Create a new execution context from config
    pub fn new(config: &ExecutorConfig) -> Self {
        Self {
            start_time: Instant::now(),
            timeout: config.timeout,
            memory_used: 0,
            memory_limit: config.memory_limit,
            rows_produced: 0,
            max_rows: config.max_result_rows,
            cancelled: false,
        }
    }

    /// Check if query has timed out
    pub fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    /// Check if memory limit exceeded
    pub fn is_memory_exceeded(&self) -> bool {
        self.memory_used > self.memory_limit
    }

    /// Check if row limit exceeded
    pub fn is_row_limit_exceeded(&self) -> bool {
        self.rows_produced >= self.max_rows
    }

    /// Check if execution should stop (timeout, cancelled, or limits exceeded)
    pub fn should_stop(&self) -> bool {
        self.cancelled || self.is_timed_out() || self.is_memory_exceeded()
    }

    /// Allocate memory from the budget
    ///
    /// Returns true if allocation succeeded, false if would exceed limit
    pub fn allocate_memory(&mut self, bytes: usize) -> bool {
        if self.memory_used + bytes > self.memory_limit {
            false
        } else {
            self.memory_used += bytes;
            true
        }
    }

    /// Release memory back to the budget
    pub fn release_memory(&mut self, bytes: usize) {
        self.memory_used = self.memory_used.saturating_sub(bytes);
    }

    /// Record rows produced
    pub fn record_rows(&mut self, count: usize) {
        self.rows_produced += count;
    }

    /// Cancel execution
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Get remaining time before timeout
    pub fn remaining_time(&self) -> Duration {
        self.timeout.saturating_sub(self.start_time.elapsed())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_config_defaults() {
        let config = ExecutorConfig::default();
        assert!(config.max_parallelism > 0);
        assert_eq!(config.batch_size, 4096);
        assert_eq!(config.memory_limit, 256 * 1024 * 1024);
        assert!(config.enable_simd);
        assert!(config.enable_parallel);
    }

    #[test]
    fn test_executor_config_builder() {
        let config = ExecutorConfig::new()
            .with_parallelism(4)
            .with_batch_size(1000) // Will be rounded to 1024
            .with_timeout(Duration::from_secs(60))
            .without_caching();

        assert_eq!(config.max_parallelism, 4);
        assert_eq!(config.batch_size, 1024); // Rounded to power of 2
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert!(!config.enable_caching);
    }

    #[test]
    fn test_execution_stats() {
        let stats = ExecutionStats {
            cache_hits: 80,
            cache_misses: 20,
            rows_scanned: 1000,
            rows_returned: 100,
            chunks_read: 5,
            chunks_pruned: 15,
            ..Default::default()
        };

        assert!((stats.cache_hit_ratio() - 0.8).abs() < 0.001);
        assert!((stats.selectivity() - 0.1).abs() < 0.001);
        assert!((stats.pruning_efficiency() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_execution_context() {
        let config = ExecutorConfig::new().with_memory_limit(1000);
        let mut ctx = ExecutionContext::new(&config);

        // Test memory allocation
        assert!(ctx.allocate_memory(500));
        assert_eq!(ctx.memory_used, 500);
        assert!(ctx.allocate_memory(400));
        assert_eq!(ctx.memory_used, 900);
        assert!(!ctx.allocate_memory(200)); // Would exceed limit
        assert_eq!(ctx.memory_used, 900);

        // Test memory release
        ctx.release_memory(300);
        assert_eq!(ctx.memory_used, 600);
    }

    #[test]
    fn test_execution_context_cancellation() {
        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        assert!(!ctx.should_stop());
        ctx.cancel();
        assert!(ctx.should_stop());
    }

    #[test]
    fn test_executor_creation() {
        let executor = QueryExecutor::new();
        assert_eq!(executor.stats().total_queries, 0);
    }
}
