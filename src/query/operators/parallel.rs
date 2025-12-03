//! Parallel Execution Operators
//!
//! This module provides parallel execution primitives using rayon:
//! - ParallelScanner: Reads multiple chunks in parallel
//! - ParallelAggregator: Aggregates data using morsel-driven parallelism
//!
//! # Morsel-Driven Parallelism
//!
//! Work is divided into "morsels" (small batches of ~1000-4000 rows) that
//! are distributed across worker threads. This provides:
//! - Better cache locality than full partitioning
//! - Dynamic load balancing across cores
//! - Minimal synchronization overhead
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::query::operators::ParallelConfig;
//!
//! // Use default configuration for parallel execution
//! let config = ParallelConfig::default();
//!
//! assert!(config.num_workers > 0);
//! assert!(config.morsel_size > 0);
//! ```

use crate::query::ast::{AggregationFunction, Predicate, SeriesSelector};
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::aggregation::AggregationState;
use crate::query::operators::{DataBatch, Operator};
use crate::storage::chunk::ChunkMetadata;
use crate::storage::{ChunkReader, LocalDiskEngine, QueryOptions};
use crate::types::{SeriesId, TimeRange};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

// ============================================================================
// Parallel Configuration
// ============================================================================

/// Configuration for parallel execution
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of worker threads (default: num_cpus)
    pub num_workers: usize,

    /// Size of each morsel (rows per work unit)
    pub morsel_size: usize,

    /// Minimum rows to enable parallelism (below this, run sequential)
    pub parallel_threshold: usize,

    /// Maximum chunks to read in parallel
    pub max_parallel_chunks: usize,

    /// Enable parallel chunk reading
    pub enable_parallel_io: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            morsel_size: 4096,
            parallel_threshold: 10_000,
            max_parallel_chunks: 4,
            enable_parallel_io: true,
        }
    }
}

impl ParallelConfig {
    /// Create config with custom worker count
    pub fn with_workers(mut self, n: usize) -> Self {
        self.num_workers = n.max(1);
        self
    }

    /// Set morsel size (rows per parallel work unit)
    pub fn with_morsel_size(mut self, size: usize) -> Self {
        self.morsel_size = size.max(256);
        self
    }

    /// Set threshold below which to run sequentially
    pub fn with_threshold(mut self, rows: usize) -> Self {
        self.parallel_threshold = rows;
        self
    }

    /// Disable parallel I/O
    pub fn without_parallel_io(mut self) -> Self {
        self.enable_parallel_io = false;
        self
    }
}

// ============================================================================
// Parallel Scanner
// ============================================================================

/// Parallel chunk scanner that reads multiple chunks concurrently
///
/// Uses rayon's thread pool to read and decompress chunks in parallel,
/// then outputs batches serially to maintain ordering.
pub struct ParallelScanner {
    /// Storage engine reference (reserved for future storage-aware optimization)
    #[allow(dead_code)]
    storage: Arc<LocalDiskEngine>,

    /// Chunks to scan (ordered by time)
    chunks: Vec<ChunkMetadata>,

    /// Parallel configuration
    config: ParallelConfig,

    /// Time range filter
    time_range: Option<TimeRange>,

    /// Predicate for value filtering
    predicate: Option<Predicate>,

    /// Series selector (reserved for series-aware optimization)
    #[allow(dead_code)]
    selector: SeriesSelector,

    /// Batch size for output
    batch_size: usize,

    /// Current chunk window start index
    window_start: usize,

    /// Prefetched batches from parallel chunk reads
    prefetched: Vec<DataBatch>,

    /// Current position in prefetched batches
    prefetch_pos: usize,

    /// Whether scan is exhausted
    exhausted: bool,

    /// Cancellation flag (shared with context)
    cancelled: Arc<AtomicBool>,

    /// Total rows read
    rows_read: Arc<AtomicUsize>,
}

impl ParallelScanner {
    /// Create a new parallel scanner
    pub fn new(
        storage: Arc<LocalDiskEngine>,
        selector: SeriesSelector,
        chunks: Vec<ChunkMetadata>,
        config: ParallelConfig,
    ) -> Self {
        Self {
            storage,
            chunks,
            config,
            time_range: None,
            predicate: None,
            selector,
            batch_size: 4096,
            window_start: 0,
            prefetched: Vec::new(),
            prefetch_pos: 0,
            exhausted: false,
            cancelled: Arc::new(AtomicBool::new(false)),
            rows_read: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Set time range filter
    pub fn with_time_range(mut self, range: TimeRange) -> Self {
        self.time_range = Some(range);
        self
    }

    /// Set predicate filter
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set output batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Prefetch the next window of chunks in parallel
    ///
    /// Reads up to max_parallel_chunks chunks concurrently using rayon,
    /// decompresses them, applies filters, and stores results.
    fn prefetch_next_window(&mut self) -> Result<(), QueryError> {
        if self.window_start >= self.chunks.len() {
            self.exhausted = true;
            return Ok(());
        }

        // Determine window of chunks to read
        let window_end =
            (self.window_start + self.config.max_parallel_chunks).min(self.chunks.len());
        let chunk_window = &self.chunks[self.window_start..window_end];

        // Build query options
        let options = QueryOptions {
            start_time: self.time_range.as_ref().map(|r| r.start),
            end_time: self.time_range.as_ref().map(|r| r.end),
            limit: None,
            use_mmap: true,
        };

        // Read chunks in parallel using rayon
        let cancelled = self.cancelled.clone();
        let predicate = self.predicate.clone();
        let batch_size = self.batch_size;
        let rows_read = self.rows_read.clone();

        // Parallel chunk reading
        let results: Vec<Result<Vec<DataBatch>, QueryError>> = if self.config.enable_parallel_io {
            chunk_window
                .par_iter()
                .map(|chunk| {
                    // Check cancellation
                    if cancelled.load(Ordering::Relaxed) {
                        return Ok(Vec::new());
                    }

                    // Read chunk (blocking - rayon handles threading)
                    let reader = ChunkReader::new();
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| QueryError::execution(format!("Runtime error: {}", e)))?;

                    let points = rt
                        .block_on(reader.read_chunk(&chunk.path, options.clone()))
                        .map_err(|e| QueryError::execution(format!("Chunk read error: {}", e)))?;

                    // Apply predicate filter
                    let filtered = if let Some(ref pred) = predicate {
                        points
                            .into_iter()
                            .filter(|p| pred.evaluate(p.value))
                            .collect()
                    } else {
                        points
                    };

                    rows_read.fetch_add(filtered.len(), Ordering::Relaxed);

                    // Form batches
                    let mut batches = Vec::new();
                    let mut current_batch = DataBatch::with_capacity(batch_size);

                    for point in filtered {
                        current_batch.push_with_series(
                            point.timestamp,
                            point.value,
                            point.series_id,
                        );

                        if current_batch.len() >= batch_size {
                            batches.push(std::mem::replace(
                                &mut current_batch,
                                DataBatch::with_capacity(batch_size),
                            ));
                        }
                    }

                    if !current_batch.is_empty() {
                        batches.push(current_batch);
                    }

                    Ok(batches)
                })
                .collect()
        } else {
            // Sequential fallback
            chunk_window
                .iter()
                .map(|chunk| {
                    let reader = ChunkReader::new();
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| QueryError::execution(format!("Runtime error: {}", e)))?;

                    let points = rt
                        .block_on(reader.read_chunk(&chunk.path, options.clone()))
                        .map_err(|e| QueryError::execution(format!("Chunk read error: {}", e)))?;

                    let filtered = if let Some(ref pred) = predicate {
                        points
                            .into_iter()
                            .filter(|p| pred.evaluate(p.value))
                            .collect()
                    } else {
                        points
                    };

                    rows_read.fetch_add(filtered.len(), Ordering::Relaxed);

                    let mut batches = Vec::new();
                    let mut current_batch = DataBatch::with_capacity(batch_size);

                    for point in filtered {
                        current_batch.push_with_series(
                            point.timestamp,
                            point.value,
                            point.series_id,
                        );
                        if current_batch.len() >= batch_size {
                            batches.push(std::mem::replace(
                                &mut current_batch,
                                DataBatch::with_capacity(batch_size),
                            ));
                        }
                    }

                    if !current_batch.is_empty() {
                        batches.push(current_batch);
                    }

                    Ok(batches)
                })
                .collect()
        };

        // Collect results (maintain chunk order for time ordering)
        self.prefetched.clear();
        for result in results {
            let batches = result?;
            self.prefetched.extend(batches);
        }

        self.prefetch_pos = 0;
        self.window_start = window_end;

        if self.prefetched.is_empty() && self.window_start >= self.chunks.len() {
            self.exhausted = true;
        }

        Ok(())
    }
}

impl Operator for ParallelScanner {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // Check cancellation
        if ctx.should_stop() {
            self.cancelled.store(true, Ordering::Relaxed);
            if ctx.is_timed_out() {
                return Err(QueryError::timeout("Parallel scan timed out"));
            }
            return Ok(None);
        }

        // Return from prefetched if available
        if self.prefetch_pos < self.prefetched.len() {
            let batch = std::mem::take(&mut self.prefetched[self.prefetch_pos]);
            self.prefetch_pos += 1;

            let mem_size = batch.memory_size();
            if !ctx.allocate_memory(mem_size) {
                return Err(QueryError::resource_limit(
                    "Memory limit exceeded during parallel scan",
                ));
            }
            ctx.record_rows(batch.len());

            return Ok(Some(batch));
        }

        // Need to prefetch more
        if self.exhausted {
            return Ok(None);
        }

        self.prefetch_next_window()?;

        // Try again after prefetch
        if self.prefetch_pos < self.prefetched.len() {
            let batch = std::mem::take(&mut self.prefetched[self.prefetch_pos]);
            self.prefetch_pos += 1;

            let mem_size = batch.memory_size();
            if !ctx.allocate_memory(mem_size) {
                return Err(QueryError::resource_limit(
                    "Memory limit exceeded during parallel scan",
                ));
            }
            ctx.record_rows(batch.len());

            return Ok(Some(batch));
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.window_start = 0;
        self.prefetched.clear();
        self.prefetch_pos = 0;
        self.exhausted = false;
        self.cancelled.store(false, Ordering::Relaxed);
        self.rows_read.store(0, Ordering::Relaxed);
    }

    fn name(&self) -> &'static str {
        "ParallelScan"
    }

    fn estimated_cardinality(&self) -> usize {
        self.chunks.iter().map(|c| c.point_count as usize).sum()
    }
}

// ============================================================================
// Parallel Aggregator
// ============================================================================

/// Parallel aggregation using morsel-driven parallelism
///
/// Splits input into morsels and processes them in parallel using rayon.
/// Each morsel computes partial aggregates which are then merged.
pub struct ParallelAggregator {
    /// Input operator
    input: Box<dyn Operator>,

    /// Aggregation function
    function: AggregationFunction,

    /// Parallel configuration
    config: ParallelConfig,

    /// Group by series ID
    group_by_series: bool,

    /// Accumulated input batches (for parallel processing)
    input_buffer: Vec<DataBatch>,

    /// Total rows in buffer
    buffer_rows: usize,

    /// Whether aggregation is complete
    done: bool,

    /// Final result (produced once all input is consumed)
    result: Option<DataBatch>,
}

impl ParallelAggregator {
    /// Create a new parallel aggregator
    pub fn new(
        input: Box<dyn Operator>,
        function: AggregationFunction,
        config: ParallelConfig,
    ) -> Self {
        Self {
            input,
            function,
            config,
            group_by_series: false,
            input_buffer: Vec::new(),
            buffer_rows: 0,
            done: false,
            result: None,
        }
    }

    /// Enable grouping by series ID
    pub fn with_group_by_series(mut self) -> Self {
        self.group_by_series = true;
        self
    }

    /// Consume all input and compute parallel aggregation
    fn compute_aggregation(&mut self, ctx: &mut ExecutionContext) -> Result<(), QueryError> {
        // Collect all input
        while let Some(batch) = self.input.next_batch(ctx)? {
            self.buffer_rows += batch.len();
            self.input_buffer.push(batch);

            if ctx.should_stop() {
                return if ctx.is_timed_out() {
                    Err(QueryError::timeout("Parallel aggregation timed out"))
                } else {
                    Ok(())
                };
            }
        }

        // Decide parallel vs sequential based on data size
        if self.buffer_rows < self.config.parallel_threshold {
            self.result = Some(self.compute_sequential()?);
        } else {
            self.result = Some(self.compute_parallel()?);
        }

        self.done = true;
        Ok(())
    }

    /// Sequential aggregation (for small datasets)
    fn compute_sequential(&self) -> Result<DataBatch, QueryError> {
        if self.group_by_series {
            self.aggregate_by_series_sequential()
        } else {
            self.aggregate_global_sequential()
        }
    }

    /// Parallel aggregation (for large datasets)
    fn compute_parallel(&self) -> Result<DataBatch, QueryError> {
        if self.group_by_series {
            self.aggregate_by_series_parallel()
        } else {
            self.aggregate_global_parallel()
        }
    }

    /// Global aggregation (no grouping) - sequential
    fn aggregate_global_sequential(&self) -> Result<DataBatch, QueryError> {
        let mut state = self.create_initial_state();

        for batch in &self.input_buffer {
            self.update_state(&mut state, &batch.timestamps, &batch.values);
        }

        let value = self.finalize_state(&state);
        let timestamp = self
            .input_buffer
            .last()
            .and_then(|b| b.timestamps.last())
            .copied()
            .unwrap_or(0);

        Ok(DataBatch::new(vec![timestamp], vec![value]))
    }

    /// Global aggregation - parallel using rayon
    fn aggregate_global_parallel(&self) -> Result<DataBatch, QueryError> {
        // Flatten all batches into one for morsel splitting
        let total_len: usize = self.input_buffer.iter().map(|b| b.len()).sum();
        let mut all_timestamps = Vec::with_capacity(total_len);
        let mut all_values = Vec::with_capacity(total_len);

        for batch in &self.input_buffer {
            all_timestamps.extend(&batch.timestamps);
            all_values.extend(&batch.values);
        }

        // Split into morsels and process in parallel
        let morsel_size = self.config.morsel_size;
        let function = self.function;

        let partial_states: Vec<AggregationState> = all_values
            .par_chunks(morsel_size)
            .enumerate()
            .map(|(i, values)| {
                let start = i * morsel_size;
                let timestamps = &all_timestamps[start..start + values.len()];

                let mut state = Self::create_initial_state_static(function);
                Self::update_state_static(&mut state, timestamps, values, function);
                state
            })
            .collect();

        // Merge partial states
        let mut final_state = self.create_initial_state();
        for state in partial_states {
            self.merge_states(&mut final_state, &state);
        }

        let value = self.finalize_state(&final_state);
        let timestamp = all_timestamps.last().copied().unwrap_or(0);

        Ok(DataBatch::new(vec![timestamp], vec![value]))
    }

    /// Group-by aggregation - sequential
    fn aggregate_by_series_sequential(&self) -> Result<DataBatch, QueryError> {
        let mut states: HashMap<SeriesId, AggregationState> = HashMap::new();
        let mut last_ts: HashMap<SeriesId, i64> = HashMap::new();

        for batch in &self.input_buffer {
            if let Some(ref series_ids) = batch.series_ids {
                for (i, &sid) in series_ids.iter().enumerate().take(batch.len()) {
                    let ts = batch.timestamps[i];
                    let val = batch.values[i];

                    let state = states
                        .entry(sid)
                        .or_insert_with(|| self.create_initial_state());
                    self.update_state(state, &[ts], &[val]);
                    last_ts.insert(sid, ts);
                }
            }
        }

        // Build result batch
        let mut result = DataBatch::with_capacity(states.len());
        for (sid, state) in states {
            let value = self.finalize_state(&state);
            let ts = last_ts.get(&sid).copied().unwrap_or(0);
            result.push_with_series(ts, value, sid);
        }

        Ok(result)
    }

    /// Group-by aggregation - parallel
    fn aggregate_by_series_parallel(&self) -> Result<DataBatch, QueryError> {
        // Collect all data by series
        let mut series_data: HashMap<SeriesId, (Vec<i64>, Vec<f64>)> = HashMap::new();

        for batch in &self.input_buffer {
            if let Some(ref series_ids) = batch.series_ids {
                for (i, &sid) in series_ids.iter().enumerate().take(batch.len()) {
                    let (timestamps, values) = series_data.entry(sid).or_default();
                    timestamps.push(batch.timestamps[i]);
                    values.push(batch.values[i]);
                }
            }
        }

        // Process each series in parallel
        let function = self.function;
        let results: Vec<(SeriesId, i64, f64)> = series_data
            .into_par_iter()
            .map(|(sid, (timestamps, values))| {
                let mut state = Self::create_initial_state_static(function);
                Self::update_state_static(&mut state, &timestamps, &values, function);
                let value = Self::finalize_state_static(&state, function);
                let ts = timestamps.last().copied().unwrap_or(0);
                (sid, ts, value)
            })
            .collect();

        // Build result batch
        let mut result = DataBatch::with_capacity(results.len());
        for (sid, ts, value) in results {
            result.push_with_series(ts, value, sid);
        }

        Ok(result)
    }

    // ========================================================================
    // State Management Helpers
    // ========================================================================

    /// Create initial aggregation state for instance method
    fn create_initial_state(&self) -> AggregationState {
        AggregationState::new(&self.function)
    }

    /// Create initial aggregation state (static for parallel use)
    fn create_initial_state_static(function: AggregationFunction) -> AggregationState {
        AggregationState::new(&function)
    }

    /// Update aggregation state with new values
    fn update_state(&self, state: &mut AggregationState, timestamps: &[i64], values: &[f64]) {
        state.update_batch(timestamps, values);
    }

    /// Update aggregation state (static for parallel use)
    fn update_state_static(
        state: &mut AggregationState,
        timestamps: &[i64],
        values: &[f64],
        _function: AggregationFunction,
    ) {
        state.update_batch(timestamps, values);
    }

    /// Finalize state to produce result
    fn finalize_state(&self, state: &AggregationState) -> f64 {
        Self::finalize_state_static(state, self.function)
    }

    /// Finalize state (static for parallel use)
    fn finalize_state_static(state: &AggregationState, function: AggregationFunction) -> f64 {
        match state {
            AggregationState::Sum(kahan) => kahan.sum(),
            AggregationState::Stats(welford) => match function {
                AggregationFunction::Avg => welford.mean(),
                AggregationFunction::StdDev => welford.stddev_population(),
                AggregationFunction::Variance => welford.variance_population(),
                _ => welford.mean(),
            },
            AggregationState::Min(min) => min.unwrap_or(f64::NAN),
            AggregationState::Max(max) => max.unwrap_or(f64::NAN),
            AggregationState::Count(c) => *c as f64,
            AggregationState::First(opt) => opt.map(|(_, v)| v).unwrap_or(f64::NAN),
            AggregationState::Last(opt) => opt.map(|(_, v)| v).unwrap_or(f64::NAN),
            AggregationState::Rate { first, last } => match (first, last) {
                (Some((t1, v1)), Some((t2, v2))) if t2 > t1 => {
                    (v2 - v1) / ((t2 - t1) as f64 / 1_000_000_000.0)
                }
                _ => f64::NAN,
            },
            AggregationState::Distinct(set) => set.len() as f64,
            AggregationState::PercentileExact { target, values } => {
                if values.is_empty() {
                    return f64::NAN;
                }
                let mut sorted = values.clone();
                let idx = ((*target as f64 / 100.0) * (sorted.len() - 1) as f64).round() as usize;
                let idx = idx.min(sorted.len() - 1);
                // Use select_nth_unstable_by with partial_cmp for f64
                sorted.select_nth_unstable_by(idx, |a, b| {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                });
                sorted[idx]
            }
            AggregationState::PercentileTDigest { target, digest } => {
                if digest.is_empty() {
                    f64::NAN
                } else {
                    digest.estimate_quantile(*target as f64 / 100.0)
                }
            }
        }
    }

    /// Merge two aggregation states
    fn merge_states(&self, target: &mut AggregationState, source: &AggregationState) {
        match (target, source) {
            (AggregationState::Sum(a), AggregationState::Sum(b)) => a.merge(b),
            (AggregationState::Stats(a), AggregationState::Stats(b)) => a.merge(b),
            (AggregationState::Min(a), AggregationState::Min(b)) => match (*a, *b) {
                (Some(av), Some(bv)) => *a = Some(av.min(bv)),
                (None, Some(bv)) => *a = Some(bv),
                _ => {}
            },
            (AggregationState::Max(a), AggregationState::Max(b)) => match (*a, *b) {
                (Some(av), Some(bv)) => *a = Some(av.max(bv)),
                (None, Some(bv)) => *a = Some(bv),
                _ => {}
            },
            (AggregationState::Count(a), AggregationState::Count(b)) => *a += b,
            (AggregationState::First(a), AggregationState::First(b)) => match (*a, *b) {
                (Some((at, _)), Some((bt, bv))) if bt < at => *a = Some((bt, bv)),
                (None, Some(bv)) => *a = Some(bv),
                _ => {}
            },
            (AggregationState::Last(a), AggregationState::Last(b)) => match (*a, *b) {
                (Some((at, _)), Some((bt, bv))) if bt > at => *a = Some((bt, bv)),
                (None, Some(bv)) => *a = Some(bv),
                _ => {}
            },
            (
                AggregationState::Rate {
                    first: af,
                    last: al,
                },
                AggregationState::Rate {
                    first: bf,
                    last: bl,
                },
            ) => {
                match (*af, *bf) {
                    (Some((at, _)), Some((bt, bv))) if bt < at => *af = Some((bt, bv)),
                    (None, Some(bv)) => *af = Some(bv),
                    _ => {}
                }
                match (*al, *bl) {
                    (Some((at, _)), Some((bt, bv))) if bt > at => *al = Some((bt, bv)),
                    (None, Some(bv)) => *al = Some(bv),
                    _ => {}
                }
            }
            (AggregationState::Distinct(a), AggregationState::Distinct(b)) => {
                a.extend(b.iter());
            }
            (
                AggregationState::PercentileExact { values: a, .. },
                AggregationState::PercentileExact { values: b, .. },
            ) => {
                a.extend(b.iter());
            }
            (
                AggregationState::PercentileTDigest { digest: a, .. },
                AggregationState::PercentileTDigest { digest: b, .. },
            ) => {
                *a = tdigest::TDigest::merge_digests(vec![a.clone(), b.clone()]);
            }
            _ => {} // Mismatched types - shouldn't happen
        }
    }
}

impl Operator for ParallelAggregator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // Check if already done
        if self.done {
            return Ok(self.result.take());
        }

        // Consume all input and compute aggregation
        self.compute_aggregation(ctx)?;

        // Return result
        if let Some(ref result) = self.result {
            let mem_size = result.memory_size();
            if !ctx.allocate_memory(mem_size) {
                return Err(QueryError::resource_limit(
                    "Memory limit exceeded in parallel aggregation",
                ));
            }
            ctx.record_rows(result.len());
        }

        Ok(self.result.take())
    }

    fn reset(&mut self) {
        self.input.reset();
        self.input_buffer.clear();
        self.buffer_rows = 0;
        self.done = false;
        self.result = None;
    }

    fn name(&self) -> &'static str {
        "ParallelAggregator"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::scan::ScanOperator;

    fn make_test_data(n: usize) -> Vec<(i64, f64, SeriesId)> {
        (0..n).map(|i| (i as i64 * 1000, i as f64, 1)).collect()
    }

    // ===== ParallelConfig tests =====

    #[test]
    fn test_parallel_config_defaults() {
        let config = ParallelConfig::default();
        assert!(config.num_workers > 0);
        assert_eq!(config.morsel_size, 4096);
        assert_eq!(config.parallel_threshold, 10_000);
        assert_eq!(config.max_parallel_chunks, 4);
        assert!(config.enable_parallel_io);
    }

    #[test]
    fn test_parallel_config_with_workers() {
        let config = ParallelConfig::default().with_workers(4);
        assert_eq!(config.num_workers, 4);
    }

    #[test]
    fn test_parallel_config_with_workers_zero() {
        // Zero workers should be clamped to 1
        let config = ParallelConfig::default().with_workers(0);
        assert_eq!(config.num_workers, 1);
    }

    #[test]
    fn test_parallel_config_with_morsel_size() {
        let config = ParallelConfig::default().with_morsel_size(8192);
        assert_eq!(config.morsel_size, 8192);
    }

    #[test]
    fn test_parallel_config_with_morsel_size_min() {
        // Morsel size should be clamped to at least 256
        let config = ParallelConfig::default().with_morsel_size(100);
        assert_eq!(config.morsel_size, 256);
    }

    #[test]
    fn test_parallel_config_with_threshold() {
        let config = ParallelConfig::default().with_threshold(5000);
        assert_eq!(config.parallel_threshold, 5000);
    }

    #[test]
    fn test_parallel_config_with_threshold_zero() {
        let config = ParallelConfig::default().with_threshold(0);
        assert_eq!(config.parallel_threshold, 0);
    }

    #[test]
    fn test_parallel_config_without_parallel_io() {
        let config = ParallelConfig::default().without_parallel_io();
        assert!(!config.enable_parallel_io);
    }

    #[test]
    fn test_parallel_config_chaining() {
        let config = ParallelConfig::default()
            .with_workers(8)
            .with_morsel_size(2048)
            .with_threshold(20000)
            .without_parallel_io();

        assert_eq!(config.num_workers, 8);
        assert_eq!(config.morsel_size, 2048);
        assert_eq!(config.parallel_threshold, 20000);
        assert!(!config.enable_parallel_io);
    }

    #[test]
    fn test_parallel_config_clone() {
        let config = ParallelConfig::default().with_workers(4);
        let cloned = config.clone();
        assert_eq!(cloned.num_workers, 4);
        assert_eq!(cloned.morsel_size, config.morsel_size);
    }

    #[test]
    fn test_parallel_config_debug() {
        let config = ParallelConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ParallelConfig"));
        assert!(debug_str.contains("num_workers"));
        assert!(debug_str.contains("morsel_size"));
    }

    // ===== ParallelAggregator tests =====

    #[test]
    fn test_parallel_aggregator_sum_small() {
        // Small dataset - should use sequential path
        let data = make_test_data(100);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(1000);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 1);
        // Sum of 0..99 = 4950
        assert!((result.values[0] - 4950.0).abs() < 0.001);
    }

    #[test]
    fn test_parallel_aggregator_sum_large() {
        // Large dataset - should use parallel path
        let data = make_test_data(20000);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(4096);

        let config = ParallelConfig::default().with_threshold(10000);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 1);
        // Sum of 0..19999 = 199990000
        let expected: f64 = (0..20000).map(|i| i as f64).sum();
        assert!((result.values[0] - expected).abs() < 0.001);
    }

    #[test]
    fn test_parallel_aggregator_avg() {
        let data = make_test_data(1000);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(500);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Avg, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        // Avg of 0..999 = 499.5
        assert!((result.values[0] - 499.5).abs() < 0.001);
    }

    #[test]
    fn test_parallel_aggregator_min_max() {
        let data = make_test_data(5000);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data.clone())
            .with_batch_size(500);

        let config = ParallelConfig::default().with_threshold(1000);

        // Test Min
        let mut min_agg =
            ParallelAggregator::new(Box::new(scan), AggregationFunction::Min, config.clone());

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = min_agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.values[0], 0.0);

        // Test Max
        let scan2 = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(500);

        let mut max_agg =
            ParallelAggregator::new(Box::new(scan2), AggregationFunction::Max, config);

        let mut ctx2 = ExecutionContext::new(&exec_config);
        let result = max_agg.next_batch(&mut ctx2).unwrap().unwrap();
        assert_eq!(result.values[0], 4999.0);
    }

    #[test]
    fn test_parallel_aggregator_group_by_series() {
        // Data with multiple series
        let mut data = Vec::new();
        for i in 0..1000 {
            let series_id = (i % 3) as SeriesId + 1; // 3 series: 1, 2, 3
            data.push((i as i64 * 1000, i as f64, series_id));
        }

        // Use a selector that doesn't filter by specific series ID
        let selector = SeriesSelector {
            series_id: None, // Don't filter by series ID
            measurement: Some("test".to_string()),
            tag_filters: vec![],
        };

        let scan = ScanOperator::new(selector, None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(500);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Count, config)
            .with_group_by_series();

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 3); // 3 groups

        // Each series should have ~333-334 values
        let total: f64 = result.values.iter().sum();
        assert_eq!(total, 1000.0);
    }

    #[test]
    fn test_parallel_aggregator_count() {
        let data = make_test_data(500);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(100);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Count, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.values[0], 500.0);
    }

    #[test]
    fn test_parallel_aggregator_stddev() {
        // Data: 0, 1, 2, 3, 4 - population stddev is sqrt(2)
        let data: Vec<(i64, f64, SeriesId)> =
            (0..5).map(|i| (i as i64 * 1000, i as f64, 1)).collect();

        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(100);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::StdDev, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        // Population stddev of [0,1,2,3,4] = sqrt(2) ≈ 1.414
        assert!((result.values[0] - 1.414).abs() < 0.01);
    }

    #[test]
    fn test_parallel_aggregator_variance() {
        let data: Vec<(i64, f64, SeriesId)> =
            (0..5).map(|i| (i as i64 * 1000, i as f64, 1)).collect();

        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default().with_threshold(100);
        let mut agg =
            ParallelAggregator::new(Box::new(scan), AggregationFunction::Variance, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        // Population variance of [0,1,2,3,4] = 2.0
        assert!((result.values[0] - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_parallel_aggregator_empty_input() {
        let data: Vec<(i64, f64, SeriesId)> = vec![];
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default();
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap();
        // Empty input should return something (timestamp 0)
        assert!(result.is_some());
    }

    #[test]
    fn test_parallel_aggregator_reset() {
        let data = make_test_data(100);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default();
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        // First run
        let _ = agg.next_batch(&mut ctx).unwrap();

        // Reset
        agg.reset();

        // Should be able to run again
        let result = agg.next_batch(&mut ctx).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_parallel_aggregator_name() {
        let data = make_test_data(10);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default();
        let agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        assert_eq!(agg.name(), "ParallelAggregator");
    }

    #[test]
    fn test_parallel_aggregator_second_call_returns_none() {
        let data = make_test_data(100);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100);

        let config = ParallelConfig::default();
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        // First call returns result
        let result1 = agg.next_batch(&mut ctx).unwrap();
        assert!(result1.is_some());

        // Second call returns None
        let result2 = agg.next_batch(&mut ctx).unwrap();
        assert!(result2.is_none());
    }

    // ===== Parallel aggregation with different morsel sizes =====

    #[test]
    fn test_parallel_aggregator_small_morsel() {
        let data = make_test_data(15000);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(1000);

        let config = ParallelConfig::default()
            .with_threshold(5000)
            .with_morsel_size(256);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        let expected: f64 = (0..15000).map(|i| i as f64).sum();
        assert!((result.values[0] - expected).abs() < 0.001);
    }

    #[test]
    fn test_parallel_aggregator_large_morsel() {
        let data = make_test_data(15000);
        let scan = ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(1000);

        let config = ParallelConfig::default()
            .with_threshold(5000)
            .with_morsel_size(8192);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config);

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        let expected: f64 = (0..15000).map(|i| i as f64).sum();
        assert!((result.values[0] - expected).abs() < 0.001);
    }

    // ===== Group by series with parallel path =====

    #[test]
    fn test_parallel_aggregator_group_by_series_large() {
        // Large dataset with multiple series - should use parallel path
        let mut data = Vec::new();
        for i in 0..15000 {
            let series_id = (i % 5) as SeriesId + 1; // 5 series
            data.push((i as i64 * 1000, i as f64, series_id));
        }

        let selector = SeriesSelector {
            series_id: None,
            measurement: Some("test".to_string()),
            tag_filters: vec![],
        };

        let scan = ScanOperator::new(selector, None)
            .with_test_data(data)
            .with_batch_size(1000);

        let config = ParallelConfig::default().with_threshold(5000);
        let mut agg = ParallelAggregator::new(Box::new(scan), AggregationFunction::Sum, config)
            .with_group_by_series();

        let exec_config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&exec_config);

        let result = agg.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.len(), 5); // 5 groups

        // Total sum should match
        let total: f64 = result.values.iter().sum();
        let expected: f64 = (0..15000).map(|i| i as f64).sum();
        assert!((total - expected).abs() < 0.001);
    }

    // ===== ParallelScanner tests =====

    #[test]
    fn test_parallel_scanner_name() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalDiskEngine::new(dir.path().to_path_buf()).unwrap());

        let scanner = ParallelScanner::new(
            storage,
            SeriesSelector::by_id(1),
            vec![],
            ParallelConfig::default(),
        );

        assert_eq!(scanner.name(), "ParallelScan");
    }

    #[test]
    fn test_parallel_scanner_estimated_cardinality_empty() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalDiskEngine::new(dir.path().to_path_buf()).unwrap());

        let scanner = ParallelScanner::new(
            storage,
            SeriesSelector::by_id(1),
            vec![],
            ParallelConfig::default(),
        );

        assert_eq!(scanner.estimated_cardinality(), 0);
    }

    #[test]
    fn test_parallel_scanner_reset() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalDiskEngine::new(dir.path().to_path_buf()).unwrap());

        let mut scanner = ParallelScanner::new(
            storage,
            SeriesSelector::by_id(1),
            vec![],
            ParallelConfig::default(),
        );

        scanner.reset();

        // After reset, internal state should be cleared
        assert_eq!(scanner.window_start, 0);
        assert!(scanner.prefetched.is_empty());
        assert_eq!(scanner.prefetch_pos, 0);
        assert!(!scanner.exhausted);
    }

    #[test]
    fn test_parallel_scanner_with_time_range() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalDiskEngine::new(dir.path().to_path_buf()).unwrap());

        let scanner = ParallelScanner::new(
            storage,
            SeriesSelector::by_id(1),
            vec![],
            ParallelConfig::default(),
        )
        .with_time_range(TimeRange {
            start: 0,
            end: 1000,
        });

        assert!(scanner.time_range.is_some());
        assert_eq!(scanner.time_range.unwrap().start, 0);
        assert_eq!(scanner.time_range.unwrap().end, 1000);
    }

    #[test]
    fn test_parallel_scanner_with_batch_size() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalDiskEngine::new(dir.path().to_path_buf()).unwrap());

        let scanner = ParallelScanner::new(
            storage,
            SeriesSelector::by_id(1),
            vec![],
            ParallelConfig::default(),
        )
        .with_batch_size(8192);

        assert_eq!(scanner.batch_size, 8192);
    }
}
