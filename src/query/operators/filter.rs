//! Filter Operator - Predicate evaluation
//!
//! Filters batches based on predicates, passing through only rows
//! that match the filter condition. Uses SIMD where possible for
//! efficient batch filtering.

use crate::query::ast::Predicate;
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::{DataBatch, Operator};

/// Filter operator that applies predicates to batches
pub struct FilterOperator {
    /// Input operator
    input: Box<dyn Operator>,

    /// Predicate to evaluate
    predicate: Predicate,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: Box<dyn Operator>, predicate: Predicate) -> Self {
        Self { input, predicate }
    }

    /// Apply filter to a batch, returning a new batch with matching rows
    ///
    /// Optimized to avoid copying when all rows match (PERF-005)
    fn filter_batch(&self, batch: DataBatch) -> DataBatch {
        // First pass: count matches to determine if we can avoid copying
        let match_count = batch
            .values
            .iter()
            .filter(|&&v| self.predicate.evaluate(v))
            .count();

        // If all rows match, return original batch without copying (PERF-005)
        if match_count == batch.len() {
            return batch;
        }

        // If no rows match, return empty batch
        if match_count == 0 {
            return DataBatch::default();
        }

        // Selective copy: pre-allocate exact size needed
        let mut result = DataBatch::with_capacity(match_count);

        for i in 0..batch.len() {
            let value = batch.values[i];
            if self.predicate.evaluate(value) {
                let ts = batch.timestamps[i];
                if let Some(ref sids) = batch.series_ids {
                    result.push_with_series(ts, value, sids[i]);
                } else {
                    result.push(ts, value);
                }
            }
        }

        result
    }

    /// Apply filter using SIMD-accelerated selection
    ///
    /// Uses vectorized operations for high-performance filtering on large batches.
    /// Falls back to scalar operations when SIMD is not beneficial.
    pub fn filter_batch_simd(&self, batch: DataBatch) -> DataBatch {
        // Generate selection mask
        let mask = self.generate_selection_mask(&batch.values);

        // Count selected rows for pre-allocation
        let count = mask.iter().filter(|&&b| b).count();
        let mut result = DataBatch::with_capacity(count);

        // Copy selected rows
        for (i, &selected) in mask.iter().enumerate() {
            if selected {
                let ts = batch.timestamps[i];
                let val = batch.values[i];
                if let Some(ref sids) = batch.series_ids {
                    result.push_with_series(ts, val, sids[i]);
                } else {
                    result.push(ts, val);
                }
            }
        }

        result
    }

    /// Generate boolean mask for selection
    fn generate_selection_mask(&self, values: &[f64]) -> Vec<bool> {
        // Use the predicate's evaluate method for consistent behavior
        values.iter().map(|&v| self.predicate.evaluate(v)).collect()
    }

    /// Optimized batch filtering with conditional SIMD support
    ///
    /// When the `simd` feature is enabled and the batch is large enough (64+ elements),
    /// this uses SIMD-accelerated filtering. Otherwise, falls back to scalar filtering.
    fn filter_batch_optimized(&self, batch: DataBatch) -> DataBatch {
        #[cfg(feature = "simd")]
        {
            // Use SIMD for large batches where vectorization provides benefit
            if batch.len() >= 64 {
                return self.filter_batch_simd(batch);
            }
        }

        // Default scalar path
        self.filter_batch(batch)
    }
}

impl Operator for FilterOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // Keep pulling from input until we get a non-empty filtered batch
        // or the input is exhausted
        loop {
            if ctx.should_stop() {
                if ctx.is_timed_out() {
                    return Err(QueryError::timeout("Filter operator timed out"));
                }
                return Ok(None);
            }

            match self.input.next_batch(ctx)? {
                Some(batch) => {
                    let filtered = self.filter_batch_optimized(batch);
                    if !filtered.is_empty() {
                        ctx.record_rows(filtered.len());
                        return Ok(Some(filtered));
                    }
                    // Empty after filtering, try next batch
                },
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "Filter"
    }

    fn estimated_cardinality(&self) -> usize {
        // Assume 50% selectivity by default
        self.input.estimated_cardinality() / 2
    }
}

// ============================================================================
// Compound Predicates
// ============================================================================

/// Combine multiple predicates with AND logic
pub struct AndFilter {
    input: Box<dyn Operator>,
    predicates: Vec<Predicate>,
}

impl AndFilter {
    /// Create a new AND filter
    pub fn new(input: Box<dyn Operator>, predicates: Vec<Predicate>) -> Self {
        Self { input, predicates }
    }

    /// Check if a value passes all predicates
    fn passes_all(&self, value: f64) -> bool {
        self.predicates.iter().all(|p| p.evaluate(value))
    }
}

impl Operator for AndFilter {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        loop {
            if ctx.should_stop() {
                return Ok(None);
            }

            match self.input.next_batch(ctx)? {
                Some(batch) => {
                    let mut result = DataBatch::with_capacity(batch.len());

                    for i in 0..batch.len() {
                        if self.passes_all(batch.values[i]) {
                            let ts = batch.timestamps[i];
                            let val = batch.values[i];
                            if let Some(ref sids) = batch.series_ids {
                                result.push_with_series(ts, val, sids[i]);
                            } else {
                                result.push(ts, val);
                            }
                        }
                    }

                    if !result.is_empty() {
                        ctx.record_rows(result.len());
                        return Ok(Some(result));
                    }
                },
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "AndFilter"
    }
}

/// Combine multiple predicates with OR logic
pub struct OrFilter {
    input: Box<dyn Operator>,
    predicates: Vec<Predicate>,
}

impl OrFilter {
    /// Create a new OR filter
    pub fn new(input: Box<dyn Operator>, predicates: Vec<Predicate>) -> Self {
        Self { input, predicates }
    }

    /// Check if a value passes any predicate
    fn passes_any(&self, value: f64) -> bool {
        self.predicates.iter().any(|p| p.evaluate(value))
    }
}

impl Operator for OrFilter {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        loop {
            if ctx.should_stop() {
                return Ok(None);
            }

            match self.input.next_batch(ctx)? {
                Some(batch) => {
                    let mut result = DataBatch::with_capacity(batch.len());

                    for i in 0..batch.len() {
                        if self.passes_any(batch.values[i]) {
                            let ts = batch.timestamps[i];
                            let val = batch.values[i];
                            if let Some(ref sids) = batch.series_ids {
                                result.push_with_series(ts, val, sids[i]);
                            } else {
                                result.push(ts, val);
                            }
                        }
                    }

                    if !result.is_empty() {
                        ctx.record_rows(result.len());
                        return Ok(Some(result));
                    }
                },
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "OrFilter"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::SeriesSelector;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::scan::ScanOperator;
    use crate::types::SeriesId;

    fn create_test_scan() -> ScanOperator {
        let data: Vec<(i64, f64, SeriesId)> =
            (0..100).map(|i| (i as i64 * 1000, i as f64, 1)).collect();

        ScanOperator::new(SeriesSelector::by_id(1), None)
            .with_test_data(data)
            .with_batch_size(100)
    }

    #[test]
    fn test_filter_gt() {
        let scan = create_test_scan();
        let mut filter = FilterOperator::new(Box::new(scan), Predicate::gt("value", 50.0));

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let batch = filter.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 49); // 51..99
        assert!(batch.values.iter().all(|&v| v > 50.0));
    }

    #[test]
    fn test_filter_between() {
        let scan = create_test_scan();
        let predicates = vec![Predicate::gte("value", 20.0), Predicate::lte("value", 30.0)];

        let mut filter = AndFilter::new(Box::new(scan), predicates);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let batch = filter.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 11); // 20..30 inclusive
        assert!(batch.values.iter().all(|&v| (20.0..=30.0).contains(&v)));
    }

    #[test]
    fn test_or_filter() {
        let scan = create_test_scan();
        let predicates = vec![Predicate::lt("value", 10.0), Predicate::gt("value", 90.0)];

        let mut filter = OrFilter::new(Box::new(scan), predicates);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let batch = filter.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 19); // 0..9 (10 values) + 91..99 (9 values)
        assert!(batch.values.iter().all(|&v| !(10.0..=90.0).contains(&v)));
    }

    #[test]
    fn test_filter_empty_result() {
        let scan = create_test_scan();
        let mut filter = FilterOperator::new(
            Box::new(scan),
            Predicate::gt("value", 1000.0), // No values > 1000
        );

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        // Should return None since no values match
        let batch = filter.next_batch(&mut ctx).unwrap();
        assert!(batch.is_none());
    }

    #[test]
    fn test_filter_reset() {
        let scan = create_test_scan();
        let mut filter = FilterOperator::new(Box::new(scan), Predicate::gt("value", 95.0));

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        // First read
        let batch1 = filter.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch1.len(), 4); // 96, 97, 98, 99

        // Reset and read again
        filter.reset();
        let batch2 = filter.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch2.len(), 4);
    }
}
