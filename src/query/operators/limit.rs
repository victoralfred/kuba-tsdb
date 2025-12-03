//! Limit and Offset operators for query result limiting
//!
//! This module provides operators that limit the number of results returned
//! from a query, with optional offset for pagination.
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::query::operators::limit::LimitOperator;
//!
//! // Wrap an existing operator with limit and offset
//! let limited = LimitOperator::new(some_operator, 100, Some(50));
//! // This will skip first 50 results and return at most 100
//! ```

use super::{DataBatch, ExecutionContext, Operator, QueryError};

/// Operator that limits the number of rows returned from upstream
///
/// Applies LIMIT and optional OFFSET to query results. This operator
/// tracks how many rows have been consumed and returned, stopping
/// when the limit is reached.
///
/// # Behavior
///
/// - Skips the first `offset` rows if offset is specified
/// - Returns at most `limit` rows after the offset
/// - Passes through empty batches from upstream
/// - Signals end of stream when limit is reached
pub struct LimitOperator {
    /// Upstream operator providing data
    input: Box<dyn Operator>,
    /// Maximum number of rows to return
    limit: usize,
    /// Number of rows to skip before returning (optional)
    offset: Option<usize>,
    /// Number of rows skipped so far
    skipped: usize,
    /// Number of rows returned so far
    returned: usize,
    /// Whether we've exhausted the limit
    exhausted: bool,
}

impl LimitOperator {
    /// Create a new LimitOperator
    ///
    /// # Arguments
    ///
    /// * `input` - Upstream operator to limit
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip before returning (None = 0)
    pub fn new(input: Box<dyn Operator>, limit: usize, offset: Option<usize>) -> Self {
        Self {
            input,
            limit,
            offset,
            skipped: 0,
            returned: 0,
            exhausted: false,
        }
    }

    /// Get remaining rows that can be returned
    #[inline]
    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.returned)
    }

    /// Get remaining rows to skip
    #[inline]
    fn remaining_skip(&self) -> usize {
        self.offset.unwrap_or(0).saturating_sub(self.skipped)
    }
}

impl Operator for LimitOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // If we've already returned limit rows, we're done
        if self.exhausted || self.returned >= self.limit {
            self.exhausted = true;
            return Ok(None);
        }

        loop {
            // Get next batch from upstream
            let batch = match self.input.next_batch(ctx)? {
                Some(b) => b,
                None => return Ok(None), // Upstream exhausted
            };

            if batch.is_empty() {
                continue;
            }

            let batch_len = batch.len();

            // Handle offset (skip rows)
            let remaining_skip = self.remaining_skip();
            if remaining_skip > 0 {
                if batch_len <= remaining_skip {
                    // Skip entire batch
                    self.skipped += batch_len;
                    continue;
                } else {
                    // Skip partial batch and process rest
                    self.skipped += remaining_skip;
                    let skip_start = remaining_skip;
                    let remaining = self.remaining();
                    let take_count = (batch_len - skip_start).min(remaining);

                    let result = slice_batch(&batch, skip_start, skip_start + take_count);
                    self.returned += result.len();

                    if self.returned >= self.limit {
                        self.exhausted = true;
                    }

                    return Ok(Some(result));
                }
            }

            // No more offset to apply, just limit
            let remaining = self.remaining();
            if batch_len <= remaining {
                // Return entire batch
                self.returned += batch_len;
                return Ok(Some(batch));
            } else {
                // Return partial batch (limit reached)
                let result = slice_batch(&batch, 0, remaining);
                self.returned += result.len();
                self.exhausted = true;
                return Ok(Some(result));
            }
        }
    }

    fn reset(&mut self) {
        self.skipped = 0;
        self.returned = 0;
        self.exhausted = false;
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn estimated_cardinality(&self) -> usize {
        let upstream = self.input.estimated_cardinality();
        let offset = self.offset.unwrap_or(0);
        upstream.saturating_sub(offset).min(self.limit)
    }
}

/// Slice a DataBatch to extract a range of rows
fn slice_batch(batch: &DataBatch, start: usize, end: usize) -> DataBatch {
    let end = end.min(batch.len());
    if start >= end {
        return DataBatch::default();
    }

    DataBatch {
        timestamps: batch.timestamps[start..end].to_vec(),
        values: batch.values[start..end].to_vec(),
        series_ids: batch.series_ids.as_ref().map(|s| s[start..end].to_vec()),
        validity: batch
            .validity
            .as_ref()
            .map(|v| super::slice_validity_bitmap(v, start, end)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::DataBatch;

    /// Mock operator for testing that returns fixed batches
    struct MockOperator {
        batches: Vec<DataBatch>,
        index: usize,
    }

    impl MockOperator {
        fn new(batches: Vec<DataBatch>) -> Self {
            Self { batches, index: 0 }
        }
    }

    impl Operator for MockOperator {
        fn next_batch(
            &mut self,
            _ctx: &mut ExecutionContext,
        ) -> Result<Option<DataBatch>, QueryError> {
            if self.index >= self.batches.len() {
                Ok(None)
            } else {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            }
        }

        fn reset(&mut self) {
            self.index = 0;
        }

        fn name(&self) -> &'static str {
            "Mock"
        }
    }

    fn create_batch(count: usize) -> DataBatch {
        DataBatch::new(
            (0..count as i64).collect(),
            (0..count).map(|i| i as f64).collect(),
        )
    }

    fn create_ctx() -> ExecutionContext {
        let config = ExecutorConfig::default();
        ExecutionContext::new(&config)
    }

    #[test]
    fn test_limit_basic() {
        let mock = MockOperator::new(vec![create_batch(100)]);
        let mut limit = LimitOperator::new(Box::new(mock), 10, None);
        let mut ctx = create_ctx();

        let batch = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 10);

        // Should be exhausted
        assert!(limit.next_batch(&mut ctx).unwrap().is_none());
    }

    #[test]
    fn test_limit_with_offset() {
        let mock = MockOperator::new(vec![create_batch(100)]);
        let mut limit = LimitOperator::new(Box::new(mock), 10, Some(50));
        let mut ctx = create_ctx();

        let batch = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 10);
        // First timestamp should be 50 (after offset)
        assert_eq!(batch.timestamps[0], 50);

        // Should be exhausted
        assert!(limit.next_batch(&mut ctx).unwrap().is_none());
    }

    #[test]
    fn test_limit_across_batches() {
        let mock = MockOperator::new(vec![create_batch(5), create_batch(5), create_batch(5)]);
        let mut limit = LimitOperator::new(Box::new(mock), 8, None);
        let mut ctx = create_ctx();

        // First batch: 5 rows
        let batch1 = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch1.len(), 5);

        // Second batch: only 3 rows (to reach limit of 8)
        let batch2 = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch2.len(), 3);

        // Should be exhausted
        assert!(limit.next_batch(&mut ctx).unwrap().is_none());
    }

    #[test]
    fn test_offset_across_batches() {
        let mock = MockOperator::new(vec![create_batch(5), create_batch(5), create_batch(5)]);
        let mut limit = LimitOperator::new(Box::new(mock), 5, Some(7));
        let mut ctx = create_ctx();

        // Should skip first 7 rows (entire first batch + 2 from second)
        let batch = limit.next_batch(&mut ctx).unwrap().unwrap();
        // Second batch starts at ts=0, after skipping 2, we get ts=2,3,4
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.timestamps[0], 2);

        // Get remaining 2 from limit
        let batch2 = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch2.len(), 2);

        // Should be exhausted
        assert!(limit.next_batch(&mut ctx).unwrap().is_none());
    }

    #[test]
    fn test_limit_reset() {
        let mock = MockOperator::new(vec![create_batch(100)]);
        let mut limit = LimitOperator::new(Box::new(mock), 10, None);
        let mut ctx = create_ctx();

        // Get first 10
        let _ = limit.next_batch(&mut ctx).unwrap();
        assert!(limit.next_batch(&mut ctx).unwrap().is_none());

        // Reset and try again
        limit.reset();
        let batch = limit.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 10);
    }
}
