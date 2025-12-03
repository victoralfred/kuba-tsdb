//! Sort operator for ordering query results
//!
//! This module provides an operator that sorts data batches by specified fields.
//! Supports ascending and descending order on timestamp or value fields.
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::query::operators::sort::SortOperator;
//! use gorilla_tsdb::query::ast::OrderDirection;
//!
//! // Sort by timestamp descending
//! let sorted = SortOperator::new(
//!     some_operator,
//!     vec![("timestamp".to_string(), OrderDirection::Desc)]
//! );
//! ```

use super::{DataBatch, ExecutionContext, Operator, QueryError};
use crate::query::ast::OrderDirection;

/// Operator that sorts data by specified fields
///
/// This operator collects all data from the upstream operator, sorts it
/// according to the specified order, and then emits the sorted results.
///
/// # Note
///
/// Since sorting requires all data to be collected first, this operator
/// buffers the entire result set in memory. For large datasets, consider
/// using LIMIT before sorting or using pre-sorted indexes.
pub struct SortOperator {
    /// Upstream operator providing data
    input: Box<dyn Operator>,
    /// Sort specifications: (field_name, direction)
    order_by: Vec<(String, OrderDirection)>,
    /// Collected and sorted data (None until first pull)
    sorted_data: Option<DataBatch>,
    /// Whether we've emitted the sorted data
    emitted: bool,
}

impl SortOperator {
    /// Create a new SortOperator
    ///
    /// # Arguments
    ///
    /// * `input` - Upstream operator to sort
    /// * `order_by` - List of (field, direction) pairs for sorting
    ///
    /// Supported fields:
    /// - "timestamp" - Sort by timestamp
    /// - "value" - Sort by value
    pub fn new(input: Box<dyn Operator>, order_by: Vec<(String, OrderDirection)>) -> Self {
        Self {
            input,
            order_by,
            sorted_data: None,
            emitted: false,
        }
    }

    /// Collect all data from upstream
    fn collect_all(&mut self, ctx: &mut ExecutionContext) -> Result<DataBatch, QueryError> {
        let mut all_timestamps = Vec::new();
        let mut all_values = Vec::new();
        let mut all_series_ids: Option<Vec<crate::types::SeriesId>> = None;

        while let Some(batch) = self.input.next_batch(ctx)? {
            all_timestamps.extend_from_slice(&batch.timestamps);
            all_values.extend_from_slice(&batch.values);

            if let Some(series_ids) = batch.series_ids {
                all_series_ids
                    .get_or_insert_with(Vec::new)
                    .extend_from_slice(&series_ids);
            }
        }

        Ok(DataBatch {
            timestamps: all_timestamps,
            values: all_values,
            series_ids: all_series_ids,
            validity: None, // Validity handling would need more work for sort
        })
    }

    /// Sort the collected data according to order_by specifications
    fn sort_batch(&self, batch: DataBatch) -> DataBatch {
        if batch.is_empty() || self.order_by.is_empty() {
            return batch;
        }

        let len = batch.len();

        // Create indices for sorting
        let mut indices: Vec<usize> = (0..len).collect();

        // Sort indices based on order_by specifications
        indices.sort_by(|&a, &b| {
            for (field, direction) in &self.order_by {
                let cmp = match field.as_str() {
                    "timestamp" | "time" => batch.timestamps[a].cmp(&batch.timestamps[b]),
                    "value" => batch.values[a]
                        .partial_cmp(&batch.values[b])
                        .unwrap_or(std::cmp::Ordering::Equal),
                    _ => std::cmp::Ordering::Equal, // Unknown field, no effect
                };

                let cmp = match direction {
                    OrderDirection::Asc => cmp,
                    OrderDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Apply sorted indices to create new batch
        let sorted_timestamps: Vec<i64> = indices.iter().map(|&i| batch.timestamps[i]).collect();
        let sorted_values: Vec<f64> = indices.iter().map(|&i| batch.values[i]).collect();
        let sorted_series_ids = batch
            .series_ids
            .as_ref()
            .map(|ids| indices.iter().map(|&i| ids[i]).collect());

        DataBatch {
            timestamps: sorted_timestamps,
            values: sorted_values,
            series_ids: sorted_series_ids,
            validity: None,
        }
    }
}

impl Operator for SortOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // If already emitted, we're done
        if self.emitted {
            return Ok(None);
        }

        // Collect and sort if not already done
        if self.sorted_data.is_none() {
            let collected = self.collect_all(ctx)?;
            let sorted = self.sort_batch(collected);
            self.sorted_data = Some(sorted);
        }

        // Emit the sorted data
        self.emitted = true;
        Ok(self.sorted_data.take())
    }

    fn reset(&mut self) {
        self.sorted_data = None;
        self.emitted = false;
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "Sort"
    }

    fn estimated_cardinality(&self) -> usize {
        self.input.estimated_cardinality()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ExecutorConfig;
    use crate::query::operators::DataBatch;

    /// Mock operator for testing
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

    fn create_ctx() -> ExecutionContext {
        let config = ExecutorConfig::default();
        ExecutionContext::new(&config)
    }

    #[test]
    fn test_sort_by_timestamp_asc() {
        // Unsorted data
        let batch = DataBatch::new(vec![3, 1, 4, 1, 5], vec![30.0, 10.0, 40.0, 11.0, 50.0]);

        let mock = MockOperator::new(vec![batch]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("timestamp".to_string(), OrderDirection::Asc)],
        );
        let mut ctx = create_ctx();

        let result = sort.next_batch(&mut ctx).unwrap().unwrap();

        assert_eq!(result.timestamps, vec![1, 1, 3, 4, 5]);
        // Values should follow timestamps
        assert_eq!(result.values, vec![10.0, 11.0, 30.0, 40.0, 50.0]);

        // Should be exhausted
        assert!(sort.next_batch(&mut ctx).unwrap().is_none());
    }

    #[test]
    fn test_sort_by_timestamp_desc() {
        let batch = DataBatch::new(vec![3, 1, 4, 1, 5], vec![30.0, 10.0, 40.0, 11.0, 50.0]);

        let mock = MockOperator::new(vec![batch]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("timestamp".to_string(), OrderDirection::Desc)],
        );
        let mut ctx = create_ctx();

        let result = sort.next_batch(&mut ctx).unwrap().unwrap();

        assert_eq!(result.timestamps, vec![5, 4, 3, 1, 1]);
    }

    #[test]
    fn test_sort_by_value_asc() {
        let batch = DataBatch::new(vec![1, 2, 3, 4, 5], vec![50.0, 20.0, 40.0, 10.0, 30.0]);

        let mock = MockOperator::new(vec![batch]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("value".to_string(), OrderDirection::Asc)],
        );
        let mut ctx = create_ctx();

        let result = sort.next_batch(&mut ctx).unwrap().unwrap();

        assert_eq!(result.values, vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        // Timestamps should follow values
        assert_eq!(result.timestamps, vec![4, 2, 5, 3, 1]);
    }

    #[test]
    fn test_sort_across_batches() {
        // Data split across multiple batches
        let batch1 = DataBatch::new(vec![5, 3], vec![50.0, 30.0]);
        let batch2 = DataBatch::new(vec![1, 4, 2], vec![10.0, 40.0, 20.0]);

        let mock = MockOperator::new(vec![batch1, batch2]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("timestamp".to_string(), OrderDirection::Asc)],
        );
        let mut ctx = create_ctx();

        let result = sort.next_batch(&mut ctx).unwrap().unwrap();

        // All 5 rows combined and sorted
        assert_eq!(result.len(), 5);
        assert_eq!(result.timestamps, vec![1, 2, 3, 4, 5]);
        assert_eq!(result.values, vec![10.0, 20.0, 30.0, 40.0, 50.0]);
    }

    #[test]
    fn test_sort_empty() {
        let mock = MockOperator::new(vec![]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("timestamp".to_string(), OrderDirection::Asc)],
        );
        let mut ctx = create_ctx();

        let result = sort.next_batch(&mut ctx).unwrap();
        // Empty upstream should give empty (or None) result
        assert!(result.is_none() || result.unwrap().is_empty());
    }

    #[test]
    fn test_sort_reset() {
        let batch = DataBatch::new(vec![3, 1, 2], vec![30.0, 10.0, 20.0]);

        let mock = MockOperator::new(vec![batch]);
        let mut sort = SortOperator::new(
            Box::new(mock),
            vec![("timestamp".to_string(), OrderDirection::Asc)],
        );
        let mut ctx = create_ctx();

        // First pull
        let _ = sort.next_batch(&mut ctx).unwrap();
        assert!(sort.next_batch(&mut ctx).unwrap().is_none());

        // Reset and pull again
        sort.reset();
        let result = sort.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(result.timestamps, vec![1, 2, 3]);
    }
}
