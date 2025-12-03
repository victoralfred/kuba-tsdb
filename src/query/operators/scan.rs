//! Scan Operator - Data source for query execution
//!
//! The scan operator reads data from storage (chunks) and produces
//! batches for downstream operators. It handles:
//! - Chunk discovery and ordering
//! - Decompression (Gorilla encoding)
//! - Batch formation with configurable size
//! - Zone map filtering for chunk pruning
//!
//! # Usage
//!
//! Use `ScanOperator::with_storage()` to attach a `TimeSeriesDB` for
//! production database access.

use crate::engine::TimeSeriesDB;
use crate::query::ast::{Predicate, SeriesSelector};
use crate::query::error::QueryError;
use crate::query::executor::ExecutionContext;
use crate::query::operators::{DataBatch, Operator};
use crate::types::{DataPoint, SeriesId, TimeRange};
use std::sync::Arc;

/// Scan operator that reads from storage
pub struct ScanOperator {
    /// Series selector for filtering
    selector: SeriesSelector,

    /// Time range filter
    time_range: Option<TimeRange>,

    /// Optional predicate for value filtering (pushed down)
    predicate: Option<Predicate>,

    /// Batch size for output
    batch_size: usize,

    /// Current position in data stream
    position: usize,

    /// Whether scan is complete
    exhausted: bool,

    /// Storage reference for database access
    storage: Option<Arc<TimeSeriesDB>>,

    /// Cached data points from storage query (loaded on first next_batch call)
    cached_points: Option<Vec<DataPoint>>,

    /// Test data (only available in test builds)
    #[cfg(test)]
    test_data: Option<Vec<(i64, f64, SeriesId)>>,
}

impl ScanOperator {
    /// Create a new scan operator
    ///
    /// For production use, call `with_storage()` to attach a database.
    pub fn new(selector: SeriesSelector, time_range: Option<TimeRange>) -> Self {
        Self {
            selector,
            time_range,
            predicate: None,
            batch_size: 4096,
            position: 0,
            exhausted: false,
            storage: None,
            cached_points: None,
            #[cfg(test)]
            test_data: None,
        }
    }

    /// Attach storage backend for database access
    ///
    /// This is the primary way to use ScanOperator in production.
    /// The operator will query data from the database via TimeSeriesDB.
    ///
    /// # Arguments
    ///
    /// * `db` - Arc reference to TimeSeriesDB
    pub fn with_storage(mut self, db: Arc<TimeSeriesDB>) -> Self {
        self.storage = Some(db);
        self
    }

    /// Add predicate filter (for pushed-down predicates)
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set test data (only available in test builds)
    #[cfg(test)]
    pub fn with_test_data(mut self, data: Vec<(i64, f64, SeriesId)>) -> Self {
        self.test_data = Some(data);
        self
    }

    /// Check if a data point passes the value predicate
    fn passes_predicate(&self, value: f64) -> bool {
        match &self.predicate {
            Some(pred) => pred.evaluate(value),
            None => true,
        }
    }

    /// Check if a data point passes the time range filter (test builds only)
    #[cfg(test)]
    fn passes_time_filter(&self, timestamp: i64) -> bool {
        match &self.time_range {
            Some(range) => timestamp >= range.start && timestamp <= range.end,
            None => true,
        }
    }

    /// Check if a series ID matches the selector (test builds only)
    #[cfg(test)]
    fn matches_selector(&self, series_id: SeriesId) -> bool {
        match self.selector.series_id {
            Some(id) => id == series_id,
            None => true,
        }
    }

    /// Load data from storage into cache
    ///
    /// Uses tokio runtime to bridge sync/async boundary.
    fn load_from_storage(&mut self) -> Result<(), QueryError> {
        let db = self.storage.as_ref().unwrap();

        // Get series IDs to query
        let series_ids: Vec<SeriesId> = match self.selector.series_id {
            Some(id) => vec![id],
            None => {
                // If no specific series ID, use measurement-based lookup
                if let Some(ref measurement) = self.selector.measurement {
                    let tag_filter = self.selector.to_tag_filter();
                    let handle = tokio::runtime::Handle::try_current()
                        .map_err(|_| QueryError::execution("No tokio runtime available"))?;

                    handle
                        .block_on(db.find_series(measurement, &tag_filter))
                        .map_err(|e| {
                            QueryError::execution(format!("Series lookup failed: {}", e))
                        })?
                } else {
                    // No selector criteria - cannot scan all series
                    return Ok(());
                }
            }
        };

        if series_ids.is_empty() {
            self.cached_points = Some(Vec::new());
            return Ok(());
        }

        // Get time range for query
        let time_range = self.time_range.unwrap_or_else(|| {
            // Default to all time if no range specified
            TimeRange::new_unchecked(0, i64::MAX)
        });

        // Query all series and collect points
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| QueryError::execution("No tokio runtime available"))?;

        let mut all_points: Vec<DataPoint> = Vec::new();

        for series_id in series_ids {
            match handle.block_on(db.query(series_id, time_range)) {
                Ok(points) => all_points.extend(points),
                Err(e) => {
                    tracing::warn!(series_id = series_id, error = %e, "Error querying series");
                }
            }
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);

        self.cached_points = Some(all_points);
        Ok(())
    }

    /// Process cached points and return next batch
    fn process_cached_points(
        &mut self,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<DataBatch>, QueryError> {
        let mut batch = DataBatch::with_capacity(self.batch_size);
        let mut count = 0;

        let points = self.cached_points.as_ref().unwrap();

        while self.position < points.len() && count < self.batch_size {
            let point = &points[self.position];
            self.position += 1;

            // Apply value predicate filter
            if !self.passes_predicate(point.value) {
                continue;
            }

            batch.push_with_series(point.timestamp, point.value, point.series_id);
            count += 1;
        }

        if self.position >= points.len() {
            self.exhausted = true;
        }

        if batch.is_empty() && self.exhausted {
            return Ok(None);
        }

        // Track memory usage
        let mem_size = batch.memory_size();
        if !ctx.allocate_memory(mem_size) {
            return Err(QueryError::resource_limit(
                "Memory limit exceeded during scan",
            ));
        }

        ctx.record_rows(batch.len());
        Ok(Some(batch))
    }

    /// Process test data and return next batch (test builds only)
    #[cfg(test)]
    fn process_test_data(
        &mut self,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<DataBatch>, QueryError> {
        let mut batch = DataBatch::with_capacity(self.batch_size);
        let mut count = 0;

        let data = self.test_data.as_ref().unwrap();

        while self.position < data.len() && count < self.batch_size {
            let (ts, val, sid) = data[self.position];
            self.position += 1;

            // Apply filters
            if !self.passes_time_filter(ts) {
                continue;
            }
            if !self.passes_predicate(val) {
                continue;
            }
            if !self.matches_selector(sid) {
                continue;
            }

            batch.push_with_series(ts, val, sid);
            count += 1;
        }

        if self.position >= data.len() {
            self.exhausted = true;
        }

        if batch.is_empty() && self.exhausted {
            return Ok(None);
        }

        // Track memory usage
        let mem_size = batch.memory_size();
        if !ctx.allocate_memory(mem_size) {
            return Err(QueryError::resource_limit(
                "Memory limit exceeded during scan",
            ));
        }

        ctx.record_rows(batch.len());
        Ok(Some(batch))
    }
}

impl Operator for ScanOperator {
    fn next_batch(&mut self, ctx: &mut ExecutionContext) -> Result<Option<DataBatch>, QueryError> {
        // Check for cancellation/timeout
        if ctx.should_stop() {
            if ctx.is_timed_out() {
                return Err(QueryError::timeout("Scan operator timed out"));
            }
            return Ok(None);
        }

        if self.exhausted {
            return Ok(None);
        }

        // Load from storage if configured and not yet loaded
        if self.storage.is_some() && self.cached_points.is_none() {
            self.load_from_storage()?;
        }

        // Process cached points from storage
        if self.cached_points.is_some() {
            return self.process_cached_points(ctx);
        }

        // Process test data (test builds only)
        #[cfg(test)]
        if self.test_data.is_some() {
            return self.process_test_data(ctx);
        }

        // No data source configured - requires storage to be attached
        tracing::warn!("ScanOperator has no storage attached - returning empty result");
        self.exhausted = true;
        Ok(None)
    }

    fn reset(&mut self) {
        self.position = 0;
        self.exhausted = false;
        // Clear cached points so they're reloaded on next scan
        self.cached_points = None;
    }

    fn name(&self) -> &'static str {
        "Scan"
    }

    fn estimated_cardinality(&self) -> usize {
        // Estimate based on time range if available
        match &self.time_range {
            Some(range) => {
                // Rough estimate: 1 point per second
                let duration_secs = (range.end - range.start) / 1_000_000_000;
                duration_secs as usize
            }
            None => 10000, // Unknown
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ExecutorConfig;

    fn create_test_data() -> Vec<(i64, f64, SeriesId)> {
        (0..100).map(|i| (i as i64 * 1000, i as f64, 1)).collect()
    }

    /// Create a selector that matches all series (no specific ID filter)
    fn all_series() -> SeriesSelector {
        SeriesSelector::by_measurement("test").unwrap()
    }

    #[test]
    fn test_scan_all_data() {
        let mut scan = ScanOperator::new(all_series(), None)
            .with_test_data(create_test_data())
            .with_batch_size(50);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        // First batch
        let batch1 = scan.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch1.len(), 50);

        // Second batch
        let batch2 = scan.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch2.len(), 50);

        // No more data
        let batch3 = scan.next_batch(&mut ctx).unwrap();
        assert!(batch3.is_none());
    }

    #[test]
    fn test_scan_with_time_range() {
        let data = create_test_data(); // timestamps 0..99000

        let mut scan = ScanOperator::new(
            all_series(),
            Some(TimeRange {
                start: 10000,
                end: 20000,
            }), // Only 10 points
        )
        .with_test_data(data);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let batch = scan.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 11); // timestamps 10000, 11000, ..., 20000
    }

    #[test]
    fn test_scan_with_predicate() {
        let data = create_test_data(); // values 0..99

        let mut scan = ScanOperator::new(all_series(), None)
            .with_test_data(data)
            .with_predicate(Predicate::gt("value", 50.0));

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let batch = scan.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 49); // values 51..99

        // All values should be > 50
        assert!(batch.values.iter().all(|&v| v > 50.0));
    }

    #[test]
    fn test_scan_with_series_filter() {
        let mut data = create_test_data();
        // Add data from another series
        data.extend((0..50).map(|i| (i as i64 * 1000, i as f64, 2)));

        let mut scan = ScanOperator::new(SeriesSelector::by_id(1), None).with_test_data(data);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        let mut total = 0;
        while let Some(batch) = scan.next_batch(&mut ctx).unwrap() {
            total += batch.len();
            // All should be series 1
            assert!(batch.series_ids.as_ref().unwrap().iter().all(|&id| id == 1));
        }

        assert_eq!(total, 100); // Only series 1 data
    }

    #[test]
    fn test_scan_reset() {
        let mut scan = ScanOperator::new(all_series(), None)
            .with_test_data(create_test_data())
            .with_batch_size(100);

        let config = ExecutorConfig::default();
        let mut ctx = ExecutionContext::new(&config);

        // Read all
        let _ = scan.next_batch(&mut ctx).unwrap();
        assert!(scan.next_batch(&mut ctx).unwrap().is_none());

        // Reset and read again
        scan.reset();
        let batch = scan.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(batch.len(), 100);
    }
}
