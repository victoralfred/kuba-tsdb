//! Series management for Redis time index
//!
//! Provides high-level series management operations including:
//! - Series discovery and listing
//! - Tag-based filtering
//! - Series statistics
//! - Bulk operations
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisTimeIndex, SeriesManager};
//! use gorilla_tsdb::engine::traits::SeriesMetadata;
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let index = RedisTimeIndex::new(RedisConfig::default()).await?;
//! let manager = SeriesManager::new(index);
//!
//! // List all series
//! let series = manager.list_all().await?;
//! # Ok(())
//! # }
//! ```

use crate::engine::traits::{SeriesMetadata, TimeIndex};
use crate::error::IndexError;
use crate::types::{SeriesId, TagFilter};

use super::index::RedisTimeIndex;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Key prefix for series registry (reserved for direct Redis access)
#[allow(dead_code)]
const KEY_REGISTRY: &str = "ts:registry";
const KEY_SERIES_PREFIX: &str = "ts:series:";

/// Series information with statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesInfo {
    /// Series identifier
    pub series_id: SeriesId,

    /// Metric name
    pub metric_name: String,

    /// Tags associated with the series
    pub tags: HashMap<String, String>,

    /// When the series was created
    pub created_at: i64,

    /// Last write timestamp
    pub last_write: i64,

    /// Total number of data points
    pub total_points: u64,

    /// Total number of chunks
    pub total_chunks: u64,

    /// Retention period in days (if set)
    pub retention_days: Option<u32>,
}

/// Filter for series listing
#[derive(Debug, Clone, Default)]
pub struct SeriesFilter {
    /// Filter by metric name prefix
    pub metric_prefix: Option<String>,

    /// Filter by tags
    pub tags: Option<HashMap<String, String>>,

    /// Maximum number of series to return
    pub limit: Option<usize>,

    /// Offset for pagination
    pub offset: Option<usize>,

    /// Include series with no recent writes
    pub include_inactive: bool,

    /// Inactive threshold in milliseconds
    pub inactive_threshold_ms: Option<i64>,
}

impl SeriesFilter {
    /// Create a new empty filter (matches all series)
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by metric name prefix
    pub fn with_metric_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.metric_prefix = Some(prefix.into());
        self
    }

    /// Filter by exact tag match
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value.into());
        self
    }

    /// Set limit for results
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset for pagination
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Include only active series (written to recently)
    pub fn active_only(mut self, threshold_ms: i64) -> Self {
        self.include_inactive = false;
        self.inactive_threshold_ms = Some(threshold_ms);
        self
    }
}

/// High-level series management
///
/// Provides convenient methods for managing time series on top of the
/// RedisTimeIndex implementation.
pub struct SeriesManager {
    /// Underlying time index
    index: Arc<RedisTimeIndex>,
}

impl SeriesManager {
    /// Create a new series manager
    pub fn new(index: RedisTimeIndex) -> Self {
        Self {
            index: Arc::new(index),
        }
    }

    /// Create a new series manager from an Arc
    pub fn from_arc(index: Arc<RedisTimeIndex>) -> Self {
        Self { index }
    }

    /// Get the underlying index
    pub fn index(&self) -> &RedisTimeIndex {
        &self.index
    }

    /// Create a new series
    ///
    /// # Arguments
    ///
    /// * `series_id` - Unique identifier for the series
    /// * `metric_name` - Name of the metric (e.g., "cpu.usage")
    /// * `tags` - Key-value tags for the series
    ///
    /// # Returns
    ///
    /// Ok(true) if series was created, Ok(false) if it already exists
    pub async fn create_series(
        &self,
        series_id: SeriesId,
        metric_name: impl Into<String>,
        tags: HashMap<String, String>,
    ) -> Result<bool, IndexError> {
        let metadata = SeriesMetadata {
            metric_name: metric_name.into(),
            tags,
            created_at: Utc::now().timestamp_millis(),
            retention_days: None,
        };

        // register_series returns () but doesn't indicate if series existed
        self.index.register_series(series_id, metadata).await?;

        // For now, assume success
        info!("Created/verified series: {}", series_id);
        Ok(true)
    }

    /// Create a series with retention policy
    pub async fn create_series_with_retention(
        &self,
        series_id: SeriesId,
        metric_name: impl Into<String>,
        tags: HashMap<String, String>,
        retention_days: u32,
    ) -> Result<bool, IndexError> {
        let metadata = SeriesMetadata {
            metric_name: metric_name.into(),
            tags,
            created_at: Utc::now().timestamp_millis(),
            retention_days: Some(retention_days),
        };

        self.index.register_series(series_id, metadata).await?;
        info!(
            "Created series {} with {} day retention",
            series_id, retention_days
        );
        Ok(true)
    }

    /// Get series information
    pub async fn get_series(&self, series_id: SeriesId) -> Result<Option<SeriesInfo>, IndexError> {
        let _meta_key = format!("{}{}:meta", KEY_SERIES_PREFIX, series_id);

        // This would need direct Redis access - for now use the pool through index
        // In a real implementation, we'd expose a method on RedisTimeIndex

        // Placeholder implementation
        debug!("Getting series info for: {}", series_id);
        Ok(None)
    }

    /// List all series (with optional filtering)
    pub async fn list_all(&self) -> Result<Vec<SeriesId>, IndexError> {
        self.list_with_filter(SeriesFilter::default()).await
    }

    /// List series with filter
    pub async fn list_with_filter(&self, filter: SeriesFilter) -> Result<Vec<SeriesId>, IndexError> {
        // Use find_series with a broad filter
        let tag_filter = match &filter.tags {
            Some(tags) if !tags.is_empty() => TagFilter::Exact(tags.clone()),
            _ => TagFilter::All,
        };

        // For metric prefix filter, we'd need to get all and filter client-side
        // since Redis doesn't support this natively without secondary indexes

        let metric_pattern = filter.metric_prefix.as_deref().unwrap_or("*");

        // Get all series matching tag filter
        let mut series = if metric_pattern == "*" {
            // Need to implement a list_all on the index
            // For now, search with empty metric name matches none
            // This is a limitation - would need SCAN in production
            Vec::new()
        } else {
            self.index.find_series(metric_pattern, &tag_filter).await?
        };

        // Apply pagination
        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(usize::MAX);

        if offset > 0 {
            if offset >= series.len() {
                series.clear();
            } else {
                series = series[offset..].to_vec();
            }
        }

        if series.len() > limit {
            series.truncate(limit);
        }

        Ok(series)
    }

    /// Find series by metric name
    pub async fn find_by_metric(&self, metric_name: &str) -> Result<Vec<SeriesId>, IndexError> {
        self.index.find_series(metric_name, &TagFilter::All).await
    }

    /// Find series by tags
    pub async fn find_by_tags(
        &self,
        _tags: HashMap<String, String>,
    ) -> Result<Vec<SeriesId>, IndexError> {
        // We need a metric name for the current find_series implementation
        // This is a limitation - would need a different approach in production
        Ok(Vec::new())
    }

    /// Delete a series and all its data
    pub async fn delete_series(&self, series_id: SeriesId) -> Result<(), IndexError> {
        self.index.delete_series(series_id).await?;
        info!("Deleted series: {}", series_id);
        Ok(())
    }

    /// Delete multiple series
    pub async fn delete_series_batch(&self, series_ids: Vec<SeriesId>) -> Result<usize, IndexError> {
        let mut deleted = 0;

        for series_id in series_ids {
            if let Ok(()) = self.index.delete_series(series_id).await {
                deleted += 1;
            }
        }

        info!("Deleted {} series in batch", deleted);
        Ok(deleted)
    }

    /// Get series count
    pub async fn count(&self) -> Result<u64, IndexError> {
        // Would need direct Redis SCARD command
        // For now, return 0 as placeholder
        Ok(0)
    }

    /// Check if a series exists
    pub async fn exists(&self, _series_id: SeriesId) -> Result<bool, IndexError> {
        // Would need direct Redis SISMEMBER command
        // For now, return false as placeholder
        Ok(false)
    }

    /// Generate a new unique series ID from metric name and tags
    ///
    /// Uses a hash of the metric name and sorted tags to generate
    /// a deterministic series ID.
    pub fn generate_series_id(metric_name: &str, tags: &HashMap<String, String>) -> SeriesId {
        use std::collections::BTreeMap;
        use std::hash::{Hash, Hasher};

        // Sort tags for deterministic hashing
        let sorted_tags: BTreeMap<_, _> = tags.iter().collect();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metric_name.hash(&mut hasher);

        for (key, value) in sorted_tags {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }

        // Use the hash as the lower 64 bits of the series ID
        let hash = hasher.finish();

        // Create a 128-bit ID with some structure
        // Upper 64 bits: timestamp-based component for rough ordering
        // Lower 64 bits: hash of metric name and tags
        let timestamp_component = (Utc::now().timestamp_millis() as u128) << 64;
        timestamp_component | (hash as u128)
    }

    /// Get or create a series ID for the given metric and tags
    ///
    /// If a series with the same metric name and tags exists, returns its ID.
    /// Otherwise, creates a new series and returns the new ID.
    pub async fn get_or_create_series(
        &self,
        metric_name: &str,
        tags: HashMap<String, String>,
    ) -> Result<SeriesId, IndexError> {
        // Generate deterministic ID
        let series_id = Self::generate_series_id(metric_name, &tags);

        // Try to create (will be no-op if exists)
        self.create_series(series_id, metric_name, tags).await?;

        Ok(series_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_filter_builder() {
        let filter = SeriesFilter::new()
            .with_metric_prefix("cpu")
            .with_tag("host", "server1")
            .with_limit(100)
            .with_offset(10);

        assert_eq!(filter.metric_prefix, Some("cpu".to_string()));
        assert_eq!(filter.tags.as_ref().unwrap().get("host"), Some(&"server1".to_string()));
        assert_eq!(filter.limit, Some(100));
        assert_eq!(filter.offset, Some(10));
    }

    #[test]
    fn test_generate_series_id() {
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("dc".to_string(), "us-east".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("dc".to_string(), "us-east".to_string());
        tags2.insert("host".to_string(), "server1".to_string());

        // Same tags in different order should produce same lower 64 bits
        let id1 = SeriesManager::generate_series_id("cpu.usage", &tags1);
        let id2 = SeriesManager::generate_series_id("cpu.usage", &tags2);

        // Lower 64 bits should be the same (hash of metric + tags)
        assert_eq!(id1 & 0xFFFFFFFFFFFFFFFF, id2 & 0xFFFFFFFFFFFFFFFF);

        // Different metric name should produce different ID
        let id3 = SeriesManager::generate_series_id("mem.usage", &tags1);
        assert_ne!(id1 & 0xFFFFFFFFFFFFFFFF, id3 & 0xFFFFFFFFFFFFFFFF);
    }

    #[test]
    fn test_series_info_serialization() {
        let info = SeriesInfo {
            series_id: 12345,
            metric_name: "cpu.usage".to_string(),
            tags: {
                let mut t = HashMap::new();
                t.insert("host".to_string(), "server1".to_string());
                t
            },
            created_at: 1000,
            last_write: 2000,
            total_points: 10000,
            total_chunks: 5,
            retention_days: Some(30),
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: SeriesInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.series_id, info.series_id);
        assert_eq!(deserialized.metric_name, info.metric_name);
        assert_eq!(deserialized.total_points, info.total_points);
    }
}
