//! Multi-Dimensional Data Model
//!
//! This module defines the core data structures for multi-dimensional time-series
//! aggregation, with support for efficient tag storage via string interning.
//!
//! # Design Principles
//!
//! 1. **String Interning**: Tag keys and values are stored as integer IDs,
//!    with a dictionary mapping IDs to strings. This reduces memory usage
//!    by 80-90% for repetitive tags.
//!
//! 2. **Compact Series Identification**: Each unique metric + tag combination
//!    maps to a series ID, enabling efficient storage and lookup.
//!
//! 3. **Source Tracking**: Points track their origin (host, instance) for
//!    proper spatial aggregation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};

use crate::types::SeriesId;

// ============================================================================
// Interned Tag Types
// ============================================================================

/// Interned tag key identifier
///
/// Instead of storing "host", "datacenter", "environment" as strings,
/// we store compact u32 IDs that reference a dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct TagKeyId(pub u32);

/// Interned tag value identifier
///
/// Tag values like "server-prod-001", "us-east-1" are stored as u32 IDs
/// within the context of their parent key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct TagValueId(pub u32);

/// A single interned tag (key-value pair as IDs)
///
/// Represents a tag like "host=server1" as two u32 IDs,
/// reducing memory from ~30 bytes to 8 bytes per tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct InternedTag {
    /// The interned tag key ID
    pub key: TagKeyId,
    /// The interned tag value ID
    pub value: TagValueId,
}

impl InternedTag {
    /// Create a new interned tag
    pub fn new(key: TagKeyId, value: TagValueId) -> Self {
        Self { key, value }
    }
}

/// A set of interned tags for a series
///
/// Stored as a sorted vector for consistent hashing and efficient comparison.
/// Typical size: 3-10 tags = 24-80 bytes (vs 200-800 bytes with strings).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InternedTagSet {
    /// Sorted by (key, value) for consistent ordering
    tags: Vec<InternedTag>,
}

impl InternedTagSet {
    /// Create an empty tag set
    pub fn new() -> Self {
        Self { tags: Vec::new() }
    }

    /// Create from a vector of interned tags (will be sorted)
    pub fn from_tags(mut tags: Vec<InternedTag>) -> Self {
        tags.sort();
        tags.dedup();
        Self { tags }
    }

    /// Add a tag to the set (maintains sorted order)
    pub fn insert(&mut self, tag: InternedTag) {
        match self.tags.binary_search(&tag) {
            Ok(_) => {}, // Already exists
            Err(pos) => self.tags.insert(pos, tag),
        }
    }

    /// Get the tag value for a given key
    pub fn get(&self, key: TagKeyId) -> Option<TagValueId> {
        self.tags.iter().find(|t| t.key == key).map(|t| t.value)
    }

    /// Check if this tag set contains a specific tag
    pub fn contains(&self, tag: &InternedTag) -> bool {
        self.tags.binary_search(tag).is_ok()
    }

    /// Check if this tag set contains a specific key
    pub fn contains_key(&self, key: TagKeyId) -> bool {
        self.tags.iter().any(|t| t.key == key)
    }

    /// Get all tags as a slice
    pub fn tags(&self) -> &[InternedTag] {
        &self.tags
    }

    /// Number of tags in the set
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Iterate over tags
    pub fn iter(&self) -> impl Iterator<Item = &InternedTag> {
        self.tags.iter()
    }

    /// Calculate a hash for this tag set
    ///
    /// SEC-003: Uses a better mixing function to reduce collision risk.
    /// Since tags are always sorted, this produces consistent hashes.
    pub fn hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        // Include a salt to make hash less predictable
        0xDEADBEEF_u64.hash(&mut hasher);
        self.tags.len().hash(&mut hasher);
        self.tags.hash(&mut hasher);

        // Apply additional mixing (FNV-1a style mix)
        let h = hasher.finish();
        let h = h.wrapping_mul(0x517cc1b727220a95);
        h ^ (h >> 32)
    }
}

impl Default for InternedTagSet {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Source Identification
// ============================================================================

/// Identifies the source of a metric point
///
/// Used for spatial aggregation to track which host/instance
/// contributed each data point.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceIdentifier {
    /// Host name or IP (interned for efficiency)
    pub host_id: Option<TagValueId>,

    /// Instance identifier (e.g., pod name, process ID)
    pub instance_id: Option<TagValueId>,

    /// Collection point (e.g., agent name)
    pub collector_id: Option<TagValueId>,
}

impl SourceIdentifier {
    /// Create a new source identifier
    pub fn new(
        host_id: Option<TagValueId>,
        instance_id: Option<TagValueId>,
        collector_id: Option<TagValueId>,
    ) -> Self {
        Self {
            host_id,
            instance_id,
            collector_id,
        }
    }

    /// Create an unknown source
    pub fn unknown() -> Self {
        Self {
            host_id: None,
            instance_id: None,
            collector_id: None,
        }
    }
}

impl Default for SourceIdentifier {
    fn default() -> Self {
        Self::unknown()
    }
}

// ============================================================================
// Series Identification
// ============================================================================

/// Unique identifier for a time-series based on metric name and tags
///
/// A series is uniquely identified by the combination of its metric name
/// and tag set. The series_id is derived from this combination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesIdentifier {
    /// Interned metric name ID
    pub metric_id: TagValueId,

    /// Interned tag set
    pub tags: InternedTagSet,

    /// Computed series ID (hash of metric + tags)
    pub series_id: SeriesId,
}

impl SeriesIdentifier {
    /// Create a new series identifier
    pub fn new(metric_id: TagValueId, tags: InternedTagSet) -> Self {
        let series_id = Self::compute_series_id(metric_id, &tags);
        Self {
            metric_id,
            tags,
            series_id,
        }
    }

    /// Compute the series ID from metric and tags
    ///
    /// Uses a stable hash function to generate a 128-bit ID.
    fn compute_series_id(metric_id: TagValueId, tags: &InternedTagSet) -> SeriesId {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        metric_id.0.hash(&mut hasher);
        tags.hash().hash(&mut hasher);
        let h1 = hasher.finish();

        // Create second hash for 128-bit ID
        let mut hasher2 = DefaultHasher::new();
        h1.hash(&mut hasher2);
        "series_salt".hash(&mut hasher2);
        let h2 = hasher2.finish();

        ((h1 as u128) << 64) | (h2 as u128)
    }
}

// ============================================================================
// Metric Family
// ============================================================================

/// A metric family groups all series with the same metric name
///
/// For example, "cpu_usage" might have 1000 series (one per host/tag combination).
/// The family tracks all series and their cardinality.
#[derive(Debug, Clone)]
pub struct MetricFamily {
    /// Metric name (interned ID)
    pub metric_id: TagValueId,

    /// All series IDs belonging to this metric
    pub series_ids: Vec<SeriesId>,

    /// Total cardinality (number of unique series)
    pub cardinality: usize,

    /// Tag keys present across all series (for schema discovery)
    pub tag_keys: Vec<TagKeyId>,
}

impl MetricFamily {
    /// Create a new metric family
    pub fn new(metric_id: TagValueId) -> Self {
        Self {
            metric_id,
            series_ids: Vec::new(),
            cardinality: 0,
            tag_keys: Vec::new(),
        }
    }

    /// Add a series to this family
    pub fn add_series(&mut self, series_id: SeriesId, tag_keys: &[TagKeyId]) {
        if !self.series_ids.contains(&series_id) {
            self.series_ids.push(series_id);
            self.cardinality += 1;

            // Track all unique tag keys
            for key in tag_keys {
                if !self.tag_keys.contains(key) {
                    self.tag_keys.push(*key);
                }
            }
        }
    }
}

// ============================================================================
// Metric Point (Input Data)
// ============================================================================

/// A single metric point with full metadata
///
/// This is the input format for the aggregation engine, containing
/// all information needed for spatial and temporal aggregation.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// Metric name (string, will be interned on ingestion)
    pub metric_name: String,

    /// Tags (strings, will be interned on ingestion)
    pub tags: HashMap<String, String>,

    /// Measurement value
    pub value: f64,

    /// Timestamp in milliseconds
    pub timestamp: i64,

    /// Source identification (optional)
    pub source: Option<SourceIdentifier>,
}

impl MetricPoint {
    /// Create a new metric point
    pub fn new(metric_name: String, value: f64, timestamp: i64) -> Self {
        Self {
            metric_name,
            tags: HashMap::new(),
            value,
            timestamp,
            source: None,
        }
    }

    /// Add a tag
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Set the source identifier
    pub fn with_source(mut self, source: SourceIdentifier) -> Self {
        self.source = Some(source);
        self
    }
}

// ============================================================================
// Aggregation Output Types
// ============================================================================

/// A single aggregated data point
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AggregatedPoint {
    /// Timestamp (start of the window for windowed aggregation)
    pub timestamp: i64,

    /// Aggregated value
    pub value: f64,

    /// Number of raw points contributing to this aggregation
    pub count: u64,
}

impl AggregatedPoint {
    /// Create a new aggregated point
    pub fn new(timestamp: i64, value: f64, count: u64) -> Self {
        Self {
            timestamp,
            value,
            count,
        }
    }
}

/// Data aligned by timestamp from multiple series
///
/// Used as intermediate representation during spatial aggregation,
/// grouping values from different series at the same timestamp.
#[derive(Debug, Clone)]
pub struct TimestampData {
    /// The timestamp for this group
    pub timestamp: i64,

    /// Values from each series at this timestamp
    /// Vec of (series_id, value) for weighted aggregation support
    pub values: Vec<(SeriesId, f64)>,
}

impl TimestampData {
    /// Create new timestamp data
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            values: Vec::new(),
        }
    }

    /// Add a value from a series
    pub fn add_value(&mut self, series_id: SeriesId, value: f64) {
        self.values.push((series_id, value));
    }

    /// Get just the values (without series IDs)
    pub fn values_only(&self) -> Vec<f64> {
        self.values.iter().map(|(_, v)| *v).collect()
    }
}

/// Aligned data from multiple series
///
/// Contains data from multiple series aligned by timestamp,
/// ready for spatial aggregation.
#[derive(Debug, Clone)]
pub struct AlignedData {
    /// Series IDs that contributed data
    pub series_ids: Vec<SeriesId>,

    /// Data grouped by timestamp
    pub timestamps: Vec<TimestampData>,

    /// Start time of the data range
    pub start_time: i64,
    /// End time of the data range
    pub end_time: i64,
}

impl AlignedData {
    /// Create new aligned data
    pub fn new(series_ids: Vec<SeriesId>, start_time: i64, end_time: i64) -> Self {
        Self {
            series_ids,
            timestamps: Vec::new(),
            start_time,
            end_time,
        }
    }

    /// Add timestamp data
    pub fn add_timestamp(&mut self, data: TimestampData) {
        self.timestamps.push(data);
    }

    /// Sort timestamps in ascending order
    pub fn sort_by_time(&mut self) {
        self.timestamps.sort_by_key(|t| t.timestamp);
    }
}

/// Raw data from multiple series before alignment
#[derive(Debug, Clone)]
pub struct MultiSeriesData {
    /// Map of series_id to list of (timestamp, value) pairs
    pub series_data: HashMap<SeriesId, Vec<(i64, f64)>>,
}

impl MultiSeriesData {
    /// Create new multi-series data container
    pub fn new() -> Self {
        Self {
            series_data: HashMap::new(),
        }
    }

    /// Add data for a series
    pub fn add_series_data(&mut self, series_id: SeriesId, points: Vec<(i64, f64)>) {
        self.series_data.insert(series_id, points);
    }

    /// Get all unique timestamps across all series
    pub fn all_timestamps(&self) -> Vec<i64> {
        let mut timestamps: Vec<i64> = self
            .series_data
            .values()
            .flat_map(|points| points.iter().map(|(t, _)| *t))
            .collect();
        timestamps.sort();
        timestamps.dedup();
        timestamps
    }

    /// Number of series
    pub fn series_count(&self) -> usize {
        self.series_data.len()
    }
}

impl Default for MultiSeriesData {
    fn default() -> Self {
        Self::new()
    }
}

/// Tags that are common vs varying across series
///
/// When aggregating multiple series, some tags are the same (common)
/// and some differ (varying). This helps build result metadata.
#[derive(Debug, Clone)]
pub struct MergedTagSet {
    /// Tags that are identical across all series
    pub common_tags: InternedTagSet,

    /// Tags that vary (key -> set of values seen)
    pub varying_tags: HashMap<TagKeyId, Vec<TagValueId>>,

    /// Number of series that were merged
    pub series_count: usize,
}

impl MergedTagSet {
    /// Create a new merged tag set
    pub fn new() -> Self {
        Self {
            common_tags: InternedTagSet::new(),
            varying_tags: HashMap::new(),
            series_count: 0,
        }
    }
}

impl Default for MergedTagSet {
    fn default() -> Self {
        Self::new()
    }
}

/// The final unified time-series output
///
/// Result of space-time aggregation, representing a single logical
/// time-series that can be displayed in a UI.
#[derive(Debug, Clone)]
pub struct UnifiedTimeSeries {
    /// Aggregated data points
    pub points: Vec<AggregatedPoint>,

    /// Metadata about what was aggregated
    pub metadata: AggregationMetadata,
}

impl UnifiedTimeSeries {
    /// Create a new unified time series
    pub fn new(points: Vec<AggregatedPoint>, metadata: AggregationMetadata) -> Self {
        Self { points, metadata }
    }

    /// Create an empty unified time series
    pub fn empty() -> Self {
        Self {
            points: Vec::new(),
            metadata: AggregationMetadata::default(),
        }
    }

    /// Create from aggregated points with metadata
    pub fn from_points(points: Vec<AggregatedPoint>, metadata: AggregationMetadata) -> Self {
        Self { points, metadata }
    }

    /// Get the number of points
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Truncate to at most N points
    pub fn truncate(mut self, limit: usize) -> Self {
        if self.points.len() > limit {
            self.points.truncate(limit);
        }
        self
    }
}

/// Metadata about an aggregation operation
#[derive(Debug, Clone)]
pub struct AggregationMetadata {
    /// Metric name (resolved string)
    pub metric_name: String,

    /// Number of series that were aggregated
    pub series_count: usize,

    /// Total raw points processed
    pub points_processed: u64,

    /// Tags that were common across all series
    pub common_tags: HashMap<String, String>,

    /// Tags that varied (aggregated over)
    pub aggregated_tags: Vec<String>,

    /// Start time of the result
    pub start_time: i64,
    /// End time of the result
    pub end_time: i64,

    /// Aggregation function used
    pub aggregation_function: String,

    /// Window duration in milliseconds (if windowed)
    pub window_ms: Option<i64>,

    /// Processing time
    pub processing_time: std::time::Duration,
}

impl AggregationMetadata {
    /// Create new metadata with minimal info
    pub fn new(start_time: i64, end_time: i64) -> Self {
        Self {
            metric_name: String::new(),
            series_count: 0,
            points_processed: 0,
            common_tags: HashMap::new(),
            aggregated_tags: Vec::new(),
            start_time,
            end_time,
            aggregation_function: String::new(),
            window_ms: None,
            processing_time: std::time::Duration::ZERO,
        }
    }

    /// Create metadata with metric name
    pub fn with_metric(metric_name: String) -> Self {
        Self {
            metric_name,
            series_count: 0,
            points_processed: 0,
            common_tags: HashMap::new(),
            aggregated_tags: Vec::new(),
            start_time: 0,
            end_time: 0,
            aggregation_function: String::new(),
            window_ms: None,
            processing_time: std::time::Duration::ZERO,
        }
    }
}

impl Default for AggregationMetadata {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics for the aggregation system
#[derive(Debug, Default)]
pub struct AggregationStats {
    /// Total queries executed
    pub queries_executed: AtomicU32,

    /// Total series resolved
    pub series_resolved: AtomicU32,

    /// Total points aggregated
    pub points_aggregated: AtomicU32,

    /// Cache hits (if caching enabled)
    pub cache_hits: AtomicU32,

    /// Cache misses
    pub cache_misses: AtomicU32,
}

impl AggregationStats {
    /// Create new stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a query execution
    pub fn record_query(&self, series_count: u32, points_count: u32) {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.series_resolved
            .fetch_add(series_count, Ordering::Relaxed);
        self.points_aggregated
            .fetch_add(points_count, Ordering::Relaxed);
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        if hits + misses == 0.0 {
            0.0
        } else {
            hits / (hits + misses)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interned_tag_set_ordering() {
        let mut tags = InternedTagSet::new();
        tags.insert(InternedTag::new(TagKeyId(3), TagValueId(1)));
        tags.insert(InternedTag::new(TagKeyId(1), TagValueId(2)));
        tags.insert(InternedTag::new(TagKeyId(2), TagValueId(3)));

        // Should be sorted by key
        let keys: Vec<_> = tags.iter().map(|t| t.key.0).collect();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_interned_tag_set_dedup() {
        let tags = InternedTagSet::from_tags(vec![
            InternedTag::new(TagKeyId(1), TagValueId(1)),
            InternedTag::new(TagKeyId(1), TagValueId(1)), // Duplicate
            InternedTag::new(TagKeyId(2), TagValueId(2)),
        ]);

        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_interned_tag_set_hash_consistency() {
        let tags1 = InternedTagSet::from_tags(vec![
            InternedTag::new(TagKeyId(1), TagValueId(1)),
            InternedTag::new(TagKeyId(2), TagValueId(2)),
        ]);

        let tags2 = InternedTagSet::from_tags(vec![
            InternedTag::new(TagKeyId(2), TagValueId(2)), // Different order
            InternedTag::new(TagKeyId(1), TagValueId(1)),
        ]);

        assert_eq!(tags1.hash(), tags2.hash());
    }

    #[test]
    fn test_series_identifier_consistent_id() {
        let tags = InternedTagSet::from_tags(vec![
            InternedTag::new(TagKeyId(1), TagValueId(10)),
            InternedTag::new(TagKeyId(2), TagValueId(20)),
        ]);

        let series1 = SeriesIdentifier::new(TagValueId(100), tags.clone());
        let series2 = SeriesIdentifier::new(TagValueId(100), tags);

        assert_eq!(series1.series_id, series2.series_id);
    }

    #[test]
    fn test_metric_point_builder() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        let point = MetricPoint::new("cpu_usage".to_string(), 45.5, 1000)
            .with_tags(tags)
            .with_tag("dc".to_string(), "us-east".to_string());

        assert_eq!(point.metric_name, "cpu_usage");
        assert_eq!(point.value, 45.5);
        assert_eq!(point.tags.len(), 2);
    }

    #[test]
    fn test_timestamp_data() {
        let mut data = TimestampData::new(1000);
        data.add_value(1, 10.0);
        data.add_value(2, 20.0);
        data.add_value(3, 30.0);

        let values = data.values_only();
        assert_eq!(values, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_multi_series_data_timestamps() {
        let mut msd = MultiSeriesData::new();
        msd.add_series_data(1, vec![(100, 1.0), (200, 2.0), (300, 3.0)]);
        msd.add_series_data(2, vec![(150, 1.5), (200, 2.5), (350, 3.5)]);

        let timestamps = msd.all_timestamps();
        assert_eq!(timestamps, vec![100, 150, 200, 300, 350]);
    }

    #[test]
    fn test_aggregation_stats() {
        let stats = AggregationStats::new();
        stats.record_query(10, 1000);
        stats.record_cache_hit();
        stats.record_cache_hit();
        stats.record_cache_miss();

        assert_eq!(stats.queries_executed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.series_resolved.load(Ordering::Relaxed), 10);
        assert!((stats.cache_hit_rate() - 0.666).abs() < 0.01);
    }
}
