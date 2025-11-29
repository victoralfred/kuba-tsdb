//! Metadata Storage for Series Registry and Tag Indexes
//!
//! This module provides the metadata layer that connects metrics, tags, and series:
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │  Metric Name    │────▶│  Tag Set        │────▶│  Series ID      │
//! │  "cpu_usage"    │     │  {host:s1,dc:e} │     │  0x7a3f...      │
//! └─────────────────┘     └─────────────────┘     └─────────────────┘
//! ```
//!
//! # Storage Architecture
//!
//! - **Local (sled)**: Fast reads, no network overhead, works offline
//! - **Redis**: Distributed coordination, cross-node consistency
//!
//! # Components
//!
//! - `StringInterner`: Compact storage for tag strings via ID mapping
//! - `SeriesRegistry`: Maps metric + tags → series_id
//! - `MetadataStore`: Unified interface for all metadata operations

pub mod string_intern;

pub use string_intern::{
    InternError, StringInterner, TagDictionary, TagDictionaryStats, TagKeyInterner,
    TagValueInterner, MAX_METRIC_NAME_LENGTH, MAX_TAG_KEY_LENGTH, MAX_TAG_VALUE_LENGTH,
};

// Re-export SeriesEntryResolved (defined below)

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::data_model::{InternedTag, InternedTagSet, MetricFamily, TagKeyId, TagValueId};
use crate::error::Error;
use crate::types::SeriesId;

// ============================================================================
// Series Registry Entry
// ============================================================================

/// A registered series with its metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesEntry {
    /// The series ID
    pub series_id: SeriesId,

    /// Metric name ID (interned)
    pub metric_id: TagValueId,

    /// Interned tag set
    pub tags: Vec<(u32, u32)>, // (key_id, value_id) pairs for serde

    /// Creation timestamp
    pub created_at: i64,

    /// Last write timestamp
    pub last_write_at: i64,
}

impl SeriesEntry {
    /// Create a new series entry
    pub fn new(series_id: SeriesId, metric_id: TagValueId, tags: &InternedTagSet) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            series_id,
            metric_id,
            tags: tags.iter().map(|t| (t.key.0, t.value.0)).collect(),
            created_at: now,
            last_write_at: now,
        }
    }

    /// Get the interned tag set
    pub fn tag_set(&self) -> InternedTagSet {
        InternedTagSet::from_tags(
            self.tags
                .iter()
                .map(|(k, v)| InternedTag::new(TagKeyId(*k), TagValueId(*v)))
                .collect(),
        )
    }

    /// Update last write timestamp
    pub fn touch(&mut self) {
        self.last_write_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
    }
}

// ============================================================================
// In-Memory Series Registry
// ============================================================================

/// In-memory series registry for fast lookups
///
/// Maintains bidirectional mappings:
/// - series_id → SeriesEntry
/// - metric_id → series_ids list
/// - (metric_id, tag_hash) → series_id
#[derive(Debug)]
pub struct SeriesRegistry {
    /// All registered series
    series: RwLock<HashMap<SeriesId, SeriesEntry>>,

    /// Metric to series mapping
    metric_series: RwLock<HashMap<TagValueId, Vec<SeriesId>>>,

    /// Fast lookup by metric + tag hash
    lookup_index: RwLock<HashMap<(TagValueId, u64), SeriesId>>,

    /// Next series ID (for sequential assignment)
    next_series_id: std::sync::atomic::AtomicU64,
}

impl SeriesRegistry {
    /// Create a new series registry
    pub fn new() -> Self {
        Self {
            series: RwLock::new(HashMap::new()),
            metric_series: RwLock::new(HashMap::new()),
            lookup_index: RwLock::new(HashMap::new()),
            next_series_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Register a new series or get existing
    ///
    /// Returns the series ID (existing or newly created).
    pub fn get_or_create(&self, metric_id: TagValueId, tags: &InternedTagSet) -> SeriesId {
        let tag_hash = tags.hash();
        let lookup_key = (metric_id, tag_hash);

        // Fast path: check if already registered
        {
            let lookup = self.lookup_index.read();
            if let Some(&series_id) = lookup.get(&lookup_key) {
                return series_id;
            }
        }

        // Slow path: need to register
        let mut lookup = self.lookup_index.write();

        // Double-check after acquiring write lock
        if let Some(&series_id) = lookup.get(&lookup_key) {
            return series_id;
        }

        // Create new series
        // PERF-009: Relaxed is sufficient - we only need uniqueness, not ordering
        let series_id = self
            .next_series_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as SeriesId;
        let entry = SeriesEntry::new(series_id, metric_id, tags);

        // Update all indexes
        lookup.insert(lookup_key, series_id);

        let mut series = self.series.write();
        series.insert(series_id, entry);

        let mut metric_series = self.metric_series.write();
        metric_series.entry(metric_id).or_default().push(series_id);

        series_id
    }

    /// Look up a series by metric and tags
    pub fn lookup(&self, metric_id: TagValueId, tags: &InternedTagSet) -> Option<SeriesId> {
        let tag_hash = tags.hash();
        self.lookup_index
            .read()
            .get(&(metric_id, tag_hash))
            .copied()
    }

    /// Get a series entry by ID
    pub fn get(&self, series_id: SeriesId) -> Option<SeriesEntry> {
        self.series.read().get(&series_id).cloned()
    }

    /// Get all series for a metric
    pub fn get_metric_series(&self, metric_id: TagValueId) -> Vec<SeriesId> {
        self.metric_series
            .read()
            .get(&metric_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get total number of registered series
    pub fn series_count(&self) -> usize {
        self.series.read().len()
    }

    /// Get number of unique metrics
    pub fn metric_count(&self) -> usize {
        self.metric_series.read().len()
    }

    /// Update the last write time for a series
    pub fn touch(&self, series_id: SeriesId) {
        if let Some(entry) = self.series.write().get_mut(&series_id) {
            entry.touch();
        }
    }

    /// Get metric family information
    pub fn get_metric_family(&self, metric_id: TagValueId) -> Option<MetricFamily> {
        let series_ids = self.get_metric_series(metric_id);
        if series_ids.is_empty() {
            return None;
        }

        let series = self.series.read();
        let mut family = MetricFamily::new(metric_id);

        for series_id in &series_ids {
            if let Some(entry) = series.get(series_id) {
                let tag_keys: Vec<TagKeyId> =
                    entry.tags.iter().map(|(k, _)| TagKeyId(*k)).collect();
                family.add_series(*series_id, &tag_keys);
            }
        }

        Some(family)
    }

    /// Export all series entries (for persistence)
    pub fn export_all(&self) -> Vec<SeriesEntry> {
        self.series.read().values().cloned().collect()
    }

    /// Import series entries (for loading from persistence)
    pub fn import(&self, entries: Vec<SeriesEntry>) {
        let mut series = self.series.write();
        let mut metric_series = self.metric_series.write();
        let mut lookup = self.lookup_index.write();

        let mut max_id: u64 = 0;

        for entry in entries {
            let tag_set = entry.tag_set();
            let tag_hash = tag_set.hash();

            if entry.series_id as u64 > max_id {
                max_id = entry.series_id as u64;
            }

            lookup.insert((entry.metric_id, tag_hash), entry.series_id);
            metric_series
                .entry(entry.metric_id)
                .or_default()
                .push(entry.series_id);
            series.insert(entry.series_id, entry);
        }

        // Update next_series_id to be after the highest loaded ID
        // PERF-009: Release is sufficient - just ensure the write is visible to future reads
        self.next_series_id
            .store(max_id + 1, std::sync::atomic::Ordering::Release);
    }
}

impl Default for SeriesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Metadata Store (Unified Interface)
// ============================================================================

/// Configuration for the metadata store
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Path for local sled database
    pub db_path: Option<std::path::PathBuf>,

    /// Whether to persist to disk
    pub persist: bool,

    /// Maximum number of series to allow (cardinality limit)
    pub max_series: usize,

    /// Flush interval for persistence
    pub flush_interval_secs: u64,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            db_path: None,
            persist: false,
            max_series: 1_000_000, // 1M series default limit
            flush_interval_secs: 60,
        }
    }
}

/// Unified metadata store combining string interning and series registry
///
/// This is the main entry point for all metadata operations.
#[derive(Debug)]
pub struct MetadataStore {
    /// Configuration
    config: MetadataConfig,

    /// Tag dictionary (string interning)
    pub dictionary: Arc<TagDictionary>,

    /// Series registry
    pub registry: Arc<SeriesRegistry>,
}

impl MetadataStore {
    /// Create a new metadata store
    pub fn new(config: MetadataConfig) -> Self {
        Self {
            config,
            dictionary: Arc::new(TagDictionary::new()),
            registry: Arc::new(SeriesRegistry::new()),
        }
    }

    /// Create with default configuration
    pub fn in_memory() -> Self {
        Self::new(MetadataConfig::default())
    }

    /// Register a series from raw metric point data
    ///
    /// Interns all strings and returns the series ID.
    pub fn register_series(
        &self,
        metric_name: &str,
        tags: &[(&str, &str)],
    ) -> Result<SeriesId, Error> {
        // Check cardinality limit
        if self.registry.series_count() >= self.config.max_series {
            return Err(Error::Configuration(format!(
                "Series limit exceeded: {} >= {}",
                self.registry.series_count(),
                self.config.max_series
            )));
        }

        // Intern metric name
        let metric_id = self.dictionary.intern_metric(metric_name);

        // Intern tags
        let interned_tags: Vec<InternedTag> = tags
            .iter()
            .map(|(k, v)| {
                let (key_id, value_id) = self.dictionary.intern_tag(k, v);
                InternedTag::new(key_id, value_id)
            })
            .collect();

        let tag_set = InternedTagSet::from_tags(interned_tags);

        // Get or create series
        let series_id = self.registry.get_or_create(metric_id, &tag_set);

        Ok(series_id)
    }

    /// Look up series by metric name and tags
    pub fn lookup_series(&self, metric_name: &str, tags: &[(&str, &str)]) -> Option<SeriesId> {
        // Get metric ID
        let metric_id = self.dictionary.get_metric_id(metric_name)?;

        // Get tag IDs (all must exist)
        let interned_tags: Option<Vec<InternedTag>> = tags
            .iter()
            .map(|(k, v)| {
                let key_id = self.dictionary.get_key_id(k)?;
                let value_id = self.dictionary.get_value_id(k, v)?;
                Some(InternedTag::new(key_id, value_id))
            })
            .collect();

        let tag_set = InternedTagSet::from_tags(interned_tags?);

        self.registry.lookup(metric_id, &tag_set)
    }

    /// Get all series for a metric name
    pub fn get_metric_series(&self, metric_name: &str) -> Vec<SeriesId> {
        match self.dictionary.get_metric_id(metric_name) {
            Some(metric_id) => self.registry.get_metric_series(metric_id),
            None => Vec::new(),
        }
    }

    /// Resolve a series ID to its metric name and tags
    pub fn resolve_series(&self, series_id: SeriesId) -> Option<(String, HashMap<String, String>)> {
        let entry = self.registry.get(series_id)?;

        let metric_name = self.dictionary.resolve_metric(entry.metric_id)?;

        let tags: HashMap<String, String> = entry
            .tags
            .iter()
            .filter_map(|(key_id, value_id)| {
                let key = self.dictionary.resolve_key(TagKeyId(*key_id))?;
                let value = self
                    .dictionary
                    .resolve_value(TagKeyId(*key_id), TagValueId(*value_id))?;
                Some((key, value))
            })
            .collect();

        Some((metric_name, tags))
    }

    /// Get statistics about the metadata store
    pub fn stats(&self) -> MetadataStats {
        let dict_stats = self.dictionary.stats();

        MetadataStats {
            series_count: self.registry.series_count(),
            metric_count: self.registry.metric_count(),
            tag_key_count: dict_stats.key_count,
            tag_value_count: dict_stats.total_value_count,
            memory_bytes: dict_stats.memory_bytes,
            max_series: self.config.max_series,
        }
    }

    /// Get the tag dictionary
    pub fn dictionary(&self) -> &Arc<TagDictionary> {
        &self.dictionary
    }

    /// Get the tag dictionary (alias for compatibility)
    pub fn tag_dictionary(&self) -> &TagDictionary {
        &self.dictionary
    }

    /// Get the series registry
    pub fn registry(&self) -> &Arc<SeriesRegistry> {
        &self.registry
    }

    /// Get a series entry by ID
    ///
    /// Returns the entry with the metric name resolved and tags as InternedTag.
    pub fn get_series(&self, series_id: SeriesId) -> Option<SeriesEntryResolved> {
        let entry = self.registry.get(series_id)?;
        let metric_name = self.dictionary.resolve_metric(entry.metric_id)?;

        let tags: Vec<InternedTag> = entry
            .tags
            .iter()
            .map(|(k, v)| InternedTag::new(TagKeyId(*k), TagValueId(*v)))
            .collect();

        Some(SeriesEntryResolved {
            series_id,
            metric: metric_name,
            tags,
        })
    }
}

/// A resolved series entry with metric name and interned tags
#[derive(Debug, Clone)]
pub struct SeriesEntryResolved {
    /// The series ID
    pub series_id: SeriesId,

    /// Resolved metric name
    pub metric: String,

    /// Interned tag set
    pub tags: Vec<InternedTag>,
}

/// Statistics about the metadata store
#[derive(Debug, Clone)]
pub struct MetadataStats {
    /// Total number of series
    pub series_count: usize,

    /// Number of unique metrics
    pub metric_count: usize,

    /// Number of unique tag keys
    pub tag_key_count: usize,

    /// Total number of unique tag values
    pub tag_value_count: usize,

    /// Estimated memory usage in bytes
    pub memory_bytes: usize,

    /// Maximum allowed series
    pub max_series: usize,
}

impl MetadataStats {
    /// Get series utilization as a percentage
    pub fn series_utilization(&self) -> f64 {
        self.series_count as f64 / self.max_series as f64
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_registry_basic() {
        let registry = SeriesRegistry::new();
        let dict = TagDictionary::new();

        let metric_id = dict.intern_metric("cpu_usage");
        let tags = InternedTagSet::from_tags(vec![InternedTag::new(
            dict.intern_key("host"),
            dict.intern_value(dict.intern_key("host"), "server1"),
        )]);

        let series1 = registry.get_or_create(metric_id, &tags);
        let series2 = registry.get_or_create(metric_id, &tags);

        assert_eq!(series1, series2);
        assert_eq!(registry.series_count(), 1);
    }

    #[test]
    fn test_series_registry_multiple_series() {
        let registry = SeriesRegistry::new();
        let dict = TagDictionary::new();

        let metric_id = dict.intern_metric("cpu_usage");
        let host_key = dict.intern_key("host");

        let tags1 = InternedTagSet::from_tags(vec![InternedTag::new(
            host_key,
            dict.intern_value(host_key, "server1"),
        )]);

        let tags2 = InternedTagSet::from_tags(vec![InternedTag::new(
            host_key,
            dict.intern_value(host_key, "server2"),
        )]);

        let series1 = registry.get_or_create(metric_id, &tags1);
        let series2 = registry.get_or_create(metric_id, &tags2);

        assert_ne!(series1, series2);
        assert_eq!(registry.series_count(), 2);
        assert_eq!(registry.get_metric_series(metric_id).len(), 2);
    }

    #[test]
    fn test_metadata_store_register() {
        let store = MetadataStore::in_memory();

        let series1 = store
            .register_series("cpu_usage", &[("host", "server1")])
            .unwrap();
        let series2 = store
            .register_series("cpu_usage", &[("host", "server1")])
            .unwrap();
        let series3 = store
            .register_series("cpu_usage", &[("host", "server2")])
            .unwrap();

        assert_eq!(series1, series2);
        assert_ne!(series1, series3);
    }

    #[test]
    fn test_metadata_store_lookup() {
        let store = MetadataStore::in_memory();

        let series_id = store
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        let lookup = store.lookup_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")]);
        assert_eq!(lookup, Some(series_id));

        let lookup_missing = store.lookup_series("cpu_usage", &[("host", "server2")]);
        assert_eq!(lookup_missing, None);
    }

    #[test]
    fn test_metadata_store_resolve() {
        let store = MetadataStore::in_memory();

        let series_id = store
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        let resolved = store.resolve_series(series_id).unwrap();
        assert_eq!(resolved.0, "cpu_usage");
        assert_eq!(resolved.1.get("host"), Some(&"server1".to_string()));
        assert_eq!(resolved.1.get("dc"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_metadata_store_cardinality_limit() {
        let config = MetadataConfig {
            max_series: 2,
            ..Default::default()
        };
        let store = MetadataStore::new(config);

        store.register_series("cpu", &[("host", "s1")]).unwrap();
        store.register_series("cpu", &[("host", "s2")]).unwrap();

        // Third unique series should fail
        let result = store.register_series("cpu", &[("host", "s3")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_stats() {
        let store = MetadataStore::in_memory();

        store
            .register_series("cpu_usage", &[("host", "server1")])
            .unwrap();
        store
            .register_series("cpu_usage", &[("host", "server2")])
            .unwrap();
        store
            .register_series("memory_free", &[("host", "server1")])
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.series_count, 3);
        assert_eq!(stats.metric_count, 2);
        assert_eq!(stats.tag_key_count, 1); // Just "host"
        assert_eq!(stats.tag_value_count, 2); // "server1", "server2"
    }

    #[test]
    fn test_series_entry_serialization() {
        let entry = SeriesEntry {
            series_id: 12345,
            metric_id: TagValueId(1),
            tags: vec![(1, 10), (2, 20)],
            created_at: 1000,
            last_write_at: 2000,
        };

        let serialized = serde_json::to_string(&entry).unwrap();
        let deserialized: SeriesEntry = serde_json::from_str(&serialized).unwrap();

        assert_eq!(entry.series_id, deserialized.series_id);
        assert_eq!(entry.tags, deserialized.tags);
    }

    #[test]
    fn test_series_registry_import_export() {
        let registry1 = SeriesRegistry::new();
        let dict = TagDictionary::new();

        let metric_id = dict.intern_metric("test");
        let host_key = dict.intern_key("host");

        for i in 0..10 {
            let tags = InternedTagSet::from_tags(vec![InternedTag::new(
                host_key,
                dict.intern_value(host_key, &format!("server{}", i)),
            )]);
            registry1.get_or_create(metric_id, &tags);
        }

        let exported = registry1.export_all();
        assert_eq!(exported.len(), 10);

        let registry2 = SeriesRegistry::new();
        registry2.import(exported);

        assert_eq!(registry2.series_count(), 10);
    }
}
