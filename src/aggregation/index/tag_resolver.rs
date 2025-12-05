//! Tag Resolver - High-Level Tag-Based Series Resolution
//!
//! This module provides the `TagResolver` which integrates the `MetadataStore`
//! with `BitmapIndex` for efficient tag-based series lookups.
//!
//! # Features
//!
//! - String-based query interface (converts strings to interned IDs internally)
//! - Support for regex and wildcard matching
//! - Automatic index maintenance when series are registered
//! - Query optimization and caching
//!
//! # Example
//!
//! ```rust
//! use kuba_tsdb::aggregation::index::{TagResolver, TagMatcher};
//! use kuba_tsdb::aggregation::MetadataStore;
//! use std::sync::Arc;
//!
//! let metadata = Arc::new(MetadataStore::in_memory());
//! let resolver = TagResolver::new(metadata);
//!
//! // Register series (automatically updates bitmap index)
//! let series_id = resolver.register_series(
//!     "cpu_usage",
//!     &[("host", "server1"), ("dc", "us-east")]
//! ).unwrap();
//!
//! // Query by tags
//! let matcher = TagMatcher::new()
//!     .with("dc", "us-east");
//!
//! let series_ids = resolver.resolve(&matcher).unwrap();
//! assert!(series_ids.contains(&series_id));
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{Error, IndexError, Result};
use crate::types::SeriesId;

use super::super::data_model::{InternedTag, TagKeyId};
use super::super::metadata::{MetadataStore, TagDictionary};
use super::bitmap::{BitmapIndex, TagFilter};

// ============================================================================
// Tag Matcher (Query Builder)
// ============================================================================

/// Match mode for tag values
#[derive(Debug, Clone)]
pub enum MatchMode {
    /// Exact string match
    Exact(String),

    /// Regex pattern match
    Regex(String),

    /// Prefix match (value starts with...)
    Prefix(String),

    /// Any value (just check key exists)
    Any,

    /// Negation (NOT this value)
    Not(Box<MatchMode>),

    /// Multiple values (OR semantics)
    OneOf(Vec<String>),
}

/// Builder for tag-based queries
///
/// Constructs a filter expression from string-based tag matchers.
/// Supports multiple match modes including exact, regex, and wildcard.
#[derive(Debug, Clone, Default)]
pub struct TagMatcher {
    /// Metric name filter (optional)
    metric: Option<String>,

    /// Tag filters: key -> match mode
    tags: HashMap<String, MatchMode>,

    /// Whether to require all tags (AND) or any tag (OR)
    require_all: bool,
}

impl TagMatcher {
    /// Create a new empty matcher
    pub fn new() -> Self {
        Self {
            metric: None,
            tags: HashMap::new(),
            require_all: true, // Default to AND semantics
        }
    }

    /// Set the metric name filter
    pub fn metric(mut self, name: &str) -> Self {
        self.metric = Some(name.to_string());
        self
    }

    /// Add an exact tag match
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.tags
            .insert(key.to_string(), MatchMode::Exact(value.to_string()));
        self
    }

    /// Add a regex tag match
    pub fn with_regex(mut self, key: &str, pattern: &str) -> Self {
        self.tags
            .insert(key.to_string(), MatchMode::Regex(pattern.to_string()));
        self
    }

    /// Add a prefix match
    pub fn with_prefix(mut self, key: &str, prefix: &str) -> Self {
        self.tags
            .insert(key.to_string(), MatchMode::Prefix(prefix.to_string()));
        self
    }

    /// Add an "any value" match (check key exists)
    pub fn has_key(mut self, key: &str) -> Self {
        self.tags.insert(key.to_string(), MatchMode::Any);
        self
    }

    /// Add a negation match
    pub fn without(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(
            key.to_string(),
            MatchMode::Not(Box::new(MatchMode::Exact(value.to_string()))),
        );
        self
    }

    /// Add multiple value match (OR semantics for values)
    pub fn with_one_of(mut self, key: &str, values: &[&str]) -> Self {
        self.tags.insert(
            key.to_string(),
            MatchMode::OneOf(values.iter().map(|s| s.to_string()).collect()),
        );
        self
    }

    /// Use OR semantics instead of AND
    pub fn any(mut self) -> Self {
        self.require_all = false;
        self
    }

    /// Get metric filter
    pub fn get_metric(&self) -> Option<&str> {
        self.metric.as_deref()
    }

    /// Get tag filters
    pub fn get_tags(&self) -> &HashMap<String, MatchMode> {
        &self.tags
    }

    /// Check if using AND semantics
    pub fn is_require_all(&self) -> bool {
        self.require_all
    }

    /// Check if matcher is empty
    pub fn is_empty(&self) -> bool {
        self.metric.is_none() && self.tags.is_empty()
    }
}

// ============================================================================
// Tag Resolver Configuration
// ============================================================================

/// Configuration for the tag resolver
#[derive(Debug, Clone)]
pub struct TagResolverConfig {
    /// Maximum series to return in a single query (prevents accidental large scans)
    pub max_results: usize,

    /// Enable query result caching
    pub enable_cache: bool,

    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,

    /// Enable regex matching (can be expensive)
    pub enable_regex: bool,

    /// Maximum regex pattern length (SEC-002: ReDoS protection)
    pub max_regex_pattern_len: usize,

    /// Maximum cache entries (SEC-005: prevent unbounded growth)
    pub max_cache_entries: usize,
}

impl Default for TagResolverConfig {
    fn default() -> Self {
        Self {
            max_results: 100_000,
            enable_cache: true,
            cache_ttl_secs: 60,
            enable_regex: true,
            max_regex_pattern_len: 256, // SEC-002: Limit regex pattern size
            max_cache_entries: 10_000,  // SEC-005: Limit cache size
        }
    }
}

// ============================================================================
// Tag Resolver Statistics
// ============================================================================

/// Statistics for the tag resolver
#[derive(Debug, Default)]
pub struct TagResolverStats {
    /// Total queries executed
    pub queries: AtomicU64,

    /// Queries that hit the cache
    pub cache_hits: AtomicU64,

    /// Queries that missed the cache
    pub cache_misses: AtomicU64,

    /// Queries that were truncated due to max_results
    pub truncated_queries: AtomicU64,

    /// Total series registrations
    pub registrations: AtomicU64,

    /// Failed regex compilations
    pub regex_errors: AtomicU64,
}

impl TagResolverStats {
    /// Get a snapshot of statistics
    pub fn snapshot(&self) -> TagResolverStatsSnapshot {
        TagResolverStatsSnapshot {
            queries: self.queries.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            truncated_queries: self.truncated_queries.load(Ordering::Relaxed),
            registrations: self.registrations.load(Ordering::Relaxed),
            regex_errors: self.regex_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of resolver statistics (non-atomic copy)
#[derive(Debug, Clone)]
pub struct TagResolverStatsSnapshot {
    /// Total queries executed
    pub queries: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Truncated queries
    pub truncated_queries: u64,
    /// Series registrations
    pub registrations: u64,
    /// Regex compilation errors
    pub regex_errors: u64,
}

// ============================================================================
// Tag Resolver
// ============================================================================

/// High-level tag resolver integrating MetadataStore with BitmapIndex
///
/// Provides a string-based query interface that internally converts
/// to interned IDs for efficient bitmap operations.
pub struct TagResolver {
    /// Metadata store for string interning and series registry
    metadata: Arc<MetadataStore>,

    /// Bitmap index for fast tag-based lookups
    bitmap_index: BitmapIndex,

    /// Configuration
    config: TagResolverConfig,

    /// Statistics
    stats: TagResolverStats,

    /// Query result cache: matcher hash -> (timestamp, series_ids)
    cache: RwLock<HashMap<u64, (std::time::Instant, Vec<SeriesId>)>>,

    /// PERF-006: Compiled regex cache to avoid recompilation
    regex_cache: RwLock<HashMap<String, regex::Regex>>,
}

impl TagResolver {
    /// Create a new tag resolver with default configuration
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self::with_config(metadata, TagResolverConfig::default())
    }

    /// Create a new tag resolver with custom configuration
    pub fn with_config(metadata: Arc<MetadataStore>, config: TagResolverConfig) -> Self {
        Self {
            metadata,
            bitmap_index: BitmapIndex::new(),
            config,
            stats: TagResolverStats::default(),
            cache: RwLock::new(HashMap::new()),
            regex_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new series and update the bitmap index
    ///
    /// Returns the series ID if registration succeeds.
    pub fn register_series(&self, metric: &str, tags: &[(&str, &str)]) -> Result<SeriesId> {
        // Register with metadata store (handles interning)
        let series_id = self.metadata.register_series(metric, tags)?;

        // Get interned tag IDs for bitmap index
        let tag_dict = self.metadata.tag_dictionary();

        // Build list of (key_id, value_id) pairs
        let mut interned_tags = Vec::with_capacity(tags.len());

        for (key, value) in tags {
            // Look up or create interned IDs
            if let (Some(key_id), Some(value_id)) =
                (tag_dict.get_key_id(key), tag_dict.get_value_id(key, value))
            {
                interned_tags.push((key_id, value_id));
            }
        }

        // Add to bitmap index
        self.bitmap_index.add_series(series_id, &interned_tags);

        self.stats.registrations.fetch_add(1, Ordering::Relaxed);

        // Invalidate cache on registration
        self.invalidate_cache();

        Ok(series_id)
    }

    /// Resolve a tag matcher to a list of series IDs
    ///
    /// Converts the string-based matcher to interned IDs, builds a
    /// TagFilter, and executes against the bitmap index.
    pub fn resolve(&self, matcher: &TagMatcher) -> Result<Vec<SeriesId>> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);

        // Check cache first
        if self.config.enable_cache {
            if let Some(cached) = self.check_cache(matcher) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(cached);
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Build filter from matcher
        let filter = self.build_filter(matcher)?;

        // Execute against bitmap index
        let bitmap = self.bitmap_index.query(&filter);

        // Collect results
        let mut results: Vec<SeriesId> = bitmap.iter().collect();

        // Apply metric filter if specified (post-filter since metric isn't in bitmap)
        if let Some(metric) = &matcher.metric {
            results = self.filter_by_metric(&results, metric);
        }

        // Truncate if needed
        if results.len() > self.config.max_results {
            self.stats.truncated_queries.fetch_add(1, Ordering::Relaxed);
            results.truncate(self.config.max_results);
        }

        // Cache results
        if self.config.enable_cache {
            self.update_cache(matcher, results.clone());
        }

        Ok(results)
    }

    /// Resolve and return full series information
    ///
    /// Returns not just IDs but complete series entries with tags.
    pub fn resolve_with_info(&self, matcher: &TagMatcher) -> Result<Vec<ResolvedSeries>> {
        let series_ids = self.resolve(matcher)?;

        let mut results = Vec::with_capacity(series_ids.len());

        for id in series_ids {
            if let Some(entry) = self.metadata.get_series(id) {
                // Resolve interned tags back to strings
                let tag_dict = self.metadata.tag_dictionary();
                let tags = self.resolve_tags(&entry.tags, tag_dict);

                results.push(ResolvedSeries {
                    series_id: id,
                    metric: entry.metric.clone(),
                    tags,
                });
            }
        }

        Ok(results)
    }

    /// Get all series for a specific metric
    pub fn get_metric_series(&self, metric: &str) -> Vec<SeriesId> {
        self.metadata.get_metric_series(metric)
    }

    /// Get statistics
    pub fn stats(&self) -> TagResolverStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get bitmap index statistics
    pub fn bitmap_stats(&self) -> (u64, u64, u64) {
        self.bitmap_index.stats()
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        self.bitmap_index.memory_bytes()
    }

    /// Invalidate the query cache
    pub fn invalidate_cache(&self) {
        self.cache.write().clear();
    }

    // ========================================================================
    // Internal Methods
    // ========================================================================

    /// Build a TagFilter from a TagMatcher
    fn build_filter(&self, matcher: &TagMatcher) -> Result<TagFilter> {
        if matcher.is_empty() {
            // No filters = return all series
            return Ok(TagFilter::All);
        }

        let tag_dict = self.metadata.tag_dictionary();
        let mut filters = Vec::new();

        for (key, mode) in &matcher.tags {
            if let Some(filter) = self.build_tag_filter(key, mode, tag_dict)? {
                filters.push(filter);
            }
        }

        if filters.is_empty() {
            return Ok(TagFilter::All);
        }

        // Combine with AND or OR based on require_all
        if matcher.require_all {
            Ok(TagFilter::and(filters))
        } else {
            Ok(TagFilter::or(filters))
        }
    }

    /// Build a filter for a single tag
    fn build_tag_filter(
        &self,
        key: &str,
        mode: &MatchMode,
        tag_dict: &TagDictionary,
    ) -> Result<Option<TagFilter>> {
        // Get the key ID (if key doesn't exist, no series can match)
        let key_id = match tag_dict.get_key_id(key) {
            Some(id) => id,
            None => return Ok(None),
        };

        match mode {
            MatchMode::Exact(value) => {
                // Exact match: look up value ID
                match tag_dict.get_value_id(key, value) {
                    Some(value_id) => Ok(Some(TagFilter::exact(key_id, value_id))),
                    None => Ok(None), // Value doesn't exist, no match
                }
            },

            MatchMode::Any => {
                // Any value for this key
                Ok(Some(TagFilter::has_key(key_id)))
            },

            MatchMode::Not(inner) => {
                // Build inner filter and negate
                if let Some(inner_filter) = self.build_tag_filter(key, inner, tag_dict)? {
                    Ok(Some(TagFilter::negate(inner_filter)))
                } else {
                    // Inner doesn't match anything, so NOT matches everything
                    Ok(Some(TagFilter::All))
                }
            },

            MatchMode::OneOf(values) => {
                // Multiple values with OR semantics
                let mut value_filters = Vec::new();

                for value in values {
                    if let Some(value_id) = tag_dict.get_value_id(key, value) {
                        value_filters.push(TagFilter::exact(key_id, value_id));
                    }
                }

                if value_filters.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(TagFilter::or(value_filters)))
                }
            },

            MatchMode::Regex(pattern) => {
                if !self.config.enable_regex {
                    return Err(Error::Index(IndexError::QueryError(
                        "Regex matching is disabled".into(),
                    )));
                }

                // Regex matching requires scanning all values for this key
                self.build_regex_filter(key_id, key, pattern, tag_dict)
            },

            MatchMode::Prefix(prefix) => {
                // Prefix matching: find all values that start with prefix
                self.build_prefix_filter(key_id, key, prefix, tag_dict)
            },
        }
    }

    /// PERF-006: Get a compiled regex from cache or compile and cache it
    ///
    /// This avoids recompiling the same regex pattern multiple times,
    /// which can be expensive for complex patterns.
    fn get_or_compile_regex(&self, pattern: &str) -> Result<regex::Regex> {
        // Check cache first (read lock)
        {
            let cache = self.regex_cache.read();
            if let Some(regex) = cache.get(pattern) {
                return Ok(regex.clone());
            }
        }

        // SEC-002: ReDoS protection - limit pattern length
        if pattern.len() > self.config.max_regex_pattern_len {
            self.stats.regex_errors.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Index(IndexError::QueryError(format!(
                "Regex pattern too long: {} chars (max: {})",
                pattern.len(),
                self.config.max_regex_pattern_len
            ))));
        }

        // Compile regex with size limit - use regex builder for safety
        let regex = match regex::RegexBuilder::new(pattern)
            .size_limit(1024 * 1024) // 1MB compiled size limit
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                self.stats.regex_errors.fetch_add(1, Ordering::Relaxed);
                // SEC-007: Truncate pattern in error message to avoid information disclosure
                let truncated = if pattern.len() > 50 {
                    format!("{}...", &pattern[..50])
                } else {
                    pattern.to_string()
                };
                return Err(Error::Index(IndexError::QueryError(format!(
                    "Invalid regex '{}': {}",
                    truncated, e
                ))));
            },
        };

        // Cache the compiled regex (write lock)
        {
            let mut cache = self.regex_cache.write();
            // Limit cache size to prevent unbounded growth
            if cache.len() < self.config.max_cache_entries {
                cache.insert(pattern.to_string(), regex.clone());
            }
        }

        Ok(regex)
    }

    /// Build a filter for regex matching
    fn build_regex_filter(
        &self,
        key_id: TagKeyId,
        key: &str,
        pattern: &str,
        tag_dict: &TagDictionary,
    ) -> Result<Option<TagFilter>> {
        // PERF-006: Use cached regex compilation
        let regex = self.get_or_compile_regex(pattern)?;

        // Find all matching values
        let mut matching_filters = Vec::new();

        // Get all values for this key and test against regex
        // Note: This requires iteration over all values (can be expensive)
        if let Some(values) = tag_dict.get_values_for_key(key) {
            for (value, value_id) in values {
                if regex.is_match(&value) {
                    matching_filters.push(TagFilter::exact(key_id, value_id));
                }
            }
        }

        if matching_filters.is_empty() {
            Ok(None)
        } else {
            Ok(Some(TagFilter::or(matching_filters)))
        }
    }

    /// Build a filter for prefix matching
    fn build_prefix_filter(
        &self,
        key_id: TagKeyId,
        key: &str,
        prefix: &str,
        tag_dict: &TagDictionary,
    ) -> Result<Option<TagFilter>> {
        let mut matching_filters = Vec::new();

        if let Some(values) = tag_dict.get_values_for_key(key) {
            for (value, value_id) in values {
                if value.starts_with(prefix) {
                    matching_filters.push(TagFilter::exact(key_id, value_id));
                }
            }
        }

        if matching_filters.is_empty() {
            Ok(None)
        } else {
            Ok(Some(TagFilter::or(matching_filters)))
        }
    }

    /// Filter series by metric name (post-filter)
    fn filter_by_metric(&self, series_ids: &[SeriesId], metric: &str) -> Vec<SeriesId> {
        let metric_series = self.metadata.get_metric_series(metric);

        // Intersect with given series IDs
        series_ids
            .iter()
            .filter(|id| metric_series.contains(id))
            .copied()
            .collect()
    }

    /// Resolve interned tags back to string pairs
    fn resolve_tags(
        &self,
        tags: &[InternedTag],
        tag_dict: &TagDictionary,
    ) -> Vec<(String, String)> {
        tags.iter()
            .filter_map(|tag| {
                let key = tag_dict.resolve_key(tag.key)?;
                let value = tag_dict.resolve_value(tag.key, tag.value)?;
                Some((key, value))
            })
            .collect()
    }

    /// Check if result is in cache
    fn check_cache(&self, matcher: &TagMatcher) -> Option<Vec<SeriesId>> {
        let hash = self.hash_matcher(matcher);
        let cache = self.cache.read();

        if let Some((timestamp, results)) = cache.get(&hash) {
            // Check if still valid
            if timestamp.elapsed().as_secs() < self.config.cache_ttl_secs {
                return Some(results.clone());
            }
        }

        None
    }

    /// Update cache with query result
    fn update_cache(&self, matcher: &TagMatcher, results: Vec<SeriesId>) {
        let hash = self.hash_matcher(matcher);
        let mut cache = self.cache.write();

        // SEC-005: Enforce cache size limit - evict oldest entries if needed
        if cache.len() >= self.config.max_cache_entries {
            // Simple eviction: remove entries older than TTL first
            let now = std::time::Instant::now();
            let ttl = std::time::Duration::from_secs(self.config.cache_ttl_secs);
            cache.retain(|_, (ts, _)| now.duration_since(*ts) < ttl);

            // If still too full, remove oldest half
            if cache.len() >= self.config.max_cache_entries {
                let mut entries: Vec<_> = cache.iter().map(|(k, (ts, _))| (*k, *ts)).collect();
                entries.sort_by_key(|(_, ts)| *ts);
                let to_remove = entries.len() / 2;
                for (key, _) in entries.into_iter().take(to_remove) {
                    cache.remove(&key);
                }
            }
        }

        cache.insert(hash, (std::time::Instant::now(), results));
    }

    /// Hash a matcher for cache key
    fn hash_matcher(&self, matcher: &TagMatcher) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash metric
        matcher.metric.hash(&mut hasher);

        // Hash tags (sorted for consistent ordering)
        let mut sorted_tags: Vec<_> = matcher.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| *k);

        for (key, mode) in sorted_tags {
            key.hash(&mut hasher);
            // Hash mode discriminant
            std::mem::discriminant(mode).hash(&mut hasher);
        }

        matcher.require_all.hash(&mut hasher);

        hasher.finish()
    }
}

// ============================================================================
// Resolved Series Info
// ============================================================================

/// A resolved series with full information
#[derive(Debug, Clone)]
pub struct ResolvedSeries {
    /// The series ID
    pub series_id: SeriesId,

    /// The metric name
    pub metric: String,

    /// Resolved tag key-value pairs
    pub tags: Vec<(String, String)>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::metadata::MetadataConfig;

    fn create_test_resolver() -> TagResolver {
        let config = MetadataConfig::default();
        let metadata = Arc::new(MetadataStore::new(config));
        TagResolver::new(metadata)
    }

    #[test]
    fn test_register_and_resolve() {
        let resolver = create_test_resolver();

        // Register some series
        let id1 = resolver
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        let id2 = resolver
            .register_series("cpu_usage", &[("host", "server2"), ("dc", "us-east")])
            .unwrap();

        let _id3 = resolver
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-west")])
            .unwrap();

        // Query by single tag
        let matcher = TagMatcher::new().with("dc", "us-east");
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&id1));
        assert!(results.contains(&id2));
    }

    #[test]
    fn test_resolve_and_filter() {
        let resolver = create_test_resolver();

        resolver
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        resolver
            .register_series("cpu_usage", &[("host", "server2"), ("dc", "us-east")])
            .unwrap();

        resolver
            .register_series("memory_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        // Query by tag AND metric
        let matcher = TagMatcher::new()
            .metric("cpu_usage")
            .with("host", "server1");

        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_and_semantics() {
        let resolver = create_test_resolver();

        let id1 = resolver
            .register_series(
                "cpu",
                &[("host", "server1"), ("dc", "us-east"), ("env", "prod")],
            )
            .unwrap();

        resolver
            .register_series(
                "cpu",
                &[("host", "server1"), ("dc", "us-west"), ("env", "prod")],
            )
            .unwrap();

        resolver
            .register_series(
                "cpu",
                &[("host", "server2"), ("dc", "us-east"), ("env", "staging")],
            )
            .unwrap();

        // host=server1 AND dc=us-east AND env=prod
        let matcher = TagMatcher::new()
            .with("host", "server1")
            .with("dc", "us-east")
            .with("env", "prod");

        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], id1);
    }

    #[test]
    fn test_or_semantics() {
        let resolver = create_test_resolver();

        let id1 = resolver
            .register_series("cpu", &[("dc", "us-east")])
            .unwrap();

        let id2 = resolver
            .register_series("cpu", &[("dc", "us-west")])
            .unwrap();

        resolver
            .register_series("cpu", &[("dc", "eu-west")])
            .unwrap();

        // dc=us-east OR dc=us-west
        let matcher = TagMatcher::new().with_one_of("dc", &["us-east", "us-west"]);

        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&id1));
        assert!(results.contains(&id2));
    }

    #[test]
    fn test_has_key() {
        let resolver = create_test_resolver();

        let id1 = resolver
            .register_series("cpu", &[("host", "server1"), ("env", "prod")])
            .unwrap();

        resolver
            .register_series("cpu", &[("host", "server2")])
            .unwrap();

        // Has 'env' key
        let matcher = TagMatcher::new().has_key("env");
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], id1);
    }

    #[test]
    fn test_prefix_match() {
        let resolver = create_test_resolver();

        let id1 = resolver
            .register_series("cpu", &[("host", "prod-server-1")])
            .unwrap();

        let id2 = resolver
            .register_series("cpu", &[("host", "prod-server-2")])
            .unwrap();

        resolver
            .register_series("cpu", &[("host", "dev-server-1")])
            .unwrap();

        // host starts with "prod-"
        let matcher = TagMatcher::new().with_prefix("host", "prod-");
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&id1));
        assert!(results.contains(&id2));
    }

    #[test]
    fn test_regex_match() {
        let resolver = create_test_resolver();

        let id1 = resolver
            .register_series("cpu", &[("host", "server-01")])
            .unwrap();

        let id2 = resolver
            .register_series("cpu", &[("host", "server-02")])
            .unwrap();

        resolver
            .register_series("cpu", &[("host", "database-01")])
            .unwrap();

        // host matches server-\d+
        let matcher = TagMatcher::new().with_regex("host", r"^server-\d+$");
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&id1));
        assert!(results.contains(&id2));
    }

    #[test]
    fn test_negation() {
        let resolver = create_test_resolver();

        resolver.register_series("cpu", &[("env", "prod")]).unwrap();

        let id2 = resolver
            .register_series("cpu", &[("env", "staging")])
            .unwrap();

        let id3 = resolver.register_series("cpu", &[("env", "dev")]).unwrap();

        // NOT env=prod
        let matcher = TagMatcher::new().without("env", "prod");
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains(&id2));
        assert!(results.contains(&id3));
    }

    #[test]
    fn test_resolve_with_info() {
        let resolver = create_test_resolver();

        resolver
            .register_series("cpu_usage", &[("host", "server1"), ("dc", "us-east")])
            .unwrap();

        let matcher = TagMatcher::new().with("host", "server1");
        let results = resolver.resolve_with_info(&matcher).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric, "cpu_usage");
        assert!(results[0]
            .tags
            .contains(&("host".to_string(), "server1".to_string())));
        assert!(results[0]
            .tags
            .contains(&("dc".to_string(), "us-east".to_string())));
    }

    #[test]
    fn test_empty_matcher() {
        let resolver = create_test_resolver();

        resolver.register_series("cpu", &[("host", "s1")]).unwrap();
        resolver.register_series("cpu", &[("host", "s2")]).unwrap();

        // Empty matcher should return all
        let matcher = TagMatcher::new();
        let results = resolver.resolve(&matcher).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_cache_behavior() {
        let config = MetadataConfig::default();
        let metadata = Arc::new(MetadataStore::new(config));

        let resolver_config = TagResolverConfig {
            enable_cache: true,
            cache_ttl_secs: 3600, // Long TTL for test
            ..Default::default()
        };

        let resolver = TagResolver::with_config(metadata, resolver_config);

        resolver.register_series("cpu", &[("host", "s1")]).unwrap();

        let matcher = TagMatcher::new().with("host", "s1");

        // First query (cache miss)
        resolver.resolve(&matcher).unwrap();
        let stats = resolver.stats();
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hits, 0);

        // Second query (cache hit)
        resolver.resolve(&matcher).unwrap();
        let stats = resolver.stats();
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hits, 1);
    }

    #[test]
    fn test_stats() {
        let resolver = create_test_resolver();

        resolver.register_series("cpu", &[("host", "s1")]).unwrap();
        resolver.register_series("cpu", &[("host", "s2")]).unwrap();

        let stats = resolver.stats();
        assert_eq!(stats.registrations, 2);

        let (tag_combos, series_count, _) = resolver.bitmap_stats();
        assert!(tag_combos >= 1); // At least 1 tag combo (host)
        assert_eq!(series_count, 2);
    }
}
