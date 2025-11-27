//! Tag-based affinity routing
//!
//! Routes series to shards based on specific tag values, ensuring
//! series with the same tag value are co-located on the same shard.
//!
//! # Use Cases
//!
//! - Co-locate all metrics from the same host
//! - Group series by datacenter or region
//! - Optimize queries that filter by specific tags
//!
//! # Algorithm
//!
//! 1. Extract the affinity tag value from the series key
//! 2. Hash the tag value to determine shard assignment
//! 3. Fall back to full key hash if tag is missing
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::routing::{RoutingStrategy, TagAffinityRouter};
//!
//! // Route by "host" tag - all series from same host go to same shard
//! let router = TagAffinityRouter::new(4, "host".to_string());
//! let shard = router.route("cpu,host=server01,region=us-east");
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::{RoutingStrategy, ShardId};

/// Tag-based affinity router
///
/// Routes series to shards based on a specific tag value, ensuring
/// all series with the same tag value are co-located.
pub struct TagAffinityRouter {
    /// Number of physical shards
    num_shards: usize,
    /// Tag key to use for affinity routing
    affinity_tag: String,
    /// Whether to use fallback hash when tag is missing
    use_fallback: bool,
    /// Default shard for series without the affinity tag (if not using fallback)
    default_shard: ShardId,
}

impl TagAffinityRouter {
    /// Create a new tag affinity router
    ///
    /// # Arguments
    ///
    /// * `num_shards` - Number of physical shards
    /// * `affinity_tag` - Tag key to use for routing decisions
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is 0.
    pub fn new(num_shards: usize, affinity_tag: String) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        Self {
            num_shards,
            affinity_tag,
            use_fallback: true,
            default_shard: 0,
        }
    }

    /// Create a router with a default shard for missing tags
    ///
    /// Instead of falling back to hash-based routing when the affinity
    /// tag is missing, routes to a specific default shard.
    pub fn with_default_shard(
        num_shards: usize,
        affinity_tag: String,
        default_shard: ShardId,
    ) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(
            default_shard < num_shards,
            "default_shard must be < num_shards"
        );
        Self {
            num_shards,
            affinity_tag,
            use_fallback: false,
            default_shard,
        }
    }

    /// Get the affinity tag key
    pub fn affinity_tag(&self) -> &str {
        &self.affinity_tag
    }

    /// Set whether to use fallback hash routing
    pub fn set_use_fallback(&mut self, use_fallback: bool) {
        self.use_fallback = use_fallback;
    }

    /// Extract the affinity tag value from a series key
    ///
    /// Series key format: measurement,tag1=value1,tag2=value2
    ///
    /// Returns None if the tag is not found.
    fn extract_tag_value<'a>(&self, series_key: &'a str) -> Option<&'a str> {
        // Find the tag in the series key
        // Format: measurement,tag1=value1,tag2=value2,...

        let tag_prefix = format!("{}=", self.affinity_tag);

        // Look for the tag after a comma or at start of tags section
        for part in series_key.split(',').skip(1) {
            // Skip the measurement name (first part)
            if let Some(value_start) = part.strip_prefix(&tag_prefix) {
                // Value ends at the next comma or end of string
                // Since we split by comma, value_start is the full value
                return Some(value_start);
            }
        }

        None
    }

    /// Compute hash of a string using DefaultHasher
    fn hash(s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Route based on the full series key (fallback)
    fn route_by_key(&self, series_key: &str) -> ShardId {
        let hash = Self::hash(series_key);
        (hash as usize) % self.num_shards
    }

    /// Route based on tag value
    fn route_by_tag(&self, tag_value: &str) -> ShardId {
        let hash = Self::hash(tag_value);
        (hash as usize) % self.num_shards
    }

    /// Get statistics about tag extraction
    ///
    /// Useful for debugging routing decisions.
    pub fn analyze_series(&self, series_key: &str) -> TagAffinityAnalysis {
        let tag_value = self.extract_tag_value(series_key);
        let shard = self.route(series_key);

        TagAffinityAnalysis {
            series_key: series_key.to_string(),
            affinity_tag: self.affinity_tag.clone(),
            tag_value: tag_value.map(|s| s.to_string()),
            routed_shard: shard,
            used_fallback: tag_value.is_none() && self.use_fallback,
        }
    }
}

impl RoutingStrategy for TagAffinityRouter {
    fn route(&self, series_key: &str) -> ShardId {
        // Try to extract the affinity tag value
        if let Some(tag_value) = self.extract_tag_value(series_key) {
            return self.route_by_tag(tag_value);
        }

        // Tag not found - use fallback or default
        if self.use_fallback {
            self.route_by_key(series_key)
        } else {
            self.default_shard
        }
    }

    fn shard_count(&self) -> usize {
        self.num_shards
    }

    fn name(&self) -> &'static str {
        "tag-affinity"
    }

    fn supports_rebalancing(&self) -> bool {
        false
    }
}

/// Analysis result for tag affinity routing
#[derive(Debug, Clone)]
pub struct TagAffinityAnalysis {
    /// The series key that was analyzed
    pub series_key: String,
    /// The affinity tag being used
    pub affinity_tag: String,
    /// The extracted tag value, if found
    pub tag_value: Option<String>,
    /// The shard this series routes to
    pub routed_shard: ShardId,
    /// Whether fallback hash routing was used
    pub used_fallback: bool,
}

/// Builder for TagAffinityRouter
pub struct TagAffinityRouterBuilder {
    num_shards: usize,
    affinity_tag: String,
    use_fallback: bool,
    default_shard: ShardId,
}

impl TagAffinityRouterBuilder {
    /// Create a new builder
    pub fn new(num_shards: usize, affinity_tag: impl Into<String>) -> Self {
        Self {
            num_shards,
            affinity_tag: affinity_tag.into(),
            use_fallback: true,
            default_shard: 0,
        }
    }

    /// Disable fallback hash routing and use a default shard instead
    pub fn default_shard(mut self, shard: ShardId) -> Self {
        self.use_fallback = false;
        self.default_shard = shard;
        self
    }

    /// Enable fallback hash routing when tag is missing
    pub fn with_fallback(mut self) -> Self {
        self.use_fallback = true;
        self
    }

    /// Build the router
    pub fn build(self) -> TagAffinityRouter {
        if self.use_fallback {
            TagAffinityRouter::new(self.num_shards, self.affinity_tag)
        } else {
            TagAffinityRouter::with_default_shard(
                self.num_shards,
                self.affinity_tag,
                self.default_shard,
            )
        }
    }
}

/// Multi-tag affinity router
///
/// Routes based on multiple tags with priority ordering.
/// Falls through to next tag if previous tag is not found.
pub struct MultiTagAffinityRouter {
    /// Number of physical shards
    num_shards: usize,
    /// Ordered list of affinity tags (first match wins)
    affinity_tags: Vec<String>,
}

impl MultiTagAffinityRouter {
    /// Create a new multi-tag affinity router
    ///
    /// # Arguments
    ///
    /// * `num_shards` - Number of physical shards
    /// * `affinity_tags` - Ordered list of tag keys (first found wins)
    pub fn new(num_shards: usize, affinity_tags: Vec<String>) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(!affinity_tags.is_empty(), "affinity_tags must not be empty");
        Self {
            num_shards,
            affinity_tags,
        }
    }

    /// Extract the first matching tag value from a series key
    fn extract_first_tag_value<'a>(&self, series_key: &'a str) -> Option<&'a str> {
        for tag in &self.affinity_tags {
            let tag_prefix = format!("{}=", tag);
            for part in series_key.split(',').skip(1) {
                if let Some(value) = part.strip_prefix(&tag_prefix) {
                    return Some(value);
                }
            }
        }
        None
    }

    /// Compute hash of a string
    fn hash(s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }
}

impl RoutingStrategy for MultiTagAffinityRouter {
    fn route(&self, series_key: &str) -> ShardId {
        let hash_input = self
            .extract_first_tag_value(series_key)
            .unwrap_or(series_key);

        let hash = Self::hash(hash_input);
        (hash as usize) % self.num_shards
    }

    fn shard_count(&self) -> usize {
        self.num_shards
    }

    fn name(&self) -> &'static str {
        "multi-tag-affinity"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_affinity_basic() {
        let router = TagAffinityRouter::new(4, "host".to_string());

        // Same host should route to same shard
        let shard1 = router.route("cpu,host=server01,region=us-east");
        let shard2 = router.route("memory,host=server01,region=us-west");
        assert_eq!(shard1, shard2, "Same host should route to same shard");

        // Different hosts may route differently
        let shard3 = router.route("cpu,host=server02,region=us-east");
        // Note: might be same shard due to hash collision, just check valid
        assert!(shard3 < 4);
    }

    #[test]
    fn test_tag_affinity_extraction() {
        let router = TagAffinityRouter::new(4, "host".to_string());

        // Extract tag value
        let value = router.extract_tag_value("cpu,host=server01,region=us-east");
        assert_eq!(value, Some("server01"));

        // Tag not first
        let value = router.extract_tag_value("cpu,region=us-east,host=server02");
        assert_eq!(value, Some("server02"));

        // Tag missing
        let value = router.extract_tag_value("cpu,region=us-east");
        assert_eq!(value, None);
    }

    #[test]
    fn test_tag_affinity_fallback() {
        let router = TagAffinityRouter::new(4, "host".to_string());

        // When tag is missing, should use key hash
        let shard1 = router.route("cpu,region=us-east");
        let shard2 = router.route("cpu,region=us-east");
        assert_eq!(shard1, shard2, "Same key should route consistently");
    }

    #[test]
    fn test_tag_affinity_default_shard() {
        let router = TagAffinityRouter::with_default_shard(4, "host".to_string(), 2);

        // When tag is missing, should use default shard
        let shard = router.route("cpu,region=us-east");
        assert_eq!(shard, 2);
    }

    #[test]
    fn test_tag_affinity_analysis() {
        let router = TagAffinityRouter::new(4, "host".to_string());

        let analysis = router.analyze_series("cpu,host=server01,region=us-east");
        assert_eq!(analysis.tag_value, Some("server01".to_string()));
        assert!(!analysis.used_fallback);

        let analysis = router.analyze_series("cpu,region=us-east");
        assert_eq!(analysis.tag_value, None);
        assert!(analysis.used_fallback);
    }

    #[test]
    fn test_tag_affinity_builder() {
        let router = TagAffinityRouterBuilder::new(8, "datacenter")
            .with_fallback()
            .build();

        assert_eq!(router.shard_count(), 8);
        assert_eq!(router.affinity_tag(), "datacenter");
        assert_eq!(router.name(), "tag-affinity");
    }

    #[test]
    fn test_tag_affinity_builder_default_shard() {
        let router = TagAffinityRouterBuilder::new(8, "datacenter")
            .default_shard(3)
            .build();

        // Missing tag should use default shard 3
        let shard = router.route("cpu,region=us-east");
        assert_eq!(shard, 3);
    }

    #[test]
    fn test_multi_tag_affinity() {
        let router =
            MultiTagAffinityRouter::new(4, vec!["host".to_string(), "datacenter".to_string()]);

        // Should route by host when present
        let shard1 = router.route("cpu,host=server01,datacenter=dc1");
        let shard2 = router.route("memory,host=server01,datacenter=dc2");
        assert_eq!(shard1, shard2, "Same host should route to same shard");

        // Should fall back to datacenter when host missing
        let shard3 = router.route("cpu,datacenter=dc1");
        let shard4 = router.route("memory,datacenter=dc1");
        assert_eq!(shard3, shard4, "Same datacenter should route to same shard");

        assert_eq!(router.name(), "multi-tag-affinity");
    }

    #[test]
    fn test_distribution_by_tag() {
        let router = TagAffinityRouter::new(4, "host".to_string());

        // Route many series with different hosts
        let mut shard_counts = vec![0; 4];
        for i in 0..100 {
            let series = format!("cpu,host=server{:02},region=us-east", i);
            let shard = router.route(&series);
            shard_counts[shard] += 1;
        }

        // All shards should have some series
        for count in &shard_counts {
            assert!(*count > 0, "All shards should have series");
        }
    }

    #[test]
    fn test_co_location_guarantee() {
        let router = TagAffinityRouter::new(8, "tenant".to_string());

        // All series for a tenant should go to same shard
        let tenant_series = vec![
            "cpu,tenant=acme,host=server01",
            "memory,tenant=acme,host=server02",
            "disk,tenant=acme,host=server03",
            "network,tenant=acme,region=us-east",
        ];

        let first_shard = router.route(tenant_series[0]);
        for series in &tenant_series[1..] {
            assert_eq!(
                router.route(series),
                first_shard,
                "All series for tenant=acme should route to same shard"
            );
        }
    }

    #[test]
    #[should_panic(expected = "num_shards must be > 0")]
    fn test_zero_shards_panics() {
        TagAffinityRouter::new(0, "host".to_string());
    }

    #[test]
    #[should_panic(expected = "default_shard must be < num_shards")]
    fn test_invalid_default_shard_panics() {
        TagAffinityRouter::with_default_shard(4, "host".to_string(), 5);
    }
}
