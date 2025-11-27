//! Routing module for series distribution
//!
//! Provides routing strategies to distribute time series data across
//! multiple shards/nodes for horizontal scaling.
//!
//! # Strategies
//!
//! - **Round Robin**: Simple rotation across shards (stateless)
//! - **Consistent Hash**: Hash-based routing with virtual nodes for minimal redistribution
//! - **Tag Affinity**: Co-locate series with same tag values
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Router                                │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │              RoutingStrategy                     │    │
//! │  │  ┌───────────┬───────────────┬───────────────┐  │    │
//! │  │  │ RoundRobin│ConsistentHash │ TagAffinity   │  │    │
//! │  │  └─────┬─────┴───────┬───────┴───────┬───────┘  │    │
//! │  └────────┼─────────────┼───────────────┼──────────┘    │
//! └───────────┼─────────────┼───────────────┼───────────────┘
//!             │             │               │
//!             v             v               v
//!        ┌────────┐   ┌────────┐      ┌────────┐
//!        │ Shard 0│   │ Shard 1│ ...  │ Shard N│
//!        └────────┘   └────────┘      └────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::routing::{RoutingStrategy, ConsistentHashRouter};
//!
//! let router = ConsistentHashRouter::new(4, 100); // 4 shards, 100 virtual nodes each
//! let shard = router.route("cpu,host=server01");
//! ```

pub mod affinity;
pub mod consistent;

pub use affinity::TagAffinityRouter;
pub use consistent::ConsistentHashRouter;

use std::sync::atomic::{AtomicUsize, Ordering};

/// Shard identifier
pub type ShardId = usize;

/// Routing strategy trait
///
/// All routing strategies implement this trait, allowing for
/// uniform handling regardless of the algorithm used.
pub trait RoutingStrategy: Send + Sync {
    /// Route a series key to a shard
    ///
    /// # Arguments
    ///
    /// * `series_key` - The series key (measurement + sorted tags)
    ///
    /// # Returns
    ///
    /// The shard ID to route to
    fn route(&self, series_key: &str) -> ShardId;

    /// Get the total number of shards
    fn shard_count(&self) -> usize;

    /// Get the strategy name
    fn name(&self) -> &'static str;

    /// Check if strategy supports dynamic rebalancing
    fn supports_rebalancing(&self) -> bool {
        false
    }
}

/// Round-robin routing strategy
///
/// Distributes series across shards in rotation. Simple and fast,
/// but doesn't maintain series affinity.
///
/// # Use Cases
///
/// - Testing and development
/// - When series locality doesn't matter
/// - Uniform load distribution
pub struct RoundRobinRouter {
    /// Number of shards
    num_shards: usize,
    /// Current counter for rotation
    counter: AtomicUsize,
}

impl RoundRobinRouter {
    /// Create a new round-robin router
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        Self {
            num_shards,
            counter: AtomicUsize::new(0),
        }
    }

    /// Reset the counter
    pub fn reset(&self) {
        self.counter.store(0, Ordering::Relaxed);
    }

    /// Get current counter value
    pub fn current(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }
}

impl RoutingStrategy for RoundRobinRouter {
    fn route(&self, _series_key: &str) -> ShardId {
        // Increment and wrap around
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        count % self.num_shards
    }

    fn shard_count(&self) -> usize {
        self.num_shards
    }

    fn name(&self) -> &'static str {
        "round-robin"
    }
}

/// Hash-based routing strategy
///
/// Routes based on hash of series key. Fast and provides
/// consistent routing for the same series.
pub struct HashRouter {
    /// Number of shards
    num_shards: usize,
}

impl HashRouter {
    /// Create a new hash router
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        Self { num_shards }
    }

    /// Compute hash of a string
    fn hash(s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }
}

impl RoutingStrategy for HashRouter {
    fn route(&self, series_key: &str) -> ShardId {
        let hash = Self::hash(series_key);
        (hash as usize) % self.num_shards
    }

    fn shard_count(&self) -> usize {
        self.num_shards
    }

    fn name(&self) -> &'static str {
        "hash"
    }
}

/// Router configuration
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Routing strategy to use
    pub strategy: RoutingStrategyType,
    /// Number of shards
    pub num_shards: usize,
    /// Virtual nodes per shard (for consistent hashing)
    pub virtual_nodes: usize,
    /// Affinity tag key (for tag affinity routing)
    pub affinity_tag: Option<String>,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            strategy: RoutingStrategyType::ConsistentHash,
            num_shards: 4,
            virtual_nodes: 100,
            affinity_tag: None,
        }
    }
}

/// Routing strategy type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategyType {
    /// Round-robin distribution
    RoundRobin,
    /// Simple hash-based routing
    Hash,
    /// Consistent hashing with virtual nodes
    ConsistentHash,
    /// Tag-based affinity routing
    TagAffinity,
}

impl RoutingStrategyType {
    /// Get strategy name
    pub fn name(&self) -> &'static str {
        match self {
            RoutingStrategyType::RoundRobin => "round-robin",
            RoutingStrategyType::Hash => "hash",
            RoutingStrategyType::ConsistentHash => "consistent-hash",
            RoutingStrategyType::TagAffinity => "tag-affinity",
        }
    }
}

/// Dynamic router that can be reconfigured
///
/// Wraps a routing strategy and allows for runtime changes
/// while maintaining thread safety.
pub struct Router {
    /// Current routing strategy
    strategy: Box<dyn RoutingStrategy>,
    /// Configuration
    config: RouterConfig,
}

impl Router {
    /// Create a new router with the specified configuration
    pub fn new(config: RouterConfig) -> Self {
        let strategy = Self::create_strategy(&config);
        Self { strategy, config }
    }

    /// Create a router with default configuration
    pub fn default_with_shards(num_shards: usize) -> Self {
        let config = RouterConfig {
            num_shards,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Create a strategy from configuration
    fn create_strategy(config: &RouterConfig) -> Box<dyn RoutingStrategy> {
        match config.strategy {
            RoutingStrategyType::RoundRobin => Box::new(RoundRobinRouter::new(config.num_shards)),
            RoutingStrategyType::Hash => Box::new(HashRouter::new(config.num_shards)),
            RoutingStrategyType::ConsistentHash => Box::new(ConsistentHashRouter::new(
                config.num_shards,
                config.virtual_nodes,
            )),
            RoutingStrategyType::TagAffinity => {
                let tag = config
                    .affinity_tag
                    .clone()
                    .unwrap_or_else(|| "host".to_string());
                Box::new(TagAffinityRouter::new(config.num_shards, tag))
            }
        }
    }

    /// Route a series key to a shard
    pub fn route(&self, series_key: &str) -> ShardId {
        self.strategy.route(series_key)
    }

    /// Get the number of shards
    pub fn shard_count(&self) -> usize {
        self.strategy.shard_count()
    }

    /// Get the strategy name
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    /// Get the configuration
    pub fn config(&self) -> &RouterConfig {
        &self.config
    }

    /// Check if the strategy supports rebalancing
    pub fn supports_rebalancing(&self) -> bool {
        self.strategy.supports_rebalancing()
    }
}

/// Route a batch of series keys
///
/// Returns a vector of (series_key, shard_id) pairs grouped by shard.
pub fn route_batch<'a>(
    router: &dyn RoutingStrategy,
    series_keys: impl Iterator<Item = &'a str>,
) -> Vec<Vec<&'a str>> {
    let num_shards = router.shard_count();
    let mut shards: Vec<Vec<&str>> = vec![Vec::new(); num_shards];

    for key in series_keys {
        let shard = router.route(key);
        shards[shard].push(key);
    }

    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_router() {
        let router = RoundRobinRouter::new(4);

        // Should cycle through shards
        assert_eq!(router.route("a"), 0);
        assert_eq!(router.route("b"), 1);
        assert_eq!(router.route("c"), 2);
        assert_eq!(router.route("d"), 3);
        assert_eq!(router.route("e"), 0); // Wraps around

        assert_eq!(router.shard_count(), 4);
        assert_eq!(router.name(), "round-robin");
    }

    #[test]
    fn test_round_robin_reset() {
        let router = RoundRobinRouter::new(4);

        router.route("a");
        router.route("b");
        assert_eq!(router.current(), 2);

        router.reset();
        assert_eq!(router.current(), 0);
    }

    #[test]
    fn test_hash_router() {
        let router = HashRouter::new(4);

        // Same key should always route to same shard
        let shard1 = router.route("cpu,host=server01");
        let shard2 = router.route("cpu,host=server01");
        assert_eq!(shard1, shard2);

        // Different keys may route to different shards
        let shard_a = router.route("series_a");
        let shard_b = router.route("series_b");
        // They might be same or different, just check they're valid
        assert!(shard_a < 4);
        assert!(shard_b < 4);

        assert_eq!(router.name(), "hash");
    }

    #[test]
    fn test_router_config_default() {
        let config = RouterConfig::default();
        assert_eq!(config.strategy, RoutingStrategyType::ConsistentHash);
        assert_eq!(config.num_shards, 4);
        assert_eq!(config.virtual_nodes, 100);
    }

    #[test]
    fn test_router_creation() {
        let config = RouterConfig {
            strategy: RoutingStrategyType::Hash,
            num_shards: 8,
            ..Default::default()
        };
        let router = Router::new(config);

        assert_eq!(router.shard_count(), 8);
        assert_eq!(router.strategy_name(), "hash");
    }

    #[test]
    fn test_route_batch() {
        let router = HashRouter::new(4);
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];

        let batches = route_batch(&router, keys.iter().copied());

        // All keys should be distributed
        let total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 8);

        // Each batch should have valid keys
        for batch in batches {
            for key in batch {
                assert!(keys.contains(&key));
            }
        }
    }

    #[test]
    fn test_routing_strategy_type_name() {
        assert_eq!(RoutingStrategyType::RoundRobin.name(), "round-robin");
        assert_eq!(RoutingStrategyType::Hash.name(), "hash");
        assert_eq!(
            RoutingStrategyType::ConsistentHash.name(),
            "consistent-hash"
        );
        assert_eq!(RoutingStrategyType::TagAffinity.name(), "tag-affinity");
    }
}
