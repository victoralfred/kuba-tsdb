//! Consistent hashing router with virtual nodes
//!
//! Implements consistent hashing for minimal data movement when
//! shards are added or removed.
//!
//! # Algorithm
//!
//! 1. Each physical shard is mapped to multiple virtual nodes on a hash ring
//! 2. Series keys are hashed and mapped to the ring
//! 3. The series is assigned to the first virtual node clockwise from its position
//!
//! # Benefits
//!
//! - Adding/removing shards only redistributes ~1/N of data
//! - Virtual nodes provide better load distribution
//! - Deterministic routing for the same key
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::routing::{RoutingStrategy, ConsistentHashRouter};
//!
//! let router = ConsistentHashRouter::new(4, 100); // 4 shards, 100 virtual nodes each
//! let shard = router.route("cpu,host=server01");
//! ```

use std::collections::BTreeMap;
use std::sync::RwLock;

use super::{RoutingStrategy, ShardId};

/// Consistent hash router with virtual nodes
///
/// Uses a ring-based consistent hashing algorithm to distribute
/// series across shards with minimal redistribution on topology changes.
pub struct ConsistentHashRouter {
    /// Hash ring mapping virtual node hashes to shard IDs
    ring: RwLock<BTreeMap<u64, ShardId>>,
    /// Number of physical shards
    num_shards: usize,
    /// Number of virtual nodes per shard
    virtual_nodes: usize,
}

impl ConsistentHashRouter {
    /// Create a new consistent hash router
    ///
    /// # Arguments
    ///
    /// * `num_shards` - Number of physical shards
    /// * `virtual_nodes` - Number of virtual nodes per shard (higher = better distribution)
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is 0.
    pub fn new(num_shards: usize, virtual_nodes: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        let virtual_nodes = virtual_nodes.max(1);

        let mut router = Self {
            ring: RwLock::new(BTreeMap::new()),
            num_shards,
            virtual_nodes,
        };

        // Initialize the ring with all shards
        router.rebuild_ring();

        router
    }

    /// Rebuild the hash ring
    fn rebuild_ring(&mut self) {
        let mut ring = self.ring.write().unwrap();
        ring.clear();

        for shard_id in 0..self.num_shards {
            for vnode in 0..self.virtual_nodes {
                let key = format!("shard-{}-vnode-{}", shard_id, vnode);
                let hash = Self::hash(&key);
                ring.insert(hash, shard_id);
            }
        }
    }

    /// Add a new shard to the ring
    ///
    /// Returns the new shard ID.
    pub fn add_shard(&self) -> ShardId {
        let mut ring = self.ring.write().unwrap();
        let new_shard_id = self.num_shards;

        for vnode in 0..self.virtual_nodes {
            let key = format!("shard-{}-vnode-{}", new_shard_id, vnode);
            let hash = Self::hash(&key);
            ring.insert(hash, new_shard_id);
        }

        new_shard_id
    }

    /// Remove a shard from the ring
    ///
    /// Note: This doesn't update num_shards. For proper shard removal,
    /// the router should be reconstructed with the new shard count.
    pub fn remove_shard(&self, shard_id: ShardId) {
        let mut ring = self.ring.write().unwrap();

        for vnode in 0..self.virtual_nodes {
            let key = format!("shard-{}-vnode-{}", shard_id, vnode);
            let hash = Self::hash(&key);
            ring.remove(&hash);
        }
    }

    /// Get the distribution of series across shards
    ///
    /// Returns a histogram of shard -> virtual node count.
    pub fn distribution(&self) -> Vec<usize> {
        let ring = self.ring.read().unwrap();
        let mut counts = vec![0; self.num_shards];

        for &shard_id in ring.values() {
            if shard_id < self.num_shards {
                counts[shard_id] += 1;
            }
        }

        counts
    }

    /// Get total virtual nodes in the ring
    pub fn ring_size(&self) -> usize {
        let ring = self.ring.read().unwrap();
        ring.len()
    }

    /// Compute hash of a string using SipHash
    fn hash(s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Find the shard for a given hash value
    fn find_shard(&self, hash: u64) -> ShardId {
        let ring = self.ring.read().unwrap();

        // Find the first virtual node with hash >= key hash
        // BTreeMap range gives us nodes starting from the hash
        if let Some((&_node_hash, &shard_id)) = ring.range(hash..).next() {
            return shard_id;
        }

        // Wrap around to the first node (ring behavior)
        if let Some((&_node_hash, &shard_id)) = ring.iter().next() {
            return shard_id;
        }

        // Fallback (should never happen if ring is not empty)
        0
    }

    /// Get the virtual nodes per shard
    pub fn virtual_nodes_per_shard(&self) -> usize {
        self.virtual_nodes
    }

    /// Estimate how many series would move if a shard is added
    ///
    /// Returns approximate percentage of data that would move.
    pub fn estimate_rebalance_on_add(&self) -> f64 {
        // With consistent hashing, adding one shard moves ~1/(n+1) of data
        1.0 / (self.num_shards + 1) as f64
    }

    /// Estimate how many series would move if a shard is removed
    ///
    /// Returns approximate percentage of data that would move.
    pub fn estimate_rebalance_on_remove(&self) -> f64 {
        // Removing one shard moves ~1/n of data
        1.0 / self.num_shards as f64
    }
}

impl RoutingStrategy for ConsistentHashRouter {
    fn route(&self, series_key: &str) -> ShardId {
        let hash = Self::hash(series_key);
        self.find_shard(hash)
    }

    fn shard_count(&self) -> usize {
        self.num_shards
    }

    fn name(&self) -> &'static str {
        "consistent-hash"
    }

    fn supports_rebalancing(&self) -> bool {
        true
    }
}

/// Builder for ConsistentHashRouter with custom options
pub struct ConsistentHashRouterBuilder {
    num_shards: usize,
    virtual_nodes: usize,
}

impl ConsistentHashRouterBuilder {
    /// Create a new builder with required shard count
    pub fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            virtual_nodes: 100,
        }
    }

    /// Set the number of virtual nodes per shard
    pub fn virtual_nodes(mut self, count: usize) -> Self {
        self.virtual_nodes = count;
        self
    }

    /// Build the router
    pub fn build(self) -> ConsistentHashRouter {
        ConsistentHashRouter::new(self.num_shards, self.virtual_nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_consistent_hash_basic() {
        let router = ConsistentHashRouter::new(4, 100);

        // Same key should always route to same shard
        let shard1 = router.route("cpu,host=server01");
        let shard2 = router.route("cpu,host=server01");
        assert_eq!(shard1, shard2);

        assert_eq!(router.shard_count(), 4);
        assert_eq!(router.name(), "consistent-hash");
    }

    #[test]
    fn test_consistent_hash_distribution() {
        let router = ConsistentHashRouter::new(4, 100);

        // Route many keys and check distribution
        let mut counts = HashMap::new();
        for i in 0..10000 {
            let key = format!("series_{}", i);
            let shard = router.route(&key);
            *counts.entry(shard).or_insert(0) += 1;
        }

        // All shards should have some keys
        assert_eq!(counts.len(), 4);

        // Distribution should be roughly even (within 30% of mean)
        let mean = 10000.0 / 4.0;
        for &count in counts.values() {
            let deviation = (count as f64 - mean).abs() / mean;
            assert!(
                deviation < 0.3,
                "Distribution too uneven: {} vs mean {}",
                count,
                mean
            );
        }
    }

    #[test]
    fn test_consistent_hash_ring_size() {
        let router = ConsistentHashRouter::new(4, 100);
        assert_eq!(router.ring_size(), 400); // 4 shards * 100 vnodes
    }

    #[test]
    fn test_consistent_hash_stability() {
        let router = ConsistentHashRouter::new(4, 100);

        // Create a mapping of some keys
        let mut original_mapping = HashMap::new();
        for i in 0..100 {
            let key = format!("stable_series_{}", i);
            original_mapping.insert(key.clone(), router.route(&key));
        }

        // Verify same keys still map to same shards
        for (key, original_shard) in &original_mapping {
            assert_eq!(router.route(key), *original_shard);
        }
    }

    #[test]
    fn test_virtual_nodes_improve_distribution() {
        // With few virtual nodes, distribution is poor
        let router_few = ConsistentHashRouter::new(4, 1);

        // With many virtual nodes, distribution is better
        let router_many = ConsistentHashRouter::new(4, 200);

        // Calculate variance for each
        fn calculate_variance(router: &ConsistentHashRouter) -> f64 {
            let mut counts = vec![0; router.shard_count()];
            for i in 0..1000 {
                let key = format!("key_{}", i);
                counts[router.route(&key)] += 1;
            }
            let mean = 1000.0 / router.shard_count() as f64;
            counts
                .iter()
                .map(|&c| (c as f64 - mean).powi(2))
                .sum::<f64>()
                / router.shard_count() as f64
        }

        let var_few = calculate_variance(&router_few);
        let var_many = calculate_variance(&router_many);

        // More virtual nodes should give lower variance (better distribution)
        assert!(
            var_many < var_few,
            "More vnodes should improve distribution: {} vs {}",
            var_many,
            var_few
        );
    }

    #[test]
    fn test_distribution_histogram() {
        let router = ConsistentHashRouter::new(4, 100);
        let dist = router.distribution();

        assert_eq!(dist.len(), 4);
        assert_eq!(dist.iter().sum::<usize>(), 400);
    }

    #[test]
    fn test_builder() {
        let router = ConsistentHashRouterBuilder::new(8)
            .virtual_nodes(50)
            .build();

        assert_eq!(router.shard_count(), 8);
        assert_eq!(router.virtual_nodes_per_shard(), 50);
        assert_eq!(router.ring_size(), 400);
    }

    #[test]
    fn test_rebalance_estimates() {
        let router = ConsistentHashRouter::new(4, 100);

        let add_estimate = router.estimate_rebalance_on_add();
        assert!((add_estimate - 0.2).abs() < 0.01); // ~20% for adding 5th shard

        let remove_estimate = router.estimate_rebalance_on_remove();
        assert!((remove_estimate - 0.25).abs() < 0.01); // ~25% for removing from 4
    }

    #[test]
    fn test_supports_rebalancing() {
        let router = ConsistentHashRouter::new(4, 100);
        assert!(router.supports_rebalancing());
    }
}
