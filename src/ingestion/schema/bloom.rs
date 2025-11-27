//! Scalable Bloom Filter for deduplication
//!
//! Implements a scalable bloom filter that automatically grows
//! as more elements are added, maintaining a target false positive rate.
//!
//! # Design
//!
//! A standard bloom filter has a fixed capacity. Once full, the false positive
//! rate increases rapidly. A scalable bloom filter addresses this by:
//!
//! 1. Starting with an initial filter
//! 2. When the filter reaches capacity, creating a new, larger filter
//! 3. Queries check all filters in the chain
//!
//! # Use Cases
//!
//! - **Point Deduplication**: Detect duplicate data points based on series+timestamp
//! - **Series Counting**: Track unique series seen
//! - **Cardinality Estimation**: Approximate distinct value counts

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Standard bloom filter with fixed capacity
#[derive(Debug)]
pub struct BloomFilter {
    /// Bit vector (using u64 for efficient operations)
    bits: Vec<AtomicU64>,
    /// Number of bits (m)
    num_bits: usize,
    /// Number of hash functions (k)
    num_hashes: u32,
    /// Number of elements inserted
    count: AtomicU64,
    /// Maximum elements before filter is considered full
    capacity: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with specified capacity and false positive rate
    ///
    /// # Arguments
    ///
    /// * `capacity` - Expected number of elements
    /// * `fp_rate` - Target false positive rate (0.0 - 1.0)
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::ingestion::schema::BloomFilter;
    ///
    /// // Create filter for 10,000 elements with 1% false positive rate
    /// let filter = BloomFilter::new(10_000, 0.01);
    /// ```
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        let fp_rate = fp_rate.clamp(0.0001, 0.5);
        let capacity = capacity.max(1);

        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let num_bits = (-(capacity as f64) * fp_rate.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_bits = num_bits.max(64); // Minimum 64 bits

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / capacity as f64) * 2.0_f64.ln()).round() as u32;
        let num_hashes = num_hashes.clamp(1, 30);

        // Round up bits to u64 boundary
        let num_u64s = num_bits.div_ceil(64);
        let bits: Vec<AtomicU64> = (0..num_u64s).map(|_| AtomicU64::new(0)).collect();

        Self {
            bits,
            num_bits: num_u64s * 64,
            num_hashes,
            count: AtomicU64::new(0),
            capacity,
        }
    }

    /// Insert an element into the filter
    ///
    /// Returns true if the element was definitely not in the filter before,
    /// false if it might have been present (or is now present).
    pub fn insert<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);
        let mut all_bits_set = true;

        for i in 0..self.num_hashes {
            let index = self.get_index(h1, h2, i);
            let (word_idx, bit_idx) = (index / 64, index % 64);
            let mask = 1u64 << bit_idx;

            // Check if bit was already set
            let old_val = self.bits[word_idx].fetch_or(mask, Ordering::Relaxed);
            if (old_val & mask) == 0 {
                all_bits_set = false;
            }
        }

        // Only increment count if this was a new element (heuristic)
        if !all_bits_set {
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        !all_bits_set
    }

    /// Check if an element might be in the filter
    ///
    /// Returns true if the element might be present (possible false positive),
    /// false if the element is definitely not present.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let index = self.get_index(h1, h2, i);
            let (word_idx, bit_idx) = (index / 64, index % 64);
            let mask = 1u64 << bit_idx;

            if (self.bits[word_idx].load(Ordering::Relaxed) & mask) == 0 {
                return false;
            }
        }

        true
    }

    /// Insert if not present, return whether it was inserted
    ///
    /// Equivalent to checking contains and then inserting, but atomic.
    pub fn insert_if_absent<T: Hash>(&self, item: &T) -> bool {
        self.insert(item)
    }

    /// Get approximate number of elements inserted
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if filter is at capacity
    pub fn is_full(&self) -> bool {
        self.count() >= self.capacity as u64
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get fill ratio (0.0 - 1.0)
    pub fn fill_ratio(&self) -> f64 {
        self.count() as f64 / self.capacity as f64
    }

    /// Estimate current false positive rate
    ///
    /// Based on the formula: (1 - e^(-kn/m))^k
    pub fn estimated_fp_rate(&self) -> f64 {
        let n = self.count() as f64;
        let m = self.num_bits as f64;
        let k = self.num_hashes as f64;

        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Clear the filter
    pub fn clear(&self) {
        for word in &self.bits {
            word.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.bits.len() * 8 + std::mem::size_of::<Self>()
    }

    /// Compute two hash values for double hashing
    ///
    /// Uses SipHash-based double hashing with different seeds.
    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        use std::collections::hash_map::DefaultHasher;

        // First hash with default seed
        let mut hasher1 = DefaultHasher::new();
        item.hash(&mut hasher1);
        let h1 = hasher1.finish();

        // Second hash: combine first hash with item for different distribution
        let mut hasher2 = DefaultHasher::new();
        h1.hash(&mut hasher2);
        item.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }

    /// Get bit index using double hashing
    fn get_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        // Double hashing: h(i) = h1 + i*h2
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }
}

/// Scalable bloom filter that grows automatically
///
/// Maintains a chain of standard bloom filters, creating new ones
/// as needed to maintain the target false positive rate.
pub struct ScalableBloomFilter {
    /// Chain of bloom filters (newest last)
    filters: RwLock<Vec<BloomFilter>>,
    /// Initial capacity (stored for potential reset/clear operations)
    #[allow(dead_code)]
    initial_capacity: usize,
    /// Target false positive rate
    fp_rate: f64,
    /// Growth factor for new filters
    growth_factor: usize,
    /// Tightening ratio for FP rate in subsequent filters
    tightening_ratio: f64,
    /// Maximum number of filters in chain
    max_filters: usize,
}

impl ScalableBloomFilter {
    /// Create a new scalable bloom filter
    ///
    /// # Arguments
    ///
    /// * `initial_capacity` - Initial expected number of elements
    /// * `fp_rate` - Target false positive rate
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::ingestion::schema::ScalableBloomFilter;
    ///
    /// let filter = ScalableBloomFilter::new(10_000, 0.01);
    /// ```
    pub fn new(initial_capacity: usize, fp_rate: f64) -> Self {
        Self::with_config(initial_capacity, fp_rate, 2, 0.9, 20)
    }

    /// Create with custom configuration
    ///
    /// # Arguments
    ///
    /// * `initial_capacity` - Initial expected number of elements
    /// * `fp_rate` - Target false positive rate
    /// * `growth_factor` - Capacity multiplier for each new filter
    /// * `tightening_ratio` - FP rate multiplier for each new filter
    /// * `max_filters` - Maximum number of filters in chain
    pub fn with_config(
        initial_capacity: usize,
        fp_rate: f64,
        growth_factor: usize,
        tightening_ratio: f64,
        max_filters: usize,
    ) -> Self {
        let initial_filter = BloomFilter::new(initial_capacity, fp_rate);

        Self {
            filters: RwLock::new(vec![initial_filter]),
            initial_capacity,
            fp_rate,
            growth_factor: growth_factor.max(2),
            tightening_ratio: tightening_ratio.clamp(0.5, 0.99),
            max_filters,
        }
    }

    /// Insert an element
    ///
    /// Returns true if the element was definitely new.
    pub fn insert<T: Hash>(&self, item: &T) -> bool {
        // Check if element exists in any filter
        if self.contains(item) {
            return false;
        }

        // Get write access to insert
        let mut filters = self.filters.write().unwrap();

        // Double-check after acquiring lock
        for filter in filters.iter() {
            if filter.contains(item) {
                return false;
            }
        }

        // Find a non-full filter or create new one
        let current = filters.last().unwrap();
        if current.is_full() && filters.len() < self.max_filters {
            // Create new filter with increased capacity and tighter FP rate
            let new_capacity = current.capacity() * self.growth_factor;
            let filter_index = filters.len();
            let new_fp_rate = self.fp_rate * self.tightening_ratio.powi(filter_index as i32);
            let new_filter = BloomFilter::new(new_capacity, new_fp_rate);
            filters.push(new_filter);
        }

        // Insert into the last (current) filter
        filters.last().unwrap().insert(item)
    }

    /// Check if element might be present
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let filters = self.filters.read().unwrap();

        // Check all filters in the chain
        for filter in filters.iter() {
            if filter.contains(item) {
                return true;
            }
        }

        false
    }

    /// Insert if not present
    pub fn insert_if_absent<T: Hash>(&self, item: &T) -> bool {
        self.insert(item)
    }

    /// Get total count across all filters
    pub fn count(&self) -> u64 {
        let filters = self.filters.read().unwrap();
        filters.iter().map(|f| f.count()).sum()
    }

    /// Get number of filters in chain
    pub fn filter_count(&self) -> usize {
        let filters = self.filters.read().unwrap();
        filters.len()
    }

    /// Get total memory usage
    pub fn memory_usage(&self) -> usize {
        let filters = self.filters.read().unwrap();
        filters.iter().map(|f| f.memory_usage()).sum::<usize>() + std::mem::size_of::<Self>()
    }

    /// Estimate current false positive rate
    ///
    /// For scalable bloom filter: 1 - prod(1 - fp_i)
    pub fn estimated_fp_rate(&self) -> f64 {
        let filters = self.filters.read().unwrap();

        let prob_no_fp: f64 = filters
            .iter()
            .map(|f| 1.0 - f.estimated_fp_rate())
            .product();

        1.0 - prob_no_fp
    }

    /// Clear all filters
    pub fn clear(&self) {
        let mut filters = self.filters.write().unwrap();

        // Keep only the first filter and clear it
        if let Some(first) = filters.first() {
            first.clear();
        }
        filters.truncate(1);
    }

    /// Get statistics
    pub fn stats(&self) -> BloomFilterStats {
        let filters = self.filters.read().unwrap();

        BloomFilterStats {
            total_count: filters.iter().map(|f| f.count()).sum(),
            filter_count: filters.len(),
            memory_bytes: filters.iter().map(|f| f.memory_usage()).sum(),
            estimated_fp_rate: self.estimated_fp_rate(),
        }
    }
}

/// Statistics for bloom filter
#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    /// Total elements inserted
    pub total_count: u64,
    /// Number of filters in chain
    pub filter_count: usize,
    /// Total memory usage in bytes
    pub memory_bytes: usize,
    /// Estimated current false positive rate
    pub estimated_fp_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let filter = BloomFilter::new(1000, 0.01);

        // Insert elements
        assert!(filter.insert(&"hello"));
        assert!(filter.insert(&"world"));

        // Check presence
        assert!(filter.contains(&"hello"));
        assert!(filter.contains(&"world"));
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.count(), 2);
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let filter = BloomFilter::new(10000, 0.01);

        // Insert many elements
        for i in 0..1000 {
            filter.insert(&i);
        }

        // All inserted elements must be found (no false negatives)
        for i in 0..1000 {
            assert!(filter.contains(&i), "False negative for {}", i);
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let filter = BloomFilter::new(10000, 0.01);

        // Insert elements
        for i in 0..10000 {
            filter.insert(&i);
        }

        // Count false positives
        let mut false_positives = 0;
        for i in 10000..20000 {
            if filter.contains(&i) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        // Allow some tolerance (should be around 1%)
        assert!(fp_rate < 0.05, "False positive rate too high: {}", fp_rate);
    }

    #[test]
    fn test_bloom_filter_clear() {
        let filter = BloomFilter::new(1000, 0.01);

        filter.insert(&"hello");
        assert!(filter.contains(&"hello"));

        filter.clear();

        assert!(!filter.contains(&"hello"));
        assert_eq!(filter.count(), 0);
    }

    #[test]
    fn test_scalable_bloom_filter_basic() {
        let filter = ScalableBloomFilter::new(100, 0.01);

        assert!(filter.insert(&"hello"));
        assert!(!filter.insert(&"hello")); // Duplicate
        assert!(filter.insert(&"world"));

        assert!(filter.contains(&"hello"));
        assert!(filter.contains(&"world"));
        assert!(!filter.contains(&"foo"));
    }

    #[test]
    fn test_scalable_bloom_filter_growth() {
        // Small initial capacity to trigger growth
        let filter = ScalableBloomFilter::with_config(10, 0.01, 2, 0.9, 5);

        // Insert enough elements to trigger growth
        for i in 0..100 {
            filter.insert(&i);
        }

        assert!(filter.filter_count() > 1, "Filter should have grown");

        // All elements should still be findable
        for i in 0..100 {
            assert!(filter.contains(&i), "Element {} not found", i);
        }
    }

    #[test]
    fn test_scalable_bloom_filter_clear() {
        let filter = ScalableBloomFilter::with_config(10, 0.01, 2, 0.9, 5);

        // Trigger growth
        for i in 0..50 {
            filter.insert(&i);
        }

        assert!(filter.filter_count() > 1);

        filter.clear();

        assert_eq!(filter.filter_count(), 1);
        assert_eq!(filter.count(), 0);
        assert!(!filter.contains(&0));
    }

    #[test]
    fn test_scalable_bloom_filter_stats() {
        let filter = ScalableBloomFilter::new(1000, 0.01);

        for i in 0..100 {
            filter.insert(&i);
        }

        let stats = filter.stats();
        assert_eq!(stats.total_count, 100);
        assert!(stats.memory_bytes > 0);
        assert!(stats.estimated_fp_rate < 1.0);
    }

    #[test]
    fn test_bloom_filter_memory_usage() {
        let filter = BloomFilter::new(10000, 0.01);
        let usage = filter.memory_usage();

        // Should be roughly: n * ln(p) / ln(2)^2 / 8 bytes
        // For 10000 elements at 0.01 FP: ~12KB
        assert!(usage > 1000, "Memory usage too low: {}", usage);
        assert!(usage < 100000, "Memory usage too high: {}", usage);
    }
}
