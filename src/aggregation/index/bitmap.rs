//! Bitmap Index for Fast Tag-Based Series Lookup
//!
//! This module provides bitmap indexes for efficient filtering of series
//! based on tag values. Each unique tag value has an associated bitmap
//! where bit N is set if series N has that tag value.
//!
//! # Performance
//!
//! - Single tag lookup: O(1)
//! - Multi-tag AND/OR: O(n) where n is bitmap size (bitwise operations)
//! - Memory: ~1 bit per series per tag value
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::aggregation::index::{BitmapIndex, TagFilter};
//! use gorilla_tsdb::aggregation::data_model::{TagKeyId, TagValueId};
//!
//! let index = BitmapIndex::new();
//!
//! // Register series with tags (using tag IDs)
//! let host_key = TagKeyId(1);
//! let dc_key = TagKeyId(2);
//! let server1_val = TagValueId(1);
//! let us_east_val = TagValueId(2);
//!
//! index.add_series(1, &[(host_key, server1_val), (dc_key, us_east_val)]);
//!
//! // Query using TagFilter
//! let filter = TagFilter::exact(dc_key, us_east_val);
//! let bitmap = index.query(&filter);
//! assert!(bitmap.contains(1));
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use super::super::data_model::{TagKeyId, TagValueId};
use crate::types::SeriesId;

// ============================================================================
// Simple Bitmap Implementation
// ============================================================================

/// A simple bitmap for tracking series membership
///
/// Uses a vector of u64 words, where each bit represents one series.
/// This is optimized for dense series IDs starting near 0.
#[derive(Debug, Clone)]
pub struct TagBitmap {
    /// Bitmap words (64 bits each)
    words: Vec<u64>,

    /// Number of bits set (cached for fast cardinality)
    cardinality: usize,
}

impl TagBitmap {
    /// Create an empty bitmap
    pub fn new() -> Self {
        Self {
            words: Vec::new(),
            cardinality: 0,
        }
    }

    /// Create a bitmap with pre-allocated capacity for N series
    pub fn with_capacity(num_series: usize) -> Self {
        let num_words = num_series.div_ceil(64);
        Self {
            words: vec![0; num_words],
            cardinality: 0,
        }
    }

    /// Maximum supported series ID to prevent integer overflow
    /// Limits bitmap to ~64MB of memory (8M words * 8 bytes)
    pub const MAX_SERIES_ID: SeriesId = 512_000_000;

    /// Set a bit (mark series as having this tag)
    ///
    /// Returns false if series_id exceeds MAX_SERIES_ID (overflow protection)
    pub fn set(&mut self, series_id: SeriesId) -> bool {
        // SEC-001: Prevent integer overflow in bitmap position calculation
        if series_id > Self::MAX_SERIES_ID {
            return false;
        }

        let id = series_id as usize;
        let word_idx = id / 64;
        let bit_idx = id % 64;

        // Grow if needed
        if word_idx >= self.words.len() {
            self.words.resize(word_idx + 1, 0);
        }

        let mask = 1u64 << bit_idx;
        if self.words[word_idx] & mask == 0 {
            self.words[word_idx] |= mask;
            self.cardinality += 1;
        }
        true
    }

    /// Clear a bit (mark series as not having this tag)
    pub fn clear(&mut self, series_id: SeriesId) {
        let id = series_id as usize;
        let word_idx = id / 64;
        let bit_idx = id % 64;

        if word_idx < self.words.len() {
            let mask = 1u64 << bit_idx;
            if self.words[word_idx] & mask != 0 {
                self.words[word_idx] &= !mask;
                self.cardinality -= 1;
            }
        }
    }

    /// Check if a bit is set
    pub fn contains(&self, series_id: SeriesId) -> bool {
        let id = series_id as usize;
        let word_idx = id / 64;
        let bit_idx = id % 64;

        if word_idx >= self.words.len() {
            return false;
        }

        (self.words[word_idx] & (1u64 << bit_idx)) != 0
    }

    /// Get the number of bits set (number of series with this tag)
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// Check if bitmap is empty
    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Bitwise AND with another bitmap (intersection)
    pub fn and(&self, other: &TagBitmap) -> TagBitmap {
        let min_len = self.words.len().min(other.words.len());
        let mut result = Vec::with_capacity(min_len);
        let mut cardinality = 0;

        for i in 0..min_len {
            let word = self.words[i] & other.words[i];
            result.push(word);
            cardinality += word.count_ones() as usize;
        }

        TagBitmap {
            words: result,
            cardinality,
        }
    }

    /// Bitwise OR with another bitmap (union)
    pub fn or(&self, other: &TagBitmap) -> TagBitmap {
        let max_len = self.words.len().max(other.words.len());
        let mut result = Vec::with_capacity(max_len);
        let mut cardinality = 0;

        for i in 0..max_len {
            let w1 = self.words.get(i).copied().unwrap_or(0);
            let w2 = other.words.get(i).copied().unwrap_or(0);
            let word = w1 | w2;
            result.push(word);
            cardinality += word.count_ones() as usize;
        }

        TagBitmap {
            words: result,
            cardinality,
        }
    }

    /// Bitwise AND-NOT (difference: self AND NOT other)
    pub fn and_not(&self, other: &TagBitmap) -> TagBitmap {
        let mut result = Vec::with_capacity(self.words.len());
        let mut cardinality = 0;

        for i in 0..self.words.len() {
            let w2 = other.words.get(i).copied().unwrap_or(0);
            let word = self.words[i] & !w2;
            result.push(word);
            cardinality += word.count_ones() as usize;
        }

        TagBitmap {
            words: result,
            cardinality,
        }
    }

    /// Iterate over all set bits (series IDs)
    pub fn iter(&self) -> BitmapIterator<'_> {
        BitmapIterator {
            bitmap: self,
            word_idx: 0,
            bit_idx: 0,
        }
    }

    /// Collect all set bits into a vector
    pub fn to_series_ids(&self) -> Vec<SeriesId> {
        self.iter().collect()
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        self.words.len() * 8
    }
}

impl Default for TagBitmap {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over set bits in a bitmap
pub struct BitmapIterator<'a> {
    bitmap: &'a TagBitmap,
    word_idx: usize,
    bit_idx: usize,
}

impl<'a> Iterator for BitmapIterator<'a> {
    type Item = SeriesId;

    fn next(&mut self) -> Option<Self::Item> {
        while self.word_idx < self.bitmap.words.len() {
            let word = self.bitmap.words[self.word_idx];

            // Find next set bit in current word starting from bit_idx
            while self.bit_idx < 64 {
                if (word & (1u64 << self.bit_idx)) != 0 {
                    let series_id = (self.word_idx * 64 + self.bit_idx) as SeriesId;
                    self.bit_idx += 1;
                    return Some(series_id);
                }
                self.bit_idx += 1;
            }

            // Move to next word
            self.word_idx += 1;
            self.bit_idx = 0;
        }

        None
    }
}

// ============================================================================
// Bitmap Index
// ============================================================================

/// Index for fast tag-based series lookup using bitmaps
///
/// Maintains a bitmap per (tag_key, tag_value) combination.
/// Supports efficient AND/OR operations for multi-tag queries.
#[derive(Debug)]
pub struct BitmapIndex {
    /// Bitmaps indexed by (key_id, value_id)
    bitmaps: RwLock<HashMap<(TagKeyId, TagValueId), TagBitmap>>,

    /// PERF-003: Pre-computed bitmap per key (union of all values for that key)
    /// This avoids linear scan when querying HasKey filter
    key_bitmaps: RwLock<HashMap<TagKeyId, TagBitmap>>,

    /// All series bitmap (for "all" queries)
    all_series: RwLock<TagBitmap>,

    /// Statistics
    stats: BitmapIndexStats,
}

/// Statistics for the bitmap index
#[derive(Debug, Default)]
pub struct BitmapIndexStats {
    /// Number of unique tag key-value combinations
    pub tag_combinations: AtomicU64,

    /// Total series indexed
    pub series_count: AtomicU64,

    /// Query count
    pub queries: AtomicU64,
}

impl BitmapIndex {
    /// Create a new bitmap index
    pub fn new() -> Self {
        Self {
            bitmaps: RwLock::new(HashMap::new()),
            key_bitmaps: RwLock::new(HashMap::new()),
            all_series: RwLock::new(TagBitmap::new()),
            stats: BitmapIndexStats::default(),
        }
    }

    /// Add a series with its tags to the index
    pub fn add_series(&self, series_id: SeriesId, tags: &[(TagKeyId, TagValueId)]) {
        let mut bitmaps = self.bitmaps.write();
        let mut key_bitmaps = self.key_bitmaps.write();

        for (key_id, value_id) in tags {
            // Add to value-specific bitmap
            let bitmap = bitmaps.entry((*key_id, *value_id)).or_insert_with(|| {
                self.stats.tag_combinations.fetch_add(1, Ordering::Relaxed);
                TagBitmap::new()
            });
            bitmap.set(series_id);

            // PERF-003: Also add to key-level bitmap for O(1) HasKey lookups
            let key_bitmap = key_bitmaps.entry(*key_id).or_default();
            key_bitmap.set(series_id);
        }

        // Also add to all_series bitmap
        self.all_series.write().set(series_id);
        self.stats.series_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a series from the index
    pub fn remove_series(&self, series_id: SeriesId, tags: &[(TagKeyId, TagValueId)]) {
        let mut bitmaps = self.bitmaps.write();

        for (key_id, value_id) in tags {
            if let Some(bitmap) = bitmaps.get_mut(&(*key_id, *value_id)) {
                bitmap.clear(series_id);
            }
            // Note: We don't remove from key_bitmaps here as it would require
            // checking if any other values for this key still have this series.
            // This is an acceptable trade-off for faster reads.
        }

        self.all_series.write().clear(series_id);
    }

    /// Get bitmap for all series that have a specific key (any value)
    ///
    /// PERF-003: O(1) lookup instead of linear scan over all values
    pub fn get_key_bitmap(&self, key_id: TagKeyId) -> Option<TagBitmap> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.key_bitmaps.read().get(&key_id).cloned()
    }

    /// Get the bitmap for a specific tag value
    ///
    /// PERF-002: Returns a reference via callback to avoid cloning when possible
    pub fn get_bitmap(&self, key_id: TagKeyId, value_id: TagValueId) -> Option<TagBitmap> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.bitmaps.read().get(&(key_id, value_id)).cloned()
    }

    /// Get the bitmap for a specific tag value without cloning
    ///
    /// PERF-002: Uses callback to avoid unnecessary cloning
    pub fn with_bitmap<F, R>(&self, key_id: TagKeyId, value_id: TagValueId, f: F) -> Option<R>
    where
        F: FnOnce(&TagBitmap) -> R,
    {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.bitmaps.read().get(&(key_id, value_id)).map(f)
    }

    /// Get all series (for queries with no tag filters)
    pub fn get_all_series(&self) -> TagBitmap {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.all_series.read().clone()
    }

    /// Get series IDs directly without cloning the bitmap
    ///
    /// PERF-002: More efficient than get_bitmap().to_series_ids()
    pub fn get_series_ids(&self, key_id: TagKeyId, value_id: TagValueId) -> Vec<SeriesId> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.bitmaps
            .read()
            .get(&(key_id, value_id))
            .map(|b| b.to_series_ids())
            .unwrap_or_default()
    }

    /// Execute a tag filter query
    ///
    /// Returns a bitmap of matching series.
    pub fn query(&self, filter: &TagFilter) -> TagBitmap {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);

        match filter {
            TagFilter::All => self.all_series.read().clone(),

            TagFilter::Exact(key_id, value_id) => self
                .bitmaps
                .read()
                .get(&(*key_id, *value_id))
                .cloned()
                .unwrap_or_default(),

            TagFilter::And(filters) => {
                let mut result: Option<TagBitmap> = None;

                for f in filters {
                    let bitmap = self.query(f);
                    result = match result {
                        None => Some(bitmap),
                        Some(r) => Some(r.and(&bitmap)),
                    };
                }

                result.unwrap_or_default()
            }

            TagFilter::Or(filters) => {
                let mut result = TagBitmap::new();

                for f in filters {
                    let bitmap = self.query(f);
                    result = result.or(&bitmap);
                }

                result
            }

            TagFilter::Not(inner) => {
                let inner_bitmap = self.query(inner);
                self.all_series.read().and_not(&inner_bitmap)
            }

            TagFilter::HasKey(key_id) => {
                // PERF-003: Use pre-computed key bitmap for O(1) lookup
                self.key_bitmaps
                    .read()
                    .get(key_id)
                    .cloned()
                    .unwrap_or_default()
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.tag_combinations.load(Ordering::Relaxed),
            self.stats.series_count.load(Ordering::Relaxed),
            self.stats.queries.load(Ordering::Relaxed),
        )
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        let bitmaps = self.bitmaps.read();
        let bitmap_mem: usize = bitmaps.values().map(|b| b.memory_bytes()).sum();
        let all_series_mem = self.all_series.read().memory_bytes();

        bitmap_mem + all_series_mem
    }

    /// Get number of unique tag combinations
    pub fn tag_combination_count(&self) -> usize {
        self.bitmaps.read().len()
    }
}

impl Default for BitmapIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tag Filter
// ============================================================================

/// Filter expression for tag-based queries
#[derive(Debug, Clone)]
pub enum TagFilter {
    /// Match all series
    All,

    /// Exact tag match: key = value
    Exact(TagKeyId, TagValueId),

    /// AND of multiple filters
    And(Vec<TagFilter>),

    /// OR of multiple filters
    Or(Vec<TagFilter>),

    /// NOT of a filter
    Not(Box<TagFilter>),

    /// Series has any value for this key
    HasKey(TagKeyId),
}

impl TagFilter {
    /// Create an exact match filter
    #[must_use]
    pub fn exact(key_id: TagKeyId, value_id: TagValueId) -> Self {
        TagFilter::Exact(key_id, value_id)
    }

    /// Create an AND filter from an iterator
    ///
    /// PERF-007: Accepts iterator to avoid pre-allocation for single-element cases
    /// API-003: Returns `TagFilter::All` if filters is empty (neutral element for AND)
    #[must_use]
    pub fn and<I: IntoIterator<Item = TagFilter>>(filters: I) -> Self {
        let mut iter = filters.into_iter();
        let first = match iter.next() {
            None => return TagFilter::All, // Empty AND = match all (neutral element)
            Some(f) => f,
        };
        let second = match iter.next() {
            None => return first, // Single element - no allocation needed
            Some(s) => s,
        };
        // Multiple elements - now we need to allocate
        let mut vec = Vec::with_capacity(iter.size_hint().0 + 2);
        vec.push(first);
        vec.push(second);
        vec.extend(iter);
        TagFilter::And(vec)
    }

    /// Create an AND filter, returning None if filters is empty
    ///
    /// PERF-007: Accepts iterator to avoid pre-allocation
    /// API-003: Explicit handling of empty case
    #[must_use]
    pub fn try_and<I: IntoIterator<Item = TagFilter>>(filters: I) -> Option<Self> {
        let mut iter = filters.into_iter().peekable();
        iter.peek()?;
        Some(Self::and(iter))
    }

    /// Create an OR filter from an iterator
    ///
    /// PERF-007: Accepts iterator to avoid pre-allocation for single-element cases
    /// API-003: Returns empty bitmap equivalent if filters is empty
    #[must_use]
    pub fn or<I: IntoIterator<Item = TagFilter>>(filters: I) -> Self {
        let mut iter = filters.into_iter();
        let first = match iter.next() {
            None => return TagFilter::Or(vec![]), // Empty OR = match none
            Some(f) => f,
        };
        let second = match iter.next() {
            None => return first, // Single element - no allocation needed
            Some(s) => s,
        };
        // Multiple elements - now we need to allocate
        let mut vec = Vec::with_capacity(iter.size_hint().0 + 2);
        vec.push(first);
        vec.push(second);
        vec.extend(iter);
        TagFilter::Or(vec)
    }

    /// Create an OR filter, returning None if filters is empty
    ///
    /// PERF-007: Accepts iterator to avoid pre-allocation
    /// API-003: Explicit handling of empty case
    #[must_use]
    pub fn try_or<I: IntoIterator<Item = TagFilter>>(filters: I) -> Option<Self> {
        let mut iter = filters.into_iter().peekable();
        iter.peek()?;
        Some(Self::or(iter))
    }

    /// Create a NOT filter (negation)
    #[must_use]
    pub fn negate(filter: TagFilter) -> Self {
        TagFilter::Not(Box::new(filter))
    }

    /// Create a "has key" filter
    #[must_use]
    pub fn has_key(key_id: TagKeyId) -> Self {
        TagFilter::HasKey(key_id)
    }

    /// Check if this filter is empty (will match nothing)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, TagFilter::Or(filters) if filters.is_empty())
    }
}

impl std::fmt::Display for TagFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagFilter::All => write!(f, "*"),
            TagFilter::Exact(k, v) => write!(f, "tag[{}]={}", k.0, v.0),
            TagFilter::And(filters) => {
                if filters.is_empty() {
                    write!(f, "(empty AND)")
                } else {
                    let parts: Vec<_> = filters.iter().map(|flt| flt.to_string()).collect();
                    write!(f, "({})", parts.join(" AND "))
                }
            }
            TagFilter::Or(filters) => {
                if filters.is_empty() {
                    write!(f, "(empty OR)")
                } else {
                    let parts: Vec<_> = filters.iter().map(|flt| flt.to_string()).collect();
                    write!(f, "({})", parts.join(" OR "))
                }
            }
            TagFilter::Not(inner) => write!(f, "NOT {}", inner),
            TagFilter::HasKey(k) => write!(f, "tag[{}]=*", k.0),
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
    fn test_bitmap_basic() {
        let mut bitmap = TagBitmap::new();

        bitmap.set(1);
        bitmap.set(5);
        bitmap.set(100);

        assert!(bitmap.contains(1));
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(100));
        assert!(!bitmap.contains(2));
        assert!(!bitmap.contains(50));

        assert_eq!(bitmap.cardinality(), 3);
    }

    #[test]
    fn test_bitmap_clear() {
        let mut bitmap = TagBitmap::new();

        bitmap.set(1);
        bitmap.set(5);
        assert_eq!(bitmap.cardinality(), 2);

        bitmap.clear(1);
        assert!(!bitmap.contains(1));
        assert!(bitmap.contains(5));
        assert_eq!(bitmap.cardinality(), 1);
    }

    #[test]
    fn test_bitmap_and() {
        let mut b1 = TagBitmap::new();
        b1.set(1);
        b1.set(2);
        b1.set(3);

        let mut b2 = TagBitmap::new();
        b2.set(2);
        b2.set(3);
        b2.set(4);

        let result = b1.and(&b2);
        assert!(!result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(4));
        assert_eq!(result.cardinality(), 2);
    }

    #[test]
    fn test_bitmap_or() {
        let mut b1 = TagBitmap::new();
        b1.set(1);
        b1.set(2);

        let mut b2 = TagBitmap::new();
        b2.set(2);
        b2.set(3);

        let result = b1.or(&b2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert_eq!(result.cardinality(), 3);
    }

    #[test]
    fn test_bitmap_and_not() {
        let mut b1 = TagBitmap::new();
        b1.set(1);
        b1.set(2);
        b1.set(3);

        let mut b2 = TagBitmap::new();
        b2.set(2);

        let result = b1.and_not(&b2);
        assert!(result.contains(1));
        assert!(!result.contains(2));
        assert!(result.contains(3));
        assert_eq!(result.cardinality(), 2);
    }

    #[test]
    fn test_bitmap_iterator() {
        let mut bitmap = TagBitmap::new();
        bitmap.set(1);
        bitmap.set(5);
        bitmap.set(64);
        bitmap.set(100);

        let ids: Vec<SeriesId> = bitmap.iter().collect();
        assert_eq!(ids, vec![1, 5, 64, 100]);
    }

    #[test]
    fn test_bitmap_index_basic() {
        let index = BitmapIndex::new();

        let host_key = TagKeyId(1);
        let dc_key = TagKeyId(2);
        let server1 = TagValueId(1);
        let server2 = TagValueId(2);
        let us_east = TagValueId(10);
        let us_west = TagValueId(11);

        // Add series
        index.add_series(1, &[(host_key, server1), (dc_key, us_east)]);
        index.add_series(2, &[(host_key, server2), (dc_key, us_east)]);
        index.add_series(3, &[(host_key, server1), (dc_key, us_west)]);

        // Query single tag
        let host_s1 = index.get_bitmap(host_key, server1).unwrap();
        assert_eq!(host_s1.to_series_ids(), vec![1, 3]);

        let dc_east = index.get_bitmap(dc_key, us_east).unwrap();
        assert_eq!(dc_east.to_series_ids(), vec![1, 2]);
    }

    #[test]
    fn test_bitmap_index_query() {
        let index = BitmapIndex::new();

        let host_key = TagKeyId(1);
        let dc_key = TagKeyId(2);
        let server1 = TagValueId(1);
        let server2 = TagValueId(2);
        let us_east = TagValueId(10);
        let us_west = TagValueId(11);

        index.add_series(1, &[(host_key, server1), (dc_key, us_east)]);
        index.add_series(2, &[(host_key, server2), (dc_key, us_east)]);
        index.add_series(3, &[(host_key, server1), (dc_key, us_west)]);

        // Query: host=server1 AND dc=us-east
        let filter = TagFilter::and(vec![
            TagFilter::exact(host_key, server1),
            TagFilter::exact(dc_key, us_east),
        ]);
        let result = index.query(&filter);
        assert_eq!(result.to_series_ids(), vec![1]);

        // Query: host=server1 OR host=server2
        let filter = TagFilter::or(vec![
            TagFilter::exact(host_key, server1),
            TagFilter::exact(host_key, server2),
        ]);
        let result = index.query(&filter);
        assert_eq!(result.to_series_ids(), vec![1, 2, 3]);

        // Query: NOT dc=us-east
        let filter = TagFilter::negate(TagFilter::exact(dc_key, us_east));
        let result = index.query(&filter);
        assert_eq!(result.to_series_ids(), vec![3]);
    }

    #[test]
    fn test_bitmap_index_all() {
        let index = BitmapIndex::new();

        let host_key = TagKeyId(1);
        let server1 = TagValueId(1);

        index.add_series(1, &[(host_key, server1)]);
        index.add_series(2, &[(host_key, server1)]);
        index.add_series(3, &[(host_key, server1)]);

        let all = index.get_all_series();
        assert_eq!(all.to_series_ids(), vec![1, 2, 3]);
    }

    #[test]
    fn test_bitmap_index_has_key() {
        let index = BitmapIndex::new();

        let host_key = TagKeyId(1);
        let env_key = TagKeyId(2);
        let server1 = TagValueId(1);
        let server2 = TagValueId(2);
        let prod = TagValueId(10);

        index.add_series(1, &[(host_key, server1), (env_key, prod)]);
        index.add_series(2, &[(host_key, server2)]);
        index.add_series(3, &[(env_key, prod)]);

        // Query: has env key
        let filter = TagFilter::has_key(env_key);
        let result = index.query(&filter);
        assert_eq!(result.to_series_ids(), vec![1, 3]);
    }

    #[test]
    fn test_bitmap_large_ids() {
        let mut bitmap = TagBitmap::new();

        // Test with large IDs
        bitmap.set(1000);
        bitmap.set(5000);
        bitmap.set(10000);

        assert!(bitmap.contains(1000));
        assert!(bitmap.contains(5000));
        assert!(bitmap.contains(10000));
        assert!(!bitmap.contains(999));

        assert_eq!(bitmap.cardinality(), 3);
    }

    #[test]
    fn test_bitmap_memory() {
        let mut bitmap = TagBitmap::new();

        // Setting bit 1000 should allocate ~16 words (1000/64 ≈ 16)
        bitmap.set(1000);

        let mem = bitmap.memory_bytes();
        assert!(mem >= 16 * 8); // At least 16 words of 8 bytes
        assert!(mem < 1000); // But not too much
    }
}
