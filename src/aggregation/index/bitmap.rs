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
//! ```rust,ignore
//! use gorilla_tsdb::aggregation::index::{BitmapIndex, TagBitmap};
//!
//! let mut index = BitmapIndex::new();
//!
//! // Register series with tags
//! index.add_series(series_id_1, &[("host", "server1"), ("dc", "us-east")]);
//! index.add_series(series_id_2, &[("host", "server2"), ("dc", "us-east")]);
//! index.add_series(series_id_3, &[("host", "server1"), ("dc", "us-west")]);
//!
//! // Query: all series with dc=us-east
//! let bitmap = index.get_bitmap("dc", "us-east");
//! // Returns bitmap with bits 1 and 2 set
//!
//! // Query: all series with host=server1 AND dc=us-east
//! let host_bitmap = index.get_bitmap("host", "server1");
//! let dc_bitmap = index.get_bitmap("dc", "us-east");
//! let result = host_bitmap.and(&dc_bitmap);
//! // Returns bitmap with only bit 1 set
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
            all_series: RwLock::new(TagBitmap::new()),
            stats: BitmapIndexStats::default(),
        }
    }

    /// Add a series with its tags to the index
    pub fn add_series(&self, series_id: SeriesId, tags: &[(TagKeyId, TagValueId)]) {
        let mut bitmaps = self.bitmaps.write();

        for (key_id, value_id) in tags {
            let bitmap = bitmaps.entry((*key_id, *value_id)).or_insert_with(|| {
                self.stats.tag_combinations.fetch_add(1, Ordering::Relaxed);
                TagBitmap::new()
            });
            bitmap.set(series_id);
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
        }

        self.all_series.write().clear(series_id);
    }

    /// Get the bitmap for a specific tag value
    pub fn get_bitmap(&self, key_id: TagKeyId, value_id: TagValueId) -> Option<TagBitmap> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.bitmaps.read().get(&(key_id, value_id)).cloned()
    }

    /// Get all series (for queries with no tag filters)
    pub fn get_all_series(&self) -> TagBitmap {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.all_series.read().clone()
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
                // Get all values for this key and OR them together
                let bitmaps = self.bitmaps.read();
                let mut result = TagBitmap::new();

                for ((k, _), bitmap) in bitmaps.iter() {
                    if k == key_id {
                        result = result.or(bitmap);
                    }
                }

                result
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
    pub fn exact(key_id: TagKeyId, value_id: TagValueId) -> Self {
        TagFilter::Exact(key_id, value_id)
    }

    /// Create an AND filter
    pub fn and(filters: Vec<TagFilter>) -> Self {
        if filters.len() == 1 {
            filters.into_iter().next().unwrap()
        } else {
            TagFilter::And(filters)
        }
    }

    /// Create an OR filter
    pub fn or(filters: Vec<TagFilter>) -> Self {
        if filters.len() == 1 {
            filters.into_iter().next().unwrap()
        } else {
            TagFilter::Or(filters)
        }
    }

    /// Create a NOT filter (negation)
    pub fn negate(filter: TagFilter) -> Self {
        TagFilter::Not(Box::new(filter))
    }

    /// Create a "has key" filter
    pub fn has_key(key_id: TagKeyId) -> Self {
        TagFilter::HasKey(key_id)
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
