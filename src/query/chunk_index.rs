//! Chunk Index with Bloom Filter Pruning
//!
//! Provides efficient chunk discovery and pruning using bloom filters
//! to quickly eliminate chunks that cannot contain requested series.
//!
//! # Architecture
//!
//! ```text
//! Query (series_ids, time_range)
//!            │
//!            ▼
//! ┌─────────────────────────────┐
//! │     ChunkIndex              │
//! │  ┌───────────────────────┐  │
//! │  │ Bloom Filter (series) │  │◄── Quick series membership test
//! │  └───────────────────────┘  │
//! │  ┌───────────────────────┐  │
//! │  │ Zone Maps (time)      │  │◄── Time range overlap check
//! │  └───────────────────────┘  │
//! │  ┌───────────────────────┐  │
//! │  │ Chunk Metadata        │  │◄── Full metadata for matching chunks
//! │  └───────────────────────┘  │
//! └─────────────────────────────┘
//!            │
//!            ▼
//!    Pruned Chunk List
//! ```
//!
//! # Usage
//!
//! Reuses the existing `BloomFilter` from `crate::ingestion::schema::BloomFilter`
//! to avoid duplicating bloom filter logic.

use crate::ingestion::schema::BloomFilter;
use crate::storage::chunk::ChunkMetadata;
use crate::types::{ChunkId, SeriesId, TimeRange};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Chunk Index Configuration
// ============================================================================

/// Configuration for the chunk index
#[derive(Debug, Clone)]
pub struct ChunkIndexConfig {
    /// Expected number of series per chunk for bloom filter sizing
    pub bloom_expected_series: usize,

    /// Target false positive rate for bloom filters (0.0 - 1.0)
    pub bloom_fp_rate: f64,

    /// Enable bloom filter pruning
    pub enable_bloom_pruning: bool,

    /// Enable zone map (time range) pruning
    pub enable_zone_map_pruning: bool,
}

impl Default for ChunkIndexConfig {
    fn default() -> Self {
        Self {
            bloom_expected_series: 1000,
            bloom_fp_rate: 0.01, // 1% false positive rate
            enable_bloom_pruning: true,
            enable_zone_map_pruning: true,
        }
    }
}

impl ChunkIndexConfig {
    /// Create config with custom bloom filter settings
    pub fn with_bloom_settings(mut self, expected_series: usize, fp_rate: f64) -> Self {
        self.bloom_expected_series = expected_series;
        self.bloom_fp_rate = fp_rate;
        self
    }

    /// Disable bloom filter pruning
    pub fn without_bloom_pruning(mut self) -> Self {
        self.enable_bloom_pruning = false;
        self
    }

    /// Disable zone map pruning
    pub fn without_zone_map_pruning(mut self) -> Self {
        self.enable_zone_map_pruning = false;
        self
    }
}

// ============================================================================
// Indexed Chunk Entry
// ============================================================================

/// A chunk entry with associated bloom filter for series membership testing
pub struct IndexedChunk {
    /// Full chunk metadata
    pub metadata: ChunkMetadata,

    /// Bloom filter for series IDs in this chunk
    /// Uses the existing BloomFilter from ingestion::schema
    series_bloom: BloomFilter,
}

impl IndexedChunk {
    /// Create a new indexed chunk with bloom filter
    pub fn new(metadata: ChunkMetadata, config: &ChunkIndexConfig) -> Self {
        let series_bloom = BloomFilter::new(config.bloom_expected_series, config.bloom_fp_rate);

        Self {
            metadata,
            series_bloom,
        }
    }

    /// Add a series ID to the bloom filter
    ///
    /// Call this when inserting data points to track which series are in this chunk.
    pub fn add_series(&self, series_id: SeriesId) {
        self.series_bloom.insert(&series_id);
    }

    /// Check if this chunk might contain the given series
    ///
    /// Returns:
    /// - `false`: Chunk definitely does NOT contain this series
    /// - `true`: Chunk might contain this series (possible false positive)
    pub fn may_contain_series(&self, series_id: SeriesId) -> bool {
        self.series_bloom.contains(&series_id)
    }

    /// Check if chunk time range overlaps with query range
    pub fn overlaps_time_range(&self, range: &TimeRange) -> bool {
        self.metadata.overlaps(range.start, range.end)
    }

    /// Get bloom filter fill ratio for monitoring
    pub fn bloom_fill_ratio(&self) -> f64 {
        self.series_bloom.fill_ratio()
    }

    /// Get estimated false positive rate
    pub fn estimated_fp_rate(&self) -> f64 {
        self.series_bloom.estimated_fp_rate()
    }
}

// ============================================================================
// Chunk Index
// ============================================================================

/// Index for efficient chunk discovery with bloom filter and zone map pruning
///
/// Uses the existing `BloomFilter` implementation from `ingestion::schema`
/// to maintain a single, well-tested bloom filter implementation.
pub struct ChunkIndex {
    /// Configuration
    config: ChunkIndexConfig,

    /// Indexed chunks by chunk ID (String-based UUID)
    chunks: RwLock<HashMap<String, IndexedChunk>>,

    /// Statistics
    stats: ChunkIndexStats,
}

/// Statistics for chunk index operations
#[derive(Debug, Default)]
pub struct ChunkIndexStats {
    /// Total chunks indexed
    pub total_chunks: AtomicU64,

    /// Chunks pruned by bloom filter
    pub bloom_pruned: AtomicU64,

    /// Chunks pruned by zone map (time range)
    pub zone_map_pruned: AtomicU64,

    /// Total queries processed
    pub queries: AtomicU64,

    /// Chunks returned after pruning
    pub chunks_returned: AtomicU64,
}

impl ChunkIndex {
    /// Create a new chunk index with default configuration
    pub fn new() -> Self {
        Self::with_config(ChunkIndexConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: ChunkIndexConfig) -> Self {
        Self {
            config,
            chunks: RwLock::new(HashMap::new()),
            stats: ChunkIndexStats::default(),
        }
    }

    /// Register a chunk in the index
    ///
    /// Creates an indexed entry with a bloom filter for the chunk.
    /// Call `add_series_to_chunk` to populate the bloom filter.
    pub fn register_chunk(&self, metadata: ChunkMetadata) {
        let chunk_id = metadata.chunk_id.to_string();
        let indexed = IndexedChunk::new(metadata, &self.config);

        let mut chunks = self.chunks.write();
        chunks.insert(chunk_id, indexed);
        self.stats.total_chunks.fetch_add(1, Ordering::Relaxed);
    }

    /// Add a series ID to a chunk's bloom filter
    ///
    /// Call this when data points are written to track series membership.
    pub fn add_series_to_chunk(&self, chunk_id: &ChunkId, series_id: SeriesId) {
        let chunks = self.chunks.read();
        if let Some(indexed) = chunks.get(&chunk_id.to_string()) {
            indexed.add_series(series_id);
        }
    }

    /// Find chunks that might contain the given series within the time range
    ///
    /// Applies two-level pruning:
    /// 1. Bloom filter: Skip chunks that definitely don't have the series
    /// 2. Zone map: Skip chunks outside the time range
    ///
    /// Returns chunk metadata for chunks that pass both filters.
    pub fn find_chunks(
        &self,
        series_id: SeriesId,
        time_range: Option<&TimeRange>,
    ) -> Vec<ChunkMetadata> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);

        let chunks = self.chunks.read();
        let mut result = Vec::new();
        let mut bloom_pruned = 0u64;
        let mut zone_map_pruned = 0u64;

        for indexed in chunks.values() {
            // Level 1: Bloom filter pruning (series membership)
            if self.config.enable_bloom_pruning && !indexed.may_contain_series(series_id) {
                bloom_pruned += 1;
                continue;
            }

            // Level 2: Zone map pruning (time range overlap)
            if self.config.enable_zone_map_pruning {
                if let Some(range) = time_range {
                    if !indexed.overlaps_time_range(range) {
                        zone_map_pruned += 1;
                        continue;
                    }
                }
            }

            result.push(indexed.metadata.clone());
        }

        // Update statistics
        self.stats
            .bloom_pruned
            .fetch_add(bloom_pruned, Ordering::Relaxed);
        self.stats
            .zone_map_pruned
            .fetch_add(zone_map_pruned, Ordering::Relaxed);
        self.stats
            .chunks_returned
            .fetch_add(result.len() as u64, Ordering::Relaxed);

        result
    }

    /// Find chunks for multiple series (OR semantics)
    ///
    /// Returns chunks that might contain ANY of the given series.
    pub fn find_chunks_multi_series(
        &self,
        series_ids: &[SeriesId],
        time_range: Option<&TimeRange>,
    ) -> Vec<ChunkMetadata> {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);

        let chunks = self.chunks.read();
        let mut result = Vec::new();
        let mut bloom_pruned = 0u64;
        let mut zone_map_pruned = 0u64;

        for indexed in chunks.values() {
            // Level 1: Bloom filter pruning - skip if NONE of the series might be present
            if self.config.enable_bloom_pruning {
                let any_match = series_ids
                    .iter()
                    .any(|&sid| indexed.may_contain_series(sid));
                if !any_match {
                    bloom_pruned += 1;
                    continue;
                }
            }

            // Level 2: Zone map pruning
            if self.config.enable_zone_map_pruning {
                if let Some(range) = time_range {
                    if !indexed.overlaps_time_range(range) {
                        zone_map_pruned += 1;
                        continue;
                    }
                }
            }

            result.push(indexed.metadata.clone());
        }

        self.stats
            .bloom_pruned
            .fetch_add(bloom_pruned, Ordering::Relaxed);
        self.stats
            .zone_map_pruned
            .fetch_add(zone_map_pruned, Ordering::Relaxed);
        self.stats
            .chunks_returned
            .fetch_add(result.len() as u64, Ordering::Relaxed);

        result
    }

    /// Remove a chunk from the index
    pub fn remove_chunk(&self, chunk_id: &ChunkId) {
        let mut chunks = self.chunks.write();
        if chunks.remove(&chunk_id.to_string()).is_some() {
            self.stats.total_chunks.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> IndexStatsSnapshot {
        IndexStatsSnapshot {
            total_chunks: self.stats.total_chunks.load(Ordering::Relaxed),
            bloom_pruned: self.stats.bloom_pruned.load(Ordering::Relaxed),
            zone_map_pruned: self.stats.zone_map_pruned.load(Ordering::Relaxed),
            queries: self.stats.queries.load(Ordering::Relaxed),
            chunks_returned: self.stats.chunks_returned.load(Ordering::Relaxed),
        }
    }

    /// Calculate pruning efficiency
    ///
    /// Returns the ratio of chunks pruned to total chunks examined.
    pub fn pruning_efficiency(&self) -> f64 {
        let queries = self.stats.queries.load(Ordering::Relaxed);
        if queries == 0 {
            return 0.0;
        }

        let total_chunks = self.stats.total_chunks.load(Ordering::Relaxed);
        let returned = self.stats.chunks_returned.load(Ordering::Relaxed);

        if total_chunks == 0 {
            return 0.0;
        }

        // Efficiency = (examined - returned) / examined
        let examined = queries * total_chunks;
        let pruned = examined.saturating_sub(returned);
        pruned as f64 / examined as f64
    }

    /// Clear all indexed chunks
    pub fn clear(&self) {
        let mut chunks = self.chunks.write();
        chunks.clear();
        self.stats.total_chunks.store(0, Ordering::Relaxed);
    }

    /// Get number of indexed chunks
    pub fn len(&self) -> usize {
        self.chunks.read().len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.chunks.read().is_empty()
    }
}

impl Default for ChunkIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of index statistics for monitoring and tuning
#[derive(Debug, Clone)]
pub struct IndexStatsSnapshot {
    /// Total number of chunks in the index
    pub total_chunks: u64,
    /// Number of chunks pruned by bloom filter (series not present)
    pub bloom_pruned: u64,
    /// Number of chunks pruned by zone map (time range not overlapping)
    pub zone_map_pruned: u64,
    /// Total number of queries processed
    pub queries: u64,
    /// Total number of chunks returned after pruning
    pub chunks_returned: u64,
}

impl IndexStatsSnapshot {
    /// Calculate bloom filter pruning ratio
    pub fn bloom_prune_ratio(&self) -> f64 {
        let total = self.bloom_pruned + self.zone_map_pruned + self.chunks_returned;
        if total == 0 {
            0.0
        } else {
            self.bloom_pruned as f64 / total as f64
        }
    }

    /// Calculate zone map pruning ratio
    pub fn zone_map_prune_ratio(&self) -> f64 {
        let total = self.bloom_pruned + self.zone_map_pruned + self.chunks_returned;
        if total == 0 {
            0.0
        } else {
            self.zone_map_pruned as f64 / total as f64
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::CompressionType;
    use crate::types::ChunkId;
    use std::path::PathBuf;

    fn make_chunk_metadata(series_id: SeriesId, start: i64, end: i64) -> ChunkMetadata {
        ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id,
            path: PathBuf::from("/tmp/test.gor"),
            start_timestamp: start,
            end_timestamp: end,
            point_count: 100,
            size_bytes: 1024,
            compression: CompressionType::Gorilla,
            created_at: 0,
            last_accessed: 0,
        }
    }

    #[test]
    fn test_chunk_index_basic() {
        let index = ChunkIndex::new();

        let metadata = make_chunk_metadata(1, 0, 1000);
        let chunk_id = metadata.chunk_id.clone();

        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_bloom_filter_pruning() {
        let index = ChunkIndex::new();

        // Register a chunk with series 1
        let metadata = make_chunk_metadata(1, 0, 1000);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);

        // Query for series 1 should find the chunk
        let results = index.find_chunks(1, None);
        assert_eq!(results.len(), 1);

        // Query for series 2 should NOT find the chunk (bloom filter prunes it)
        let results = index.find_chunks(2, None);
        assert_eq!(results.len(), 0);

        // Check stats show bloom pruning occurred
        let stats = index.stats();
        assert!(stats.bloom_pruned > 0);
    }

    #[test]
    fn test_zone_map_pruning() {
        let index = ChunkIndex::new();

        // Register a chunk covering time 100-200
        let metadata = make_chunk_metadata(1, 100, 200);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);

        // Query overlapping time range should find chunk
        let range = TimeRange {
            start: 150,
            end: 250,
        };
        let results = index.find_chunks(1, Some(&range));
        assert_eq!(results.len(), 1);

        // Query non-overlapping time range should NOT find chunk
        let range = TimeRange {
            start: 300,
            end: 400,
        };
        let results = index.find_chunks(1, Some(&range));
        assert_eq!(results.len(), 0);

        // Check stats show zone map pruning occurred
        let stats = index.stats();
        assert!(stats.zone_map_pruned > 0);
    }

    #[test]
    fn test_multi_series_query() {
        let index = ChunkIndex::new();

        // Register chunk with series 1 and 2
        let metadata = make_chunk_metadata(1, 0, 1000);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);
        index.add_series_to_chunk(&chunk_id, 2);

        // Query for [1, 3] should find chunk (contains series 1)
        let results = index.find_chunks_multi_series(&[1, 3], None);
        assert_eq!(results.len(), 1);

        // Query for [3, 4] should NOT find chunk
        let results = index.find_chunks_multi_series(&[3, 4], None);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_config_disable_pruning() {
        let config = ChunkIndexConfig::default()
            .without_bloom_pruning()
            .without_zone_map_pruning();

        let index = ChunkIndex::with_config(config);

        let metadata = make_chunk_metadata(1, 100, 200);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);

        // With pruning disabled, queries should return all chunks
        let results = index.find_chunks(
            2,
            Some(&TimeRange {
                start: 300,
                end: 400,
            }),
        );
        assert_eq!(results.len(), 1); // No pruning applied
    }

    #[test]
    fn test_remove_chunk() {
        let index = ChunkIndex::new();

        let metadata = make_chunk_metadata(1, 0, 1000);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);

        assert_eq!(index.len(), 1);

        index.remove_chunk(&chunk_id);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_clear() {
        let index = ChunkIndex::new();

        for i in 0..5 {
            let metadata = make_chunk_metadata(i as u128, i * 100, (i + 1) * 100);
            index.register_chunk(metadata);
        }

        assert_eq!(index.len(), 5);

        index.clear();
        assert!(index.is_empty());
    }

    #[test]
    fn test_stats_snapshot() {
        let index = ChunkIndex::new();

        let metadata = make_chunk_metadata(1, 0, 1000);
        let chunk_id = metadata.chunk_id.clone();
        index.register_chunk(metadata);
        index.add_series_to_chunk(&chunk_id, 1);

        // Run some queries
        let _ = index.find_chunks(1, None);
        let _ = index.find_chunks(2, None);

        let stats = index.stats();
        assert_eq!(stats.total_chunks, 1);
        assert_eq!(stats.queries, 2);
    }
}
