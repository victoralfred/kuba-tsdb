//! Chunk Coalescing Module
//!
//! This module provides functionality to merge small chunks from the same series
//! before compression to improve compression ratio and reduce chunk count.
//!
//! # Overview
//!
//! When time-series data is written in small batches, it can result in many small
//! chunks. Small chunks have:
//! - Lower compression ratios (less data to find patterns in)
//! - Higher metadata overhead (each chunk has a 64-byte header)
//! - More index entries to manage
//!
//! Chunk coalescing addresses this by merging adjacent small chunks into larger
//! ones, which typically achieve better compression and reduce storage overhead.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     ChunkCoalescer                               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────────┐     ┌───────────────────────────────────┐ │
//! │  │ find_candidates()│────▶│ CoalescingCandidate               │ │
//! │  │                  │     │ (group of mergeable chunks)       │ │
//! │  └──────────────────┘     └───────────────────────────────────┘ │
//! │           │                              │                       │
//! │           ▼                              ▼                       │
//! │  ┌──────────────────┐     ┌───────────────────────────────────┐ │
//! │  │ should_coalesce()│────▶│ coalesce()                        │ │
//! │  │ (evaluate merit) │     │ (merge chunks)                    │ │
//! │  └──────────────────┘     └───────────────────────────────────┘ │
//! │                                          │                       │
//! │                                          ▼                       │
//! │                           ┌───────────────────────────────────┐ │
//! │                           │ CoalescingResult                  │ │
//! │                           │ (new merged chunk + old chunks)   │ │
//! │                           └───────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::chunk_coalescing::{ChunkCoalescer, CoalescingConfig};
//!
//! // Create coalescer with default configuration
//! let coalescer = ChunkCoalescer::new(CoalescingConfig::default());
//!
//! // Find candidates for a specific series
//! let candidates = coalescer.find_candidates(series_id, &chunks);
//!
//! // Coalesce if beneficial
//! for candidate in candidates {
//!     if coalescer.should_coalesce(&candidate) {
//!         let result = coalescer.coalesce(candidate).await?;
//!         println!("Merged {} chunks into 1", result.chunks_merged);
//!     }
//! }
//! ```

use crate::storage::chunk::{ChunkMetadata, CompressionType};
use crate::types::{ChunkId, DataPoint, SeriesId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for chunk coalescing behavior
///
/// Controls when and how chunks are merged together.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::storage::chunk_coalescing::CoalescingConfig;
///
/// // Create configuration for aggressive coalescing
/// let config = CoalescingConfig {
///     min_chunk_size_points: 100,     // Chunks under 100 points are small
///     max_coalesced_size_points: 50_000,  // Merged chunk can have up to 50K points
///     min_chunks_to_merge: 2,         // Need at least 2 chunks to merge
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct CoalescingConfig {
    /// Minimum point count for a chunk to be considered "full"
    ///
    /// Chunks with fewer points than this are candidates for merging.
    /// Default: 1000 points
    pub min_chunk_size_points: usize,

    /// Maximum point count for a coalesced chunk
    ///
    /// Merged chunks won't exceed this size. This prevents creating
    /// chunks that are too large to decompress efficiently.
    /// Default: 100,000 points
    pub max_coalesced_size_points: usize,

    /// Maximum time span for a coalesced chunk in milliseconds
    ///
    /// Prevents merging chunks that span too long a time period,
    /// which could hurt query efficiency.
    /// Default: 4 hours (14,400,000 ms)
    pub max_coalesced_time_span_ms: i64,

    /// Minimum number of chunks required to consider coalescing
    ///
    /// Don't bother coalescing unless we have at least this many small chunks.
    /// Default: 2
    pub min_chunks_to_merge: usize,

    /// Maximum number of chunks to merge in a single operation
    ///
    /// Limits memory usage during coalescing.
    /// Default: 10
    pub max_chunks_per_merge: usize,

    /// Minimum age in milliseconds before a chunk is eligible for coalescing
    ///
    /// Prevents coalescing chunks that are still actively being read.
    /// Default: 5 minutes (300,000 ms)
    pub min_age_for_coalesce_ms: i64,

    /// Minimum compression ratio improvement expected from coalescing
    ///
    /// If estimated improvement is below this threshold, skip coalescing.
    /// Default: 1.1 (10% improvement)
    pub min_improvement_ratio: f64,

    /// Enable gap detection between chunks
    ///
    /// When true, won't merge chunks that have time gaps between them.
    /// Default: true
    pub detect_time_gaps: bool,

    /// Maximum allowed gap between adjacent chunks in milliseconds
    ///
    /// If the gap between two chunks exceeds this, they won't be merged.
    /// Only used when `detect_time_gaps` is true.
    /// Default: 60,000 ms (1 minute)
    pub max_time_gap_ms: i64,

    /// Compression type to use for coalesced chunks
    ///
    /// Default: AHPAC (adaptive compression)
    pub compression_type: CompressionType,
}

impl Default for CoalescingConfig {
    fn default() -> Self {
        Self {
            min_chunk_size_points: 1000,
            max_coalesced_size_points: 100_000,
            max_coalesced_time_span_ms: 4 * 60 * 60 * 1000, // 4 hours
            min_chunks_to_merge: 2,
            max_chunks_per_merge: 10,
            min_age_for_coalesce_ms: 5 * 60 * 1000, // 5 minutes
            min_improvement_ratio: 1.1,
            detect_time_gaps: true,
            max_time_gap_ms: 60_000, // 1 minute
            compression_type: CompressionType::Ahpac,
        }
    }
}

impl CoalescingConfig {
    /// Create configuration optimized for better compression ratio
    ///
    /// Uses larger merged chunks and more aggressive coalescing.
    pub fn optimize_ratio() -> Self {
        Self {
            min_chunk_size_points: 2000,
            max_coalesced_size_points: 200_000,
            max_coalesced_time_span_ms: 8 * 60 * 60 * 1000, // 8 hours
            min_chunks_to_merge: 2,
            max_chunks_per_merge: 20,
            min_age_for_coalesce_ms: 10 * 60 * 1000, // 10 minutes
            min_improvement_ratio: 1.05,             // Lower threshold
            detect_time_gaps: true,
            max_time_gap_ms: 120_000, // 2 minutes
            compression_type: CompressionType::Ahpac,
        }
    }

    /// Create configuration optimized for query performance
    ///
    /// Creates smaller merged chunks for faster query response times.
    pub fn optimize_query() -> Self {
        Self {
            min_chunk_size_points: 500,
            max_coalesced_size_points: 50_000,
            max_coalesced_time_span_ms: 2 * 60 * 60 * 1000, // 2 hours
            min_chunks_to_merge: 3,
            max_chunks_per_merge: 5,
            min_age_for_coalesce_ms: 3 * 60 * 1000, // 3 minutes
            min_improvement_ratio: 1.2,             // Higher threshold
            detect_time_gaps: true,
            max_time_gap_ms: 30_000, // 30 seconds
            compression_type: CompressionType::Ahpac,
        }
    }

    /// Create configuration for low-memory environments
    ///
    /// Limits the size of merge operations to reduce memory usage.
    pub fn low_memory() -> Self {
        Self {
            min_chunk_size_points: 500,
            max_coalesced_size_points: 25_000,
            max_coalesced_time_span_ms: 60 * 60 * 1000, // 1 hour
            min_chunks_to_merge: 2,
            max_chunks_per_merge: 3,
            min_age_for_coalesce_ms: 5 * 60 * 1000,
            min_improvement_ratio: 1.15,
            detect_time_gaps: true,
            max_time_gap_ms: 60_000,
            compression_type: CompressionType::Ahpac,
        }
    }

    /// Validate the configuration
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `min_chunk_size_points` is 0
    /// - `max_coalesced_size_points` is less than `min_chunk_size_points`
    /// - `min_chunks_to_merge` is less than 2
    /// - `max_chunks_per_merge` is less than `min_chunks_to_merge`
    /// - `min_improvement_ratio` is less than 1.0
    pub fn validate(&self) -> Result<(), String> {
        if self.min_chunk_size_points == 0 {
            return Err("min_chunk_size_points must be greater than 0".to_string());
        }

        if self.max_coalesced_size_points < self.min_chunk_size_points {
            return Err(format!(
                "max_coalesced_size_points ({}) must be >= min_chunk_size_points ({})",
                self.max_coalesced_size_points, self.min_chunk_size_points
            ));
        }

        if self.min_chunks_to_merge < 2 {
            return Err(format!(
                "min_chunks_to_merge ({}) must be at least 2",
                self.min_chunks_to_merge
            ));
        }

        if self.max_chunks_per_merge < self.min_chunks_to_merge {
            return Err(format!(
                "max_chunks_per_merge ({}) must be >= min_chunks_to_merge ({})",
                self.max_chunks_per_merge, self.min_chunks_to_merge
            ));
        }

        if self.min_improvement_ratio < 1.0 {
            return Err(format!(
                "min_improvement_ratio ({}) must be >= 1.0",
                self.min_improvement_ratio
            ));
        }

        if self.max_coalesced_time_span_ms <= 0 {
            return Err("max_coalesced_time_span_ms must be positive".to_string());
        }

        if self.min_age_for_coalesce_ms < 0 {
            return Err("min_age_for_coalesce_ms must be non-negative".to_string());
        }

        if self.detect_time_gaps && self.max_time_gap_ms <= 0 {
            return Err(
                "max_time_gap_ms must be positive when detect_time_gaps is enabled".to_string(),
            );
        }

        Ok(())
    }
}

// ============================================================================
// Coalescing Candidate
// ============================================================================

/// A group of chunks that are candidates for merging
///
/// Represents a set of adjacent small chunks from the same series
/// that can potentially be merged into a single larger chunk.
#[derive(Debug, Clone)]
pub struct CoalescingCandidate {
    /// Series ID for all chunks in this candidate
    pub series_id: SeriesId,

    /// Metadata for chunks to be merged (sorted by start_timestamp)
    pub chunks: Vec<ChunkMetadata>,

    /// Estimated compression improvement from merging
    pub estimated_improvement: f64,

    /// Time when this candidate was identified
    pub identified_at: Instant,
}

impl CoalescingCandidate {
    /// Create a new coalescing candidate
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series these chunks belong to
    /// * `chunks` - Vector of chunk metadata (will be sorted by timestamp)
    pub fn new(series_id: SeriesId, mut chunks: Vec<ChunkMetadata>) -> Self {
        // Sort chunks by start timestamp for proper ordering
        chunks.sort_by_key(|c| c.start_timestamp);

        Self {
            series_id,
            chunks,
            estimated_improvement: 1.0,
            identified_at: Instant::now(),
        }
    }

    /// Get the total number of points across all chunks
    pub fn total_points(&self) -> u64 {
        self.chunks.iter().map(|c| c.point_count as u64).sum()
    }

    /// Get the total compressed size across all chunks
    pub fn total_size_bytes(&self) -> u64 {
        self.chunks.iter().map(|c| c.size_bytes).sum()
    }

    /// Get the time span covered by all chunks
    ///
    /// Returns (start_timestamp, end_timestamp) tuple.
    pub fn time_span(&self) -> (i64, i64) {
        if self.chunks.is_empty() {
            return (0, 0);
        }

        let start = self.chunks.first().unwrap().start_timestamp;
        let end = self.chunks.last().unwrap().end_timestamp;
        (start, end)
    }

    /// Get the duration covered by all chunks in milliseconds
    pub fn duration_ms(&self) -> i64 {
        let (start, end) = self.time_span();
        end.saturating_sub(start)
    }

    /// Get the number of chunks in this candidate
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Check if the candidate is empty
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Get paths of all chunks in this candidate
    pub fn chunk_paths(&self) -> Vec<PathBuf> {
        self.chunks.iter().map(|c| c.path.clone()).collect()
    }

    /// Get chunk IDs of all chunks in this candidate
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.chunk_id.clone()).collect()
    }
}

// ============================================================================
// Coalescing Result
// ============================================================================

/// Result of a successful coalescing operation
#[derive(Debug, Clone)]
pub struct CoalescingResult {
    /// Series ID for the coalesced chunk
    pub series_id: SeriesId,

    /// Metadata of the new merged chunk
    pub new_chunk: ChunkMetadata,

    /// Metadata of the original chunks that were merged
    pub original_chunks: Vec<ChunkMetadata>,

    /// Number of chunks that were merged
    pub chunks_merged: usize,

    /// Total points in the new chunk
    pub total_points: u64,

    /// Original total size before merge (bytes)
    pub original_size_bytes: u64,

    /// New size after merge (bytes)
    pub new_size_bytes: u64,

    /// Actual compression improvement ratio
    pub improvement_ratio: f64,

    /// Time taken for the merge operation
    pub duration: Duration,
}

impl CoalescingResult {
    /// Get the space saved in bytes
    pub fn space_saved_bytes(&self) -> i64 {
        self.original_size_bytes as i64 - self.new_size_bytes as i64
    }

    /// Get the space saved as a percentage
    pub fn space_saved_percent(&self) -> f64 {
        if self.original_size_bytes == 0 {
            return 0.0;
        }
        (1.0 - (self.new_size_bytes as f64 / self.original_size_bytes as f64)) * 100.0
    }
}

// ============================================================================
// Coalescing Statistics
// ============================================================================

/// Statistics for the chunk coalescer
#[derive(Debug)]
struct CoalescingStats {
    /// Total number of coalescing operations performed
    operations_total: AtomicU64,

    /// Total chunks merged across all operations
    chunks_merged_total: AtomicU64,

    /// Total bytes saved by coalescing
    bytes_saved_total: AtomicU64,

    /// Total points processed
    points_processed_total: AtomicU64,

    /// Number of candidates skipped (didn't meet criteria)
    candidates_skipped: AtomicU64,

    /// Number of failed coalescing attempts
    failures_total: AtomicU64,

    /// Total duration spent coalescing (microseconds)
    duration_total_us: AtomicU64,
}

impl CoalescingStats {
    fn new() -> Self {
        Self {
            operations_total: AtomicU64::new(0),
            chunks_merged_total: AtomicU64::new(0),
            bytes_saved_total: AtomicU64::new(0),
            points_processed_total: AtomicU64::new(0),
            candidates_skipped: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
            duration_total_us: AtomicU64::new(0),
        }
    }

    fn record_success(&self, result: &CoalescingResult) {
        self.operations_total.fetch_add(1, Ordering::Relaxed);
        self.chunks_merged_total
            .fetch_add(result.chunks_merged as u64, Ordering::Relaxed);
        self.points_processed_total
            .fetch_add(result.total_points, Ordering::Relaxed);

        let saved = result.space_saved_bytes();
        if saved > 0 {
            self.bytes_saved_total
                .fetch_add(saved as u64, Ordering::Relaxed);
        }

        self.duration_total_us
            .fetch_add(result.duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_skip(&self) {
        self.candidates_skipped.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot of coalescing statistics
#[derive(Debug, Clone)]
pub struct CoalescingStatsSnapshot {
    /// Total coalescing operations performed
    pub operations_total: u64,

    /// Total chunks merged
    pub chunks_merged_total: u64,

    /// Total bytes saved
    pub bytes_saved_total: u64,

    /// Total points processed
    pub points_processed_total: u64,

    /// Candidates skipped
    pub candidates_skipped: u64,

    /// Failed operations
    pub failures_total: u64,

    /// Average duration per operation in microseconds
    pub avg_duration_us: u64,
}

// ============================================================================
// Chunk Coalescer
// ============================================================================

/// Main component for finding and merging small chunks
///
/// The `ChunkCoalescer` analyzes chunks for a series and identifies groups
/// of small chunks that can be merged together for better compression.
///
/// # Thread Safety
///
/// `ChunkCoalescer` is thread-safe and can be shared across threads.
/// Statistics are updated atomically.
pub struct ChunkCoalescer {
    /// Configuration for coalescing behavior
    config: CoalescingConfig,

    /// Statistics tracking
    stats: CoalescingStats,

    /// Series currently being coalesced (to prevent concurrent coalescing)
    in_progress: RwLock<HashMap<SeriesId, Instant>>,
}

impl ChunkCoalescer {
    /// Create a new chunk coalescer with the given configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for coalescing behavior
    ///
    /// # Example
    ///
    /// ```rust
    /// use kuba_tsdb::storage::chunk_coalescing::{ChunkCoalescer, CoalescingConfig};
    ///
    /// let coalescer = ChunkCoalescer::new(CoalescingConfig::default());
    /// ```
    pub fn new(config: CoalescingConfig) -> Self {
        Self {
            config,
            stats: CoalescingStats::new(),
            in_progress: RwLock::new(HashMap::new()),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &CoalescingConfig {
        &self.config
    }

    /// Find coalescing candidates for a series
    ///
    /// Analyzes the provided chunks and identifies groups of small chunks
    /// that can be merged together.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series to find candidates for
    /// * `chunks` - All chunks for this series
    ///
    /// # Returns
    ///
    /// Vector of coalescing candidates, each representing a group of
    /// chunks that can be merged.
    pub fn find_candidates(
        &self,
        series_id: SeriesId,
        chunks: &[ChunkMetadata],
    ) -> Vec<CoalescingCandidate> {
        let mut candidates = Vec::new();

        // Filter to chunks for this series that are small and old enough
        let now = chrono::Utc::now().timestamp_millis();
        let mut small_chunks: Vec<&ChunkMetadata> = chunks
            .iter()
            .filter(|c| {
                // Only include chunks for the requested series
                let is_correct_series = c.series_id == series_id;

                // Check if chunk is small enough
                let is_small = (c.point_count as usize) < self.config.min_chunk_size_points;

                // Check if chunk is old enough
                let age_ms = now.saturating_sub(c.created_at);
                let is_old_enough = age_ms >= self.config.min_age_for_coalesce_ms;

                is_correct_series && is_small && is_old_enough
            })
            .collect();

        // Sort by start timestamp
        small_chunks.sort_by_key(|c| c.start_timestamp);

        if small_chunks.len() < self.config.min_chunks_to_merge {
            return candidates;
        }

        // Group adjacent chunks into candidates
        let mut current_group: Vec<ChunkMetadata> = Vec::new();
        let mut current_points: u64 = 0;
        let mut group_start_ts: i64 = 0;

        for chunk in small_chunks {
            let can_add = if current_group.is_empty() {
                true
            } else {
                // Check if adding this chunk would exceed limits
                let new_points = current_points + chunk.point_count as u64;
                let new_end_ts = chunk.end_timestamp;
                let time_span = new_end_ts.saturating_sub(group_start_ts);

                let within_point_limit = new_points <= self.config.max_coalesced_size_points as u64;
                let within_time_limit = time_span <= self.config.max_coalesced_time_span_ms;
                let within_count_limit = current_group.len() < self.config.max_chunks_per_merge;

                // Check for time gap if enabled
                let no_gap = if self.config.detect_time_gaps {
                    let last_end = current_group.last().unwrap().end_timestamp;
                    let gap = chunk.start_timestamp.saturating_sub(last_end);
                    gap <= self.config.max_time_gap_ms
                } else {
                    true
                };

                within_point_limit && within_time_limit && within_count_limit && no_gap
            };

            if can_add {
                if current_group.is_empty() {
                    group_start_ts = chunk.start_timestamp;
                }
                current_points += chunk.point_count as u64;
                current_group.push(chunk.clone());
            } else {
                // Finalize current group if it meets minimum size
                if current_group.len() >= self.config.min_chunks_to_merge {
                    let mut candidate = CoalescingCandidate::new(series_id, current_group.clone());
                    candidate.estimated_improvement = self.estimate_benefit(&candidate);
                    candidates.push(candidate);
                }

                // Start new group with current chunk
                current_group.clear();
                current_group.push(chunk.clone());
                current_points = chunk.point_count as u64;
                group_start_ts = chunk.start_timestamp;
            }
        }

        // Handle last group
        if current_group.len() >= self.config.min_chunks_to_merge {
            let mut candidate = CoalescingCandidate::new(series_id, current_group);
            candidate.estimated_improvement = self.estimate_benefit(&candidate);
            candidates.push(candidate);
        }

        candidates
    }

    /// Check if a candidate should be coalesced
    ///
    /// Evaluates whether coalescing this candidate is beneficial based on
    /// the estimated improvement ratio.
    ///
    /// # Arguments
    ///
    /// * `candidate` - The candidate to evaluate
    ///
    /// # Returns
    ///
    /// `true` if the candidate should be coalesced
    pub fn should_coalesce(&self, candidate: &CoalescingCandidate) -> bool {
        // Must have minimum number of chunks
        if candidate.chunk_count() < self.config.min_chunks_to_merge {
            return false;
        }

        // Must meet minimum improvement threshold
        if candidate.estimated_improvement < self.config.min_improvement_ratio {
            self.stats.record_skip();
            return false;
        }

        // Check if series is already being coalesced
        if let Ok(in_progress) = self.in_progress.read() {
            if in_progress.contains_key(&candidate.series_id) {
                return false;
            }
        }

        true
    }

    /// Estimate the compression benefit from coalescing
    ///
    /// Uses heuristics based on chunk sizes and point counts to estimate
    /// how much improvement we can expect from merging.
    ///
    /// # Arguments
    ///
    /// * `candidate` - The candidate to evaluate
    ///
    /// # Returns
    ///
    /// Estimated improvement ratio (e.g., 1.2 = 20% improvement)
    pub fn estimate_benefit(&self, candidate: &CoalescingCandidate) -> f64 {
        if candidate.is_empty() {
            return 1.0;
        }

        let total_points = candidate.total_points();
        let total_size = candidate.total_size_bytes();

        if total_size == 0 {
            return 1.0;
        }

        // Current bytes per point (available for future use in more sophisticated estimation)
        let _current_bpp = total_size as f64 / total_points as f64;

        // Estimate merged bytes per point based on larger chunk efficiency
        // Larger chunks typically have better compression due to:
        // 1. Header overhead is amortized (64 bytes per chunk)
        // 2. More data to find compression patterns
        let header_overhead_per_chunk = 64.0;
        let current_header_overhead =
            header_overhead_per_chunk * candidate.chunk_count() as f64 / total_points as f64;

        // After merge, we'll have just one header
        let merged_header_overhead = header_overhead_per_chunk / total_points as f64;

        // Header savings
        let header_improvement = current_header_overhead / merged_header_overhead.max(0.001);

        // Compression pattern efficiency improvement (empirical estimate)
        // Larger chunks tend to have 5-15% better compression ratios
        let avg_chunk_size = total_points as f64 / candidate.chunk_count() as f64;
        let compression_efficiency = if avg_chunk_size < 100.0 {
            1.15 // Small chunks benefit a lot from merging
        } else if avg_chunk_size < 500.0 {
            1.10
        } else if avg_chunk_size < 1000.0 {
            1.05
        } else {
            1.02 // Already decent size, less benefit
        };

        // Combined improvement (cap at 2.0 to be conservative)
        (header_improvement * compression_efficiency).min(2.0)
    }

    /// Validate that chunks are in proper time order
    ///
    /// Ensures all chunks have valid time ranges and are properly ordered
    /// with no overlapping timestamps.
    ///
    /// # Arguments
    ///
    /// * `chunks` - Chunks to validate
    ///
    /// # Returns
    ///
    /// `Ok(())` if valid, `Err` with description if invalid
    pub fn validate_time_order(&self, chunks: &[ChunkMetadata]) -> Result<(), String> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Check each chunk's internal consistency
        for (i, chunk) in chunks.iter().enumerate() {
            if chunk.start_timestamp > chunk.end_timestamp {
                return Err(format!(
                    "Chunk {} has inverted time range: start {} > end {}",
                    i, chunk.start_timestamp, chunk.end_timestamp
                ));
            }
        }

        // Check ordering between chunks
        for i in 1..chunks.len() {
            let prev = &chunks[i - 1];
            let curr = &chunks[i];

            // Current chunk should start after or at previous chunk's end
            if curr.start_timestamp < prev.end_timestamp {
                return Err(format!(
                    "Chunks {} and {} overlap: prev ends at {}, curr starts at {}",
                    i - 1,
                    i,
                    prev.end_timestamp,
                    curr.start_timestamp
                ));
            }
        }

        Ok(())
    }

    /// Merge points from multiple chunks
    ///
    /// Combines and sorts points from all chunks in the candidate.
    /// Points are sorted by timestamp to ensure proper ordering.
    ///
    /// # Arguments
    ///
    /// * `points_per_chunk` - Vector of point vectors, one per chunk
    ///
    /// # Returns
    ///
    /// Single merged and sorted vector of all points
    pub fn merge_points(&self, points_per_chunk: Vec<Vec<DataPoint>>) -> Vec<DataPoint> {
        // Calculate total points for pre-allocation
        let total_points: usize = points_per_chunk.iter().map(|p| p.len()).sum();

        let mut merged = Vec::with_capacity(total_points);

        // Add all points
        for points in points_per_chunk {
            merged.extend(points);
        }

        // Sort by timestamp
        merged.sort_by_key(|p| p.timestamp);

        // Remove duplicates (same timestamp) - keep first occurrence
        merged.dedup_by_key(|p| p.timestamp);

        merged
    }

    /// Perform the coalescing operation
    ///
    /// Reads all chunks in the candidate, merges their points, compresses
    /// the merged data, and writes a new chunk.
    ///
    /// # Arguments
    ///
    /// * `candidate` - The candidate to coalesce
    /// * `output_path` - Path for the new merged chunk
    ///
    /// # Returns
    ///
    /// `CoalescingResult` on success, error string on failure
    pub async fn coalesce(
        &self,
        candidate: CoalescingCandidate,
        output_path: PathBuf,
    ) -> Result<CoalescingResult, String> {
        let start = Instant::now();
        let series_id = candidate.series_id;

        // Mark series as in-progress
        {
            let mut in_progress = self.in_progress.write().map_err(|e| e.to_string())?;
            if in_progress.contains_key(&series_id) {
                return Err(format!("Series {} is already being coalesced", series_id));
            }
            in_progress.insert(series_id, Instant::now());
        }

        // Ensure we remove in-progress marker on exit
        let result = self.coalesce_inner(candidate.clone(), output_path).await;

        // Remove in-progress marker
        {
            if let Ok(mut in_progress) = self.in_progress.write() {
                in_progress.remove(&series_id);
            }
        }

        match result {
            Ok(mut result) => {
                result.duration = start.elapsed();
                self.stats.record_success(&result);
                Ok(result)
            },
            Err(e) => {
                self.stats.record_failure();
                Err(e)
            },
        }
    }

    /// Internal coalescing implementation
    async fn coalesce_inner(
        &self,
        candidate: CoalescingCandidate,
        output_path: PathBuf,
    ) -> Result<CoalescingResult, String> {
        use crate::compression::AhpacCompressor;
        use crate::engine::traits::Compressor;
        use crate::storage::chunk::{Chunk, ChunkFlags, ChunkHeader};
        use tokio::fs;
        use tokio::io::AsyncWriteExt;

        let series_id = candidate.series_id;
        let original_chunks = candidate.chunks.clone();
        let original_size_bytes = candidate.total_size_bytes();

        // Validate time ordering
        self.validate_time_order(&original_chunks)?;

        // Read and decompress all chunks
        let mut all_points_per_chunk = Vec::with_capacity(original_chunks.len());

        for chunk_meta in &original_chunks {
            // Read chunk from disk
            let chunk = Chunk::read(chunk_meta.path.clone())
                .await
                .map_err(|e| format!("Failed to read chunk {:?}: {}", chunk_meta.path, e))?;

            // Decompress
            let points = chunk
                .decompress()
                .await
                .map_err(|e| format!("Failed to decompress chunk {:?}: {}", chunk_meta.path, e))?;

            all_points_per_chunk.push(points);
        }

        // Merge all points
        let merged_points = self.merge_points(all_points_per_chunk);

        if merged_points.is_empty() {
            return Err("No points after merging".to_string());
        }

        let total_points = merged_points.len() as u64;
        let start_timestamp = merged_points.first().unwrap().timestamp;
        let end_timestamp = merged_points.last().unwrap().timestamp;

        // Compress merged points
        let compressor = AhpacCompressor::new();
        let compressed = compressor
            .compress(&merged_points)
            .await
            .map_err(|e| format!("Compression failed: {}", e))?;

        // Create chunk header
        let mut header = ChunkHeader::new(series_id);
        header.start_timestamp = start_timestamp;
        header.end_timestamp = end_timestamp;
        header.point_count = merged_points.len() as u32;
        header.compressed_size = compressed.compressed_size as u32;
        header.uncompressed_size = compressed.original_size as u32;
        header.checksum = compressed.checksum;
        header.compression_type = self.config.compression_type;
        header.flags = ChunkFlags::sealed();

        // Validate header
        header
            .validate()
            .map_err(|e| format!("Invalid header: {}", e))?;

        // Create parent directory if needed
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }

        // Write merged chunk to disk
        let mut file = fs::File::create(&output_path)
            .await
            .map_err(|e| format!("Failed to create file: {}", e))?;

        // Write header
        file.write_all(&header.to_bytes())
            .await
            .map_err(|e| format!("Failed to write header: {}", e))?;

        // Write compressed data
        file.write_all(&compressed.data)
            .await
            .map_err(|e| format!("Failed to write data: {}", e))?;

        // Sync to disk
        file.sync_all()
            .await
            .map_err(|e| format!("Failed to sync: {}", e))?;

        let new_size_bytes = (64 + compressed.compressed_size) as u64;

        // Create new chunk metadata
        let new_chunk = ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id,
            path: output_path,
            start_timestamp,
            end_timestamp,
            point_count: merged_points.len() as u32,
            size_bytes: new_size_bytes,
            uncompressed_size: compressed.original_size as u64,
            compression: self.config.compression_type,
            created_at: chrono::Utc::now().timestamp_millis(),
            last_accessed: chrono::Utc::now().timestamp_millis(),
        };

        let improvement_ratio = if new_size_bytes > 0 {
            original_size_bytes as f64 / new_size_bytes as f64
        } else {
            1.0
        };

        Ok(CoalescingResult {
            series_id,
            new_chunk,
            original_chunks,
            chunks_merged: candidate.chunk_count(),
            total_points,
            original_size_bytes,
            new_size_bytes,
            improvement_ratio,
            duration: Duration::ZERO, // Set by caller
        })
    }

    /// Get paths of chunks that should be deleted after coalescing
    ///
    /// # Arguments
    ///
    /// * `result` - The result of a successful coalesce operation
    ///
    /// # Returns
    ///
    /// Vector of paths to the original chunks that can be deleted
    pub fn cleanup_old_chunks(&self, result: &CoalescingResult) -> Vec<PathBuf> {
        result
            .original_chunks
            .iter()
            .map(|c| c.path.clone())
            .collect()
    }

    /// Get current statistics snapshot
    pub fn stats(&self) -> CoalescingStatsSnapshot {
        let operations = self.stats.operations_total.load(Ordering::Relaxed);
        let duration_us = self.stats.duration_total_us.load(Ordering::Relaxed);

        CoalescingStatsSnapshot {
            operations_total: operations,
            chunks_merged_total: self.stats.chunks_merged_total.load(Ordering::Relaxed),
            bytes_saved_total: self.stats.bytes_saved_total.load(Ordering::Relaxed),
            points_processed_total: self.stats.points_processed_total.load(Ordering::Relaxed),
            candidates_skipped: self.stats.candidates_skipped.load(Ordering::Relaxed),
            failures_total: self.stats.failures_total.load(Ordering::Relaxed),
            avg_duration_us: if operations > 0 {
                duration_us / operations
            } else {
                0
            },
        }
    }

    /// Check if a series is currently being coalesced
    pub fn is_series_in_progress(&self, series_id: SeriesId) -> bool {
        if let Ok(in_progress) = self.in_progress.read() {
            in_progress.contains_key(&series_id)
        } else {
            false
        }
    }

    /// Get the number of series currently being coalesced
    pub fn in_progress_count(&self) -> usize {
        if let Ok(in_progress) = self.in_progress.read() {
            in_progress.len()
        } else {
            0
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Configuration Tests ---

    #[test]
    fn test_config_default() {
        let config = CoalescingConfig::default();

        assert_eq!(config.min_chunk_size_points, 1000);
        assert_eq!(config.max_coalesced_size_points, 100_000);
        assert_eq!(config.min_chunks_to_merge, 2);
        assert_eq!(config.max_chunks_per_merge, 10);
        assert!(config.detect_time_gaps);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_optimize_ratio() {
        let config = CoalescingConfig::optimize_ratio();

        assert!(
            config.max_coalesced_size_points
                > CoalescingConfig::default().max_coalesced_size_points
        );
        assert!(config.min_improvement_ratio < CoalescingConfig::default().min_improvement_ratio);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_optimize_query() {
        let config = CoalescingConfig::optimize_query();

        assert!(
            config.max_coalesced_size_points
                < CoalescingConfig::default().max_coalesced_size_points
        );
        assert!(config.min_improvement_ratio > CoalescingConfig::default().min_improvement_ratio);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_low_memory() {
        let config = CoalescingConfig::low_memory();

        assert!(
            config.max_coalesced_size_points
                < CoalescingConfig::default().max_coalesced_size_points
        );
        assert!(config.max_chunks_per_merge < CoalescingConfig::default().max_chunks_per_merge);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_min_points_zero() {
        let config = CoalescingConfig {
            min_chunk_size_points: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_max_less_than_min() {
        let config = CoalescingConfig {
            min_chunk_size_points: 1000,
            max_coalesced_size_points: 500,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_min_chunks_one() {
        let config = CoalescingConfig {
            min_chunks_to_merge: 1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_max_chunks_less_than_min() {
        let config = CoalescingConfig {
            min_chunks_to_merge: 5,
            max_chunks_per_merge: 3,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_improvement_ratio() {
        let config = CoalescingConfig {
            min_improvement_ratio: 0.9,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_time_span() {
        let config = CoalescingConfig {
            max_coalesced_time_span_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_max_gap() {
        let config = CoalescingConfig {
            detect_time_gaps: true,
            max_time_gap_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // --- Coalescing Candidate Tests ---

    fn create_test_chunk_metadata(
        series_id: SeriesId,
        start_ts: i64,
        end_ts: i64,
        points: u32,
        size: u64,
    ) -> ChunkMetadata {
        ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id,
            path: PathBuf::from(format!("/tmp/chunk_{}_{}.kub", series_id, start_ts)),
            start_timestamp: start_ts,
            end_timestamp: end_ts,
            point_count: points,
            size_bytes: size,
            uncompressed_size: size * 2,
            compression: CompressionType::Ahpac,
            created_at: chrono::Utc::now().timestamp_millis() - 600_000, // 10 minutes ago
            last_accessed: chrono::Utc::now().timestamp_millis(),
        }
    }

    #[test]
    fn test_candidate_new() {
        let chunks = vec![
            create_test_chunk_metadata(1, 2000, 3000, 100, 1000),
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);

        // Should be sorted by start_timestamp
        assert_eq!(candidate.chunks[0].start_timestamp, 1000);
        assert_eq!(candidate.chunks[1].start_timestamp, 2000);
    }

    #[test]
    fn test_candidate_total_points() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 2000, 3000, 150, 1000),
            create_test_chunk_metadata(1, 3000, 4000, 50, 500),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        assert_eq!(candidate.total_points(), 300);
    }

    #[test]
    fn test_candidate_total_size() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 2000, 3000, 100, 1500),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        assert_eq!(candidate.total_size_bytes(), 2500);
    }

    #[test]
    fn test_candidate_time_span() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 3000, 5000, 100, 1000),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        let (start, end) = candidate.time_span();

        assert_eq!(start, 1000);
        assert_eq!(end, 5000);
    }

    #[test]
    fn test_candidate_duration() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 3000, 5000, 100, 1000),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        assert_eq!(candidate.duration_ms(), 4000);
    }

    #[test]
    fn test_candidate_chunk_count() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 2000, 3000, 100, 1000),
            create_test_chunk_metadata(1, 3000, 4000, 100, 1000),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        assert_eq!(candidate.chunk_count(), 3);
    }

    #[test]
    fn test_candidate_empty() {
        let candidate = CoalescingCandidate::new(1, vec![]);

        assert!(candidate.is_empty());
        assert_eq!(candidate.total_points(), 0);
        assert_eq!(candidate.time_span(), (0, 0));
    }

    #[test]
    fn test_candidate_chunk_paths() {
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 1000),
            create_test_chunk_metadata(1, 2000, 3000, 100, 1000),
        ];

        let candidate = CoalescingCandidate::new(1, chunks);
        let paths = candidate.chunk_paths();

        assert_eq!(paths.len(), 2);
    }

    // --- Coalescer Tests ---

    #[test]
    fn test_coalescer_creation() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        assert_eq!(coalescer.config().min_chunk_size_points, 1000);
        assert_eq!(coalescer.in_progress_count(), 0);
    }

    #[test]
    fn test_find_candidates_no_small_chunks() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        // All chunks are large
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 5000, 10000),
            create_test_chunk_metadata(1, 2000, 3000, 5000, 10000),
        ];

        let candidates = coalescer.find_candidates(1, &chunks);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_small_chunks() {
        let config = CoalescingConfig {
            min_chunk_size_points: 1000,
            min_age_for_coalesce_ms: 0, // No age requirement for test
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        // Small chunks that should be candidates
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 200, 600),
            create_test_chunk_metadata(1, 3000, 4000, 150, 550),
        ];

        let candidates = coalescer.find_candidates(1, &chunks);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].chunk_count(), 3);
    }

    #[test]
    fn test_find_candidates_respects_max_size() {
        let config = CoalescingConfig {
            min_chunk_size_points: 1000,
            max_coalesced_size_points: 250, // Very small limit
            min_age_for_coalesce_ms: 0,
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
            create_test_chunk_metadata(1, 3000, 4000, 100, 500),
            create_test_chunk_metadata(1, 4000, 5000, 100, 500),
        ];

        let candidates = coalescer.find_candidates(1, &chunks);

        // Should split into groups of 2 to respect the 250 point limit
        assert_eq!(candidates.len(), 2);
        for candidate in &candidates {
            assert!(candidate.total_points() <= 250);
        }
    }

    #[test]
    fn test_find_candidates_respects_time_gap() {
        let config = CoalescingConfig {
            min_chunk_size_points: 1000,
            detect_time_gaps: true,
            max_time_gap_ms: 100, // Very small gap allowed
            min_age_for_coalesce_ms: 0,
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2050, 3000, 100, 500), // 50ms gap - OK
            create_test_chunk_metadata(1, 5000, 6000, 100, 500), // 2000ms gap - too big
            create_test_chunk_metadata(1, 6050, 7000, 100, 500), // 50ms gap - OK
        ];

        let candidates = coalescer.find_candidates(1, &chunks);

        // Should create 2 separate groups due to large gap
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].chunk_count(), 2);
        assert_eq!(candidates[1].chunk_count(), 2);
    }

    #[test]
    fn test_find_candidates_not_enough_chunks() {
        let config = CoalescingConfig {
            min_chunk_size_points: 1000,
            min_chunks_to_merge: 3,
            min_age_for_coalesce_ms: 0,
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        // Only 2 chunks, but need 3
        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
        ];

        let candidates = coalescer.find_candidates(1, &chunks);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_should_coalesce_true() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
        ];

        let mut candidate = CoalescingCandidate::new(1, chunks);
        candidate.estimated_improvement = 1.5; // 50% improvement

        assert!(coalescer.should_coalesce(&candidate));
    }

    #[test]
    fn test_should_coalesce_low_improvement() {
        let config = CoalescingConfig {
            min_improvement_ratio: 1.2,
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
        ];

        let mut candidate = CoalescingCandidate::new(1, chunks);
        candidate.estimated_improvement = 1.1; // Only 10% improvement

        assert!(!coalescer.should_coalesce(&candidate));
    }

    #[test]
    fn test_should_coalesce_not_enough_chunks() {
        let config = CoalescingConfig {
            min_chunks_to_merge: 3,
            ..Default::default()
        };
        let coalescer = ChunkCoalescer::new(config);

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
        ];

        let mut candidate = CoalescingCandidate::new(1, chunks);
        candidate.estimated_improvement = 2.0;

        assert!(!coalescer.should_coalesce(&candidate));
    }

    #[test]
    fn test_estimate_benefit() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        // Small chunks should have good improvement
        let small_chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 50, 500),
            create_test_chunk_metadata(1, 2000, 3000, 50, 500),
        ];
        let small_candidate = CoalescingCandidate::new(1, small_chunks);
        let small_improvement = coalescer.estimate_benefit(&small_candidate);
        assert!(small_improvement > 1.0);
        // Small chunks get capped at 2.0 due to combined high improvement
        assert!(small_improvement <= 2.0);

        // Single very large chunk scenario has less benefit
        // (effectively no header overhead savings possible)
        let very_large_chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 5000, 25000),
            create_test_chunk_metadata(1, 2000, 3000, 5000, 25000),
        ];
        let large_candidate = CoalescingCandidate::new(1, very_large_chunks);
        let large_improvement = coalescer.estimate_benefit(&large_candidate);

        // Large chunks still benefit, but less from compression efficiency
        assert!(large_improvement > 1.0);
        // With avg 5000 points/chunk, compression_efficiency = 1.02
        // Combined improvement = 2.0 * 1.02 = 2.04, capped at 2.0
        assert!(large_improvement <= 2.0);
    }

    #[test]
    fn test_estimate_benefit_empty() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());
        let candidate = CoalescingCandidate::new(1, vec![]);

        assert_eq!(coalescer.estimate_benefit(&candidate), 1.0);
    }

    #[test]
    fn test_validate_time_order_valid() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 2000, 100, 500),
            create_test_chunk_metadata(1, 2000, 3000, 100, 500),
            create_test_chunk_metadata(1, 3000, 4000, 100, 500),
        ];

        assert!(coalescer.validate_time_order(&chunks).is_ok());
    }

    #[test]
    fn test_validate_time_order_inverted_range() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let chunks = vec![ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id: 1,
            path: PathBuf::from("/tmp/chunk.kub"),
            start_timestamp: 3000,
            end_timestamp: 2000, // Inverted!
            point_count: 100,
            size_bytes: 500,
            uncompressed_size: 1000,
            compression: CompressionType::Ahpac,
            created_at: 0,
            last_accessed: 0,
        }];

        assert!(coalescer.validate_time_order(&chunks).is_err());
    }

    #[test]
    fn test_validate_time_order_overlap() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let chunks = vec![
            create_test_chunk_metadata(1, 1000, 3000, 100, 500),
            create_test_chunk_metadata(1, 2000, 4000, 100, 500), // Overlaps!
        ];

        assert!(coalescer.validate_time_order(&chunks).is_err());
    }

    #[test]
    fn test_validate_time_order_empty() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());
        assert!(coalescer.validate_time_order(&[]).is_ok());
    }

    #[test]
    fn test_merge_points() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let points1 = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 10.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 3000,
                value: 30.0,
            },
        ];
        let points2 = vec![
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 20.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 4000,
                value: 40.0,
            },
        ];

        let merged = coalescer.merge_points(vec![points1, points2]);

        assert_eq!(merged.len(), 4);
        assert_eq!(merged[0].timestamp, 1000);
        assert_eq!(merged[1].timestamp, 2000);
        assert_eq!(merged[2].timestamp, 3000);
        assert_eq!(merged[3].timestamp, 4000);
    }

    #[test]
    fn test_merge_points_dedup() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let points1 = vec![
            DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 10.0,
            },
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 20.0,
            },
        ];
        let points2 = vec![
            DataPoint {
                series_id: 1,
                timestamp: 2000,
                value: 25.0,
            }, // Duplicate timestamp
            DataPoint {
                series_id: 1,
                timestamp: 3000,
                value: 30.0,
            },
        ];

        let merged = coalescer.merge_points(vec![points1, points2]);

        assert_eq!(merged.len(), 3);
        // First occurrence kept
        assert_eq!(merged[1].value, 20.0);
    }

    #[test]
    fn test_merge_points_empty() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let merged = coalescer.merge_points(vec![]);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_cleanup_old_chunks() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let result = CoalescingResult {
            series_id: 1,
            new_chunk: create_test_chunk_metadata(1, 1000, 4000, 300, 1000),
            original_chunks: vec![
                create_test_chunk_metadata(1, 1000, 2000, 100, 500),
                create_test_chunk_metadata(1, 2000, 3000, 100, 500),
                create_test_chunk_metadata(1, 3000, 4000, 100, 500),
            ],
            chunks_merged: 3,
            total_points: 300,
            original_size_bytes: 1500,
            new_size_bytes: 1000,
            improvement_ratio: 1.5,
            duration: Duration::from_millis(100),
        };

        let paths = coalescer.cleanup_old_chunks(&result);
        assert_eq!(paths.len(), 3);
    }

    #[test]
    fn test_stats_snapshot() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        let stats = coalescer.stats();
        assert_eq!(stats.operations_total, 0);
        assert_eq!(stats.chunks_merged_total, 0);
        assert_eq!(stats.bytes_saved_total, 0);
    }

    #[test]
    fn test_is_series_in_progress() {
        let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

        assert!(!coalescer.is_series_in_progress(1));
        assert_eq!(coalescer.in_progress_count(), 0);
    }

    // --- Coalescing Result Tests ---

    #[test]
    fn test_result_space_saved() {
        let result = CoalescingResult {
            series_id: 1,
            new_chunk: create_test_chunk_metadata(1, 1000, 4000, 300, 1000),
            original_chunks: vec![],
            chunks_merged: 3,
            total_points: 300,
            original_size_bytes: 1500,
            new_size_bytes: 1000,
            improvement_ratio: 1.5,
            duration: Duration::from_millis(100),
        };

        assert_eq!(result.space_saved_bytes(), 500);
        assert!((result.space_saved_percent() - 33.33).abs() < 0.1);
    }

    #[test]
    fn test_result_space_saved_negative() {
        let result = CoalescingResult {
            series_id: 1,
            new_chunk: create_test_chunk_metadata(1, 1000, 4000, 300, 2000),
            original_chunks: vec![],
            chunks_merged: 3,
            total_points: 300,
            original_size_bytes: 1000,
            new_size_bytes: 2000,
            improvement_ratio: 0.5,
            duration: Duration::from_millis(100),
        };

        assert_eq!(result.space_saved_bytes(), -1000);
    }

    #[test]
    fn test_result_space_saved_zero_original() {
        let result = CoalescingResult {
            series_id: 1,
            new_chunk: create_test_chunk_metadata(1, 1000, 4000, 300, 1000),
            original_chunks: vec![],
            chunks_merged: 0,
            total_points: 0,
            original_size_bytes: 0,
            new_size_bytes: 1000,
            improvement_ratio: 0.0,
            duration: Duration::from_millis(100),
        };

        assert_eq!(result.space_saved_percent(), 0.0);
    }
}
