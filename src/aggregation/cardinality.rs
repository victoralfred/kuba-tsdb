//! Cardinality Management and Control
//!
//! This module provides tools for managing high-cardinality metrics, which is
//! one of the most common challenges in time-series databases. High cardinality
//! occurs when metrics have many unique label combinations, leading to:
//!
//! - Memory exhaustion
//! - Slow query performance
//! - Index bloat
//! - Storage costs
//!
//! # Strategies
//!
//! This module implements several cardinality control strategies:
//!
//! 1. **Hard Limits**: Reject new series after a threshold
//! 2. **Rate Limiting**: Limit new series creation rate
//! 3. **Label Dropping**: Automatically drop high-cardinality labels
//! 4. **Label Value Limiting**: Cap values per label
//! 5. **Bloom Filter Detection**: Efficient cardinality estimation
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::aggregation::{CardinalityController, CardinalityConfig};
//!
//! let config = CardinalityConfig::default();
//! let controller = CardinalityController::new(config);
//!
//! // Check before registering new series
//! if controller.can_add_series() {
//!     // Try to add a series
//!     controller.try_add_series().unwrap();
//! }
//!
//! // Check cardinality stats
//! let stats = controller.stats();
//! assert_eq!(stats.active_series, 1);
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;

// ============================================================================
// Cardinality Errors
// ============================================================================

/// Error when merging cardinality estimators
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardinalityMergeError {
    /// The two estimators have different k values and cannot be merged
    MismatchedK {
        /// k value of the first estimator
        self_k: usize,
        /// k value of the second estimator
        other_k: usize,
    },
}

impl std::fmt::Display for CardinalityMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CardinalityMergeError::MismatchedK { self_k, other_k } => {
                write!(
                    f,
                    "Cannot merge estimators with different k values: {} vs {}",
                    self_k, other_k
                )
            }
        }
    }
}

impl std::error::Error for CardinalityMergeError {}

// ============================================================================
// Cardinality Configuration
// ============================================================================

/// Configuration for cardinality control
#[derive(Debug, Clone)]
pub struct CardinalityConfig {
    /// Maximum number of active series (hard limit)
    pub max_series: usize,

    /// Maximum number of labels per series
    pub max_labels_per_series: usize,

    /// Maximum unique values per label (per-label cardinality limit)
    pub max_values_per_label: usize,

    /// Maximum new series per minute (rate limit)
    pub max_series_per_minute: usize,

    /// Labels that should not be limited (e.g., "__name__", "job")
    pub exempt_labels: HashSet<String>,

    /// Labels to automatically drop if cardinality exceeds threshold
    pub droppable_labels: HashSet<String>,

    /// Threshold for auto-dropping labels (values per label)
    pub auto_drop_threshold: usize,

    /// Whether to enable automatic label dropping
    pub enable_auto_drop: bool,

    /// Time window for rate limiting (in seconds)
    pub rate_limit_window_secs: u64,

    /// Whether to enable cardinality estimation (bloom filters)
    pub enable_estimation: bool,
}

impl Default for CardinalityConfig {
    fn default() -> Self {
        Self {
            max_series: 1_000_000,         // 1M series default
            max_labels_per_series: 30,     // Max 30 labels per series
            max_values_per_label: 100_000, // 100K unique values per label
            max_series_per_minute: 10_000, // 10K new series per minute
            exempt_labels: HashSet::new(),
            droppable_labels: HashSet::new(),
            auto_drop_threshold: 50_000,
            enable_auto_drop: false,
            rate_limit_window_secs: 60,
            enable_estimation: true,
        }
    }
}

impl CardinalityConfig {
    /// Create a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set max series limit
    pub fn with_max_series(mut self, max: usize) -> Self {
        self.max_series = max;
        self
    }

    /// Set max labels per series
    pub fn with_max_labels_per_series(mut self, max: usize) -> Self {
        self.max_labels_per_series = max;
        self
    }

    /// Set max values per label
    pub fn with_max_values_per_label(mut self, max: usize) -> Self {
        self.max_values_per_label = max;
        self
    }

    /// Set series rate limit
    pub fn with_max_series_per_minute(mut self, max: usize) -> Self {
        self.max_series_per_minute = max;
        self
    }

    /// Add an exempt label
    pub fn with_exempt_label(mut self, label: &str) -> Self {
        self.exempt_labels.insert(label.to_string());
        self
    }

    /// Add a droppable label
    pub fn with_droppable_label(mut self, label: &str) -> Self {
        self.droppable_labels.insert(label.to_string());
        self
    }

    /// Enable auto-drop with threshold
    pub fn with_auto_drop(mut self, threshold: usize) -> Self {
        self.enable_auto_drop = true;
        self.auto_drop_threshold = threshold;
        self
    }
}

// ============================================================================
// Cardinality Statistics
// ============================================================================

/// Statistics about cardinality
#[derive(Debug, Clone)]
pub struct CardinalityStats {
    /// Total active series count
    pub active_series: u64,

    /// Series created in current rate limit window
    pub series_this_window: u64,

    /// Number of labels tracked
    pub tracked_labels: usize,

    /// Labels with cardinality exceeding threshold
    pub high_cardinality_labels: Vec<(String, usize)>,

    /// Series rejected due to limits
    pub rejected_series: u64,

    /// Labels dropped due to auto-drop
    pub dropped_labels: u64,

    /// Rate limit violations
    pub rate_limit_hits: u64,

    /// Memory estimate in bytes
    pub memory_bytes: usize,
}

impl std::fmt::Display for CardinalityStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Cardinality Statistics:")?;
        writeln!(f, "  Active series: {}", self.active_series)?;
        writeln!(f, "  Series this window: {}", self.series_this_window)?;
        writeln!(f, "  Tracked labels: {}", self.tracked_labels)?;
        writeln!(f, "  Rejected series: {}", self.rejected_series)?;
        writeln!(f, "  Dropped labels: {}", self.dropped_labels)?;
        writeln!(f, "  Rate limit hits: {}", self.rate_limit_hits)?;
        writeln!(f, "  Memory: {} bytes", self.memory_bytes)?;
        if !self.high_cardinality_labels.is_empty() {
            writeln!(f, "  High cardinality labels:")?;
            for (label, count) in &self.high_cardinality_labels {
                writeln!(f, "    - {}: {} values", label, count)?;
            }
        }
        Ok(())
    }
}

// ============================================================================
// Label Cardinality Tracker
// ============================================================================

/// Tracks cardinality for a single label
#[derive(Debug)]
struct LabelTracker {
    /// Unique values seen (actual values for small sets)
    values: HashSet<String>,

    /// Whether we've exceeded the tracking threshold
    overflow: bool,

    /// Estimated count if overflow
    estimated_count: usize,

    /// Last seen timestamp
    last_seen: Instant,
}

impl LabelTracker {
    /// Maximum values to track exactly before switching to estimation
    const MAX_EXACT_VALUES: usize = 10_000;

    fn new() -> Self {
        Self {
            values: HashSet::new(),
            overflow: false,
            estimated_count: 0,
            last_seen: Instant::now(),
        }
    }

    /// Add a value and return true if it's new
    fn add_value(&mut self, value: &str) -> bool {
        self.last_seen = Instant::now();

        if self.overflow {
            // Use simple estimation: assume 10% collision rate
            self.estimated_count += 1;
            true
        } else if self.values.contains(value) {
            false
        } else if self.values.len() >= Self::MAX_EXACT_VALUES {
            // Switch to overflow mode
            self.overflow = true;
            self.estimated_count = self.values.len() + 1;
            self.values.clear(); // Free memory
            true
        } else {
            self.values.insert(value.to_string());
            true
        }
    }

    /// Get cardinality count
    fn cardinality(&self) -> usize {
        if self.overflow {
            // Adjust estimated count for collisions
            (self.estimated_count as f64 * 0.9) as usize
        } else {
            self.values.len()
        }
    }

    /// Estimate memory usage
    fn memory_bytes(&self) -> usize {
        if self.overflow {
            std::mem::size_of::<Self>()
        } else {
            std::mem::size_of::<Self>()
                + self.values.iter().map(|s| s.len()).sum::<usize>()
                + self.values.capacity() * std::mem::size_of::<String>()
        }
    }
}

// ============================================================================
// Rate Limiter
// ============================================================================

/// Token bucket rate limiter for series creation
///
/// SEC-004: Uses proper atomic operations to prevent race conditions
#[derive(Debug)]
struct RateLimiter {
    /// Tokens available (scaled by 1000 for sub-token precision)
    tokens_scaled: AtomicU64,

    /// Maximum tokens (bucket size, scaled)
    max_tokens_scaled: u64,

    /// Last refill timestamp in milliseconds
    last_refill_ms: AtomicU64,

    /// Refill rate (tokens per millisecond, scaled by 1000)
    refill_rate_scaled: u64,
}

impl RateLimiter {
    const SCALE: u64 = 1000;

    fn new(max_per_minute: usize) -> Self {
        let max_tokens_scaled = (max_per_minute as u64) * Self::SCALE;
        // tokens per ms = max_per_minute / 60000
        let refill_rate_scaled = (max_per_minute as u64 * Self::SCALE) / 60_000;

        Self {
            tokens_scaled: AtomicU64::new(max_tokens_scaled),
            max_tokens_scaled,
            last_refill_ms: AtomicU64::new(Self::current_time_ms()),
            refill_rate_scaled: refill_rate_scaled.max(1), // At least 1 to ensure progress
        }
    }

    fn current_time_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Try to acquire a token, return true if successful
    /// SEC-004: All operations are atomic to prevent race conditions
    fn try_acquire(&self) -> bool {
        // Atomically refill and try to acquire in one operation
        let now_ms = Self::current_time_ms();

        let last_ms = self.last_refill_ms.load(Ordering::Acquire);
        let elapsed_ms = now_ms.saturating_sub(last_ms);

        // Calculate tokens to add
        let tokens_to_add = elapsed_ms * self.refill_rate_scaled;

        // Try to update last_refill atomically (collapsed if per clippy)
        if tokens_to_add > 0
            && self
                .last_refill_ms
                .compare_exchange(last_ms, now_ms, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            // Successfully claimed the refill, add tokens
            let _ =
                self.tokens_scaled
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                        Some((current + tokens_to_add).min(self.max_tokens_scaled))
                    });
        }
        // If CAS failed, another thread did the refill, continue to acquire

        // Try to acquire a token atomically
        self.tokens_scaled
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |tokens| {
                if tokens >= Self::SCALE {
                    Some(tokens - Self::SCALE)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Get current available tokens
    fn available(&self) -> u64 {
        // Trigger a refill check first
        let now_ms = Self::current_time_ms();
        let last_ms = self.last_refill_ms.load(Ordering::Acquire);
        let elapsed_ms = now_ms.saturating_sub(last_ms);
        let tokens_to_add = elapsed_ms * self.refill_rate_scaled;

        let current = self.tokens_scaled.load(Ordering::Relaxed);
        ((current + tokens_to_add).min(self.max_tokens_scaled)) / Self::SCALE
    }
}

// ============================================================================
// Cardinality Controller
// ============================================================================

/// Main cardinality controller
///
/// Thread-safe controller for managing cardinality limits.
pub struct CardinalityController {
    /// Configuration
    config: CardinalityConfig,

    /// Current active series count
    active_series: AtomicU64,

    /// Per-label cardinality trackers
    label_trackers: RwLock<HashMap<String, LabelTracker>>,

    /// Rate limiter for new series
    rate_limiter: RateLimiter,

    /// Labels that have been auto-dropped
    dropped_labels: RwLock<HashSet<String>>,

    /// Statistics
    rejected_count: AtomicU64,
    rate_limit_hits: AtomicU64,
    dropped_label_count: AtomicU64,
}

impl CardinalityController {
    /// Create a new cardinality controller
    pub fn new(config: CardinalityConfig) -> Self {
        let rate_limiter = RateLimiter::new(config.max_series_per_minute);

        Self {
            config,
            active_series: AtomicU64::new(0),
            label_trackers: RwLock::new(HashMap::new()),
            rate_limiter,
            dropped_labels: RwLock::new(HashSet::new()),
            rejected_count: AtomicU64::new(0),
            rate_limit_hits: AtomicU64::new(0),
            dropped_label_count: AtomicU64::new(0),
        }
    }

    /// Check if a new series can be added
    ///
    /// Returns true if the series can be added, false if it would exceed limits.
    pub fn can_add_series(&self) -> bool {
        let current = self.active_series.load(Ordering::Relaxed);

        if current >= self.config.max_series as u64 {
            return false;
        }

        self.rate_limiter.try_acquire()
    }

    /// Try to add a new series
    ///
    /// Returns Ok(()) if the series was added, Err with reason if rejected.
    pub fn try_add_series(&self) -> Result<(), CardinalityError> {
        let current = self.active_series.load(Ordering::Relaxed);

        if current >= self.config.max_series as u64 {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(CardinalityError::SeriesLimitExceeded {
                current: current as usize,
                limit: self.config.max_series,
            });
        }

        if !self.rate_limiter.try_acquire() {
            self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
            return Err(CardinalityError::RateLimitExceeded {
                rate_per_minute: self.config.max_series_per_minute,
            });
        }

        self.active_series.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a series (decrement counter)
    ///
    /// ERR-001: Uses saturating subtraction to prevent underflow
    pub fn remove_series(&self) {
        // Use fetch_update to ensure we don't underflow
        let _ = self
            .active_series
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
    }

    /// Observe a label value
    ///
    /// Returns true if the label is still active (not dropped).
    /// Returns false if the label was auto-dropped.
    pub fn observe_label(&self, label: &str, value: &str) -> bool {
        // Check if label was already dropped
        if self.dropped_labels.read().contains(label) {
            return false;
        }

        // Check if exempt
        if self.config.exempt_labels.contains(label) {
            return true;
        }

        let mut trackers = self.label_trackers.write();
        let tracker = trackers
            .entry(label.to_string())
            .or_insert_with(LabelTracker::new);

        tracker.add_value(value);

        // Check if we should auto-drop
        if self.config.enable_auto_drop
            && self.config.droppable_labels.contains(label)
            && tracker.cardinality() > self.config.auto_drop_threshold
        {
            // Drop this label
            self.dropped_labels.write().insert(label.to_string());
            self.dropped_label_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        true
    }

    /// Check if label values exceed limit
    pub fn check_label_limit(&self, label: &str) -> Result<(), CardinalityError> {
        if self.config.exempt_labels.contains(label) {
            return Ok(());
        }

        let trackers = self.label_trackers.read();

        if let Some(tracker) = trackers.get(label) {
            if tracker.cardinality() > self.config.max_values_per_label {
                return Err(CardinalityError::LabelCardinalityExceeded {
                    label: label.to_string(),
                    current: tracker.cardinality(),
                    limit: self.config.max_values_per_label,
                });
            }
        }

        Ok(())
    }

    /// Validate that a label set doesn't exceed limits
    pub fn validate_labels(&self, labels: &[(&str, &str)]) -> Result<(), CardinalityError> {
        // Check number of labels
        if labels.len() > self.config.max_labels_per_series {
            return Err(CardinalityError::TooManyLabels {
                count: labels.len(),
                limit: self.config.max_labels_per_series,
            });
        }

        // Check each label's cardinality
        for (label, _) in labels {
            self.check_label_limit(label)?;
        }

        Ok(())
    }

    /// Get current active series count
    pub fn active_series(&self) -> u64 {
        self.active_series.load(Ordering::Relaxed)
    }

    /// Get remaining capacity
    pub fn remaining_capacity(&self) -> usize {
        let current = self.active_series.load(Ordering::Relaxed) as usize;
        self.config.max_series.saturating_sub(current)
    }

    /// Get utilization percentage (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        let current = self.active_series.load(Ordering::Relaxed) as f64;
        current / self.config.max_series as f64
    }

    /// Get high cardinality labels
    pub fn high_cardinality_labels(&self, threshold: usize) -> Vec<(String, usize)> {
        let trackers = self.label_trackers.read();
        let mut high_cardinality: Vec<_> = trackers
            .iter()
            .filter(|(_, t)| t.cardinality() > threshold)
            .map(|(label, tracker)| (label.clone(), tracker.cardinality()))
            .collect();

        high_cardinality.sort_by(|a, b| b.1.cmp(&a.1));
        high_cardinality
    }

    /// Get dropped labels
    pub fn dropped_labels(&self) -> Vec<String> {
        self.dropped_labels.read().iter().cloned().collect()
    }

    /// Check if a label is dropped
    pub fn is_label_dropped(&self, label: &str) -> bool {
        self.dropped_labels.read().contains(label)
    }

    /// Get statistics
    pub fn stats(&self) -> CardinalityStats {
        let trackers = self.label_trackers.read();

        let high_cardinality: Vec<_> = trackers
            .iter()
            .filter(|(_, t)| t.cardinality() > 1000)
            .map(|(l, t)| (l.clone(), t.cardinality()))
            .collect();

        let memory_bytes: usize = trackers.values().map(|t| t.memory_bytes()).sum();

        // Calculate series created this window based on available tokens
        let max_tokens = self.rate_limiter.max_tokens_scaled / RateLimiter::SCALE;
        let available = self.rate_limiter.available();
        let series_this_window = max_tokens.saturating_sub(available);

        CardinalityStats {
            active_series: self.active_series.load(Ordering::Relaxed),
            series_this_window,
            tracked_labels: trackers.len(),
            high_cardinality_labels: high_cardinality,
            rejected_series: self.rejected_count.load(Ordering::Relaxed),
            dropped_labels: self.dropped_label_count.load(Ordering::Relaxed),
            rate_limit_hits: self.rate_limit_hits.load(Ordering::Relaxed),
            memory_bytes,
        }
    }

    /// Reset all counters and trackers
    pub fn reset(&self) {
        self.active_series.store(0, Ordering::Relaxed);
        self.label_trackers.write().clear();
        self.dropped_labels.write().clear();
        self.rejected_count.store(0, Ordering::Relaxed);
        self.rate_limit_hits.store(0, Ordering::Relaxed);
        self.dropped_label_count.store(0, Ordering::Relaxed);
    }
}

// ============================================================================
// Cardinality Errors
// ============================================================================

/// Errors related to cardinality limits
#[derive(Debug, Clone)]
pub enum CardinalityError {
    /// Series limit exceeded
    SeriesLimitExceeded {
        /// Current series count
        current: usize,
        /// Configured limit
        limit: usize,
    },

    /// Rate limit exceeded
    RateLimitExceeded {
        /// Configured rate per minute
        rate_per_minute: usize,
    },

    /// Label cardinality exceeded
    LabelCardinalityExceeded {
        /// Label name
        label: String,
        /// Current cardinality
        current: usize,
        /// Configured limit
        limit: usize,
    },

    /// Too many labels on a series
    TooManyLabels {
        /// Actual count
        count: usize,
        /// Configured limit
        limit: usize,
    },
}

impl std::fmt::Display for CardinalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CardinalityError::SeriesLimitExceeded { current, limit } => {
                write!(f, "Series limit exceeded: {} >= {} (max)", current, limit)
            }
            CardinalityError::RateLimitExceeded { rate_per_minute } => {
                write!(f, "Rate limit exceeded: {} series/minute", rate_per_minute)
            }
            CardinalityError::LabelCardinalityExceeded {
                label,
                current,
                limit,
            } => {
                write!(
                    f,
                    "Label '{}' cardinality exceeded: {} > {}",
                    label, current, limit
                )
            }
            CardinalityError::TooManyLabels { count, limit } => {
                write!(f, "Too many labels: {} > {}", count, limit)
            }
        }
    }
}

impl std::error::Error for CardinalityError {}

// ============================================================================
// Cardinality Estimator (K-Minimum Values)
// ============================================================================

/// KMV (K-Minimum Values) cardinality estimator
///
/// Provides approximate cardinality estimation with fixed memory usage.
/// Tracks the k smallest hash values seen, then estimates cardinality
/// as (k-1) / k-th_smallest_hash.
#[derive(Debug, Clone)]
pub struct CardinalityEstimator {
    /// K smallest hash values seen (sorted, smallest first)
    min_values: Vec<u64>,

    /// Maximum number of values to track (k)
    k: usize,

    /// Count of unique items seen (for exact counting when < k)
    count: usize,
}

impl CardinalityEstimator {
    /// Minimum allowed k value to prevent division by zero
    pub const MIN_K: usize = 1;

    /// Create a new estimator with specified accuracy
    ///
    /// Higher k = higher accuracy but more memory.
    /// - k=64: ~15% error
    /// - k=256: ~7% error
    /// - k=1024: ~3% error
    ///
    /// # Panics
    /// Panics if k < MIN_K (1)
    pub fn new(k: usize) -> Self {
        // EDGE-002: Prevent k=0 which would cause division by zero
        assert!(
            k >= Self::MIN_K,
            "k must be at least {} to prevent division by zero",
            Self::MIN_K
        );

        Self {
            min_values: Vec::with_capacity(k + 1),
            k,
            count: 0,
        }
    }

    /// Create a new estimator with specified accuracy, returning None if k is invalid
    pub fn try_new(k: usize) -> Option<Self> {
        if k < Self::MIN_K {
            return None;
        }
        Some(Self {
            min_values: Vec::with_capacity(k + 1),
            k,
            count: 0,
        })
    }

    /// Create with default k=256 (~7% error)
    pub fn default_accuracy() -> Self {
        Self::new(256)
    }

    /// Add an item to the estimator
    ///
    /// PERF-004: Optimized to use single binary search instead of two
    pub fn add(&mut self, item: &str) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();

        // Single binary search - use result for both duplicate check and insertion
        match self.min_values.binary_search(&hash) {
            Ok(_) => {
                // Duplicate - already present, skip
            }
            Err(pos) => {
                // Not a duplicate - pos is the insertion point
                if self.min_values.len() < self.k {
                    // Haven't filled k values yet, just insert
                    self.min_values.insert(pos, hash);
                    self.count += 1;
                } else if pos < self.k {
                    // Full but hash is smaller than k-th largest - insert and truncate
                    self.min_values.insert(pos, hash);
                    self.min_values.truncate(self.k);
                    self.count += 1;
                } else {
                    // Hash is larger than all k smallest - just count it
                    self.count += 1;
                }
            }
        }
    }

    /// Estimate cardinality using KMV formula
    pub fn estimate(&self) -> u64 {
        let n = self.min_values.len();

        if n == 0 {
            return 0;
        }

        // If we haven't filled k values yet, return exact count
        if n < self.k {
            return n as u64;
        }

        // KMV estimator: (k-1) / (k-th smallest hash / max_hash)
        // Normalize the k-th hash to [0, 1] range
        let kth_hash = self.min_values[self.k - 1];
        let normalized = kth_hash as f64 / u64::MAX as f64;

        if normalized == 0.0 {
            return self.k as u64;
        }

        // Estimate: (k-1) / normalized_kth_value
        let estimate = (self.k - 1) as f64 / normalized;
        estimate as u64
    }

    /// Merge another estimator into this one
    ///
    /// ERR-003: Returns Result instead of panicking on mismatched k values
    ///
    /// # Errors
    ///
    /// Returns `CardinalityMergeError::MismatchedK` if the two estimators have
    /// different k values.
    pub fn merge(&mut self, other: &CardinalityEstimator) -> Result<(), CardinalityMergeError> {
        if self.k != other.k {
            return Err(CardinalityMergeError::MismatchedK {
                self_k: self.k,
                other_k: other.k,
            });
        }

        // Merge all values and keep only k smallest
        for &hash in &other.min_values {
            if self.min_values.binary_search(&hash).is_err() {
                let pos = self.min_values.binary_search(&hash).unwrap_or_else(|p| p);
                self.min_values.insert(pos, hash);
            }
        }

        // Keep only k smallest
        self.min_values.truncate(self.k);
        self.count += other.count;
        Ok(())
    }

    /// Clear the estimator
    pub fn clear(&mut self) {
        self.min_values.clear();
        self.count = 0;
    }

    /// Memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        self.min_values.capacity() * std::mem::size_of::<u64>()
    }
}

// ============================================================================
// Per-Metric Cardinality Tracker
// ============================================================================

/// Tracks cardinality per metric name
#[derive(Debug)]
pub struct MetricCardinalityTracker {
    /// Estimators per metric
    metrics: RwLock<HashMap<String, CardinalityEstimator>>,

    /// Accuracy level (bucket count)
    accuracy: usize,
}

impl MetricCardinalityTracker {
    /// Create a new tracker
    pub fn new(accuracy: usize) -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
            accuracy,
        }
    }

    /// Track a series for a metric
    pub fn track(&self, metric: &str, label_hash: &str) {
        let mut metrics = self.metrics.write();
        let estimator = metrics
            .entry(metric.to_string())
            .or_insert_with(|| CardinalityEstimator::new(self.accuracy));
        estimator.add(label_hash);
    }

    /// Get estimated cardinality for a metric
    pub fn cardinality(&self, metric: &str) -> u64 {
        self.metrics
            .read()
            .get(metric)
            .map(|e| e.estimate())
            .unwrap_or(0)
    }

    /// Get all metrics with their cardinalities
    pub fn all_cardinalities(&self) -> Vec<(String, u64)> {
        let metrics = self.metrics.read();
        let mut result: Vec<_> = metrics
            .iter()
            .map(|(name, est)| (name.clone(), est.estimate()))
            .collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }

    /// Get top N metrics by cardinality
    pub fn top_n(&self, n: usize) -> Vec<(String, u64)> {
        let mut all = self.all_cardinalities();
        all.truncate(n);
        all
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_controller_basic() {
        let config = CardinalityConfig::default().with_max_series(10);
        let controller = CardinalityController::new(config);

        // Should be able to add series up to limit
        for _ in 0..10 {
            assert!(controller.try_add_series().is_ok());
        }

        // Should reject after limit
        assert!(matches!(
            controller.try_add_series(),
            Err(CardinalityError::SeriesLimitExceeded { .. })
        ));
    }

    #[test]
    fn test_label_observation() {
        let config = CardinalityConfig::default();
        let controller = CardinalityController::new(config);

        // Observe some label values
        for i in 0..100 {
            controller.observe_label("user_id", &format!("user_{}", i));
        }

        let stats = controller.stats();
        assert_eq!(stats.tracked_labels, 1);
    }

    #[test]
    fn test_label_auto_drop() {
        let config = CardinalityConfig::default()
            .with_droppable_label("trace_id")
            .with_auto_drop(50);

        let controller = CardinalityController::new(config);

        // Observe many trace_ids
        for i in 0..100 {
            controller.observe_label("trace_id", &format!("trace_{}", i));
        }

        // Label should be dropped
        assert!(controller.is_label_dropped("trace_id"));
        assert!(!controller.observe_label("trace_id", "new_trace"));
    }

    #[test]
    fn test_validate_labels() {
        let config = CardinalityConfig::default().with_max_labels_per_series(3);
        let controller = CardinalityController::new(config);

        // Valid label set
        let labels = vec![("a", "1"), ("b", "2"), ("c", "3")];
        assert!(controller.validate_labels(&labels).is_ok());

        // Too many labels
        let labels = vec![("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")];
        assert!(matches!(
            controller.validate_labels(&labels),
            Err(CardinalityError::TooManyLabels { .. })
        ));
    }

    #[test]
    fn test_utilization() {
        let config = CardinalityConfig::default().with_max_series(100);
        let controller = CardinalityController::new(config);

        assert_eq!(controller.utilization(), 0.0);

        for _ in 0..50 {
            controller.try_add_series().ok();
        }

        assert!((controller.utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_remaining_capacity() {
        let config = CardinalityConfig::default().with_max_series(100);
        let controller = CardinalityController::new(config);

        assert_eq!(controller.remaining_capacity(), 100);

        for _ in 0..30 {
            controller.try_add_series().ok();
        }

        assert_eq!(controller.remaining_capacity(), 70);
    }

    #[test]
    fn test_cardinality_estimator() {
        let mut estimator = CardinalityEstimator::new(256);

        // Add 1000 unique items
        for i in 0..1000 {
            estimator.add(&format!("item_{}", i));
        }

        let estimate = estimator.estimate();

        // Should be within 10% of actual
        assert!(estimate > 900 && estimate < 1100);
    }

    #[test]
    fn test_cardinality_estimator_merge() {
        let mut est1 = CardinalityEstimator::new(64);
        let mut est2 = CardinalityEstimator::new(64);

        // Add different items to each
        for i in 0..500 {
            est1.add(&format!("item_{}", i));
        }
        for i in 500..1000 {
            est2.add(&format!("item_{}", i));
        }

        est1.merge(&est2).expect("merge should succeed with same k");

        let estimate = est1.estimate();

        // Should be close to 1000
        assert!(estimate > 800 && estimate < 1200);
    }

    #[test]
    fn test_metric_cardinality_tracker() {
        let tracker = MetricCardinalityTracker::new(128);

        // Track series for different metrics
        for i in 0..100 {
            tracker.track("http_requests", &format!("labels_{}", i));
        }
        for i in 0..50 {
            tracker.track("db_queries", &format!("labels_{}", i));
        }

        let http_card = tracker.cardinality("http_requests");
        let db_card = tracker.cardinality("db_queries");

        assert!(http_card > db_card);
    }

    #[test]
    fn test_high_cardinality_labels() {
        let config = CardinalityConfig::default();
        let controller = CardinalityController::new(config);

        // Add high cardinality label
        for i in 0..5000 {
            controller.observe_label("user_id", &format!("user_{}", i));
        }

        // Add low cardinality label
        for status in &["200", "404", "500"] {
            controller.observe_label("status", status);
        }

        let high_card = controller.high_cardinality_labels(100);
        assert_eq!(high_card.len(), 1);
        assert_eq!(high_card[0].0, "user_id");
    }
}
