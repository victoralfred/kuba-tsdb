//! Core data types used throughout the time-series database
//!
//! This module defines the fundamental data structures used across the system:
//!
//! # Key Types
//!
//! - **`DataPoint`**: A single time-series measurement (timestamp + value)
//! - **`SeriesId`**: Unique identifier for a time-series (128-bit integer)
//! - **`ChunkId`**: Unique identifier for a compressed chunk (UUID)
//! - **`TimeRange`**: Time window for queries (start, end)
//! - **`TagSet`**: Key-value metadata tags for metrics (e.g., host=server1)
//! - **`TagFilter`**: Query filters for selecting series by tags
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::types::{DataPoint, SeriesId, TimeRange, TagSet};
//!
//! // Create a data point
//! let point = DataPoint::new(1, 1000, 42.5);
//!
//! // Create a time range for queries
//! let range = TimeRange::new(1000, 2000).unwrap();
//! assert!(range.contains(1500));
//!
//! // Create tags for metrics
//! let mut tags = TagSet::new();
//! tags.add("host".to_string(), "server1".to_string());
//! tags.add("dc".to_string(), "us-east".to_string());
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for a time-series
///
/// A 128-bit unsigned integer that uniquely identifies a time-series across the system.
/// This provides sufficient space for globally unique IDs without collision concerns.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::SeriesId;
///
/// let series_id: SeriesId = 12345678901234567890;
/// ```
pub type SeriesId = u128;

/// Unique identifier for a compressed chunk
///
/// Each compressed data block is assigned a unique UUID-based identifier. This allows
/// chunks to be referenced, tracked, and stored across different storage backends.
///
/// # Implementation Notes
///
/// - Uses UUID v4 for random generation
/// - String representation for easy serialization
/// - Implements common traits for use in collections
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub String);

impl ChunkId {
    /// Create a new randomly generated chunk ID using UUID v4
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::ChunkId;
    ///
    /// let chunk_id = ChunkId::new();
    /// println!("Generated chunk ID: {}", chunk_id);
    /// ```
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Create a chunk ID from an existing string with validation
    ///
    /// This validates the input to prevent path traversal attacks.
    /// The string must be a valid UUID format and cannot contain
    /// path traversal characters.
    ///
    /// # Arguments
    ///
    /// * `s` - String representation of the chunk ID (must be valid UUID)
    ///
    /// # Returns
    ///
    /// Returns `Ok(ChunkId)` if the string is valid, or `Err` with description.
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::ChunkId;
    ///
    /// // Valid UUID
    /// let id = ChunkId::from_string("550e8400-e29b-41d4-a716-446655440000").unwrap();
    ///
    /// // Invalid: contains path traversal
    /// assert!(ChunkId::from_string("../malicious").is_err());
    /// ```
    pub fn from_string(s: &str) -> Result<Self, String> {
        // Check for path traversal characters
        if s.contains('/') || s.contains('\\') || s.contains("..") || s.contains('\0') {
            return Err("Invalid chunk ID: contains path traversal characters".to_string());
        }

        // Validate UUID format for extra safety
        if uuid::Uuid::parse_str(s).is_err() {
            return Err(format!(
                "Invalid chunk ID: '{}' is not a valid UUID format",
                s
            ));
        }

        Ok(Self(s.to_string()))
    }

    /// Create a chunk ID from an existing string without validation
    ///
    /// # Safety
    ///
    /// This method should only be used when the input is known to be safe,
    /// such as when reading from trusted internal storage. For external input,
    /// always use `from_string()` which performs validation.
    ///
    /// # Arguments
    ///
    /// * `s` - String representation of the chunk ID
    #[allow(dead_code)]
    pub(crate) fn from_string_unchecked(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Default for ChunkId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for ChunkId parsing failures
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkIdError {
    /// The input contains path traversal characters
    PathTraversalAttempt,
    /// The input is not a valid UUID format
    InvalidFormat(String),
}

impl fmt::Display for ChunkIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChunkIdError::PathTraversalAttempt => {
                write!(f, "Invalid chunk ID: contains path traversal characters")
            }
            ChunkIdError::InvalidFormat(s) => {
                write!(f, "Invalid chunk ID: '{}' is not a valid UUID format", s)
            }
        }
    }
}

impl std::error::Error for ChunkIdError {}

/// TryFrom implementation for creating ChunkId from string references
///
/// This provides idiomatic Rust conversion with proper error handling.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::ChunkId;
/// use std::convert::TryFrom;
///
/// // Valid UUID
/// let id = ChunkId::try_from("550e8400-e29b-41d4-a716-446655440000").unwrap();
///
/// // Invalid: path traversal
/// assert!(ChunkId::try_from("../malicious").is_err());
/// ```
impl TryFrom<&str> for ChunkId {
    type Error = ChunkIdError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        // Check for path traversal characters
        if s.contains('/') || s.contains('\\') || s.contains("..") || s.contains('\0') {
            return Err(ChunkIdError::PathTraversalAttempt);
        }

        // Validate UUID format for extra safety
        if uuid::Uuid::parse_str(s).is_err() {
            return Err(ChunkIdError::InvalidFormat(s.to_string()));
        }

        Ok(Self(s.to_string()))
    }
}

/// TryFrom implementation for creating ChunkId from owned strings
impl TryFrom<String> for ChunkId {
    type Error = ChunkIdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        ChunkId::try_from(s.as_str())
    }
}

/// A single data point in a time-series
///
/// The fundamental unit of time-series data, consisting of a series identifier,
/// timestamp, and floating-point value. DataPoints are grouped into series and
/// compressed using the Gorilla algorithm.
///
/// # Fields
///
/// - `series_id`: Which time-series this point belongs to
/// - `timestamp`: Unix timestamp in milliseconds (i64)
/// - `value`: IEEE 754 double-precision floating-point value
///
/// # Memory Layout
///
/// This struct is `Copy` and takes 24 bytes (128-bit + 64-bit + 64-bit):
/// ```text
/// |--SeriesId(16)--|--Timestamp(8)--|--Value(8)--|
/// ```
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::DataPoint;
///
/// // CPU usage at 1-second interval
/// let point1 = DataPoint::new(1, 1000, 45.2);  // 45.2% CPU
/// let point2 = DataPoint::new(1, 1001, 46.1);  // 46.1% CPU (1 second later)
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    /// Unique identifier for the series this point belongs to
    pub series_id: SeriesId,

    /// Unix timestamp in milliseconds since epoch (1970-01-01 00:00:00 UTC)
    ///
    /// Example: 1700000000000 represents 2023-11-14 22:13:20 UTC
    pub timestamp: i64,

    /// Floating-point measurement value
    ///
    /// Supports full IEEE 754 double precision including:
    /// - Normal values (e.g., 42.5, -100.123)
    /// - Special values (NaN, Infinity, -Infinity)
    /// - Subnormal values
    pub value: f64,
}

impl DataPoint {
    /// Create a new data point
    ///
    /// # Arguments
    ///
    /// * `series_id` - Unique identifier for the time-series
    /// * `timestamp` - Unix timestamp in milliseconds
    /// * `value` - Measurement value
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let point = DataPoint::new(
    ///     1,                  // series_id
    ///     1700000000000,      // timestamp (2023-11-14)
    ///     42.5                // value (CPU usage %)
    /// );
    /// ```
    pub fn new(series_id: SeriesId, timestamp: i64, value: f64) -> Self {
        Self {
            series_id,
            timestamp,
            value,
        }
    }
}

/// Time range for queries (inclusive on both ends)
///
/// Represents a time window [start, end] for querying time-series data.
/// Both start and end are inclusive, meaning timestamps equal to either bound are included.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::TimeRange;
///
/// // Query data from 1000ms to 2000ms (inclusive)
/// let range = TimeRange::new(1000, 2000).unwrap();
///
/// assert!(range.contains(1000));  // Start is inclusive
/// assert!(range.contains(1500));  // Middle included
/// assert!(range.contains(2000));  // End is inclusive
/// assert!(!range.contains(999));  // Before start
/// assert!(!range.contains(2001)); // After end
///
/// // Get duration
/// assert_eq!(range.duration_ms(), Some(1000));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp in milliseconds (inclusive)
    pub start: i64,

    /// End timestamp in milliseconds (inclusive)
    pub end: i64,
}

impl TimeRange {
    /// Create a new time range with validation
    ///
    /// Validates that start <= end to prevent invalid ranges.
    ///
    /// # Arguments
    ///
    /// * `start` - Start timestamp (inclusive)
    /// * `end` - End timestamp (inclusive)
    ///
    /// # Returns
    ///
    /// - `Ok(TimeRange)` if start <= end
    /// - `Err` if start > end
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::TimeRange;
    ///
    /// // Valid range
    /// let range = TimeRange::new(1000, 2000).unwrap();
    ///
    /// // Invalid range (start > end) returns error
    /// assert!(TimeRange::new(2000, 1000).is_err());
    /// ```
    pub fn new(start: i64, end: i64) -> Result<Self, crate::error::Error> {
        if start > end {
            return Err(crate::error::Error::Configuration(format!(
                "Invalid time range: start {} > end {}",
                start, end
            )));
        }
        Ok(Self { start, end })
    }

    /// Create a new time range without validation (use with caution)
    ///
    /// Creates a range without checking that start <= end. Only use this when you've
    /// already validated the inputs or when performance is critical.
    ///
    /// # Safety
    ///
    /// This is not `unsafe` in the Rust sense, but can create logically invalid ranges.
    /// The range operations (contains, duration_ms) may behave unexpectedly if start > end.
    pub fn new_unchecked(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp falls within this range (inclusive)
    ///
    /// Returns true if `start <= timestamp <= end`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::TimeRange;
    ///
    /// let range = TimeRange::new(1000, 2000).unwrap();
    /// assert!(range.contains(1000));  // Boundaries included
    /// assert!(range.contains(1500));
    /// assert!(range.contains(2000));
    /// ```
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    /// Get the duration of this range in milliseconds
    ///
    /// Uses checked subtraction to prevent overflow. Returns None if the calculation
    /// would overflow (which shouldn't happen for valid ranges).
    ///
    /// # Returns
    ///
    /// - `Some(duration)` if subtraction succeeds
    /// - `None` if overflow occurs
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::TimeRange;
    ///
    /// let range = TimeRange::new(1000, 2000).unwrap();
    /// assert_eq!(range.duration_ms(), Some(1000));
    /// ```
    pub fn duration_ms(&self) -> Option<i64> {
        self.end.checked_sub(self.start)
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self {
            start: 0,
            end: i64::MAX,
        }
    }
}

/// Tag set for metrics (key-value metadata)
///
/// Tags are used to add dimensional metadata to time-series, enabling filtering
/// and grouping in queries. Common examples: host, datacenter, environment, etc.
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::TagSet;
///
/// let mut tags = TagSet::new();
/// tags.add("host".to_string(), "web-01".to_string());
/// tags.add("dc".to_string(), "us-east-1".to_string());
/// tags.add("env".to_string(), "production".to_string());
///
/// // Tags provide consistent hashing for series identification
/// let hash = tags.hash();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSet {
    /// Key-value pairs representing metric dimensions
    ///
    /// Example: {"host": "server1", "dc": "us-east", "app": "api"}
    pub tags: HashMap<String, String>,
}

impl TagSet {
    /// Create a new empty tag set
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    /// Create from a hashmap
    pub fn from_map(tags: HashMap<String, String>) -> Self {
        Self { tags }
    }

    /// Add a tag
    pub fn add(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Get a tag value
    pub fn get(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }

    /// Calculate a consistent hash for this tag set
    ///
    /// Produces the same hash regardless of insertion order by sorting keys before hashing.
    /// This is useful for series identification and deduplication.
    ///
    /// # Returns
    ///
    /// 64-bit hash value
    ///
    /// # Example
    ///
    /// ```rust
    /// use gorilla_tsdb::types::TagSet;
    ///
    /// let mut tags1 = TagSet::new();
    /// tags1.add("host".to_string(), "server1".to_string());
    /// tags1.add("dc".to_string(), "us-east".to_string());
    ///
    /// let mut tags2 = TagSet::new();
    /// tags2.add("dc".to_string(), "us-east".to_string());  // Different insertion order
    /// tags2.add("host".to_string(), "server1".to_string());
    ///
    /// assert_eq!(tags1.hash(), tags2.hash());  // Same hash despite different order
    /// ```
    pub fn hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Sort keys for consistent hashing regardless of insertion order
        let mut keys: Vec<_> = self.tags.keys().collect();
        keys.sort();

        // Hash each key-value pair in sorted order
        for key in keys {
            key.hash(&mut hasher);
            self.tags[key].hash(&mut hasher);
        }

        hasher.finish()
    }
}

impl Default for TagSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Tag filter for queries
///
/// Specifies which series to include in a query based on their tags.
///
/// # Variants
///
/// - `All`: Match all series regardless of tags
/// - `Exact`: Match series with exact tag key-value pairs
/// - `Pattern`: Pattern-based matching (not yet implemented)
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::types::TagFilter;
/// use std::collections::HashMap;
///
/// let mut tags = HashMap::new();
/// tags.insert("host".to_string(), "server1".to_string());
/// tags.insert("dc".to_string(), "us-east".to_string());
///
/// // Match all series
/// assert!(TagFilter::All.matches(&tags));
///
/// // Match specific tags
/// let mut filter_tags = HashMap::new();
/// filter_tags.insert("host".to_string(), "server1".to_string());
/// assert!(TagFilter::Exact(filter_tags).matches(&tags));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagFilter {
    /// Match all series regardless of tags
    All,

    /// Exact match: series must have all specified tag key-value pairs
    /// (can have additional tags not in the filter)
    Exact(HashMap<String, String>),

    /// Pattern matching (not yet implemented)
    /// Future: Support wildcards like "host=web-*" or regex patterns
    Pattern(String),
}

impl TagFilter {
    /// Check if a tag set matches this filter
    ///
    /// # Arguments
    ///
    /// * `tags` - Tag set to test against the filter
    ///
    /// # Returns
    ///
    /// `true` if the tags match the filter criteria
    ///
    /// # Matching Logic
    ///
    /// - `All`: Always returns true
    /// - `Exact`: Returns true if tags contain all filter key-value pairs
    /// - `Pattern`: Returns false (not implemented yet)
    pub fn matches(&self, tags: &HashMap<String, String>) -> bool {
        match self {
            // Match everything
            TagFilter::All => true,

            // Check if all filter tags exist in the target with matching values
            TagFilter::Exact(filter_tags) => {
                filter_tags.iter().all(|(k, v)| tags.get(k) == Some(v))
            }

            // Pattern matching not implemented yet
            TagFilter::Pattern(_) => {
                // TODO: Implement pattern matching (wildcards, regex)
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range() {
        let range = TimeRange::new(100, 200).unwrap();
        assert!(range.contains(150));
        assert!(!range.contains(50));
        assert!(!range.contains(250));
        assert_eq!(range.duration_ms(), Some(100));

        // Test invalid range
        assert!(TimeRange::new(200, 100).is_err());
    }

    #[test]
    fn test_tag_set_hash() {
        let mut tags1 = TagSet::new();
        tags1.add("host".to_string(), "server1".to_string());
        tags1.add("dc".to_string(), "us-east".to_string());

        let mut tags2 = TagSet::new();
        tags2.add("dc".to_string(), "us-east".to_string());
        tags2.add("host".to_string(), "server1".to_string());

        // Hashes should be equal regardless of insertion order
        assert_eq!(tags1.hash(), tags2.hash());
    }

    #[test]
    fn test_tag_filter() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("dc".to_string(), "us-east".to_string());

        let filter = TagFilter::All;
        assert!(filter.matches(&tags));

        let mut filter_tags = HashMap::new();
        filter_tags.insert("host".to_string(), "server1".to_string());
        let filter = TagFilter::Exact(filter_tags);
        assert!(filter.matches(&tags));

        let mut filter_tags = HashMap::new();
        filter_tags.insert("host".to_string(), "server2".to_string());
        let filter = TagFilter::Exact(filter_tags);
        assert!(!filter.matches(&tags));
    }

    #[test]
    fn test_chunk_id_try_from_valid() {
        use std::convert::TryFrom;

        // Valid UUID should succeed
        let id = ChunkId::try_from("550e8400-e29b-41d4-a716-446655440000");
        assert!(id.is_ok());
        assert_eq!(
            id.unwrap().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_chunk_id_try_from_path_traversal() {
        use std::convert::TryFrom;

        // Path traversal attempts should fail
        assert!(matches!(
            ChunkId::try_from("../malicious"),
            Err(ChunkIdError::PathTraversalAttempt)
        ));
        assert!(matches!(
            ChunkId::try_from("..\\windows\\system32"),
            Err(ChunkIdError::PathTraversalAttempt)
        ));
        assert!(matches!(
            ChunkId::try_from("/etc/passwd"),
            Err(ChunkIdError::PathTraversalAttempt)
        ));
        assert!(matches!(
            ChunkId::try_from("chunk\0null"),
            Err(ChunkIdError::PathTraversalAttempt)
        ));
    }

    #[test]
    fn test_chunk_id_try_from_invalid_format() {
        use std::convert::TryFrom;

        // Invalid UUID format should fail
        assert!(matches!(
            ChunkId::try_from("not-a-uuid"),
            Err(ChunkIdError::InvalidFormat(_))
        ));
        assert!(matches!(
            ChunkId::try_from("abc-123"),
            Err(ChunkIdError::InvalidFormat(_))
        ));
        assert!(matches!(
            ChunkId::try_from(""),
            Err(ChunkIdError::InvalidFormat(_))
        ));
    }

    #[test]
    fn test_chunk_id_error_display() {
        let err = ChunkIdError::PathTraversalAttempt;
        assert!(err.to_string().contains("path traversal"));

        let err = ChunkIdError::InvalidFormat("bad".to_string());
        assert!(err.to_string().contains("bad"));
        assert!(err.to_string().contains("UUID"));
    }
}
