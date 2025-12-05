//! String Interning for Tag Keys and Values
//!
//! This module provides efficient string interning to reduce memory usage
//! when storing repetitive tag keys and values. Instead of storing full
//! strings like "host", "datacenter", "us-east-1" multiple times, we store
//! them once and reference them by compact u32 IDs.
//!
//! # Memory Savings
//!
//! Example with 100,000 series, each with 5 tags:
//! - Without interning: 100K × 5 × ~40 bytes = 20 MB
//! - With interning: 1K unique strings × 40 bytes + 100K × 5 × 8 bytes = 4 MB
//! - Savings: ~80%
//!
//! # Thread Safety
//!
//! The `StringInterner` uses interior mutability with `RwLock` for thread-safe
//! concurrent access. Reads are lock-free when the string is already interned.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::RwLock;

use super::super::data_model::{TagKeyId, TagValueId};

/// VAL-003: Maximum allowed length for tag keys
pub const MAX_TAG_KEY_LENGTH: usize = 256;

/// VAL-003: Maximum allowed length for tag values
pub const MAX_TAG_VALUE_LENGTH: usize = 4096;

/// VAL-003: Maximum allowed length for metric names
pub const MAX_METRIC_NAME_LENGTH: usize = 512;

/// Error when interning fails due to validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternError {
    /// String exceeds maximum allowed length
    StringTooLong {
        /// Actual length of the string
        actual: usize,
        /// Maximum allowed length
        max: usize,
    },
}

impl std::fmt::Display for InternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternError::StringTooLong { actual, max } => {
                write!(f, "String too long: {} bytes (max: {} bytes)", actual, max)
            },
        }
    }
}

impl std::error::Error for InternError {}

// ============================================================================
// String Interner
// ============================================================================

/// A thread-safe string interner
///
/// Maps strings to compact integer IDs and back. Once a string is interned,
/// it remains in the interner for the lifetime of the process.
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::aggregation::metadata::StringInterner;
///
/// let interner = StringInterner::new();
///
/// let id1 = interner.intern("host");
/// let id2 = interner.intern("host");  // Returns same ID
/// assert_eq!(id1, id2);
///
/// let resolved = interner.resolve(id1);
/// assert_eq!(resolved, Some("host".to_string()));
/// ```
#[derive(Debug)]
pub struct StringInterner {
    /// Forward map: string -> ID
    string_to_id: RwLock<HashMap<String, u32>>,

    /// Reverse map: ID -> string
    id_to_string: RwLock<Vec<String>>,

    /// Next ID to assign (atomic for lock-free increment)
    next_id: AtomicU32,
}

impl StringInterner {
    /// Create a new string interner
    pub fn new() -> Self {
        Self {
            string_to_id: RwLock::new(HashMap::new()),
            id_to_string: RwLock::new(Vec::new()),
            next_id: AtomicU32::new(0),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            string_to_id: RwLock::new(HashMap::with_capacity(capacity)),
            id_to_string: RwLock::new(Vec::with_capacity(capacity)),
            next_id: AtomicU32::new(0),
        }
    }

    /// Intern a string, returning its ID
    ///
    /// If the string is already interned, returns the existing ID.
    /// Otherwise, assigns a new ID and stores the string.
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe. Multiple threads can intern strings
    /// concurrently without data races.
    pub fn intern(&self, s: &str) -> u32 {
        // Fast path: check if already interned (read lock only)
        {
            let map = self.string_to_id.read();
            if let Some(&id) = map.get(s) {
                return id;
            }
        }

        // Slow path: need to insert (write lock)
        let mut map = self.string_to_id.write();

        // Double-check after acquiring write lock (another thread might have inserted)
        if let Some(&id) = map.get(s) {
            return id;
        }

        // Assign new ID
        // PERF-009: Relaxed is sufficient - we only need uniqueness, not ordering
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // Store in both maps
        map.insert(s.to_string(), id);

        let mut reverse = self.id_to_string.write();
        // Ensure the vector is large enough
        if reverse.len() <= id as usize {
            reverse.resize(id as usize + 1, String::new());
        }
        reverse[id as usize] = s.to_string();

        id
    }

    /// Get the ID for a string if it exists (without interning)
    ///
    /// Returns `None` if the string has not been interned.
    pub fn get_id(&self, s: &str) -> Option<u32> {
        self.string_to_id.read().get(s).copied()
    }

    /// Resolve an ID back to its string
    ///
    /// Returns `None` if the ID is not valid.
    pub fn resolve(&self, id: u32) -> Option<String> {
        let reverse = self.id_to_string.read();
        reverse.get(id as usize).cloned()
    }

    /// Check if a string is interned
    pub fn contains(&self, s: &str) -> bool {
        self.string_to_id.read().contains_key(s)
    }

    /// Try to intern a string with length validation
    ///
    /// VAL-003: Returns error if string exceeds max_length
    pub fn try_intern(&self, s: &str, max_length: usize) -> Result<u32, InternError> {
        if s.len() > max_length {
            return Err(InternError::StringTooLong {
                actual: s.len(),
                max: max_length,
            });
        }
        Ok(self.intern(s))
    }

    /// Get the number of interned strings
    #[must_use]
    pub fn len(&self) -> usize {
        self.string_to_id.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all interned strings (for debugging/export)
    pub fn all_strings(&self) -> Vec<String> {
        self.id_to_string.read().clone()
    }

    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        let map = self.string_to_id.read();
        let strings = self.id_to_string.read();

        // HashMap overhead + string contents
        let map_overhead =
            map.capacity() * (std::mem::size_of::<String>() + std::mem::size_of::<u32>());
        let string_bytes: usize = strings.iter().map(|s| s.len()).sum();

        map_overhead + string_bytes + strings.capacity() * std::mem::size_of::<String>()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tag Key Interner
// ============================================================================

/// Specialized interner for tag keys
///
/// Wraps `StringInterner` with `TagKeyId` type for type safety.
/// Tag keys are typically low cardinality (10-100 unique keys).
#[derive(Debug)]
pub struct TagKeyInterner {
    /// Internal string interner (public for TagDictionary access)
    pub(super) interner: StringInterner,
}

impl TagKeyInterner {
    /// Create a new tag key interner
    pub fn new() -> Self {
        // Pre-allocate for ~100 tag keys (typical)
        Self {
            interner: StringInterner::with_capacity(100),
        }
    }

    /// Intern a tag key
    pub fn intern(&self, key: &str) -> TagKeyId {
        TagKeyId(self.interner.intern(key))
    }

    /// Get the ID for a key if it exists
    pub fn get_id(&self, key: &str) -> Option<TagKeyId> {
        self.interner.get_id(key).map(TagKeyId)
    }

    /// Resolve a key ID to string
    pub fn resolve(&self, id: TagKeyId) -> Option<String> {
        self.interner.resolve(id.0)
    }

    /// Number of interned keys
    pub fn len(&self) -> usize {
        self.interner.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.interner.is_empty()
    }
}

impl Default for TagKeyInterner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tag Value Interner (Per-Key)
// ============================================================================

/// Specialized interner for tag values within a specific key
///
/// Tag values are interned per-key because the same value string
/// can have different meanings for different keys:
/// - "production" for key "env" -> value_id 1
/// - "production" for key "cluster" -> value_id 1 (different namespace)
#[derive(Debug)]
pub struct TagValueInterner {
    /// Internal string interner (public for TagDictionary access)
    pub(super) interner: StringInterner,
}

impl TagValueInterner {
    /// Create a new tag value interner
    pub fn new() -> Self {
        // Pre-allocate for ~1000 values per key (typical)
        Self {
            interner: StringInterner::with_capacity(1000),
        }
    }

    /// Create with specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            interner: StringInterner::with_capacity(capacity),
        }
    }

    /// Intern a tag value
    pub fn intern(&self, value: &str) -> TagValueId {
        TagValueId(self.interner.intern(value))
    }

    /// Get the ID for a value if it exists
    pub fn get_id(&self, value: &str) -> Option<TagValueId> {
        self.interner.get_id(value).map(TagValueId)
    }

    /// Resolve a value ID to string
    pub fn resolve(&self, id: TagValueId) -> Option<String> {
        self.interner.resolve(id.0)
    }

    /// Number of interned values
    pub fn len(&self) -> usize {
        self.interner.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.interner.is_empty()
    }

    /// Estimate memory usage
    pub fn memory_usage(&self) -> usize {
        self.interner.memory_usage()
    }
}

impl Default for TagValueInterner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Unified Tag Dictionary
// ============================================================================

/// Complete tag dictionary managing all tag keys and values
///
/// This is the main entry point for tag interning. It manages:
/// - A single `TagKeyInterner` for all tag keys
/// - One `TagValueInterner` per tag key for values
/// - A separate interner for metric names
///
/// # Example
///
/// ```rust
/// use kuba_tsdb::aggregation::metadata::TagDictionary;
///
/// let dict = TagDictionary::new();
///
/// // Intern a complete tag set
/// let interned = dict.intern_tags(&[
///     ("host", "server1"),
///     ("dc", "us-east"),
/// ]);
/// assert_eq!(interned.len(), 2);
///
/// // Resolve back to strings
/// let resolved = dict.resolve_tags(&interned);
/// assert_eq!(resolved.len(), 2);
/// ```
#[derive(Debug)]
pub struct TagDictionary {
    /// Interner for tag keys (e.g., "host", "datacenter")
    key_interner: TagKeyInterner,

    /// Interners for tag values, one per key
    /// Using `RwLock<HashMap>` because new keys can be added dynamically
    value_interners: RwLock<HashMap<TagKeyId, TagValueInterner>>,

    /// Interner for metric names
    metric_interner: TagValueInterner,
}

impl TagDictionary {
    /// Create a new tag dictionary
    pub fn new() -> Self {
        Self {
            key_interner: TagKeyInterner::new(),
            value_interners: RwLock::new(HashMap::new()),
            metric_interner: TagValueInterner::with_capacity(10000), // Metrics can be numerous
        }
    }

    /// Intern a tag key
    pub fn intern_key(&self, key: &str) -> TagKeyId {
        self.key_interner.intern(key)
    }

    /// Try to intern a tag key with length validation
    ///
    /// VAL-003: Returns error if key exceeds MAX_TAG_KEY_LENGTH
    pub fn try_intern_key(&self, key: &str) -> Result<TagKeyId, InternError> {
        if key.len() > MAX_TAG_KEY_LENGTH {
            return Err(InternError::StringTooLong {
                actual: key.len(),
                max: MAX_TAG_KEY_LENGTH,
            });
        }
        Ok(self.intern_key(key))
    }

    /// Intern a tag value for a specific key
    pub fn intern_value(&self, key_id: TagKeyId, value: &str) -> TagValueId {
        // Fast path: check if value interner exists
        {
            let interners = self.value_interners.read();
            if let Some(interner) = interners.get(&key_id) {
                return interner.intern(value);
            }
        }

        // Slow path: create new value interner for this key
        let mut interners = self.value_interners.write();

        // Double-check after acquiring write lock
        if let Some(interner) = interners.get(&key_id) {
            return interner.intern(value);
        }

        // Create new interner
        let interner = TagValueInterner::new();
        let value_id = interner.intern(value);
        interners.insert(key_id, interner);

        value_id
    }

    /// Try to intern a tag value with length validation
    ///
    /// VAL-003: Returns error if value exceeds MAX_TAG_VALUE_LENGTH
    pub fn try_intern_value(
        &self,
        key_id: TagKeyId,
        value: &str,
    ) -> Result<TagValueId, InternError> {
        if value.len() > MAX_TAG_VALUE_LENGTH {
            return Err(InternError::StringTooLong {
                actual: value.len(),
                max: MAX_TAG_VALUE_LENGTH,
            });
        }
        Ok(self.intern_value(key_id, value))
    }

    /// Intern a complete tag (key + value)
    pub fn intern_tag(&self, key: &str, value: &str) -> (TagKeyId, TagValueId) {
        let key_id = self.intern_key(key);
        let value_id = self.intern_value(key_id, value);
        (key_id, value_id)
    }

    /// Try to intern a complete tag with length validation
    ///
    /// VAL-003: Returns error if key or value exceeds limits
    pub fn try_intern_tag(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(TagKeyId, TagValueId), InternError> {
        let key_id = self.try_intern_key(key)?;
        let value_id = self.try_intern_value(key_id, value)?;
        Ok((key_id, value_id))
    }

    /// Intern a metric name
    pub fn intern_metric(&self, name: &str) -> TagValueId {
        self.metric_interner.intern(name)
    }

    /// Try to intern a metric name with length validation
    ///
    /// VAL-003: Returns error if name exceeds MAX_METRIC_NAME_LENGTH
    pub fn try_intern_metric(&self, name: &str) -> Result<TagValueId, InternError> {
        if name.len() > MAX_METRIC_NAME_LENGTH {
            return Err(InternError::StringTooLong {
                actual: name.len(),
                max: MAX_METRIC_NAME_LENGTH,
            });
        }
        Ok(self.intern_metric(name))
    }

    /// Resolve a tag key ID to string
    pub fn resolve_key(&self, id: TagKeyId) -> Option<String> {
        self.key_interner.resolve(id)
    }

    /// Resolve a tag value ID to string
    pub fn resolve_value(&self, key_id: TagKeyId, value_id: TagValueId) -> Option<String> {
        let interners = self.value_interners.read();
        interners.get(&key_id).and_then(|i| i.resolve(value_id))
    }

    /// Resolve a metric name ID to string
    pub fn resolve_metric(&self, id: TagValueId) -> Option<String> {
        self.metric_interner.resolve(id)
    }

    /// Intern multiple tags at once
    pub fn intern_tags(&self, tags: &[(&str, &str)]) -> Vec<(TagKeyId, TagValueId)> {
        tags.iter().map(|(k, v)| self.intern_tag(k, v)).collect()
    }

    /// Resolve multiple tags at once
    pub fn resolve_tags(&self, tags: &[(TagKeyId, TagValueId)]) -> Vec<(String, String)> {
        tags.iter()
            .filter_map(|(key_id, value_id)| {
                let key = self.resolve_key(*key_id)?;
                let value = self.resolve_value(*key_id, *value_id)?;
                Some((key, value))
            })
            .collect()
    }

    /// Get the number of unique tag keys
    pub fn key_count(&self) -> usize {
        self.key_interner.len()
    }

    /// Get the number of unique values for a specific key
    pub fn value_count(&self, key_id: TagKeyId) -> usize {
        self.value_interners
            .read()
            .get(&key_id)
            .map(|i| i.len())
            .unwrap_or(0)
    }

    /// Get the number of unique metric names
    pub fn metric_count(&self) -> usize {
        self.metric_interner.len()
    }

    /// Estimate total memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        let value_interners = self.value_interners.read();
        let value_mem: usize = value_interners.values().map(|i| i.memory_usage()).sum();

        self.key_interner.interner.memory_usage() + value_mem + self.metric_interner.memory_usage()
    }

    /// Get statistics about the dictionary
    pub fn stats(&self) -> TagDictionaryStats {
        let value_interners = self.value_interners.read();
        let total_values: usize = value_interners.values().map(|i| i.len()).sum();
        let max_values_per_key = value_interners.values().map(|i| i.len()).max().unwrap_or(0);

        TagDictionaryStats {
            key_count: self.key_interner.len(),
            total_value_count: total_values,
            metric_count: self.metric_interner.len(),
            max_values_per_key,
            memory_bytes: self.memory_usage(),
        }
    }

    /// Get metric ID by name (without interning)
    pub fn get_metric_id(&self, name: &str) -> Option<TagValueId> {
        self.metric_interner.get_id(name)
    }

    /// Get tag key ID by name (without interning)
    pub fn get_key_id(&self, key: &str) -> Option<TagKeyId> {
        self.key_interner.get_id(key)
    }

    /// Get tag value ID by key and value (without interning)
    pub fn get_value_id_by_key_id(&self, key_id: TagKeyId, value: &str) -> Option<TagValueId> {
        let interners = self.value_interners.read();
        interners.get(&key_id)?.get_id(value)
    }

    /// Get tag value ID by string key and value (without interning)
    ///
    /// Convenience method that looks up the key first, then the value.
    pub fn get_value_id(&self, key: &str, value: &str) -> Option<TagValueId> {
        let key_id = self.get_key_id(key)?;
        self.get_value_id_by_key_id(key_id, value)
    }

    /// Get all values and their IDs for a specific key
    ///
    /// Returns an iterator over (value_string, value_id) pairs.
    /// Used for regex and prefix matching which need to scan all values.
    pub fn get_values_for_key(&self, key: &str) -> Option<Vec<(String, TagValueId)>> {
        let key_id = self.get_key_id(key)?;
        let interners = self.value_interners.read();
        let interner = interners.get(&key_id)?;

        // Get all strings from the value interner
        let strings = interner.interner.all_strings();

        Some(
            strings
                .into_iter()
                .enumerate()
                .filter(|(_, s)| !s.is_empty()) // Filter out empty placeholder strings
                .map(|(idx, s)| (s, TagValueId(idx as u32)))
                .collect(),
        )
    }

    /// Get all keys and their IDs
    ///
    /// Returns an iterator over (key_string, key_id) pairs.
    pub fn get_all_keys(&self) -> Vec<(String, TagKeyId)> {
        let strings = self.key_interner.interner.all_strings();

        strings
            .into_iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty())
            .map(|(idx, s)| (s, TagKeyId(idx as u32)))
            .collect()
    }
}

impl Default for TagDictionary {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about a tag dictionary
#[derive(Debug, Clone)]
pub struct TagDictionaryStats {
    /// Number of unique tag keys
    pub key_count: usize,

    /// Total number of unique tag values (across all keys)
    pub total_value_count: usize,

    /// Number of unique metric names
    pub metric_count: usize,

    /// Maximum number of values for any single key (indicates high cardinality)
    pub max_values_per_key: usize,

    /// Estimated memory usage in bytes
    pub memory_bytes: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interner_basic() {
        let interner = StringInterner::new();

        let id1 = interner.intern("hello");
        let id2 = interner.intern("world");
        let id3 = interner.intern("hello"); // Same as id1

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);

        assert_eq!(interner.resolve(id1), Some("hello".to_string()));
        assert_eq!(interner.resolve(id2), Some("world".to_string()));
    }

    #[test]
    fn test_string_interner_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let interner = Arc::new(StringInterner::new());
        let mut handles = vec![];

        // Spawn multiple threads interning the same strings
        for _ in 0..4 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let s = format!("string_{}", i % 10); // Only 10 unique strings
                    interner.intern(&s);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have exactly 10 unique strings
        assert_eq!(interner.len(), 10);
    }

    #[test]
    fn test_tag_key_interner() {
        let interner = TagKeyInterner::new();

        let host_id = interner.intern("host");
        let dc_id = interner.intern("datacenter");
        let host_id2 = interner.intern("host");

        assert_eq!(host_id, host_id2);
        assert_ne!(host_id, dc_id);

        assert_eq!(interner.resolve(host_id), Some("host".to_string()));
    }

    #[test]
    fn test_tag_dictionary_basic() {
        let dict = TagDictionary::new();

        // Intern some tags
        let (host_key, server1_val) = dict.intern_tag("host", "server1");
        let (host_key2, server2_val) = dict.intern_tag("host", "server2");
        let (dc_key, useast_val) = dict.intern_tag("dc", "us-east");

        // Same key should return same ID
        assert_eq!(host_key, host_key2);

        // Different values for same key
        assert_ne!(server1_val, server2_val);

        // Resolve back
        assert_eq!(dict.resolve_key(host_key), Some("host".to_string()));
        assert_eq!(
            dict.resolve_value(host_key, server1_val),
            Some("server1".to_string())
        );
        assert_eq!(
            dict.resolve_value(dc_key, useast_val),
            Some("us-east".to_string())
        );
    }

    #[test]
    fn test_tag_dictionary_metrics() {
        let dict = TagDictionary::new();

        let cpu_id = dict.intern_metric("cpu_usage");
        let mem_id = dict.intern_metric("memory_free");
        let cpu_id2 = dict.intern_metric("cpu_usage");

        assert_eq!(cpu_id, cpu_id2);
        assert_ne!(cpu_id, mem_id);

        assert_eq!(dict.resolve_metric(cpu_id), Some("cpu_usage".to_string()));
    }

    #[test]
    fn test_tag_dictionary_batch() {
        let dict = TagDictionary::new();

        let tags = [("host", "server1"), ("dc", "us-east"), ("env", "prod")];

        let interned = dict.intern_tags(&tags);
        let resolved =
            dict.resolve_tags(&interned.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>());

        assert_eq!(resolved.len(), 3);
        assert!(resolved.contains(&("host".to_string(), "server1".to_string())));
        assert!(resolved.contains(&("dc".to_string(), "us-east".to_string())));
    }

    #[test]
    fn test_tag_dictionary_stats() {
        let dict = TagDictionary::new();

        dict.intern_tag("host", "server1");
        dict.intern_tag("host", "server2");
        dict.intern_tag("host", "server3");
        dict.intern_tag("dc", "us-east");
        dict.intern_metric("cpu_usage");

        let stats = dict.stats();
        assert_eq!(stats.key_count, 2); // host, dc
        assert_eq!(stats.total_value_count, 4); // server1, server2, server3, us-east
        assert_eq!(stats.metric_count, 1);
        assert_eq!(stats.max_values_per_key, 3); // host has 3 values
    }

    #[test]
    fn test_memory_estimation() {
        let dict = TagDictionary::new();

        // Intern many values
        for i in 0..100 {
            dict.intern_tag("host", &format!("server-{}", i));
        }

        let mem = dict.memory_usage();
        assert!(mem > 0);

        // With 100 strings (each ~12 bytes), total string data is ~1.2KB
        // HashMap overhead adds capacity-based allocation
        // Just verify memory is being tracked and is reasonable
        assert!(mem < 1_000_000); // Less than 1MB for 100 strings (generous bound)
    }
}
