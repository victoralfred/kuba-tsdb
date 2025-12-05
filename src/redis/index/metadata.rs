//! Redis chunk metadata structures
//!
//! Defines the metadata stored in Redis for each chunk.

use serde::{Deserialize, Serialize};

/// Chunk metadata stored in Redis
///
/// Contains all metadata about a chunk needed for index operations
/// including location, time range, and compression details.
///
/// # Redis Storage
///
/// Stored as JSON in the hash field `ts:chunks:{chunk_id}:metadata`.
///
/// # ENH-003 Statistics
///
/// The `min_value` and `max_value` fields support zone map pruning
/// and cost estimation in the query planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisChunkMetadata {
    /// Series identifier (as string for JSON compatibility)
    pub series_id: String,
    /// Filesystem path to chunk data
    pub path: String,
    /// Start timestamp (Unix milliseconds)
    pub start_time: i64,
    /// End timestamp (Unix milliseconds)
    pub end_time: i64,
    /// Number of data points in chunk
    pub point_count: usize,
    /// Chunk size in bytes
    pub size_bytes: usize,
    /// Compression algorithm used (e.g., "Kuba", "Snappy")
    pub compression: String,
    /// Chunk status (active, sealed, compressed, archived)
    pub status: String,
    /// Creation timestamp (Unix milliseconds)
    pub created_at: i64,

    /// Minimum value in the chunk (optional, for zone map pruning)
    ///
    /// Populated during compaction or first scan.
    #[serde(default)]
    pub min_value: Option<f64>,

    /// Maximum value in the chunk (optional, for zone map pruning)
    ///
    /// Populated during compaction or first scan.
    #[serde(default)]
    pub max_value: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_chunk_metadata_serialization() {
        let metadata = RedisChunkMetadata {
            series_id: "12345".to_string(),
            path: "/data/chunks/chunk1.bin".to_string(),
            start_time: 1000,
            end_time: 2000,
            point_count: 100,
            size_bytes: 4096,
            compression: "Kuba".to_string(),
            status: "sealed".to_string(),
            created_at: 1700000000000,
            min_value: Some(10.0),
            max_value: Some(100.0),
        };

        // Serialize
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("12345"));
        assert!(json.contains("/data/chunks/chunk1.bin"));
        assert!(json.contains("Kuba"));

        // Deserialize
        let deserialized: RedisChunkMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.series_id, "12345");
        assert_eq!(deserialized.path, "/data/chunks/chunk1.bin");
        assert_eq!(deserialized.start_time, 1000);
        assert_eq!(deserialized.end_time, 2000);
        assert_eq!(deserialized.point_count, 100);
        assert_eq!(deserialized.size_bytes, 4096);
        assert_eq!(deserialized.compression, "Kuba");
        assert_eq!(deserialized.status, "sealed");
    }

    #[test]
    fn test_redis_chunk_metadata_clone() {
        let metadata = RedisChunkMetadata {
            series_id: "999".to_string(),
            path: "/path/to/chunk".to_string(),
            start_time: 0,
            end_time: 1000,
            point_count: 50,
            size_bytes: 2048,
            compression: "Kuba".to_string(),
            status: "active".to_string(),
            created_at: 0,
            min_value: None,
            max_value: None,
        };

        let cloned = metadata.clone();
        assert_eq!(cloned.series_id, metadata.series_id);
        assert_eq!(cloned.path, metadata.path);
        assert_eq!(cloned.size_bytes, metadata.size_bytes);
    }

    #[test]
    fn test_redis_chunk_metadata_optional_values() {
        // Test with None values
        let metadata = RedisChunkMetadata {
            series_id: "1".to_string(),
            path: "/path".to_string(),
            start_time: 0,
            end_time: 100,
            point_count: 10,
            size_bytes: 1024,
            compression: "Kuba".to_string(),
            status: "sealed".to_string(),
            created_at: 0,
            min_value: None,
            max_value: None,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: RedisChunkMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.min_value.is_none());
        assert!(deserialized.max_value.is_none());
    }

    #[test]
    fn test_redis_chunk_metadata_deserialization_missing_optional() {
        // JSON without min_value and max_value should deserialize with None
        let json = r#"{
            "series_id": "123",
            "path": "/test",
            "start_time": 0,
            "end_time": 100,
            "point_count": 5,
            "size_bytes": 512,
            "compression": "Kuba",
            "status": "sealed",
            "created_at": 0
        }"#;

        let metadata: RedisChunkMetadata = serde_json::from_str(json).unwrap();
        assert!(metadata.min_value.is_none());
        assert!(metadata.max_value.is_none());
    }

    #[test]
    fn test_redis_chunk_metadata_debug() {
        let metadata = RedisChunkMetadata {
            series_id: "1".to_string(),
            path: "/test".to_string(),
            start_time: 0,
            end_time: 100,
            point_count: 10,
            size_bytes: 1024,
            compression: "Kuba".to_string(),
            status: "sealed".to_string(),
            created_at: 0,
            min_value: Some(1.0),
            max_value: Some(99.0),
        };

        let debug_str = format!("{:?}", metadata);
        assert!(debug_str.contains("RedisChunkMetadata"));
        assert!(debug_str.contains("series_id"));
        assert!(debug_str.contains("path"));
    }
}
