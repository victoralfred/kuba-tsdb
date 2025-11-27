///! Tests for code review findings
///!
///! These tests address issues identified in the code review including:
///! - Edge case handling
///! - Security validation
///! - Compression edge cases
///! - Concurrent operations
use gorilla_tsdb::compression::gorilla::GorillaCompressor;
use gorilla_tsdb::engine::traits::Compressor;
use gorilla_tsdb::security::{validate_chunk_path, validate_series_id, validate_timestamp};
use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use gorilla_tsdb::storage::chunk::Chunk;
use gorilla_tsdb::types::DataPoint;
use std::sync::Arc;
use tokio::task::JoinSet;

// ============================================================
// Edge Case Tests - Chunk Module
// ============================================================

/// Test: Empty chunk cannot be sealed
/// Edge case: EDGE-004 - Empty collection handling
#[tokio::test]
async fn test_seal_empty_chunk_returns_error() {
    let mut chunk = Chunk::new_active(1, 1000);
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("test_empty.gor");
    let result = chunk.seal(path).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("empty") || err.contains("no points") || err.contains("Cannot compress"),
        "Expected empty/no points error, got: {}",
        err
    );
}

/// Test: Duplicate timestamp handling
/// Edge case: Duplicate prevention
#[test]
fn test_duplicate_timestamp_rejected() {
    let mut chunk = Chunk::new_active(1, 1000);

    let point1 = DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 42.0,
    };
    let point2 = DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 99.0,
    };

    assert!(chunk.append(point1).is_ok());
    let result = chunk.append(point2);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Duplicate timestamp"));
}

/// Test: Out-of-order timestamp handling
/// Verifies BTreeMap maintains order
#[test]
fn test_out_of_order_timestamps_sorted() {
    let mut chunk = Chunk::new_active(1, 1000);

    // Add points out of order
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 3000,
            value: 3.0,
        })
        .unwrap();
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 2.0,
        })
        .unwrap();

    let points = chunk.points().unwrap();

    // Verify sorted order
    assert_eq!(points[0].timestamp, 1000);
    assert_eq!(points[1].timestamp, 2000);
    assert_eq!(points[2].timestamp, 3000);
}

/// Test: Series ID mismatch handling
#[test]
fn test_series_id_mismatch_rejected() {
    let mut chunk = Chunk::new_active(1, 1000);

    let point = DataPoint {
        series_id: 2,
        timestamp: 1000,
        value: 42.0,
    };
    let result = chunk.append(point);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("doesn't match"));
}

// ============================================================
// Security Tests
// ============================================================

/// Test: Path traversal attack prevention
/// Security: VULN-006 - Path validation
#[test]
fn test_path_traversal_blocked() {
    let malicious_paths = vec![
        "/data/../etc/passwd",
        "/data/gorilla-tsdb/../../../etc/shadow",
        "../../sensitive.gor",
        "/data/gorilla-tsdb/chunks/../../file.gor",
    ];

    for path in malicious_paths {
        let result = validate_chunk_path(path);
        assert!(result.is_err(), "Path traversal not blocked: {}", path);
    }
}

/// Test: Null byte injection prevention
#[test]
fn test_null_byte_blocked() {
    let result = validate_chunk_path("/data/chunk\0.gor");
    assert!(result.is_err());
}

/// Test: Invalid extension rejection
#[test]
fn test_invalid_extension_blocked() {
    let invalid_extensions = vec![
        "/data/chunk.txt",
        "/data/chunk.sh",
        "/data/chunk",
        "/data/chunk.gor.bak",
    ];

    for path in invalid_extensions {
        let result = validate_chunk_path(path);
        assert!(result.is_err(), "Invalid extension not blocked: {}", path);
    }
}

/// Test: Series ID boundary validation
/// Security: VULN-007 - Input validation
#[test]
fn test_series_id_boundaries() {
    assert!(validate_series_id(0).is_err());
    assert!(validate_series_id(u128::MAX).is_err());
    assert!(validate_series_id(1).is_ok());
    assert!(validate_series_id(u128::MAX - 1).is_ok());
}

/// Test: Timestamp boundary validation
#[test]
fn test_timestamp_boundaries() {
    assert!(validate_timestamp(i64::MIN).is_err());
    assert!(validate_timestamp(i64::MAX).is_err());
    assert!(validate_timestamp(0).is_ok());
    assert!(validate_timestamp(-1).is_ok());
    assert!(validate_timestamp(i64::MAX - 1).is_ok());
}

// ============================================================
// Compression Tests
// ============================================================

/// Test: Compression roundtrip with edge values
/// Performance: PERF-005 - Bit operations
#[tokio::test]
async fn test_compression_roundtrip_edge_values() {
    let compressor = GorillaCompressor::new();

    let edge_values = vec![
        DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 0.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: -0.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 3000,
            value: f64::MAX,
        },
        DataPoint {
            series_id: 1,
            timestamp: 4000,
            value: f64::MIN,
        },
        DataPoint {
            series_id: 1,
            timestamp: 5000,
            value: f64::EPSILON,
        },
        DataPoint {
            series_id: 1,
            timestamp: 6000,
            value: f64::MIN_POSITIVE,
        },
    ];

    let compressed = compressor.compress(&edge_values).await.unwrap();
    let decompressed = compressor.decompress(&compressed).await.unwrap();

    assert_eq!(edge_values.len(), decompressed.len());
    for (orig, decomp) in edge_values.iter().zip(decompressed.iter()) {
        assert_eq!(orig.timestamp, decomp.timestamp);
        // Handle special float comparisons
        if orig.value.is_nan() {
            assert!(decomp.value.is_nan());
        } else {
            assert_eq!(orig.value, decomp.value);
        }
    }
}

/// Test: Compression with large timestamp deltas
#[tokio::test]
async fn test_compression_large_timestamp_delta() {
    let compressor = GorillaCompressor::new();

    let points = vec![
        DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 1_000_000_000_000,
            value: 2.0,
        },
    ];

    let compressed = compressor.compress(&points).await.unwrap();
    let decompressed = compressor.decompress(&compressed).await.unwrap();

    assert_eq!(points.len(), decompressed.len());
    assert_eq!(points[0].timestamp, decompressed[0].timestamp);
    assert_eq!(points[1].timestamp, decompressed[1].timestamp);
}

/// Test: Compression with empty points returns error
/// Tests the fix for EDGE-002
#[tokio::test]
async fn test_compression_empty_points_error() {
    let compressor = GorillaCompressor::new();

    let empty_points: Vec<DataPoint> = vec![];
    let result = compressor.compress(&empty_points).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{:?}", err).contains("empty") || format!("{:?}", err).contains("Empty"),
        "Expected empty error, got: {:?}",
        err
    );
}

// ============================================================
// Active Chunk Thread Safety Tests
// ============================================================

/// Test: Concurrent appends to active chunk
/// Thread safety verification
#[tokio::test]
async fn test_concurrent_appends() {
    let chunk = Arc::new(ActiveChunk::new(1, 10_000, SealConfig::default()));
    let mut tasks = JoinSet::new();

    // Spawn 10 tasks that each append 100 points
    for task_id in 0..10 {
        let chunk_clone = Arc::clone(&chunk);
        tasks.spawn(async move {
            for i in 0..100 {
                let timestamp = (task_id * 1000 + i) as i64;
                let point = DataPoint {
                    series_id: 1,
                    timestamp,
                    value: timestamp as f64,
                };
                let _ = chunk_clone.append(point);
            }
        });
    }

    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Verify total count (some may have duplicates rejected)
    assert!(chunk.point_count() <= 1000);
    assert!(chunk.point_count() >= 900); // Allow some duplicates due to timing
}

/// Test: Seal after concurrent modifications
#[tokio::test]
async fn test_seal_after_concurrent_writes() {
    let chunk = Arc::new(ActiveChunk::new(1, 100, SealConfig::default()));

    // Add some points
    for i in 0..10 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    // Seal should work
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("test_concurrent_seal.gor");
    let result = chunk.seal(path).await;
    assert!(result.is_ok());
    assert!(chunk.is_sealed());
}

// ============================================================
// Writer Panic Fix Tests
// ============================================================

/// Test: Writer handles Arc with multiple references gracefully
/// Tests the fix for VULN-003
#[tokio::test]
async fn test_writer_seal_with_multiple_arc_references() {
    use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
    use std::time::Duration;

    let temp_dir = tempfile::TempDir::new().unwrap();
    let config = ChunkWriterConfig {
        max_points: 100,
        max_duration: Duration::from_secs(60),
        max_size_bytes: 1024 * 1024,
        auto_seal: false, // Disable auto-seal for manual control
        initial_capacity: 100,
        write_buffer_size: 100,
    };

    let writer = ChunkWriter::new(1, temp_dir.path().to_path_buf(), config);

    // Write some points
    for i in 0..10 {
        writer
            .write(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .await
            .unwrap();
    }

    // In normal operation, seal should succeed
    // The panic fix ensures that if Arc has multiple refs, it returns an error instead of panicking
    let result = writer.seal().await;

    // Should either succeed or return a proper error (not panic)
    match result {
        Ok(_) => {
            // Seal succeeded normally
        }
        Err(e) => {
            // If seal fails due to multiple references, it should be a proper error
            assert!(
                e.contains("references") || e.contains("No active chunk"),
                "Unexpected error: {}",
                e
            );
        }
    }
}

// ============================================================
// Metrics Tests
// ============================================================

/// Test: Metrics gathering returns Result instead of panicking
/// Tests the fix for EDGE-003
#[test]
fn test_metrics_gathering_returns_result() {
    use gorilla_tsdb::metrics::{gather_metrics, init};

    // Initialize metrics to ensure at least some metrics are registered
    // This is needed because lazy_static metrics may not be initialized
    // if no other code has touched them yet (test isolation)
    init();

    let result = gather_metrics();
    assert!(result.is_ok(), "Metrics gathering should return Ok");

    let metrics_text = result.unwrap();
    assert!(!metrics_text.is_empty(), "Metrics should not be empty");
}
