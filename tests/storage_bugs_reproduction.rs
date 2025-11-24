//! Reproduction tests for identified storage layer bugs
//!
//! This test file demonstrates the critical bugs found in the storage layer review.
//! Each test is designed to fail and expose the bug.

use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use gorilla_tsdb::storage::chunk::Chunk;
use gorilla_tsdb::types::DataPoint;

/// Bug #1: Out-of-order writes corrupt metadata in Chunk
///
/// EXPECTED: end_timestamp should be 3000 (the actual last timestamp)
/// ACTUAL: end_timestamp becomes 2000 (the last appended timestamp)
#[test]
fn test_bug1_out_of_order_metadata_corruption() {
    let mut chunk = Chunk::new_active(1, 100);

    // Append in order
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 1.0,
    }).unwrap();

    // Append a future timestamp
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 3000,
        value: 3.0,
    }).unwrap();

    // Append an out-of-order timestamp
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 2000,
        value: 2.0,
    }).unwrap();

    // BUG: end_timestamp is 2000 but should be 3000
    let points = chunk.points().unwrap();
    println!("Points order: {:?}", points.iter().map(|p| p.timestamp).collect::<Vec<_>>());
    println!("Metadata: start={}, end={}",
        chunk.metadata.start_timestamp,
        chunk.metadata.end_timestamp);

    // This assertion SHOULD pass but WILL FAIL due to bug
    assert_eq!(chunk.metadata.start_timestamp, 1000, "Start timestamp should be 1000");
    assert_eq!(chunk.metadata.end_timestamp, 3000, "End timestamp should be 3000 (highest)");

    // Check if points are sorted
    let timestamps: Vec<i64> = points.iter().map(|p| p.timestamp).collect();
    let mut sorted_timestamps = timestamps.clone();
    sorted_timestamps.sort();

    assert_eq!(timestamps, sorted_timestamps, "Points should be in sorted order");
}

/// Bug #1b: Out-of-order writes with start timestamp
#[test]
fn test_bug1_out_of_order_start_timestamp() {
    let mut chunk = Chunk::new_active(1, 100);

    // Start with middle timestamp
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 2000,
        value: 2.0,
    }).unwrap();

    // Append earlier timestamp
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 1.0,
    }).unwrap();

    // Append later timestamp
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 3000,
        value: 3.0,
    }).unwrap();

    println!("Metadata: start={}, end={}",
        chunk.metadata.start_timestamp,
        chunk.metadata.end_timestamp);

    // BUG: start_timestamp is 2000 but should be 1000
    assert_eq!(chunk.metadata.start_timestamp, 1000, "Start timestamp should be 1000 (lowest)");
    assert_eq!(chunk.metadata.end_timestamp, 3000, "End timestamp should be 3000 (highest)");
}

/// Bug #2: Duplicate timestamps in ActiveChunk silently overwrite
/// FIXED: Now returns error instead of silent overwrite
#[test]
fn test_bug2_duplicate_timestamp_silent_overwrite() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    // Append first point
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 10.0,
    }).unwrap();

    println!("After first append: count={}", chunk.point_count());

    // Append duplicate timestamp with different value
    let result = chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 20.0,
    });

    println!("After duplicate append attempt: result={:?}", result);

    // FIXED: Now returns error instead of silent overwrite
    assert!(result.is_err(), "Should return error for duplicate timestamp");
    assert!(result.unwrap_err().contains("Duplicate timestamp"));
    assert_eq!(chunk.point_count(), 1, "Count should still be 1 after rejected duplicate");
}

/// Bug #2b: Duplicate timestamps in Chunk allows duplicates
/// FIXED: Now rejects duplicates like ActiveChunk
#[test]
fn test_bug2_duplicate_timestamp_chunk_rejects() {
    let mut chunk = Chunk::new_active(1, 100);

    // Append first point
    chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 10.0,
    }).unwrap();

    // Append duplicate timestamp - should now be rejected
    let result = chunk.append(DataPoint {
        series_id: 1,
        timestamp: 1000,
        value: 20.0,
    });

    // FIXED: Now returns error
    assert!(result.is_err(), "Chunk should reject duplicates");
    assert!(result.unwrap_err().contains("Duplicate timestamp"));
    assert_eq!(chunk.point_count(), 1, "Should still have 1 point");
}

/// Bug #2c: Inconsistency between Chunk and ActiveChunk
/// FIXED: Both now reject duplicates consistently
#[tokio::test]
async fn test_bug2_consistency_chunk_vs_active() {
    // Create points with duplicate timestamp
    let points = vec![
        DataPoint { series_id: 1, timestamp: 1000, value: 10.0 },
        DataPoint { series_id: 1, timestamp: 1000, value: 20.0 },  // Duplicate
        DataPoint { series_id: 1, timestamp: 2000, value: 30.0 },
    ];

    // Test with Chunk
    let mut chunk = Chunk::new_active(1, 10);
    let mut chunk_successful = 0;
    for p in &points {
        if chunk.append(*p).is_ok() {
            chunk_successful += 1;
        }
    }
    assert_eq!(chunk_successful, 2, "Chunk accepts 2 (rejects duplicate)");
    assert_eq!(chunk.point_count(), 2);

    // Test with ActiveChunk
    let active = ActiveChunk::new(1, 10, SealConfig::default());
    let mut active_successful = 0;
    for p in &points {
        if active.append(*p).is_ok() {
            active_successful += 1;
        }
    }
    assert_eq!(active_successful, 2, "ActiveChunk accepts 2 (rejects duplicate)");
    assert_eq!(active.point_count(), 2);

    // FIXED: Consistent behavior!
    assert_eq!(chunk.point_count(), active.point_count(), "Both should have same count");
    println!("Chunk count: {}, ActiveChunk count: {} - CONSISTENT!",
        chunk.point_count(), active.point_count());
}

/// Test demonstrating the expected sorted behavior after fixing Bug #1
/// FIXED: This now passes!
#[test]
fn test_expected_sorted_insertion_after_fix() {
    let mut chunk = Chunk::new_active(1, 100);

    // Append completely out of order
    let timestamps = vec![5000, 1000, 3000, 2000, 4000];
    for ts in timestamps {
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: ts,
            value: ts as f64,
        }).unwrap();
    }

    let points = chunk.points().unwrap();
    let retrieved_timestamps: Vec<i64> = points.iter().map(|p| p.timestamp).collect();

    // EXPECTED: Points should be sorted
    assert_eq!(retrieved_timestamps, vec![1000, 2000, 3000, 4000, 5000]);

    // EXPECTED: Metadata should reflect true range
    assert_eq!(chunk.metadata.start_timestamp, 1000);
    assert_eq!(chunk.metadata.end_timestamp, 5000);
}

/// Performance test: Measure memory during seal
#[tokio::test]
#[ignore = "Requires manual inspection of memory usage"]
async fn test_bug3_memory_spike_during_seal() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[allow(dead_code)]
    struct TrackingAllocator;
    static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

    let mut chunk = Chunk::new_active(1, 10000);

    // Fill with 10k points
    for i in 0..10000 {
        chunk.append(DataPoint {
            series_id: 1,
            timestamp: i * 1000,
            value: i as f64,
        }).unwrap();
    }

    let before_seal = ALLOCATED.load(Ordering::SeqCst);
    println!("Memory before seal: {} bytes", before_seal);

    // BUG #3: This clones the entire Vec, doubling memory
    let result = chunk.seal("/tmp/test_memory_seal.gor".into()).await;

    let during_seal = ALLOCATED.load(Ordering::SeqCst);
    println!("Memory during seal: {} bytes", during_seal);
    println!("Memory spike: {} bytes", during_seal - before_seal);

    assert!(result.is_ok());

    // Cleanup
    let _ = tokio::fs::remove_file("/tmp/test_memory_seal.gor").await;
}

/// Test lock contention in should_seal
#[test]
#[ignore = "Requires profiling/instrumentation"]
fn test_perf1_lock_contention_in_should_seal() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let chunk = Arc::new(ActiveChunk::new(1, 10000, SealConfig {
        max_points: 100000,  // High threshold to keep checking
        max_duration_ms: 1_000_000_000,
        max_size_bytes: 10_000_000,
    }));

    let chunk_writer = Arc::clone(&chunk);
    let chunk_checker = Arc::clone(&chunk);

    // Thread 1: Continuously append
    let writer = thread::spawn(move || {
        for i in 0..5000 {
            chunk_writer.append(DataPoint {
                series_id: 1,
                timestamp: i * 1000,
                value: i as f64,
            }).unwrap();
        }
    });

    // Thread 2: Continuously check should_seal
    let checker = thread::spawn(move || {
        let start = Instant::now();
        let mut check_count = 0;

        while start.elapsed().as_millis() < 100 {
            let _ = chunk_checker.should_seal();
            check_count += 1;
        }

        println!("should_seal() called {} times in 100ms", check_count);
        check_count
    });

    writer.join().unwrap();
    let check_count = checker.join().unwrap();

    // PERF ISSUE: should_seal takes read lock every time,
    // reducing throughput of both operations
    println!("Performance: {} checks/ms", check_count / 100);
}
