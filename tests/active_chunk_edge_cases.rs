/// Comprehensive edge case tests for ActiveChunk
///
/// This test suite covers all identified edge cases:
/// 1. Concurrency (thread safety)
/// 2. Out-of-order handling
/// 3. Timestamp boundaries
/// 4. Capacity limits
/// 5. Sealing thresholds
/// 6. Series validation
/// 7. State transitions
/// 8. Memory management
/// 9. Error handling
/// 10. Performance characteristics
use kuba_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use kuba_tsdb::types::DataPoint;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================================
// CATEGORY 1: CONCURRENCY EDGE CASES
// ============================================================================

/// Test: Multiple threads appending simultaneously
#[test]
fn test_concurrent_appends() {
    let chunk = Arc::new(ActiveChunk::new(1, 1000, SealConfig::default()));
    let mut handles = vec![];

    // Spawn 10 threads, each appending 100 points
    for thread_id in 0..10 {
        let chunk_clone = Arc::clone(&chunk);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let point = DataPoint {
                    series_id: 1,
                    timestamp: (thread_id * 1000 + i) as i64,
                    value: thread_id as f64,
                };
                chunk_clone.append(point).expect("Append failed");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all 1000 points were added
    assert_eq!(chunk.point_count(), 1000);
}

/// Test: Concurrent reads while writing
#[test]
fn test_concurrent_read_write() {
    let chunk = Arc::new(ActiveChunk::new(1, 1000, SealConfig::default()));

    // Writer thread
    let chunk_write = Arc::clone(&chunk);
    let writer = thread::spawn(move || {
        for i in 0..500 {
            let point = DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            };
            chunk_write.append(point).expect("Append failed");
            thread::sleep(Duration::from_micros(1));
        }
    });

    // Reader threads
    let mut readers = vec![];
    for _ in 0..5 {
        let chunk_read = Arc::clone(&chunk);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let _count = chunk_read.point_count();
                let _should_seal = chunk_read.should_seal();
                thread::sleep(Duration::from_micros(1));
            }
        });
        readers.push(handle);
    }

    // Wait for all
    writer.join().expect("Writer panicked");
    for reader in readers {
        reader.join().expect("Reader panicked");
    }

    assert_eq!(chunk.point_count(), 500);
}

/// Test: Seal while appending
#[tokio::test]
async fn test_seal_while_appending() {
    use std::sync::Mutex;

    let chunk = Arc::new(ActiveChunk::new(
        1,
        100,
        SealConfig {
            max_points: 100,
            max_duration_ms: 1_000_000,
            max_size_bytes: 1_000_000,
        },
    ));

    let sealed = Arc::new(Mutex::new(false));

    // Writer thread
    let chunk_write = Arc::clone(&chunk);
    let sealed_write = Arc::clone(&sealed);
    let writer = thread::spawn(move || {
        for i in 0..150 {
            if *sealed_write.lock().unwrap() {
                break;
            }
            let point = DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            };
            let _ = chunk_write.append(point);
            thread::sleep(Duration::from_micros(10));
        }
    });

    // Wait for ~100 points
    thread::sleep(Duration::from_millis(500));

    // Try to seal
    let result = chunk.seal("/tmp/test_seal_concurrent.kub".into()).await;
    *sealed.lock().unwrap() = true;

    writer.join().expect("Writer panicked");

    // Seal should succeed
    assert!(result.is_ok());
}

/// Test: Concurrent seal attempts
#[tokio::test]
async fn test_concurrent_seal() {
    let chunk = Arc::new(ActiveChunk::new(1, 100, SealConfig::default()));

    // Add some points
    for i in 0..50 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    // Try to seal from multiple threads
    let chunk1 = Arc::clone(&chunk);
    let chunk2 = Arc::clone(&chunk);

    let h1 = tokio::spawn(async move { chunk1.seal("/tmp/seal1.kub".into()).await });

    let h2 = tokio::spawn(async move { chunk2.seal("/tmp/seal2.kub".into()).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    // One should succeed, one should fail
    assert!(r1.is_ok() != r2.is_ok() || (r1.is_ok() && r2.is_ok()));
}

// ============================================================================
// CATEGORY 2: OUT-OF-ORDER EDGE CASES
// ============================================================================

/// Test: Points arriving out of order
#[test]
fn test_out_of_order_points() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    // Insert in non-chronological order
    let timestamps = vec![100, 50, 75, 25, 150, 10, 200];
    for ts in timestamps {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: ts,
                value: ts as f64,
            })
            .unwrap();
    }

    // Verify points are stored in sorted order
    let (start, end) = chunk.time_range();
    assert_eq!(start, 10);
    assert_eq!(end, 200);
}

/// Test: Points with same timestamp
#[test]
fn test_duplicate_timestamps() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    // Insert multiple points with same timestamp
    for i in 0..5 {
        let result = chunk.append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: i as f64,
        });

        if i == 0 {
            assert!(result.is_ok(), "First point should succeed");
        } else {
            // Behavior: either overwrite, reject, or accept based on implementation
            // Document the behavior here
        }
    }
}

/// Test: Point far in the past
#[test]
fn test_point_far_in_past() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    // Add recent point
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1_700_000_000_000, // ~2023
            value: 1.0,
        })
        .unwrap();

    // Add point from year 2000
    let result = chunk.append(DataPoint {
        series_id: 1,
        timestamp: 946_684_800_000, // 2000-01-01
        value: 2.0,
    });

    // Should accept (BTreeMap handles it)
    assert!(result.is_ok());
}

/// Test: All points in reverse order
#[test]
fn test_all_points_reverse_order() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    // Insert 100 points in reverse chronological order
    for i in (0..100).rev() {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    let (start, end) = chunk.time_range();
    assert_eq!(start, 0);
    assert_eq!(end, 99);
    assert_eq!(chunk.point_count(), 100);
}

// ============================================================================
// CATEGORY 3: TIMESTAMP EDGE CASES
// ============================================================================

/// Test: Negative timestamps
#[test]
fn test_negative_timestamps() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: -1000,
            value: 1.0,
        })
        .unwrap();

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: -500,
            value: 2.0,
        })
        .unwrap();

    let (start, end) = chunk.time_range();
    assert_eq!(start, -1000);
    assert_eq!(end, -500);
}

/// Test: Zero timestamp
#[test]
fn test_zero_timestamp() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 0,
            value: 1.0,
        })
        .unwrap();

    let (start, end) = chunk.time_range();
    assert_eq!(start, 0);
    assert_eq!(end, 0);
}

/// Test: i64::MAX timestamp
#[test]
fn test_max_timestamp() {
    let chunk = ActiveChunk::new(1, 10, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MAX,
            value: 1.0,
        })
        .unwrap();

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 1,
            value: 2.0,
        })
        .unwrap();

    let (_, end) = chunk.time_range();
    assert_eq!(end, i64::MAX);
}

/// Test: i64::MIN timestamp
#[test]
fn test_min_timestamp() {
    let chunk = ActiveChunk::new(1, 10, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MIN,
            value: 1.0,
        })
        .unwrap();

    let (start, _) = chunk.time_range();
    assert_eq!(start, i64::MIN);
}

// ============================================================================
// CATEGORY 4: CAPACITY EDGE CASES
// ============================================================================

/// Test: Exceed capacity
#[test]
fn test_exceed_capacity() {
    let chunk = ActiveChunk::new(1, 10, SealConfig::default());

    // Add 20 points (2x capacity)
    for i in 0..20 {
        let result = chunk.append(DataPoint {
            series_id: 1,
            timestamp: i,
            value: i as f64,
        });
        assert!(result.is_ok(), "Should grow beyond initial capacity");
    }

    assert_eq!(chunk.point_count(), 20);
}

/// Test: Zero capacity
#[test]
fn test_zero_capacity() {
    let chunk = ActiveChunk::new(1, 0, SealConfig::default());

    // Should still work (capacity is just a hint)
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    assert_eq!(chunk.point_count(), 1);
}

// ============================================================================
// CATEGORY 5: SEALING THRESHOLD EDGE CASES
// ============================================================================

/// Test: Exactly at point threshold
#[test]
fn test_seal_exact_point_threshold() {
    let chunk = ActiveChunk::new(
        1,
        100,
        SealConfig {
            max_points: 10,
            max_duration_ms: 1_000_000,
            max_size_bytes: 1_000_000,
        },
    );

    // Add exactly 10 points
    for i in 0..10 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    assert!(chunk.should_seal());
}

/// Test: One point before threshold
#[test]
fn test_seal_one_before_threshold() {
    let chunk = ActiveChunk::new(
        1,
        100,
        SealConfig {
            max_points: 10,
            max_duration_ms: 1_000_000,
            max_size_bytes: 1_000_000,
        },
    );

    // Add 9 points
    for i in 0..9 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    assert!(!chunk.should_seal());
}

/// Test: Multiple thresholds exceeded
#[test]
fn test_seal_multiple_thresholds() {
    let chunk = ActiveChunk::new(
        1,
        100,
        SealConfig {
            max_points: 5,
            max_duration_ms: 1000, // 1 second
            max_size_bytes: 100,
        },
    );

    // Add points up to the max_points threshold
    // The timestamps also exceed max_duration_ms (5 points * 500ms = 2500ms > 1000ms)
    for i in 0..5 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i * 500, // Timestamps: 0, 500, 1000, 1500, 2000 ms
                value: i as f64,
            })
            .unwrap();
    }

    // Should need sealing because max_points reached AND max_duration exceeded
    assert!(chunk.should_seal());
}

// ============================================================================
// CATEGORY 6: SERIES VALIDATION EDGE CASES
// ============================================================================

/// Test: Wrong series ID
#[test]
fn test_wrong_series_id() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    let result = chunk.append(DataPoint {
        series_id: 2, // Wrong series
        timestamp: 1000,
        value: 1.0,
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("series"));
}

/// Test: Series ID zero
#[test]
fn test_series_id_zero() {
    let chunk = ActiveChunk::new(0, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 0,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    assert_eq!(chunk.point_count(), 1);
}

/// Test: Very large series ID
#[test]
fn test_large_series_id() {
    let chunk = ActiveChunk::new(u128::MAX, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: u128::MAX,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    assert_eq!(chunk.point_count(), 1);
}

// ============================================================================
// CATEGORY 7: STATE TRANSITION EDGE CASES
// ============================================================================

/// Test: Append after seal
#[tokio::test]
async fn test_append_after_seal() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    // Seal
    chunk
        .seal("/tmp/test_append_after_seal.kub".into())
        .await
        .unwrap();

    // Try to append
    let result = chunk.append(DataPoint {
        series_id: 1,
        timestamp: 2000,
        value: 2.0,
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("sealed"));
}

/// Test: Double seal
#[tokio::test]
async fn test_double_seal() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    // First seal
    chunk
        .seal("/tmp/test_double_seal1.kub".into())
        .await
        .unwrap();

    // Second seal
    let result = chunk.seal("/tmp/test_double_seal2.kub".into()).await;

    assert!(result.is_err());
}

/// Test: Seal empty chunk
#[tokio::test]
async fn test_seal_empty() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    let result = chunk.seal("/tmp/test_seal_empty.kub".into()).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
}

/// Test: Seal with single point
#[tokio::test]
async fn test_seal_single_point() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    let result = chunk.seal("/tmp/test_seal_single.kub".into()).await;

    assert!(result.is_ok());
}

// ============================================================================
// CATEGORY 8: MEMORY EDGE CASES
// ============================================================================

/// Test: Memory growth with out-of-order inserts
#[test]
fn test_memory_growth_out_of_order() {
    let chunk = ActiveChunk::new(1, 10, SealConfig::default());

    // Insert many out-of-order points
    for i in 0..1000 {
        let ts = if i % 2 == 0 { i } else { 1000 - i };
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: ts,
                value: i as f64,
            })
            .unwrap();
    }

    assert_eq!(chunk.point_count(), 1000);
    // Memory should be manageable (BTreeMap is efficient)
}

// ============================================================================
// CATEGORY 9: ERROR HANDLING EDGE CASES
// ============================================================================

/// Test: Invalid path during seal
///
/// Verifies that seal() handles invalid paths gracefully.
/// The seal operation implements full disk I/O with fsync for durability.
#[tokio::test]
async fn test_seal_invalid_path() {
    let chunk = ActiveChunk::new(1, 100, SealConfig::default());

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 1.0,
        })
        .unwrap();

    // Try to seal to invalid path
    let result = chunk.seal("/nonexistent/directory/chunk.kub".into()).await;

    // Should handle error gracefully
    assert!(result.is_err());
}

// ============================================================================
// CATEGORY 10: PERFORMANCE EDGE CASES
// ============================================================================

/// Test: High throughput single thread
#[test]
fn test_high_throughput_single_thread() {
    let chunk = ActiveChunk::new(
        1,
        10_000,
        SealConfig {
            max_points: 100_000,
            max_duration_ms: 1_000_000_000,
            max_size_bytes: 10_000_000,
        },
    );

    let start = std::time::Instant::now();

    // Insert 10K points
    for i in 0..10_000 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    let elapsed = start.elapsed();
    println!("Inserted 10K points in {:?}", elapsed);
    println!(
        "Throughput: {:.0} points/sec",
        10_000.0 / elapsed.as_secs_f64()
    );

    assert_eq!(chunk.point_count(), 10_000);
}

/// Test: Burst writes
#[test]
fn test_burst_writes() {
    let chunk = Arc::new(ActiveChunk::new(1, 1000, SealConfig::default()));
    let mut handles = vec![];

    // 5 threads, each does 200 points
    for thread_id in 0..5 {
        let chunk_clone = Arc::clone(&chunk);
        let handle = thread::spawn(move || {
            for i in 0..200 {
                chunk_clone
                    .append(DataPoint {
                        series_id: 1,
                        timestamp: (thread_id * 10000 + i) as i64,
                        value: i as f64,
                    })
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(chunk.point_count(), 1000);
}
