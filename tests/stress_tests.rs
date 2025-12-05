//! Stress tests for production readiness validation
//!
//! These tests simulate real-world load patterns and edge cases to validate
//! system behavior under stress. Run with --release flag for realistic performance.
//!
//! Usage:
//!   cargo test --release --test stress_tests -- --ignored --test-threads=1

// Allow manual modulo checks since is_multiple_of is unstable on stable Rust (Docker builds)
#![allow(clippy::manual_is_multiple_of)]
use kuba_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use kuba_tsdb::storage::chunk::Chunk;
use kuba_tsdb::types::DataPoint;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test sustained high throughput (1M points/sec target)
#[tokio::test]
#[ignore] // Run with: cargo test --release sustained_high_throughput -- --ignored
async fn test_sustained_high_throughput() {
    const DURATION_SECS: u64 = 60; // 1 minute
    const TARGET_RATE: u64 = 1_000_000; // 1M points/sec
    const POINTS_PER_BATCH: usize = 10_000;

    // Allow enough points for full 60-second test (60M points)
    let config = SealConfig {
        max_points: 100_000_000,            // 100M (way more than needed)
        max_duration_ms: 60_000_000,        // 1000 minutes
        max_size_bytes: 1024 * 1024 * 1024, // 1GB
    };

    let chunk = Arc::new(ActiveChunk::new(1, 10_000_000, config));
    let start = Instant::now();
    let end_time = start + Duration::from_secs(DURATION_SECS);
    let mut total_points = 0u64;
    let mut timestamp = 0i64;

    println!("🚀 Starting sustained throughput test...");
    println!(
        "   Target: {} points/sec for {} seconds",
        TARGET_RATE, DURATION_SECS
    );

    while Instant::now() < end_time {
        let batch_start = Instant::now();

        for _ in 0..POINTS_PER_BATCH {
            let point = DataPoint {
                series_id: 1,
                timestamp,
                value: (timestamp as f64) * 1.5,
            };

            if chunk.append(point).is_ok() {
                total_points += 1;
                timestamp += 1;
            }

            if chunk.should_seal() {
                break;
            }
        }

        let batch_duration = batch_start.elapsed();
        let current_rate = POINTS_PER_BATCH as f64 / batch_duration.as_secs_f64();

        if total_points % 1_000_000 == 0 {
            println!(
                "   ✓ {} points processed (current rate: {:.0} pts/sec)",
                total_points, current_rate
            );
        }
    }

    let elapsed = start.elapsed();
    let actual_rate = total_points as f64 / elapsed.as_secs_f64();

    println!("✅ Test complete!");
    println!("   Total points: {}", total_points);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Average rate: {:.0} points/sec", actual_rate);
    println!(
        "   Peak memory: {} MB",
        chunk.point_count() * 24 / 1_024 / 1_024
    );

    assert!(
        actual_rate >= TARGET_RATE as f64 * 0.8,
        "Throughput too low: {:.0} < 80% of target",
        actual_rate
    );
}

/// Test concurrent writes from multiple threads
#[tokio::test]
#[ignore]
async fn test_concurrent_write_stress() {
    const NUM_THREADS: usize = 16;
    const POINTS_PER_THREAD: usize = 100_000;

    let config = SealConfig {
        max_points: 10_000_000,
        max_duration_ms: 3_600_000,
        max_size_bytes: 1024 * 1024 * 1024, // 1GB
    };

    let chunk = Arc::new(ActiveChunk::new(1, 1_000_000, config));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    println!("🚀 Starting concurrent write stress test...");
    println!(
        "   Threads: {}, Points/thread: {}",
        NUM_THREADS, POINTS_PER_THREAD
    );

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let chunk = Arc::clone(&chunk);
        let errors = Arc::clone(&errors);

        let handle = tokio::spawn(async move {
            let mut success_count = 0u64;
            let base_timestamp = (thread_id * POINTS_PER_THREAD * 1000) as i64;

            for i in 0..POINTS_PER_THREAD {
                let point = DataPoint {
                    series_id: 1,
                    timestamp: base_timestamp + (i as i64),
                    value: (thread_id as f64) * 100.0 + (i as f64),
                };

                match chunk.append(point) {
                    Ok(_) => success_count += 1,
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    },
                }

                if chunk.should_seal() {
                    break;
                }
            }

            success_count
        });

        handles.push(handle);
    }

    let results: Vec<u64> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let elapsed = start.elapsed();
    let total_success: u64 = results.iter().sum();
    let total_errors = errors.load(Ordering::Relaxed);
    let throughput = total_success as f64 / elapsed.as_secs_f64();

    println!("✅ Test complete!");
    println!("   Successful writes: {}", total_success);
    println!("   Failed writes: {}", total_errors);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!("   Throughput: {:.0} points/sec", throughput);
    println!("   Final count: {}", chunk.point_count());

    assert!(
        total_errors < total_success / 100,
        "Error rate too high: {} errors",
        total_errors
    );
    assert_eq!(
        chunk.point_count() as u64,
        total_success,
        "Point count mismatch"
    );
}

/// Test memory behavior under sustained load
#[tokio::test]
#[ignore]
async fn test_memory_stability() {
    const TEST_DURATION_SECS: u64 = 300; // 5 minutes
    const SEAL_INTERVAL_SECS: u64 = 30;

    println!("🚀 Starting memory stability test...");
    println!("   Duration: {} seconds", TEST_DURATION_SECS);

    let start = Instant::now();
    let end_time = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_chunks_sealed = 0u64;
    let mut timestamp = 0i64;

    while Instant::now() < end_time {
        let _config = SealConfig {
            max_points: 10_000,
            max_duration_ms: (SEAL_INTERVAL_SECS * 1000) as i64,
            max_size_bytes: 10 * 1024 * 1024,
        };

        let mut chunk = Chunk::new_active(1, 10_000);

        // Fill chunk
        for _ in 0..10_000 {
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp,
                    value: timestamp as f64,
                })
                .unwrap();
            timestamp += 1;
        }

        // Seal to disk
        let path = format!("/tmp/stress_test_chunk_{}.kub", total_chunks_sealed);
        chunk.seal(path.into()).await.unwrap();
        total_chunks_sealed += 1;

        if total_chunks_sealed % 10 == 0 {
            println!(
                "   ✓ {} chunks sealed ({:.1}s elapsed)",
                total_chunks_sealed,
                start.elapsed().as_secs_f64()
            );
        }

        // Brief pause to simulate real workload
        sleep(Duration::from_millis(100)).await;
    }

    let elapsed = start.elapsed();

    println!("✅ Test complete!");
    println!("   Total chunks sealed: {}", total_chunks_sealed);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!(
        "   Average seal rate: {:.2} chunks/sec",
        total_chunks_sealed as f64 / elapsed.as_secs_f64()
    );

    // Cleanup
    for i in 0..total_chunks_sealed {
        let _ = std::fs::remove_file(format!("/tmp/stress_test_chunk_{}.kub", i));
    }
}

/// Test seal operations under concurrent load
#[tokio::test]
#[ignore]
async fn test_concurrent_seal_stress() {
    const NUM_CONCURRENT_SEALS: usize = 100;
    const POINTS_PER_CHUNK: usize = 1000;

    println!("🚀 Starting concurrent seal stress test...");
    println!("   Concurrent seals: {}", NUM_CONCURRENT_SEALS);

    let start = Instant::now();
    let mut handles = vec![];

    for chunk_id in 0..NUM_CONCURRENT_SEALS {
        let handle = tokio::spawn(async move {
            let mut chunk = Chunk::new_active(1, POINTS_PER_CHUNK);

            // Fill chunk
            for i in 0..POINTS_PER_CHUNK {
                let timestamp = (chunk_id * POINTS_PER_CHUNK + i) as i64;
                chunk
                    .append(DataPoint {
                        series_id: 1,
                        timestamp,
                        value: timestamp as f64,
                    })
                    .unwrap();
            }

            // Seal to disk
            let path = format!("/tmp/concurrent_seal_test_{}.kub", chunk_id);
            let result = chunk.seal(path.into()).await;

            result.is_ok()
        });

        handles.push(handle);
    }

    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let elapsed = start.elapsed();
    let successful_seals = results.iter().filter(|&&r| r).count();

    println!("✅ Test complete!");
    println!(
        "   Successful seals: {}/{}",
        successful_seals, NUM_CONCURRENT_SEALS
    );
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!(
        "   Seal rate: {:.0} seals/sec",
        successful_seals as f64 / elapsed.as_secs_f64()
    );

    assert_eq!(successful_seals, NUM_CONCURRENT_SEALS, "Some seals failed");

    // Cleanup
    for i in 0..NUM_CONCURRENT_SEALS {
        let _ = std::fs::remove_file(format!("/tmp/concurrent_seal_test_{}.kub", i));
    }
}

/// Test recovery from disk full scenario
#[tokio::test]
#[ignore]
async fn test_disk_full_recovery() {
    println!("🚀 Starting disk full recovery test...");

    let config = SealConfig {
        max_points: 1000,
        max_duration_ms: 60_000,
        max_size_bytes: 1024 * 1024,
    };

    // Test that seal failures return errors and don't panic
    let chunk1 = Arc::new(ActiveChunk::new(1, 1000, config.clone()));

    // Fill first chunk
    for i in 0..1000 {
        chunk1
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    // Try to seal to invalid path (simulates disk full)
    let invalid_path = "/dev/null/invalid/path/chunk.kub";
    let result = chunk1.seal(invalid_path.into()).await;

    println!(
        "   Seal to invalid path (should fail): {:?}",
        result.is_err()
    );
    assert!(result.is_err(), "Seal should fail to invalid path");

    // Data is preserved but chunk is marked as sealed (cannot retry same chunk)
    assert_eq!(chunk1.point_count(), 1000, "Data should be preserved");

    // In production, you'd create a new chunk and continue writing
    let chunk2 = Arc::new(ActiveChunk::new(1, 1000, config));

    // Fill second chunk with same data
    for i in 0..1000 {
        chunk2
            .append(DataPoint {
                series_id: 1,
                timestamp: i,
                value: i as f64,
            })
            .unwrap();
    }

    // Seal to valid path (recovery by creating new chunk)
    let valid_path = "/tmp/recovery_test_chunk.kub";
    let result = chunk2.seal(valid_path.into()).await;

    println!(
        "   Seal to valid path (should succeed): {:?}",
        result.is_ok()
    );
    assert!(result.is_ok(), "Seal should succeed with valid path");

    println!("✅ Test complete! Recovery strategy: create new chunk after seal failure.");

    // Cleanup
    let _ = std::fs::remove_file(valid_path);
}

/// Test timestamp wraparound behavior
#[tokio::test]
#[ignore]
async fn test_timestamp_wraparound() {
    println!("🚀 Starting timestamp wraparound test...");

    let config = SealConfig::default();
    let mut chunk = Chunk::new_active(1, 1000);

    // Test near i64::MAX
    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 1000,
            value: 1.0,
        })
        .unwrap();

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 500,
            value: 2.0,
        })
        .unwrap();

    chunk
        .append(DataPoint {
            series_id: 1,
            timestamp: i64::MAX - 1,
            value: 3.0,
        })
        .unwrap();

    assert_eq!(chunk.point_count(), 3);

    // Verify should_seal handles extreme ranges
    let should_seal = chunk.should_seal(&config);
    println!("   Should seal with extreme timestamps: {}", should_seal);

    println!("✅ Test complete! Extreme timestamps handled correctly.");
}

/// Test system behavior with maximum chunk size
#[tokio::test]
#[ignore]
async fn test_maximum_chunk_size() {
    const MAX_POINTS: usize = 10_000_000; // 10M limit

    println!("🚀 Starting maximum chunk size test...");
    println!("   Target: {} points (hard limit)", MAX_POINTS);

    let start = Instant::now();
    let config = SealConfig {
        max_points: MAX_POINTS,
        max_duration_ms: i64::MAX,
        max_size_bytes: usize::MAX,
    };

    let chunk = Arc::new(ActiveChunk::new(1, MAX_POINTS, config));
    let mut last_success = 0usize;

    for i in 0..MAX_POINTS + 1000 {
        let result = chunk.append(DataPoint {
            series_id: 1,
            timestamp: i as i64,
            value: i as f64,
        });

        if result.is_ok() {
            last_success = i;
        } else {
            println!("   Hard limit hit at {} points", last_success + 1);
            break;
        }

        if i % 1_000_000 == 0 && i > 0 {
            println!(
                "   ✓ {} M points ({:.1}s)",
                i / 1_000_000,
                start.elapsed().as_secs_f64()
            );
        }
    }

    let elapsed = start.elapsed();
    let final_count = chunk.point_count() as usize;

    println!("✅ Test complete!");
    println!("   Final count: {} points", final_count);
    println!("   Duration: {:.2}s", elapsed.as_secs_f64());
    println!(
        "   Write rate: {:.0} points/sec",
        final_count as f64 / elapsed.as_secs_f64()
    );

    assert!(
        final_count <= MAX_POINTS,
        "Exceeded hard limit: {} > {}",
        final_count,
        MAX_POINTS
    );
}
