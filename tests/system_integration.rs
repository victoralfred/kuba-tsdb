//! End-to-End System Integration Tests
//!
//! This module provides comprehensive integration tests that verify all
//! components of the Gorilla TSDB work together correctly as a complete system.
//!
//! # Test Coverage
//!
//! 1. **Database Lifecycle** - Create, write, query, shutdown
//! 2. **Data Integrity** - Write and read back data accurately
//! 3. **Compression** - Verify compression/decompression roundtrip
//! 4. **Multi-Series** - Handle multiple concurrent series
//! 5. **Time Range Queries** - Query specific time windows
//! 6. **Series Management** - Register, find, and delete series
//! 7. **Pluggable Engines** - Test with different storage/compression backends
//! 8. **Concurrent Operations** - Thread-safe access patterns
//! 9. **Edge Cases** - Empty queries, boundary conditions
//! 10. **Performance Validation** - Basic throughput verification

use gorilla_tsdb::{
    compression::gorilla::GorillaCompressor,
    engine::{DatabaseConfig, InMemoryTimeIndex, TimeSeriesDB, TimeSeriesDBBuilder},
    storage::LocalDiskEngine,
    types::{DataPoint, SeriesId, TagFilter, TimeRange},
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test database with default configuration
async fn create_test_db() -> (TimeSeriesDB, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let storage = LocalDiskEngine::new(temp_dir.path().to_path_buf())
        .expect("Failed to create storage engine");

    let index = InMemoryTimeIndex::new();
    let compressor = GorillaCompressor::new();

    let config = DatabaseConfig {
        data_dir: temp_dir.path().to_path_buf(),
        redis_url: None,
        max_chunk_size: 1024 * 1024,
        retention_days: None,
        custom_options: HashMap::new(),
    };

    let db = TimeSeriesDBBuilder::new()
        .with_config(config)
        .with_compressor(compressor)
        .with_storage(storage)
        .with_index(index)
        .build()
        .await
        .expect("Failed to build database");

    (db, temp_dir)
}

/// Generate test data points for a series
fn generate_test_points(series_id: SeriesId, count: usize, start_ts: i64) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new(
                series_id,
                start_ts + (i as i64 * 1000), // 1 second intervals
                42.0 + (i as f64 * 0.1),      // Gradually increasing values
            )
        })
        .collect()
}

/// Generate sine wave test data (more realistic time-series)
fn generate_sine_wave(series_id: SeriesId, count: usize, start_ts: i64) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            let x = i as f64 * 0.1;
            DataPoint::new(
                series_id,
                start_ts + (i as i64 * 1000),
                50.0 + 30.0 * x.sin(), // Sine wave between 20 and 80
            )
        })
        .collect()
}

// =============================================================================
// Test: Database Lifecycle
// =============================================================================

/// Test basic database creation and configuration
#[tokio::test]
async fn test_database_lifecycle() {
    let (db, _temp_dir) = create_test_db().await;

    // Verify database is created with correct configuration
    let stats = db.stats();
    assert_eq!(stats.total_chunks, 0);
    assert_eq!(stats.total_series, 0);
}

// =============================================================================
// Test: Write and Query Basic
// =============================================================================

/// Test writing and reading back data
#[tokio::test]
async fn test_write_and_query_basic() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 1;

    // Generate test data
    let points = generate_test_points(series_id, 100, 1000);

    // Write data
    let chunk_id = db
        .write(series_id, points.clone())
        .await
        .expect("Write failed");

    // Verify chunk was created
    assert!(!chunk_id.to_string().is_empty());

    // Query all data back
    let time_range = TimeRange::new(1000, 1000 + 100 * 1000).expect("Invalid time range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    // Verify data integrity
    assert_eq!(result.len(), points.len());

    for (original, queried) in points.iter().zip(result.iter()) {
        assert_eq!(original.timestamp, queried.timestamp);
        // Use approximate comparison for floating point
        assert!(
            (original.value - queried.value).abs() < 0.001,
            "Value mismatch: {} vs {}",
            original.value,
            queried.value
        );
    }
}

// =============================================================================
// Test: Time Range Queries
// =============================================================================

/// Test querying specific time ranges
#[tokio::test]
async fn test_time_range_queries() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 2;

    // Generate 1000 points over 1000 seconds
    let points = generate_test_points(series_id, 1000, 0);
    db.write(series_id, points.clone())
        .await
        .expect("Write failed");

    // Query first 100 points
    let range1 = TimeRange::new(0, 99_999).expect("Invalid range");
    let result1 = db.query(series_id, range1).await.expect("Query failed");
    assert_eq!(result1.len(), 100);

    // Query middle 200 points
    let range2 = TimeRange::new(400_000, 599_999).expect("Invalid range");
    let result2 = db.query(series_id, range2).await.expect("Query failed");
    assert_eq!(result2.len(), 200);

    // Query last 100 points
    let range3 = TimeRange::new(900_000, 1_000_000).expect("Invalid range");
    let result3 = db.query(series_id, range3).await.expect("Query failed");
    assert_eq!(result3.len(), 100);

    // Query empty range (future timestamps)
    let range4 = TimeRange::new(2_000_000, 3_000_000).expect("Invalid range");
    let result4 = db.query(series_id, range4).await.expect("Query failed");
    assert_eq!(result4.len(), 0);
}

// =============================================================================
// Test: Multi-Series Operations
// =============================================================================

/// Test handling multiple series concurrently
#[tokio::test]
async fn test_multi_series_operations() {
    let (db, _temp_dir) = create_test_db().await;

    // Write data to multiple series
    let series_count = 10;
    let points_per_series = 100;

    for series_id in 1..=series_count {
        let points = generate_test_points(series_id as SeriesId, points_per_series, 0);
        db.write(series_id as SeriesId, points)
            .await
            .expect("Write failed");
    }

    // Query each series and verify isolation
    for series_id in 1..=series_count {
        let time_range = TimeRange::new(0, 100_000).expect("Invalid range");
        let result = db
            .query(series_id as SeriesId, time_range)
            .await
            .expect("Query failed");

        assert_eq!(result.len(), points_per_series);

        // Verify all points belong to this series
        for point in &result {
            assert_eq!(point.series_id, series_id as SeriesId);
        }
    }
}

// =============================================================================
// Test: Series Registration and Discovery
// =============================================================================

/// Test series registration and tag-based discovery
#[tokio::test]
async fn test_series_registration() {
    let (db, _temp_dir) = create_test_db().await;

    // Register series with tags
    let mut tags1 = HashMap::new();
    tags1.insert("host".to_string(), "server1".to_string());
    tags1.insert("region".to_string(), "us-east".to_string());

    db.register_series(1, "cpu.usage", tags1.clone())
        .await
        .expect("Register failed");

    let mut tags2 = HashMap::new();
    tags2.insert("host".to_string(), "server2".to_string());
    tags2.insert("region".to_string(), "us-west".to_string());

    db.register_series(2, "cpu.usage", tags2.clone())
        .await
        .expect("Register failed");

    // Find all cpu.usage series
    let all_series = db
        .find_series("cpu.usage", &TagFilter::All)
        .await
        .expect("Find failed");
    assert_eq!(all_series.len(), 2);

    // Find by specific tag
    let us_east_series = db
        .find_series("cpu.usage", &TagFilter::Exact(tags1))
        .await
        .expect("Find failed");
    assert_eq!(us_east_series.len(), 1);
    assert_eq!(us_east_series[0], 1);
}

// =============================================================================
// Test: Compression Roundtrip
// =============================================================================

/// Test that compression preserves data accurately
#[tokio::test]
async fn test_compression_roundtrip() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 100;

    // Use sine wave data (better compression characteristics)
    let points = generate_sine_wave(series_id, 500, 0);

    // Write (compresses data)
    db.write(series_id, points.clone())
        .await
        .expect("Write failed");

    // Query (decompresses data)
    let time_range = TimeRange::new(0, 500_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    // Verify exact roundtrip for timestamps
    assert_eq!(result.len(), points.len());
    for (original, queried) in points.iter().zip(result.iter()) {
        assert_eq!(original.timestamp, queried.timestamp);
        // Gorilla compression preserves values exactly for IEEE 754 floats
        assert!(
            (original.value - queried.value).abs() < 0.0001,
            "Value mismatch at ts {}: {} vs {}",
            original.timestamp,
            original.value,
            queried.value
        );
    }
}

// =============================================================================
// Test: Multiple Writes to Same Series
// =============================================================================

/// Test appending multiple chunks to same series
#[tokio::test]
async fn test_multiple_writes_same_series() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 200;

    // Write three batches at different times
    let batch1 = generate_test_points(series_id, 100, 0);
    let batch2 = generate_test_points(series_id, 100, 100_000);
    let batch3 = generate_test_points(series_id, 100, 200_000);

    db.write(series_id, batch1).await.expect("Write 1 failed");
    db.write(series_id, batch2).await.expect("Write 2 failed");
    db.write(series_id, batch3).await.expect("Write 3 failed");

    // Query all data - should combine all chunks
    let time_range = TimeRange::new(0, 300_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    // Should have all 300 points
    assert_eq!(result.len(), 300);

    // Verify ordering
    for i in 1..result.len() {
        assert!(
            result[i].timestamp > result[i - 1].timestamp,
            "Points not sorted"
        );
    }
}

// =============================================================================
// Test: Empty and Edge Cases
// =============================================================================

/// Test edge cases and boundary conditions
#[tokio::test]
async fn test_edge_cases() {
    let (db, _temp_dir) = create_test_db().await;

    // Query non-existent series
    let time_range = TimeRange::new(0, 1000).expect("Invalid range");
    let result = db.query(999, time_range).await.expect("Query failed");
    assert_eq!(result.len(), 0);

    // Single point write
    let single_point = vec![DataPoint::new(300, 1000, 42.5)];
    db.write(300, single_point.clone())
        .await
        .expect("Write failed");

    let result = db
        .query(300, TimeRange::new(0, 2000).unwrap())
        .await
        .expect("Query failed");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timestamp, 1000);

    // Query with exact boundaries
    let result = db
        .query(300, TimeRange::new(1000, 1000).unwrap())
        .await
        .expect("Query failed");
    assert_eq!(result.len(), 1);
}

// =============================================================================
// Test: Concurrent Access
// =============================================================================

/// Test thread-safe concurrent operations
#[tokio::test]
async fn test_concurrent_operations() {
    let (db, _temp_dir) = create_test_db().await;
    let db = Arc::new(db);

    // Spawn multiple concurrent writers
    let mut handles = vec![];

    for series_id in 1..=5 {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            for batch in 0..10 {
                let points = generate_test_points(
                    series_id as SeriesId,
                    50,
                    batch * 50_000, // Non-overlapping time ranges
                );
                db_clone
                    .write(series_id as SeriesId, points)
                    .await
                    .expect("Concurrent write failed");
            }
        });
        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.expect("Task failed");
    }

    // Verify all data was written
    for series_id in 1..=5 {
        let time_range = TimeRange::new(0, 500_000).expect("Invalid range");
        let result = db
            .query(series_id as SeriesId, time_range)
            .await
            .expect("Query failed");
        assert_eq!(
            result.len(),
            500,
            "Series {} has wrong point count",
            series_id
        );
    }
}

// =============================================================================
// Test: Data Statistics
// =============================================================================

/// Test database statistics tracking
#[tokio::test]
async fn test_database_statistics() {
    let (db, _temp_dir) = create_test_db().await;

    // Initial stats should be zero
    let initial_stats = db.stats();
    assert_eq!(initial_stats.total_chunks, 0);

    // Write some data
    for series_id in 1..=3 {
        let points = generate_test_points(series_id as SeriesId, 100, 0);
        db.write(series_id as SeriesId, points)
            .await
            .expect("Write failed");
    }

    // Stats should reflect writes
    let stats = db.stats();
    assert!(stats.total_chunks >= 3, "Expected at least 3 chunks");
    assert!(stats.total_bytes > 0, "Expected non-zero bytes");
}

// =============================================================================
// Test: Large Dataset
// =============================================================================

/// Test handling larger datasets
#[tokio::test]
async fn test_large_dataset() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 500;

    // Write 10,000 points
    let points = generate_test_points(series_id, 10_000, 0);
    db.write(series_id, points.clone())
        .await
        .expect("Large write failed");

    // Query all back
    let time_range = TimeRange::new(0, 10_000_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    assert_eq!(result.len(), 10_000);

    // Verify sampling of points
    assert_eq!(result[0].timestamp, 0);
    assert_eq!(result[9999].timestamp, 9_999_000);
}

// =============================================================================
// Test: Value Extremes
// =============================================================================

/// Test handling extreme values
#[tokio::test]
async fn test_extreme_values() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 600;

    let extreme_points = vec![
        DataPoint::new(series_id, 1000, f64::MAX / 2.0),
        DataPoint::new(series_id, 2000, f64::MIN_POSITIVE),
        DataPoint::new(series_id, 3000, 0.0),
        DataPoint::new(series_id, 4000, -0.0),
        DataPoint::new(series_id, 5000, 1e-300),
        DataPoint::new(series_id, 6000, 1e300),
    ];

    db.write(series_id, extreme_points.clone())
        .await
        .expect("Write extreme values failed");

    let time_range = TimeRange::new(0, 10_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    assert_eq!(result.len(), extreme_points.len());

    // Verify extreme values preserved
    for (original, queried) in extreme_points.iter().zip(result.iter()) {
        if original.value == 0.0 {
            assert!(queried.value == 0.0);
        } else {
            let ratio = (original.value / queried.value).abs();
            assert!(
                (ratio - 1.0).abs() < 0.001,
                "Extreme value not preserved: {} vs {}",
                original.value,
                queried.value
            );
        }
    }
}

// =============================================================================
// Test: Series Deletion
// =============================================================================

/// Test deleting a series
#[tokio::test]
async fn test_series_deletion() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 700;

    // Register and write data
    let tags = HashMap::new();
    db.register_series(series_id, "test.metric", tags)
        .await
        .expect("Register failed");

    let points = generate_test_points(series_id, 100, 0);
    db.write(series_id, points).await.expect("Write failed");

    // Verify data exists
    let time_range = TimeRange::new(0, 100_000).expect("Invalid range");
    let before_delete = db.query(series_id, time_range).await.expect("Query failed");
    assert_eq!(before_delete.len(), 100);

    // Delete series
    db.delete_series(series_id)
        .await
        .expect("Delete series failed");

    // Verify data is gone
    let after_delete = db.query(series_id, time_range).await.expect("Query failed");
    assert_eq!(after_delete.len(), 0);
}

// =============================================================================
// Test: Pluggable Engine Integration
// =============================================================================

/// Test that different engine implementations work correctly
#[tokio::test]
async fn test_pluggable_engines() {
    // Test with LocalDiskEngine + GorillaCompressor + InMemoryIndex
    // This is the default configuration

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Explicitly create each component
    let storage =
        LocalDiskEngine::new(temp_dir.path().to_path_buf()).expect("Failed to create storage");
    let index = InMemoryTimeIndex::new();
    let compressor = GorillaCompressor::new();

    let config = DatabaseConfig {
        data_dir: temp_dir.path().to_path_buf(),
        redis_url: None,
        max_chunk_size: 1024 * 1024,
        retention_days: Some(30),
        custom_options: HashMap::new(),
    };

    let db = TimeSeriesDBBuilder::new()
        .with_config(config)
        .with_compressor(compressor)
        .with_storage(storage)
        .with_index(index)
        .build()
        .await
        .expect("Failed to build with pluggable engines");

    // Verify basic operations work
    let series_id: SeriesId = 1000;
    let points = generate_test_points(series_id, 50, 0);

    db.write(series_id, points.clone())
        .await
        .expect("Write failed");

    let time_range = TimeRange::new(0, 50_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");

    assert_eq!(result.len(), 50);
}

// =============================================================================
// Test: Maintenance Operations
// =============================================================================

/// Test database maintenance operations
#[tokio::test]
async fn test_maintenance_operations() {
    let (db, _temp_dir) = create_test_db().await;

    // Use current timestamp so data is not considered "expired"
    let now = chrono::Utc::now().timestamp_millis();

    // Write some data with recent timestamps
    let points = generate_test_points(1, 100, now);
    db.write(1, points).await.expect("Write failed");

    // Run maintenance (should not fail even with no work to do)
    db.maintenance().await.expect("Maintenance failed");

    // Verify data still accessible after maintenance
    let time_range = TimeRange::new(now, now + 100_000).expect("Invalid range");
    let result = db.query(1, time_range).await.expect("Query failed");
    assert_eq!(result.len(), 100);
}

// =============================================================================
// Test: Validation Errors
// =============================================================================

/// Test that invalid inputs are properly rejected
#[tokio::test]
async fn test_validation_errors() {
    let (db, _temp_dir) = create_test_db().await;

    // Empty points should fail
    let empty_result = db.write(1, vec![]).await;
    assert!(empty_result.is_err());

    // Unsorted points should fail
    let unsorted = vec![
        DataPoint::new(1, 2000, 42.0),
        DataPoint::new(1, 1000, 43.0), // Out of order
    ];
    let unsorted_result = db.write(1, unsorted).await;
    assert!(unsorted_result.is_err());

    // Duplicate timestamps should fail
    let duplicates = vec![
        DataPoint::new(1, 1000, 42.0),
        DataPoint::new(1, 1000, 43.0), // Duplicate timestamp
    ];
    let duplicate_result = db.write(1, duplicates).await;
    assert!(duplicate_result.is_err());
}

// =============================================================================
// Test: Full System Workflow
// =============================================================================

/// Test a complete realistic workflow
#[tokio::test]
async fn test_full_workflow() {
    let (db, _temp_dir) = create_test_db().await;

    // Use current timestamp so data is not considered "expired" during maintenance
    let now = chrono::Utc::now().timestamp_millis();

    // === Step 1: Register metrics ===
    let mut cpu_tags = HashMap::new();
    cpu_tags.insert("host".to_string(), "web-01".to_string());
    cpu_tags.insert("env".to_string(), "production".to_string());

    let mut mem_tags = HashMap::new();
    mem_tags.insert("host".to_string(), "web-01".to_string());
    mem_tags.insert("env".to_string(), "production".to_string());

    db.register_series(1, "system.cpu", cpu_tags.clone())
        .await
        .expect("Register CPU failed");
    db.register_series(2, "system.memory", mem_tags.clone())
        .await
        .expect("Register memory failed");

    // === Step 2: Simulate data collection ===
    // CPU usage data (percentage, varies around 50%)
    let cpu_data: Vec<DataPoint> = (0..1000)
        .map(|i| {
            let usage = 50.0 + 20.0 * ((i as f64 * 0.05).sin());
            DataPoint::new(1, now + (i as i64 * 1000), usage)
        })
        .collect();

    // Memory usage data (bytes, gradually increasing)
    let mem_data: Vec<DataPoint> = (0..1000)
        .map(|i| {
            let usage = 4_000_000_000.0 + (i as f64 * 1_000_000.0);
            DataPoint::new(2, now + (i as i64 * 1000), usage)
        })
        .collect();

    db.write(1, cpu_data.clone())
        .await
        .expect("Write CPU failed");
    db.write(2, mem_data.clone())
        .await
        .expect("Write mem failed");

    // === Step 3: Query recent data ===
    let recent_range = TimeRange::new(now + 900_000, now + 999_999).expect("Invalid range");

    let recent_cpu = db.query(1, recent_range).await.expect("Query failed");
    let recent_mem = db.query(2, recent_range).await.expect("Query failed");

    assert_eq!(recent_cpu.len(), 100);
    assert_eq!(recent_mem.len(), 100);

    // === Step 4: Find series by tags ===
    let prod_series = db
        .find_series("system.cpu", &TagFilter::Exact(cpu_tags))
        .await
        .expect("Find failed");
    assert!(prod_series.contains(&1));

    // === Step 5: Get statistics ===
    let stats = db.stats();
    assert!(stats.total_chunks >= 2);
    assert!(stats.total_bytes > 0);

    // === Step 6: Run maintenance ===
    db.maintenance().await.expect("Maintenance failed");

    // === Step 7: Verify data still intact ===
    let all_range = TimeRange::new(now, now + 1_000_000).expect("Invalid range");
    let all_cpu = db.query(1, all_range).await.expect("Query failed");
    assert_eq!(all_cpu.len(), 1000);

    println!("Full workflow test completed successfully!");
    println!("  - Registered 2 series with tags");
    println!("  - Wrote {} CPU data points", cpu_data.len());
    println!("  - Wrote {} memory data points", mem_data.len());
    println!("  - Queried recent data");
    println!("  - Found series by tags");
    println!("  - Stats: {:?}", stats);
}

// =============================================================================
// Test: Performance Baseline
// =============================================================================

/// Verify basic performance characteristics
#[tokio::test]
async fn test_performance_baseline() {
    let (db, _temp_dir) = create_test_db().await;
    let series_id: SeriesId = 9999;

    // Generate 50,000 points
    let points = generate_test_points(series_id, 50_000, 0);

    // Measure write time
    let write_start = std::time::Instant::now();
    db.write(series_id, points.clone())
        .await
        .expect("Write failed");
    let write_duration = write_start.elapsed();

    // Measure query time
    let query_start = std::time::Instant::now();
    let time_range = TimeRange::new(0, 50_000_000).expect("Invalid range");
    let result = db.query(series_id, time_range).await.expect("Query failed");
    let query_duration = query_start.elapsed();

    // Verify results
    assert_eq!(result.len(), 50_000);

    // Performance assertions (generous limits for CI environments)
    let write_rate = 50_000.0 / write_duration.as_secs_f64();
    let query_rate = 50_000.0 / query_duration.as_secs_f64();

    println!("Performance baseline:");
    println!(
        "  Write: {:.0} points/sec ({:?})",
        write_rate, write_duration
    );
    println!(
        "  Query: {:.0} points/sec ({:?})",
        query_rate, query_duration
    );

    // Minimum acceptable performance (very conservative for CI)
    assert!(
        write_rate > 10_000.0,
        "Write performance too low: {} points/sec",
        write_rate
    );
    assert!(
        query_rate > 50_000.0,
        "Query performance too low: {} points/sec",
        query_rate
    );
}
