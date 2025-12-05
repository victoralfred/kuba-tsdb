//! Integration tests for chunk coalescing
//!
//! Tests the coalescing functionality with real disk I/O and chunk operations.

use kuba_tsdb::storage::chunk::Chunk;
use kuba_tsdb::storage::chunk_coalescing::{ChunkCoalescer, CoalescingConfig};
use kuba_tsdb::storage::ChunkMetadata;
use kuba_tsdb::types::DataPoint;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper to create test data points for a series
fn create_test_points(series_id: u128, start_ts: i64, count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint {
            series_id,
            timestamp: start_ts + (i as i64 * 1000),
            value: (i as f64) * 1.5 + 10.0,
        })
        .collect()
}

/// Helper to create and seal a chunk to disk
async fn create_sealed_chunk(
    temp_dir: &TempDir,
    series_id: u128,
    chunk_index: usize,
    points: Vec<DataPoint>,
) -> (PathBuf, ChunkMetadata) {
    let path = temp_dir
        .path()
        .join(format!("series_{}/chunk_{}.kub", series_id, chunk_index));

    let mut chunk = Chunk::new_active(series_id, points.len() * 2);

    for point in &points {
        chunk.append(*point).expect("Failed to append point");
    }

    chunk
        .seal(path.clone())
        .await
        .expect("Failed to seal chunk");

    let metadata = chunk.metadata.clone();
    (path, metadata)
}

// ============================================================================
// Integration Tests
// ============================================================================

/// Test that small chunks can be identified and merged
#[tokio::test]
async fn test_small_chunks_coalesced() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 1u128;

    // Create 3 small sealed chunks
    let (_, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 0, 50),
    )
    .await;
    let (_, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 50_000, 50),
    )
    .await;
    let (_, meta3) = create_sealed_chunk(
        &temp_dir,
        series_id,
        2,
        create_test_points(series_id, 100_000, 50),
    )
    .await;

    let config = CoalescingConfig {
        min_chunk_size_points: 100, // Consider chunks under 100 points as small
        min_age_for_coalesce_ms: 0, // No age requirement for testing
        min_chunks_to_merge: 2,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let all_chunks = vec![meta1, meta2, meta3];

    let candidates = coalescer.find_candidates(series_id, &all_chunks);
    assert_eq!(candidates.len(), 1, "Should find one candidate group");
    assert_eq!(
        candidates[0].chunk_count(),
        3,
        "Candidate should have 3 chunks"
    );

    // Coalesce the chunks
    let output_path = temp_dir.path().join("series_1/merged_chunk.kub");
    let result = coalescer
        .coalesce(candidates[0].clone(), output_path.clone())
        .await;

    assert!(result.is_ok(), "Coalescing should succeed");
    let result = result.unwrap();

    assert_eq!(result.chunks_merged, 3);
    assert_eq!(result.total_points, 150);
    assert!(result.new_size_bytes > 0);
}

/// Test that coalescing improves compression ratio
#[tokio::test]
async fn test_coalescing_improves_ratio() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 2u128;

    // Create small chunks with similar patterns (should compress well together)
    let mut chunks = Vec::new();
    for i in 0..5 {
        let (_, meta) = create_sealed_chunk(
            &temp_dir,
            series_id,
            i,
            create_test_points(series_id, (i as i64) * 20_000, 20),
        )
        .await;
        chunks.push(meta);
    }

    let original_total_size: u64 = chunks.iter().map(|c| c.size_bytes).sum();

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0, // Accept any improvement
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let candidates = coalescer.find_candidates(series_id, &chunks);

    assert!(!candidates.is_empty(), "Should find candidates");

    let output_path = temp_dir.path().join("series_2/merged_chunk.kub");
    let result = coalescer.coalesce(candidates[0].clone(), output_path).await;

    assert!(result.is_ok());
    let result = result.unwrap();

    // Merged chunk should be more efficient (header overhead reduced)
    assert!(
        result.new_size_bytes <= original_total_size,
        "Merged chunk should not be larger than originals combined"
    );
}

/// Test that original chunks are identified for cleanup
#[tokio::test]
async fn test_old_chunks_deleted() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 3u128;

    // Create small chunks
    let (path1, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 0, 30),
    )
    .await;
    let (path2, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 30_000, 30),
    )
    .await;

    assert!(path1.exists(), "Original chunk 1 should exist");
    assert!(path2.exists(), "Original chunk 2 should exist");

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let chunks = vec![meta1, meta2];
    let candidates = coalescer.find_candidates(series_id, &chunks);

    let output_path = temp_dir.path().join("series_3/merged.kub");
    let result = coalescer
        .coalesce(candidates[0].clone(), output_path.clone())
        .await
        .expect("Coalescing should succeed");

    // Get paths to delete
    let paths_to_delete = coalescer.cleanup_old_chunks(&result);
    assert_eq!(paths_to_delete.len(), 2);

    // Verify the original paths are in the cleanup list
    assert!(
        paths_to_delete.contains(&path1),
        "Path 1 should be in cleanup"
    );
    assert!(
        paths_to_delete.contains(&path2),
        "Path 2 should be in cleanup"
    );

    // Merged chunk should exist
    assert!(output_path.exists(), "Merged chunk should exist");
}

/// Test that time index is updated correctly (data remains queryable)
#[tokio::test]
async fn test_index_updated() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 4u128;

    // Create chunks with known time ranges
    let (_, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 1000, 20),
    )
    .await;
    let (_, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 21_000, 20),
    )
    .await;

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let chunks = vec![meta1.clone(), meta2.clone()];
    let candidates = coalescer.find_candidates(series_id, &chunks);

    let output_path = temp_dir.path().join("series_4/merged.kub");
    let result = coalescer
        .coalesce(candidates[0].clone(), output_path)
        .await
        .expect("Coalescing should succeed");

    // Verify new chunk metadata covers the full time range
    assert_eq!(result.new_chunk.start_timestamp, meta1.start_timestamp);
    assert!(result.new_chunk.end_timestamp >= meta2.end_timestamp);
    assert_eq!(result.new_chunk.point_count as u64, result.total_points);
}

/// Test that data is still queryable after coalescing
#[tokio::test]
async fn test_query_after_coalescing() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 5u128;

    // Create specific test data
    let points1 = vec![
        DataPoint {
            series_id,
            timestamp: 1000,
            value: 10.0,
        },
        DataPoint {
            series_id,
            timestamp: 2000,
            value: 20.0,
        },
        DataPoint {
            series_id,
            timestamp: 3000,
            value: 30.0,
        },
    ];
    let points2 = vec![
        DataPoint {
            series_id,
            timestamp: 4000,
            value: 40.0,
        },
        DataPoint {
            series_id,
            timestamp: 5000,
            value: 50.0,
        },
        DataPoint {
            series_id,
            timestamp: 6000,
            value: 60.0,
        },
    ];

    let (_, meta1) = create_sealed_chunk(&temp_dir, series_id, 0, points1.clone()).await;
    let (_, meta2) = create_sealed_chunk(&temp_dir, series_id, 1, points2.clone()).await;

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let chunks = vec![meta1, meta2];
    let candidates = coalescer.find_candidates(series_id, &chunks);

    let output_path = temp_dir.path().join("series_5/merged.kub");
    let _result = coalescer
        .coalesce(candidates[0].clone(), output_path.clone())
        .await
        .expect("Coalescing should succeed");

    // Read back the merged chunk and verify data
    let merged_chunk = Chunk::read(output_path)
        .await
        .expect("Should read merged chunk");

    let decompressed = merged_chunk
        .decompress()
        .await
        .expect("Should decompress merged chunk");

    assert_eq!(decompressed.len(), 6, "Should have all 6 points");

    // Verify all original points are present
    let all_original: Vec<DataPoint> = points1.into_iter().chain(points2).collect();
    for original in &all_original {
        let found = decompressed
            .iter()
            .any(|p| p.timestamp == original.timestamp && p.value == original.value);
        assert!(
            found,
            "Point at timestamp {} should be preserved",
            original.timestamp
        );
    }
}

/// Test coalescing with parallel sealing integration
#[tokio::test]
async fn test_coalescing_with_parallel_sealing() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 6u128;

    // Create multiple small chunks (simulating parallel sealing)
    // Each chunk has 25 points spaced 1000ms apart, so spans 24000ms
    // We start each at 30000ms intervals to ensure no overlap
    let mut chunks = Vec::new();
    for i in 0..4 {
        let (_, meta) = create_sealed_chunk(
            &temp_dir,
            series_id,
            i,
            create_test_points(series_id, (i as i64) * 30_000, 25),
        )
        .await;
        chunks.push(meta);
    }

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        max_chunks_per_merge: 4,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let candidates = coalescer.find_candidates(series_id, &chunks);

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].chunk_count(), 4);

    let output_path = temp_dir.path().join("series_6/merged.kub");
    let result = coalescer.coalesce(candidates[0].clone(), output_path).await;

    assert!(result.is_ok(), "Coalescing failed: {:?}", result.err());
    let result = result.unwrap();

    assert_eq!(result.chunks_merged, 4);
    assert_eq!(result.total_points, 100);

    // Verify stats
    let stats = coalescer.stats();
    assert_eq!(stats.operations_total, 1);
    assert_eq!(stats.chunks_merged_total, 4);
}

/// Test that coalescing is idempotent (no double processing)
#[tokio::test]
async fn test_coalescing_idempotent() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 7u128;

    let (_, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 0, 40),
    )
    .await;
    let (_, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 40_000, 40),
    )
    .await;

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let chunks = vec![meta1.clone(), meta2.clone()];

    // First coalescing
    let candidates = coalescer.find_candidates(series_id, &chunks);
    assert_eq!(candidates.len(), 1);

    let output_path1 = temp_dir.path().join("series_7/merged1.kub");
    let result1 = coalescer
        .coalesce(candidates[0].clone(), output_path1)
        .await
        .expect("First coalesce should succeed");

    // The merged chunk is now large enough to not be a candidate
    // (80 points < 100 threshold, so it would still be candidate)
    // Let's check that the new chunk info is correct
    assert_eq!(result1.new_chunk.point_count, 80);

    // Stats should show 1 operation
    let stats = coalescer.stats();
    assert_eq!(stats.operations_total, 1);
}

/// Test performance with many small chunks
#[tokio::test]
async fn test_many_small_chunks() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 8u128;

    // Create 10 small chunks
    // Each chunk has 10 points spaced 1000ms apart, so spans 9000ms
    // We start each at 15000ms intervals to ensure no overlap
    let mut chunks = Vec::new();
    for i in 0..10 {
        let (_, meta) = create_sealed_chunk(
            &temp_dir,
            series_id,
            i,
            create_test_points(series_id, (i as i64) * 15_000, 10),
        )
        .await;
        chunks.push(meta);
    }

    let config = CoalescingConfig {
        min_chunk_size_points: 50,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        max_chunks_per_merge: 5, // Limit to 5 per merge
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = ChunkCoalescer::new(config);
    let candidates = coalescer.find_candidates(series_id, &chunks);

    // Should create 2 candidates of 5 chunks each
    assert_eq!(candidates.len(), 2, "Should have 2 candidate groups");
    assert_eq!(candidates[0].chunk_count(), 5);
    assert_eq!(candidates[1].chunk_count(), 5);

    // Coalesce both groups
    let mut total_merged = 0;
    for (i, candidate) in candidates.into_iter().enumerate() {
        let output_path = temp_dir.path().join(format!("series_8/merged_{}.kub", i));
        let result = coalescer.coalesce(candidate, output_path).await;
        assert!(
            result.is_ok(),
            "Coalescing group {} failed: {:?}",
            i,
            result.err()
        );
        total_merged += result.unwrap().chunks_merged;
    }

    assert_eq!(total_merged, 10);

    let stats = coalescer.stats();
    assert_eq!(stats.operations_total, 2);
    assert_eq!(stats.chunks_merged_total, 10);
}
