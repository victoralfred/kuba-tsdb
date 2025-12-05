//! Edge case tests for chunk coalescing
//!
//! Tests boundary conditions, error handling, and unusual scenarios.

use kuba_tsdb::storage::chunk::{Chunk, ChunkMetadata, CompressionType};
use kuba_tsdb::storage::chunk_coalescing::{ChunkCoalescer, CoalescingCandidate, CoalescingConfig};
use kuba_tsdb::types::{ChunkId, DataPoint};
use std::path::PathBuf;
use std::sync::Arc;
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

/// Create test chunk metadata without actual files
fn create_test_chunk_metadata(
    series_id: u128,
    start_ts: i64,
    end_ts: i64,
    points: u32,
    size: u64,
    age_offset_ms: i64,
) -> ChunkMetadata {
    ChunkMetadata {
        chunk_id: ChunkId::new(),
        series_id,
        path: PathBuf::from(format!("/tmp/chunk_{}_{}.kub", series_id, start_ts)),
        start_timestamp: start_ts,
        end_timestamp: end_ts,
        point_count: points,
        size_bytes: size,
        uncompressed_size: size * 2,
        compression: CompressionType::Ahpac,
        created_at: chrono::Utc::now().timestamp_millis() - age_offset_ms,
        last_accessed: chrono::Utc::now().timestamp_millis(),
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test that a single chunk is not modified
#[test]
fn test_single_chunk_no_coalesce() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Single small chunk
    let chunks = vec![create_test_chunk_metadata(1, 1000, 2000, 50, 500, 600_000)];

    let candidates = coalescer.find_candidates(1, &chunks);
    assert!(
        candidates.is_empty(),
        "Single chunk should not be a candidate"
    );
}

/// Test handling of overlapping time ranges
#[test]
fn test_overlapping_time_ranges() {
    let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

    // Create chunks with overlapping time ranges
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 3000, 100, 500, 600_000),
        create_test_chunk_metadata(1, 2000, 4000, 100, 500, 600_000), // Overlaps!
    ];

    let result = coalescer.validate_time_order(&chunks);
    assert!(result.is_err(), "Overlapping chunks should fail validation");
    assert!(
        result.unwrap_err().contains("overlap"),
        "Error should mention overlap"
    );
}

/// Test handling of chunks with time gaps
#[test]
fn test_gap_in_time_ranges() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        detect_time_gaps: true,
        max_time_gap_ms: 1000, // 1 second max gap
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks with a large gap
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50, 500, 600_000),
        create_test_chunk_metadata(1, 10_000, 11_000, 50, 500, 600_000), // 8 second gap!
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should not merge due to gap
    assert!(
        candidates.is_empty(),
        "Chunks with large gap should not merge"
    );
}

/// Test that max_coalesced_size is respected
#[test]
fn test_max_size_respected() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        max_coalesced_size_points: 100, // Very small limit
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks that together exceed the limit
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 60, 500, 600_000),
        create_test_chunk_metadata(1, 2000, 3000, 60, 500, 600_000),
        create_test_chunk_metadata(1, 3000, 4000, 60, 500, 600_000),
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should create a candidate with only chunks that fit
    if !candidates.is_empty() {
        for candidate in &candidates {
            assert!(
                candidate.total_points() <= 100,
                "Candidate should not exceed max size"
            );
        }
    }
}

/// Test concurrent coalescing prevention
#[tokio::test]
async fn test_concurrent_coalescing() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 100u128;

    let (_, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 0, 30),
    )
    .await;
    let (_, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 30_000, 30),
    )
    .await;

    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        min_improvement_ratio: 1.0,
        ..Default::default()
    };

    let coalescer = Arc::new(ChunkCoalescer::new(config));
    let chunks = vec![meta1.clone(), meta2.clone()];
    let candidates = coalescer.find_candidates(series_id, &chunks);

    assert_eq!(candidates.len(), 1);

    // The coalescer prevents concurrent operations on the same series
    // using internal locking, but we can verify the in_progress check works

    // Initially not in progress
    assert!(!coalescer.is_series_in_progress(series_id));
}

/// Test coalescing during simulated writes
#[tokio::test]
async fn test_coalesce_during_writes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 101u128;

    // Create initial chunks
    let (_, meta1) = create_sealed_chunk(
        &temp_dir,
        series_id,
        0,
        create_test_points(series_id, 0, 30),
    )
    .await;
    let (_, meta2) = create_sealed_chunk(
        &temp_dir,
        series_id,
        1,
        create_test_points(series_id, 30_000, 30),
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

    // Simulate "new write" creating another chunk while coalescing
    let (_, _meta3) = create_sealed_chunk(
        &temp_dir,
        series_id,
        2,
        create_test_points(series_id, 60_000, 30),
    )
    .await;

    // Find candidates with original chunks
    let chunks = vec![meta1, meta2];
    let candidates = coalescer.find_candidates(series_id, &chunks);

    let output_path = temp_dir.path().join("series_101/merged.kub");
    let result = coalescer.coalesce(candidates[0].clone(), output_path).await;

    assert!(result.is_ok(), "Coalescing should succeed");

    // The new chunk (meta3) was not included in original candidates
    // and exists separately - this is the expected behavior
}

/// Test empty candidate handling
#[test]
fn test_empty_candidate() {
    let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

    let candidate = CoalescingCandidate::new(1, vec![]);

    assert!(candidate.is_empty());
    assert_eq!(candidate.total_points(), 0);
    assert_eq!(candidate.time_span(), (0, 0));
    assert_eq!(candidate.chunk_count(), 0);

    // Empty candidate should not coalesce
    assert!(!coalescer.should_coalesce(&candidate));
}

/// Test with chunks that have identical timestamps
#[test]
fn test_duplicate_timestamps_in_merge() {
    let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

    let points1 = vec![
        DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 10.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 20.0,
        },
        DataPoint {
            series_id: 1,
            timestamp: 3000,
            value: 30.0,
        },
    ];
    let points2 = vec![
        DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 25.0,
        }, // Duplicate!
        DataPoint {
            series_id: 1,
            timestamp: 4000,
            value: 40.0,
        },
    ];

    let merged = coalescer.merge_points(vec![points1, points2]);

    // Duplicates should be removed (first occurrence kept)
    assert_eq!(merged.len(), 4);
    assert_eq!(
        merged.iter().find(|p| p.timestamp == 2000).unwrap().value,
        20.0 // First occurrence
    );
}

/// Test with very large point count
#[test]
fn test_very_large_chunks() {
    let config = CoalescingConfig {
        min_chunk_size_points: 100_000, // Very high threshold
        max_coalesced_size_points: 1_000_000,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create "large" chunks (in point count terms)
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50_000, 500_000, 600_000),
        create_test_chunk_metadata(1, 2000, 3000, 50_000, 500_000, 600_000),
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Both chunks are "small" relative to the high threshold
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].total_points(), 100_000);
}

/// Test max_chunks_per_merge limit
#[test]
fn test_max_chunks_per_merge() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        max_chunks_per_merge: 3, // Small limit
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create many small chunks
    let chunks: Vec<ChunkMetadata> = (0..10)
        .map(|i| create_test_chunk_metadata(1, i * 1000, (i + 1) * 1000, 50, 500, 600_000))
        .collect();

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should create multiple candidates of at most 3 chunks each
    for candidate in &candidates {
        assert!(
            candidate.chunk_count() <= 3,
            "Candidate should have at most 3 chunks"
        );
    }
}

/// Test time span limit
#[test]
fn test_max_time_span() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        max_coalesced_time_span_ms: 10_000, // 10 seconds
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        detect_time_gaps: false,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks spanning more than 10 seconds
    let chunks = vec![
        create_test_chunk_metadata(1, 0, 5_000, 50, 500, 600_000),
        create_test_chunk_metadata(1, 5_000, 8_000, 50, 500, 600_000),
        create_test_chunk_metadata(1, 8_000, 15_000, 50, 500, 600_000), // Exceeds span
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should split due to time span limit
    for candidate in &candidates {
        let span = candidate.duration_ms();
        assert!(
            span <= 10_000,
            "Candidate time span {} should not exceed 10000ms",
            span
        );
    }
}

/// Test age requirement
#[test]
fn test_min_age_for_coalesce() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        min_age_for_coalesce_ms: 300_000, // 5 minutes
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks of different ages
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50, 500, 600_000), // 10 min old - OK
        create_test_chunk_metadata(1, 2000, 3000, 50, 500, 60_000),  // 1 min old - too young
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Only the old chunk qualifies, but need 2+ to merge
    assert!(
        candidates.is_empty(),
        "Should not have candidates with only 1 old chunk"
    );
}

/// Test inverted time range in chunk
#[test]
fn test_inverted_time_range() {
    let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

    let chunks = vec![ChunkMetadata {
        chunk_id: ChunkId::new(),
        series_id: 1,
        path: PathBuf::from("/tmp/chunk.kub"),
        start_timestamp: 5000,
        end_timestamp: 3000, // Inverted!
        point_count: 100,
        size_bytes: 500,
        uncompressed_size: 1000,
        compression: CompressionType::Ahpac,
        created_at: 0,
        last_accessed: 0,
    }];

    let result = coalescer.validate_time_order(&chunks);
    assert!(
        result.is_err(),
        "Inverted time range should fail validation"
    );
    assert!(result.unwrap_err().contains("inverted"));
}

/// Test with zero-size chunks
#[test]
fn test_zero_size_chunks() {
    let config = CoalescingConfig {
        min_chunk_size_points: 100,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks with zero size
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50, 0, 600_000), // Zero size
        create_test_chunk_metadata(1, 2000, 3000, 50, 0, 600_000), // Zero size
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should still find candidates
    assert!(!candidates.is_empty());

    // Estimate benefit should handle zero size gracefully
    let benefit = coalescer.estimate_benefit(&candidates[0]);
    assert!(benefit >= 1.0, "Benefit should be at least 1.0");
}

/// Test different series IDs
#[test]
fn test_mixed_series_ids() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks from different series
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50, 500, 600_000),
        create_test_chunk_metadata(2, 2000, 3000, 50, 500, 600_000), // Different series!
        create_test_chunk_metadata(1, 3000, 4000, 50, 500, 600_000),
    ];

    // When finding candidates for series 1, should only get series 1 chunks
    let candidates = coalescer.find_candidates(1, &chunks);

    for candidate in &candidates {
        assert_eq!(candidate.series_id, 1);
        for chunk in &candidate.chunks {
            assert_eq!(chunk.series_id, 1);
        }
    }
}

/// Test stats recording
#[test]
fn test_stats_recording() {
    let config = CoalescingConfig {
        min_improvement_ratio: 2.0, // Very high - will cause skips
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create candidate with low estimated improvement
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 900, 5000, 600_000),
        create_test_chunk_metadata(1, 2000, 3000, 900, 5000, 600_000),
    ];
    let candidate = CoalescingCandidate::new(1, chunks);

    // This should be skipped due to low improvement
    let should_coalesce = coalescer.should_coalesce(&candidate);
    assert!(!should_coalesce);

    let stats = coalescer.stats();
    assert_eq!(stats.candidates_skipped, 1);
}

/// Test with extremely long time spans
#[test]
fn test_extreme_time_spans() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        max_coalesced_time_span_ms: i64::MAX, // No practical limit
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        detect_time_gaps: false,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks with extreme time range
    let chunks = vec![
        create_test_chunk_metadata(1, 0, 1000, 50, 500, 600_000),
        create_test_chunk_metadata(1, 1_000_000_000_000, 1_000_000_001_000, 50, 500, 600_000),
    ];

    let candidates = coalescer.find_candidates(1, &chunks);

    // Should still work (no time span limit)
    assert!(!candidates.is_empty());
}

/// Test cleanup returns correct paths
#[tokio::test]
async fn test_cleanup_paths_correct() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let series_id = 200u128;

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

    let output_path = temp_dir.path().join("series_200/merged.kub");
    let result = coalescer
        .coalesce(candidates[0].clone(), output_path.clone())
        .await
        .expect("Coalescing should succeed");

    let cleanup_paths = coalescer.cleanup_old_chunks(&result);

    // Verify exact paths match
    assert!(cleanup_paths.contains(&path1));
    assert!(cleanup_paths.contains(&path2));
    assert!(!cleanup_paths.contains(&output_path));
}

/// Test multiple series finding candidates independently
#[test]
fn test_multiple_series_independent() {
    let config = CoalescingConfig {
        min_chunk_size_points: 1000,
        min_age_for_coalesce_ms: 0,
        min_chunks_to_merge: 2,
        ..Default::default()
    };
    let coalescer = ChunkCoalescer::new(config);

    // Create chunks for multiple series
    let chunks = vec![
        create_test_chunk_metadata(1, 1000, 2000, 50, 500, 600_000),
        create_test_chunk_metadata(1, 2000, 3000, 50, 500, 600_000),
        create_test_chunk_metadata(2, 1000, 2000, 50, 500, 600_000),
        create_test_chunk_metadata(2, 2000, 3000, 50, 500, 600_000),
        create_test_chunk_metadata(2, 3000, 4000, 50, 500, 600_000),
    ];

    // Find for series 1
    let candidates_1 = coalescer.find_candidates(1, &chunks);
    assert_eq!(candidates_1.len(), 1);
    assert_eq!(candidates_1[0].chunk_count(), 2);

    // Find for series 2
    let candidates_2 = coalescer.find_candidates(2, &chunks);
    assert_eq!(candidates_2.len(), 1);
    assert_eq!(candidates_2[0].chunk_count(), 3);
}

/// Test in_progress_count tracking
#[test]
fn test_in_progress_count() {
    let coalescer = ChunkCoalescer::new(CoalescingConfig::default());

    assert_eq!(coalescer.in_progress_count(), 0);
    assert!(!coalescer.is_series_in_progress(1));
    assert!(!coalescer.is_series_in_progress(2));
}

/// Test config validation edge cases
#[test]
fn test_config_validation_edge_cases() {
    // Negative age
    let config = CoalescingConfig {
        min_age_for_coalesce_ms: -1000,
        ..Default::default()
    };
    assert!(config.validate().is_err());

    // Zero time gap when gap detection enabled
    let config = CoalescingConfig {
        detect_time_gaps: true,
        max_time_gap_ms: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());

    // Improvement ratio exactly 1.0 is valid
    let config = CoalescingConfig {
        min_improvement_ratio: 1.0,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}
