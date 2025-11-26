//! Comprehensive storage lifecycle tests for Step 9
//!
//! These tests cover the complete chunk lifecycle from creation through
//! compression, including:
//! - Chunk lifecycle (create, write, seal, compress)
//! - Actual file I/O operations
//! - Memory-mapped file operations
//! - Chunk serialization/deserialization
//! - Concurrent reads and writes
//! - Crash recovery simulation

use gorilla_tsdb::engine::traits::StorageEngine;
use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use gorilla_tsdb::storage::chunk::{
    Chunk, ChunkHeader, CompressionType, CHUNK_MAGIC, CHUNK_VERSION,
};
use gorilla_tsdb::storage::local_disk::LocalDiskEngine;
use gorilla_tsdb::storage::mmap::MmapChunk;
use gorilla_tsdb::storage::reader::{ChunkReader, QueryOptions};
use gorilla_tsdb::storage::writer::{ChunkWriter, ChunkWriterConfig};
use gorilla_tsdb::types::DataPoint;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::fs;

// =============================================================================
// CHUNK LIFECYCLE TESTS
// =============================================================================

/// Test complete lifecycle: create -> write -> seal -> read -> decompress
#[tokio::test]
async fn test_chunk_full_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("lifecycle_test.gor");

    // Step 1: Create active chunk
    let mut chunk = Chunk::new_active(1, 10000);
    assert!(chunk.is_active(), "Chunk should start in active state");

    // Step 2: Write points
    let points_to_write = 1000;
    for i in 0..points_to_write {
        let point = DataPoint {
            series_id: 1,
            timestamp: (i * 1000) as i64,
            value: (i as f64) * 1.5,
        };
        chunk.append(point).expect("Append should succeed");
    }

    assert_eq!(
        chunk.point_count(),
        points_to_write as u32,
        "Point count should match"
    );

    // Step 3: Seal chunk (writes to disk)
    chunk
        .seal(chunk_path.clone())
        .await
        .expect("Seal should succeed");
    assert!(chunk.is_sealed(), "Chunk should be sealed after seal()");
    assert!(chunk_path.exists(), "Chunk file should exist on disk");

    // Step 4: Read chunk from disk
    let loaded_chunk = Chunk::read(chunk_path.clone())
        .await
        .expect("Reading chunk should succeed");

    assert!(loaded_chunk.is_sealed(), "Loaded chunk should be sealed");
    assert_eq!(loaded_chunk.series_id(), 1, "Series ID should match");

    // Step 5: Decompress and verify data
    let decompressed_points = loaded_chunk
        .decompress()
        .await
        .expect("Decompression should succeed");

    assert_eq!(
        decompressed_points.len(),
        points_to_write,
        "Decompressed point count should match"
    );

    // Verify each point
    for (i, point) in decompressed_points.iter().enumerate() {
        assert_eq!(
            point.timestamp,
            (i * 1000) as i64,
            "Timestamp mismatch at {}",
            i
        );
        assert!(
            (point.value - (i as f64) * 1.5).abs() < 1e-10,
            "Value mismatch at {}",
            i
        );
    }
}

/// Test lifecycle with different compression types
#[tokio::test]
async fn test_chunk_lifecycle_with_various_data_patterns() {
    let temp_dir = TempDir::new().unwrap();

    // Test pattern 1: Constant values (high compression ratio)
    let chunk_path = temp_dir.path().join("constant_values.gor");
    let mut chunk = Chunk::new_active(1, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: 42.0, // Constant value
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();
    let loaded = Chunk::read(chunk_path.clone()).await.unwrap();
    let points = loaded.decompress().await.unwrap();
    assert!(points.iter().all(|p| p.value == 42.0));

    // Test pattern 2: Sequential increasing values
    let chunk_path = temp_dir.path().join("sequential_values.gor");
    let mut chunk = Chunk::new_active(2, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 2,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();
    let loaded = Chunk::read(chunk_path.clone()).await.unwrap();
    let points = loaded.decompress().await.unwrap();

    for (i, point) in points.iter().enumerate() {
        assert_eq!(point.value, i as f64);
    }

    // Test pattern 3: Alternating values
    let chunk_path = temp_dir.path().join("alternating_values.gor");
    let mut chunk = Chunk::new_active(3, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 3,
                timestamp: i as i64,
                value: if i % 2 == 0 { 0.0 } else { 100.0 },
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();
    let loaded = Chunk::read(chunk_path.clone()).await.unwrap();
    let points = loaded.decompress().await.unwrap();

    for (i, point) in points.iter().enumerate() {
        let expected = if i % 2 == 0 { 0.0 } else { 100.0 };
        assert_eq!(point.value, expected);
    }
}

/// Test lifecycle with edge case values
#[tokio::test]
async fn test_chunk_lifecycle_edge_values() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("edge_values.gor");

    let mut chunk = Chunk::new_active(1, 100);

    // Edge values
    let edge_values = vec![
        0.0,
        -0.0,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::MIN,
        f64::EPSILON,
        1e-300,
        1e300,
    ];

    for (i, &value) in edge_values.iter().enumerate() {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();
    let loaded = Chunk::read(chunk_path.clone()).await.unwrap();
    let points = loaded.decompress().await.unwrap();

    assert_eq!(points.len(), edge_values.len());

    for (i, (point, &expected)) in points.iter().zip(edge_values.iter()).enumerate() {
        // Handle -0.0 vs 0.0 comparison
        if expected == 0.0 {
            assert!(point.value == 0.0, "Value at {} should be zero", i);
        } else {
            assert_eq!(point.value, expected, "Value mismatch at {}", i);
        }
    }
}

// =============================================================================
// FILE I/O TESTS
// =============================================================================

/// Test direct file I/O operations with header serialization
#[tokio::test]
async fn test_file_io_header_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let header_path = temp_dir.path().join("header_test.bin");

    // Create and serialize header
    let mut header = ChunkHeader::new(12345);
    header.start_timestamp = 1000;
    header.end_timestamp = 5000;
    header.point_count = 100;
    header.compressed_size = 512;
    header.uncompressed_size = 2048;
    header.checksum = 0xDEADBEEF12345678;
    header.compression_type = CompressionType::Gorilla;

    let header_bytes = header.to_bytes();
    assert_eq!(header_bytes.len(), 64, "Header should be exactly 64 bytes");

    // Write to file
    fs::write(&header_path, &header_bytes).await.unwrap();

    // Read back from file
    let read_bytes = fs::read(&header_path).await.unwrap();
    let parsed_header = ChunkHeader::from_bytes(&read_bytes).unwrap();

    // Verify all fields
    assert_eq!(parsed_header.magic, CHUNK_MAGIC);
    assert_eq!(parsed_header.version, CHUNK_VERSION);
    assert_eq!(parsed_header.series_id, 12345);
    assert_eq!(parsed_header.start_timestamp, 1000);
    assert_eq!(parsed_header.end_timestamp, 5000);
    assert_eq!(parsed_header.point_count, 100);
    assert_eq!(parsed_header.compressed_size, 512);
    assert_eq!(parsed_header.uncompressed_size, 2048);
    assert_eq!(parsed_header.checksum, 0xDEADBEEF12345678);
}

/// Test file I/O with corrupted data detection
#[tokio::test]
async fn test_file_io_corruption_detection() {
    let temp_dir = TempDir::new().unwrap();

    // Create a valid chunk first
    let chunk_path = temp_dir.path().join("corrupt_test.gor");
    let mut chunk = Chunk::new_active(1, 100);

    for i in 0..50 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Read the file and corrupt some bytes
    let mut file_contents = fs::read(&chunk_path).await.unwrap();

    // Corrupt the checksum (bytes 50-57)
    file_contents[50] = 0xFF;
    file_contents[51] = 0xFF;

    // Write corrupted file
    let corrupted_path = temp_dir.path().join("corrupted.gor");
    fs::write(&corrupted_path, &file_contents).await.unwrap();

    // Attempting to read should detect corruption via checksum
    // (The MmapChunk validates checksum on open)
    let result = MmapChunk::open(&corrupted_path);

    // Checksum validation should fail
    assert!(
        result.is_err(),
        "Should detect corruption via checksum mismatch"
    );

    // Verify we got an error (MmapError doesn't implement Debug so we can't use unwrap_err)
    if let Err(e) = result {
        let err_msg = e.to_string().to_lowercase();
        assert!(
            err_msg.contains("checksum"),
            "Error should mention checksum: {}",
            err_msg
        );
    }
}

/// Test file I/O with truncated files
#[tokio::test]
async fn test_file_io_truncated_file_detection() {
    let temp_dir = TempDir::new().unwrap();

    // Create header-only file (too small)
    let truncated_path = temp_dir.path().join("truncated.gor");

    let mut header = ChunkHeader::new(1);
    header.point_count = 100; // Claims to have points
    header.compressed_size = 500; // Claims compressed data size

    // Write only header, no data
    fs::write(&truncated_path, header.to_bytes()).await.unwrap();

    // Attempting to open should fail
    let result = MmapChunk::open(&truncated_path);
    assert!(result.is_err(), "Should detect truncated file");
}

/// Test LocalDiskEngine basic functionality
#[tokio::test]
async fn test_local_disk_engine_basic() {
    let temp_dir = TempDir::new().unwrap();
    let engine = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();

    // Engine should have correct ID
    assert_eq!(engine.engine_id(), "local-disk-v1");

    // Initially no chunks
    let chunks = engine.list_chunks(1, None).await.unwrap();
    assert!(chunks.is_empty());

    // Create a chunk using the Chunk API and verify it can be read
    let series_id = 1u128;
    let series_path = temp_dir.path().join(format!("series_{}", series_id));
    std::fs::create_dir_all(&series_path).unwrap();

    let chunk_path = series_path.join("chunk_test.gor");

    let mut chunk = Chunk::new_active(series_id, 1000);
    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id,
                timestamp: i as i64,
                value: (i as f64).sin(),
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Verify file exists
    assert!(chunk_path.exists());

    // Load index and verify chunk is found
    engine.load_index().await.unwrap();
    let chunks = engine.list_chunks(series_id, None).await.unwrap();
    assert_eq!(chunks.len(), 1);

    // Verify chunk metadata
    let metadata = &chunks[0];
    assert_eq!(metadata.point_count, 500);
}

// =============================================================================
// MEMORY-MAPPED FILE TESTS
// =============================================================================

/// Test memory-mapped file zero-copy reads
#[tokio::test]
async fn test_mmap_zero_copy_reads() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("mmap_test.gor");

    // Create and seal a chunk
    let mut chunk = Chunk::new_active(1, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Open as memory-mapped file
    let mmap_chunk = MmapChunk::open(&chunk_path).unwrap();

    // Zero-copy access to header
    let header = mmap_chunk.header();
    assert_eq!(header.series_id, 1);
    assert_eq!(header.point_count, 500);

    // Zero-copy access to compressed data
    let compressed_data = mmap_chunk.compressed_data();
    assert!(!compressed_data.is_empty());
    assert_eq!(compressed_data.len(), header.compressed_size as usize);

    // Decompress and verify
    let points = mmap_chunk.decompress().await.unwrap();
    assert_eq!(points.len(), 500);
}

/// Test memory-mapped file with large chunks
#[tokio::test]
async fn test_mmap_large_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("large_mmap_test.gor");

    // Create a larger chunk
    let mut chunk = Chunk::new_active(1, 100_000);

    for i in 0..50_000 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: (i as f64).sin(),
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Open and verify via mmap
    let mmap_chunk = MmapChunk::open(&chunk_path).unwrap();

    assert_eq!(mmap_chunk.header().point_count, 50_000);

    // Time the decompression
    let start = Instant::now();
    let points = mmap_chunk.decompress().await.unwrap();
    let elapsed = start.elapsed();

    println!(
        "Large chunk mmap decompress: {} points in {:?}",
        points.len(),
        elapsed
    );

    assert_eq!(points.len(), 50_000);
}

// =============================================================================
// CONCURRENT READ/WRITE TESTS
// =============================================================================

/// Test concurrent writes to active chunk
#[tokio::test]
async fn test_concurrent_writes_active_chunk() {
    let config = SealConfig {
        max_points: 100_000,
        max_duration_ms: 3600_000,
        max_size_bytes: 100 * 1024 * 1024,
    };

    let chunk = Arc::new(ActiveChunk::new(1, 100_000, config));
    let success_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Spawn 10 concurrent writers
    for thread_id in 0..10u64 {
        let chunk = Arc::clone(&chunk);
        let success = Arc::clone(&success_count);

        let handle = tokio::spawn(async move {
            for i in 0..1000u64 {
                // Use unique timestamps per thread to avoid duplicates
                let timestamp = (thread_id * 10_000 + i) as i64;
                let point = DataPoint {
                    series_id: 1,
                    timestamp,
                    value: (thread_id as f64) * 1000.0 + (i as f64),
                };

                if chunk.append(point).is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.unwrap();
    }

    let total_success = success_count.load(Ordering::Relaxed);
    let point_count = chunk.point_count() as u64;

    assert_eq!(
        total_success, point_count,
        "Success count should match point count"
    );
    assert_eq!(
        point_count, 10_000,
        "Should have exactly 10,000 unique points"
    );
}

/// Test concurrent reads from sealed chunk
#[tokio::test]
async fn test_concurrent_reads_sealed_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("concurrent_read_test.gor");

    // Create and seal a chunk
    let mut chunk = Chunk::new_active(1, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Spawn multiple concurrent readers
    let mut handles = vec![];

    for _ in 0..10 {
        let path = chunk_path.clone();

        let handle = tokio::spawn(async move {
            // Each reader opens and reads independently
            let loaded = Chunk::read(path).await.unwrap();
            let points = loaded.decompress().await.unwrap();
            points.len()
        });

        handles.push(handle);
    }

    // All readers should succeed
    for handle in handles {
        let count = handle.await.unwrap();
        assert_eq!(count, 500, "Each reader should see all points");
    }
}

/// Test concurrent mmap access
#[tokio::test]
async fn test_concurrent_mmap_access() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("concurrent_mmap_test.gor");

    // Create and seal
    let mut chunk = Chunk::new_active(1, 1000);

    for i in 0..500 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Open once and share across threads
    let mmap = Arc::new(MmapChunk::open(&chunk_path).unwrap());
    let mut handles = vec![];

    for _ in 0..10 {
        let mmap = Arc::clone(&mmap);

        let handle = tokio::spawn(async move {
            // Multiple concurrent decompressions from same mmap
            let points = mmap.decompress().await.unwrap();
            points.len()
        });

        handles.push(handle);
    }

    for handle in handles {
        let count = handle.await.unwrap();
        assert_eq!(count, 500);
    }
}

// =============================================================================
// CRASH RECOVERY SIMULATION TESTS
// =============================================================================

/// Test recovery from incomplete write (simulated crash during seal)
#[tokio::test]
async fn test_recovery_incomplete_write() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("incomplete.gor");

    // Simulate an incomplete write by writing only partial data
    let mut header = ChunkHeader::new(1);
    header.point_count = 100;
    header.compressed_size = 500;

    // Write only header (no data following)
    fs::write(&chunk_path, header.to_bytes()).await.unwrap();

    // Attempting to open should detect incomplete file
    let result = MmapChunk::open(&chunk_path);
    assert!(
        result.is_err(),
        "Should detect incomplete file (header only, no data)"
    );

    // The corrupted file should be detectable
    let file_size = fs::metadata(&chunk_path).await.unwrap().len();
    assert_eq!(file_size, 64, "File should only contain header");
}

/// Test recovery from corrupted magic number
#[tokio::test]
async fn test_recovery_corrupted_magic() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("bad_magic.gor");

    // Create valid chunk then corrupt magic
    let mut chunk = Chunk::new_active(1, 100);

    for i in 0..50 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    // Corrupt magic number
    let mut data = fs::read(&chunk_path).await.unwrap();
    data[0] = 0x00;
    data[1] = 0x00;
    data[2] = 0x00;
    data[3] = 0x00;
    fs::write(&chunk_path, &data).await.unwrap();

    // Should reject with invalid magic
    let result = MmapChunk::open(&chunk_path);
    assert!(result.is_err());

    if let Err(e) = result {
        let err_msg = e.to_string().to_lowercase();
        assert!(
            err_msg.contains("magic") || err_msg.contains("invalid"),
            "Error should mention magic number: {}",
            err_msg
        );
    }
}

/// Test recovery with parallel file operations
#[tokio::test]
async fn test_recovery_parallel_file_operations() {
    let temp_dir = TempDir::new().unwrap();

    // Create multiple chunks simultaneously
    let mut handles = vec![];

    for chunk_id in 0..10 {
        let dir = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let chunk_path = dir.join(format!("parallel_{}.gor", chunk_id));

            let mut chunk = Chunk::new_active(chunk_id as u128, 100);

            for i in 0..50 {
                chunk
                    .append(DataPoint {
                        series_id: chunk_id as u128,
                        timestamp: i as i64,
                        value: i as f64,
                    })
                    .unwrap();
            }

            chunk.seal(chunk_path.clone()).await.unwrap();

            // Verify the file is valid
            let loaded = Chunk::read(chunk_path).await.unwrap();
            let points = loaded.decompress().await.unwrap();

            (chunk_id, points.len())
        });

        handles.push(handle);
    }

    // All should succeed
    for handle in handles {
        let (chunk_id, count) = handle.await.unwrap();
        assert_eq!(count, 50, "Chunk {} should have all points", chunk_id);
    }
}

// =============================================================================
// CHUNK READER TESTS
// =============================================================================

/// Test ChunkReader with various query options
#[tokio::test]
async fn test_chunk_reader_query_options() {
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("reader_test.gor");

    // Create chunk with known data
    let mut chunk = Chunk::new_active(1, 1000);

    for i in 0..100 {
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: i * 100, // 0, 100, 200, ..., 9900
                value: i as f64,
            })
            .unwrap();
    }

    chunk.seal(chunk_path.clone()).await.unwrap();

    let reader = ChunkReader::new();

    // Test 1: Full read (no filter)
    let options = QueryOptions::default();
    let points = reader.read_chunk(&chunk_path, options).await.unwrap();
    assert_eq!(points.len(), 100);

    // Test 2: Time range filter
    let options = QueryOptions {
        start_time: Some(2000),
        end_time: Some(5000),
        limit: None,
        use_mmap: true,
    };
    let points = reader.read_chunk(&chunk_path, options).await.unwrap();

    // Should include timestamps 2000, 2100, ..., 5000
    assert!(points
        .iter()
        .all(|p| p.timestamp >= 2000 && p.timestamp <= 5000));
    assert_eq!(points.len(), 31); // 2000 to 5000 inclusive with step 100

    // Test 3: Limit filter
    let options = QueryOptions {
        start_time: None,
        end_time: None,
        limit: Some(10),
        use_mmap: true,
    };
    let points = reader.read_chunk(&chunk_path, options).await.unwrap();
    assert_eq!(points.len(), 10);

    // Test 4: Combined filters
    let options = QueryOptions {
        start_time: Some(1000),
        end_time: Some(9000),
        limit: Some(5),
        use_mmap: true,
    };
    let points = reader.read_chunk(&chunk_path, options).await.unwrap();
    assert_eq!(points.len(), 5);
    assert!(points.iter().all(|p| p.timestamp >= 1000));
}

/// Test ChunkReader parallel reads
#[tokio::test]
async fn test_chunk_reader_parallel() {
    let temp_dir = TempDir::new().unwrap();

    // Create multiple chunks
    let mut paths = vec![];

    for chunk_id in 0..5 {
        let chunk_path = temp_dir
            .path()
            .join(format!("parallel_read_{}.gor", chunk_id));

        let mut chunk = Chunk::new_active(1, 100);

        for i in 0..20 {
            let timestamp = (chunk_id * 1000 + i * 10) as i64;
            chunk
                .append(DataPoint {
                    series_id: 1,
                    timestamp,
                    value: timestamp as f64,
                })
                .unwrap();
        }

        chunk.seal(chunk_path.clone()).await.unwrap();
        paths.push(chunk_path);
    }

    let reader = ChunkReader::new();
    let options = QueryOptions::default();

    // Parallel read all chunks
    let all_points = reader.read_chunks_parallel(paths, options).await.unwrap();

    // Should have 5 chunks * 20 points = 100 points total
    assert_eq!(all_points.len(), 100);

    // Results should be sorted by timestamp
    for window in all_points.windows(2) {
        assert!(
            window[0].timestamp <= window[1].timestamp,
            "Results should be sorted"
        );
    }
}

// =============================================================================
// CHUNK WRITER TESTS
// =============================================================================

/// Test ChunkWriter auto-seal functionality
#[tokio::test]
async fn test_chunk_writer_auto_seal() {
    let temp_dir = TempDir::new().unwrap();

    let config = ChunkWriterConfig {
        max_points: 50, // Low threshold to trigger auto-seal
        max_duration: std::time::Duration::from_secs(3600),
        max_size_bytes: 10 * 1024 * 1024,
        auto_seal: true,
        initial_capacity: 100,
        write_buffer_size: 100,
    };

    let writer = ChunkWriter::new(1, temp_dir.path().to_path_buf(), config);

    // Write more than max_points to trigger auto-seal
    for i in 0..100 {
        writer
            .write(DataPoint {
                series_id: 1,
                timestamp: i as i64,
                value: i as f64,
            })
            .await
            .unwrap();
    }

    // Check stats
    let stats = writer.stats().await;
    assert!(
        stats.chunks_sealed >= 1,
        "Should have sealed at least one chunk"
    );
}

/// Test ChunkWriter batch operations
#[tokio::test]
async fn test_chunk_writer_batch() {
    let temp_dir = TempDir::new().unwrap();

    let config = ChunkWriterConfig {
        max_points: 10_000,
        max_duration: std::time::Duration::from_secs(3600),
        max_size_bytes: 10 * 1024 * 1024,
        auto_seal: true,
        initial_capacity: 1000,
        write_buffer_size: 1000,
    };

    let writer = ChunkWriter::new(1, temp_dir.path().to_path_buf(), config);

    // Create batch
    let batch: Vec<DataPoint> = (0..500)
        .map(|i| DataPoint {
            series_id: 1,
            timestamp: i as i64,
            value: i as f64,
        })
        .collect();

    // Write batch
    let start = Instant::now();
    writer.write_batch(batch).await.unwrap();
    let batch_time = start.elapsed();

    let stats = writer.stats().await;
    assert_eq!(stats.points_written, 500);

    println!("Batch write of 500 points took {:?}", batch_time);
}

// =============================================================================
// SERIALIZATION / DESERIALIZATION TESTS
// =============================================================================

/// Test ChunkHeader boundary values
#[test]
fn test_chunk_header_boundary_values() {
    // Test with maximum values
    let mut header = ChunkHeader::new(u128::MAX - 1); // u128::MAX may be reserved
    header.start_timestamp = i64::MIN + 1;
    header.end_timestamp = i64::MAX - 1;
    header.point_count = u32::MAX;
    header.compressed_size = u32::MAX;
    header.uncompressed_size = u32::MAX;
    header.checksum = u64::MAX;

    let bytes = header.to_bytes();
    let parsed = ChunkHeader::from_bytes(&bytes).unwrap();

    assert_eq!(parsed.series_id, u128::MAX - 1);
    assert_eq!(parsed.start_timestamp, i64::MIN + 1);
    assert_eq!(parsed.end_timestamp, i64::MAX - 1);
    assert_eq!(parsed.point_count, u32::MAX);
    assert_eq!(parsed.checksum, u64::MAX);
}

/// Test ChunkHeader with all compression types
#[test]
fn test_chunk_header_compression_types() {
    for compression in [
        CompressionType::None,
        CompressionType::Gorilla,
        CompressionType::Snappy,
        CompressionType::GorillaSnappy,
    ] {
        let mut header = ChunkHeader::new(1);
        header.point_count = 1; // Must be non-zero for validation
        header.compression_type = compression;

        let bytes = header.to_bytes();
        let parsed = ChunkHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.compression_type, compression);
    }
}

/// Test header validation edge cases
#[test]
fn test_chunk_header_validation_edge_cases() {
    // Invalid magic
    let mut header = ChunkHeader::new(1);
    header.magic = 0x12345678;
    header.point_count = 1;
    assert!(header.validate().is_err());

    // Future version
    let mut header = ChunkHeader::new(1);
    header.version = 100;
    header.point_count = 1;
    assert!(header.validate().is_err());

    // Zero points
    let header = ChunkHeader::new(1);
    assert!(header.validate().is_err());

    // Inverted time range
    let mut header = ChunkHeader::new(1);
    header.start_timestamp = 1000;
    header.end_timestamp = 500;
    header.point_count = 1;
    assert!(header.validate().is_err());

    // Valid header
    let mut header = ChunkHeader::new(1);
    header.start_timestamp = 0;
    header.end_timestamp = 1000;
    header.point_count = 10;
    assert!(header.validate().is_ok());
}
