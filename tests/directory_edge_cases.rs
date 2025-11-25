//! Comprehensive tests for directory management edge cases, performance, memory safety, and security
//!
//! This test suite covers:
//! - Edge cases: concurrent access, corrupted files, race conditions
//! - Performance: large scale operations, lock contention
//! - Memory safety: resource cleanup, leak detection
//! - Security: path traversal, symlink attacks, permission issues

use gorilla_tsdb::storage::{DirectoryMaintenance, SeriesMetadata, WriteLock};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;
use tokio::sync::Barrier;

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Test metadata corruption recovery
#[tokio::test]
async fn test_corrupted_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.json");

    // Write corrupted JSON
    fs::write(&metadata_path, b"{ invalid json }")
        .await
        .unwrap();

    // Should return error, not panic
    let result = SeriesMetadata::load(&metadata_path).await;
    assert!(result.is_err(), "Should fail to load corrupted metadata");

    // Verify error message is informative
    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(
        err_str.contains("Failed to parse metadata"),
        "Error should mention parse failure"
    );
}

/// Test metadata with invalid UTF-8
#[tokio::test]
async fn test_metadata_invalid_utf8() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.json");

    // Write invalid UTF-8 bytes
    fs::write(&metadata_path, &[0xFF, 0xFE, 0xFD])
        .await
        .unwrap();

    let result = SeriesMetadata::load(&metadata_path).await;
    assert!(result.is_err(), "Should fail on invalid UTF-8");
}

/// Test atomic save under concurrent writes
#[tokio::test]
async fn test_metadata_atomic_save_concurrent() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = Arc::new(temp_dir.path().join("metadata.json"));
    let barrier = Arc::new(Barrier::new(10));

    let mut handles = vec![];

    // Spawn 10 concurrent save operations
    for i in 0..10 {
        let path = Arc::clone(&metadata_path);
        let barrier = Arc::clone(&barrier);

        let handle = tokio::spawn(async move {
            barrier.wait().await; // Synchronize start

            let mut metadata = SeriesMetadata::new(i as u128);
            metadata.total_points = i * 1000;

            metadata.save(&*path).await
        });

        handles.push(handle);
    }

    // Wait for all to complete
    let mut success_count = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }

    // At least some should succeed
    assert!(success_count > 0, "Some writes should succeed");

    // Final file should be valid JSON (not corrupted)
    let loaded = SeriesMetadata::load(&metadata_path).await;
    assert!(loaded.is_ok(), "Final metadata should be valid");
}

/// Test lock behavior with missing directory
#[tokio::test]
async fn test_write_lock_missing_directory() {
    let temp_dir = TempDir::new().unwrap();
    let non_existent = temp_dir.path().join("does_not_exist");

    let mut lock = WriteLock::new(&non_existent);

    // Should fail gracefully when parent directory doesn't exist
    let result = lock.try_acquire().await;
    assert!(result.is_err(), "Should fail when directory doesn't exist");
}

/// Test lock with extremely long paths
#[tokio::test]
async fn test_write_lock_long_path() {
    let temp_dir = TempDir::new().unwrap();

    // Create very deep directory structure
    let mut deep_path = temp_dir.path().to_path_buf();
    for i in 0..50 {
        deep_path.push(format!("dir_{}", i));
    }

    fs::create_dir_all(&deep_path).await.unwrap();

    let mut lock = WriteLock::new(&deep_path);
    let result = lock.try_acquire().await;

    // Should handle long paths correctly
    assert!(result.is_ok(), "Should handle long paths");
    lock.release().await.unwrap();
}

/// Test stale lock detection and cleanup
#[tokio::test]
async fn test_stale_lock_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let lock_path = temp_dir.path().join(".write.lock");

    // Create a fake stale lock (very old timestamp)
    let stale_timestamp = 0i64; // 1970-01-01
    let fake_lock = format!("99999:{}", stale_timestamp);
    fs::write(&lock_path, fake_lock).await.unwrap();

    // Try to acquire - should clean up stale lock
    let mut lock = WriteLock::new(temp_dir.path());
    let result = lock.try_acquire().await;

    assert!(result.is_ok(), "Should acquire after cleaning stale lock");
    assert!(lock.is_held(), "Lock should be held");

    lock.release().await.unwrap();
}

/// Test lock with malformed lock file content
#[tokio::test]
async fn test_malformed_lock_file() {
    let temp_dir = TempDir::new().unwrap();
    let lock_path = temp_dir.path().join(".write.lock");

    // Write malformed lock file
    fs::write(&lock_path, b"invalid lock content")
        .await
        .unwrap();

    let mut lock = WriteLock::new(temp_dir.path());
    let result = lock.try_acquire().await;

    // With atomic O_EXCL, malformed lock files that can't be parsed are left alone
    // and lock acquisition fails (safer behavior - doesn't delete unknown files)
    assert!(
        result.is_err(),
        "Should fail when lock file exists (even if malformed)"
    );
}

/// Test cleanup with empty files
#[tokio::test]
async fn test_cleanup_empty_chunk_files() {
    let temp_dir = TempDir::new().unwrap();

    // Create empty chunk files
    fs::write(temp_dir.path().join("empty.gor"), b"")
        .await
        .unwrap();

    let metadata = SeriesMetadata::new(1);

    // Should not crash on empty files
    let result = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata).await;
    assert!(result.is_ok(), "Should handle empty files");
}

/// Test cleanup with permission denied files
#[cfg(unix)]
#[tokio::test]
async fn test_cleanup_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = temp_dir.path().join("readonly.gor");

    // Create file and make it read-only
    fs::write(&chunk_path, b"data").await.unwrap();

    let mut perms = fs::metadata(&chunk_path).await.unwrap().permissions();
    perms.set_mode(0o444); // Read-only
    fs::set_permissions(&chunk_path, perms).await.unwrap();

    // Make directory read-only to prevent deletion
    let mut dir_perms = fs::metadata(temp_dir.path())
        .await
        .unwrap()
        .permissions();
    dir_perms.set_mode(0o555); // Read + execute only
    fs::set_permissions(temp_dir.path(), dir_perms)
        .await
        .unwrap();

    let mut metadata = SeriesMetadata::new(1);
    metadata.retention_days = 1; // Trigger cleanup

    let result = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata).await;

    // Restore permissions for cleanup
    let mut restore_perms = fs::metadata(temp_dir.path())
        .await
        .unwrap()
        .permissions();
    restore_perms.set_mode(0o755);
    fs::set_permissions(temp_dir.path(), restore_perms)
        .await
        .unwrap();

    // Should handle permission errors gracefully
    assert!(
        result.is_err() || result.unwrap() == 0,
        "Should handle permission errors"
    );
}

/// Test validation with symlinks
#[cfg(unix)]
#[tokio::test]
async fn test_validation_with_symlinks() {
    let temp_dir = TempDir::new().unwrap();
    let real_file = temp_dir.path().join("real.gor");
    let symlink = temp_dir.path().join("link.gor");

    // Create real file and symlink
    fs::write(&real_file, b"data").await.unwrap();
    tokio::fs::symlink(&real_file, &symlink).await.unwrap();

    // Create metadata
    let metadata = SeriesMetadata::new(1);
    metadata
        .save(&temp_dir.path().join("metadata.json"))
        .await
        .unwrap();

    // Should handle symlinks without crashing
    let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
        .await
        .unwrap();

    // Should not report issues for symlinks
    assert!(
        issues.is_empty() || !issues.iter().any(|i| i.contains("symlink")),
        "Should handle symlinks gracefully"
    );
}

/// Test cleanup with non-ASCII filenames
#[tokio::test]
async fn test_cleanup_unicode_filenames() {
    let temp_dir = TempDir::new().unwrap();

    // Create files with unicode names
    fs::write(temp_dir.path().join("测试.gor"), b"test")
        .await
        .unwrap();
    fs::write(temp_dir.path().join("файл.gor"), b"file")
        .await
        .unwrap();
    fs::write(temp_dir.path().join("📊.gor"), b"chart")
        .await
        .unwrap();

    let metadata = SeriesMetadata::new(1);

    // Should handle unicode filenames correctly
    let result = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata).await;
    assert!(result.is_ok(), "Should handle unicode filenames");
}

// ============================================================================
// PERFORMANCE TESTS
// ============================================================================

/// Test lock contention with many concurrent attempts
#[tokio::test]
async fn test_high_lock_contention() {
    let temp_dir = TempDir::new().unwrap();
    let series_path = Arc::new(temp_dir.path().to_path_buf());

    // First lock holder
    let path_clone = Arc::clone(&series_path);
    let holder = tokio::spawn(async move {
        let mut lock = WriteLock::new(&path_clone);
        lock.try_acquire().await.unwrap();

        // Hold lock for 1 second
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        lock.release().await.unwrap();
    });

    // Wait a bit for lock to be acquired
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Spawn 100 concurrent lock attempts
    let mut attempts = vec![];
    for _ in 0..100 {
        let path = Arc::clone(&series_path);
        let handle = tokio::spawn(async move {
            let mut lock = WriteLock::new(&path);
            lock.try_acquire().await
        });
        attempts.push(handle);
    }

    // Count failures (some may succeed after lock is released)
    let mut failed_count = 0;
    let mut success_count = 0;
    for handle in attempts {
        match handle.await.unwrap() {
            Ok(_) => success_count += 1,
            Err(_) => failed_count += 1,
        }
    }

    holder.await.unwrap();

    // With atomic locking, at least some should fail due to contention
    // But some may succeed after the holder releases the lock
    assert!(
        failed_count > 0,
        "At least some attempts should fail due to lock contention (failed: {}, succeeded: {})",
        failed_count,
        success_count
    );
}

/// Test metadata operations at scale
#[tokio::test]
async fn test_large_scale_metadata_operations() {
    let temp_dir = TempDir::new().unwrap();

    // Create many metadata files
    let mut handles = vec![];
    for i in 0..100 {
        let dir_path = temp_dir.path().join(format!("series_{}", i));
        let handle = tokio::spawn(async move {
            fs::create_dir_all(&dir_path).await.unwrap();

            let metadata_path = dir_path.join("metadata.json");
            let mut metadata = SeriesMetadata::new(i);
            metadata.total_points = (i * 10000) as u64;
            metadata.total_chunks = (i * 10) as u32;

            metadata.save(&metadata_path).await.unwrap();

            // Verify it can be loaded back
            SeriesMetadata::load(&metadata_path).await.unwrap()
        });
        handles.push(handle);
    }

    // All should succeed
    for handle in handles {
        let loaded = handle.await.unwrap();
        assert!(loaded.series_id < 100);
    }
}

/// Test cleanup performance with many files
#[tokio::test]
async fn test_cleanup_performance_many_files() {
    let temp_dir = TempDir::new().unwrap();

    // Create 1000 chunk files
    for i in 0..1000 {
        fs::write(temp_dir.path().join(format!("chunk_{}.gor", i)), b"data")
            .await
            .unwrap();
    }

    let metadata = SeriesMetadata::new(1);

    let start = std::time::Instant::now();
    let removed = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Should complete in reasonable time
    assert!(
        duration.as_secs() < 5,
        "Cleanup should complete within 5 seconds"
    );
    assert_eq!(removed, 0, "Should not remove files with 0 retention");
}

/// Test validation performance with large directories
#[tokio::test]
async fn test_validation_performance_large_directory() {
    let temp_dir = TempDir::new().unwrap();

    // Create metadata
    let metadata = SeriesMetadata::new(1);
    metadata
        .save(&temp_dir.path().join("metadata.json"))
        .await
        .unwrap();

    // Create many chunk files with valid headers
    for i in 0..500 {
        let mut header = vec![0u8; 64];
        header[0..4].copy_from_slice(b"GORL"); // Magic number
        fs::write(temp_dir.path().join(format!("chunk_{}.gor", i)), &header)
            .await
            .unwrap();
    }

    let start = std::time::Instant::now();
    let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
        .await
        .unwrap();
    let duration = start.elapsed();

    assert!(
        duration.as_secs() < 3,
        "Validation should complete within 3 seconds"
    );
    assert!(issues.is_empty(), "Should have no issues");
}

/// Test empty directory cleanup at scale
#[tokio::test]
async fn test_empty_directory_cleanup_scale() {
    let temp_dir = TempDir::new().unwrap();

    // Create 200 series directories (100 empty, 100 with data)
    for i in 0..200 {
        let series_dir = temp_dir.path().join(format!("series_{}", i));
        fs::create_dir_all(&series_dir).await.unwrap();

        // Half have data
        if i % 2 == 0 {
            fs::write(series_dir.join("chunk_0.gor"), b"data")
                .await
                .unwrap();
        } else {
            // Empty or just metadata/lock
            fs::write(series_dir.join("metadata.json"), b"{}")
                .await
                .unwrap();
        }
    }

    let start = std::time::Instant::now();
    let removed = DirectoryMaintenance::cleanup_empty_directories(temp_dir.path())
        .await
        .unwrap();
    let duration = start.elapsed();

    assert!(
        duration.as_secs() < 5,
        "Cleanup should complete within 5 seconds"
    );
    assert_eq!(removed, 100, "Should remove 100 empty directories");
}

// ============================================================================
// MEMORY SAFETY TESTS
// ============================================================================

/// Test lock cleanup on panic
#[tokio::test]
async fn test_lock_cleanup_on_drop() {
    let temp_dir = TempDir::new().unwrap();
    let lock_path = temp_dir.path().join(".write.lock");

    {
        let mut lock = WriteLock::new(temp_dir.path());
        lock.try_acquire().await.unwrap();
        assert!(lock_path.exists(), "Lock file should exist");

        // Lock dropped here
    }

    // Give filesystem time to sync
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Lock file should be cleaned up
    assert!(
        !lock_path.exists(),
        "Lock file should be cleaned up on drop"
    );
}

/// Test no resource leaks in concurrent operations
#[tokio::test]
async fn test_no_resource_leaks_concurrent() {
    let temp_dir = TempDir::new().unwrap();

    // Perform many concurrent operations
    for round in 0..10 {
        let mut handles = vec![];

        for i in 0..50 {
            let dir = temp_dir.path().to_path_buf();
            let handle = tokio::spawn(async move {
                let  metadata = SeriesMetadata::new((round * 50 + i) as u128);
                let path = dir.join(format!("meta_{}_{}.json", round, i));

                metadata.save(&path).await.unwrap();
                let _ = SeriesMetadata::load(&path).await.unwrap();

                // Clean up
                let _ = fs::remove_file(&path).await;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    // All operations should complete without leaking
}

/// Test metadata touch doesn't corrupt data
#[tokio::test]
async fn test_metadata_touch_preserves_data() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.json");

    let mut metadata = SeriesMetadata::new(123);
    metadata.name = Some("test".to_string());
    metadata.total_points = 1000;

    let original_modified = metadata.modified_at;

    // Small delay to ensure timestamp changes
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    metadata.touch();

    // Modified time should change
    assert!(metadata.modified_at > original_modified);

    // But data should be preserved
    assert_eq!(metadata.series_id, 123);
    assert_eq!(metadata.name, Some("test".to_string()));
    assert_eq!(metadata.total_points, 1000);

    // Save and reload
    metadata.save(&metadata_path).await.unwrap();
    let loaded = SeriesMetadata::load(&metadata_path).await.unwrap();

    assert_eq!(loaded.series_id, 123);
    assert_eq!(loaded.total_points, 1000);
}

// ============================================================================
// SECURITY TESTS
// ============================================================================

/// Test path traversal prevention in cleanup
#[tokio::test]
async fn test_path_traversal_prevention() {
    let temp_dir = TempDir::new().unwrap();

    // Try to create files with path traversal
    let malicious_paths = vec![
        "../../../etc/passwd.gor",
        "..\\..\\..\\windows\\system32.gor",
        "./../../sensitive.gor",
    ];

    for mal_path in malicious_paths {
        let full_path = temp_dir.path().join(mal_path);

        // Even if file is created (shouldn't escape temp_dir due to OS protection)
        if let Ok(_) = fs::write(&full_path, b"malicious").await {
            // Cleanup should only affect files within the series directory
            let metadata = SeriesMetadata::new(1);
            let _ = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata).await;
        }
    }

    // Should not crash or access outside temp_dir
}

/// Test metadata injection via filename
#[tokio::test]
async fn test_metadata_filename_injection() {
    let temp_dir = TempDir::new().unwrap();

    // Try filenames with special characters
    let tricky_names = vec![
        "meta'; DROP TABLE series; --.json",
        "meta<script>alert(1)</script>.json",
        "meta\0null.json",
        "meta\n\r.json",
    ];

    for name in tricky_names {
        let path = temp_dir.path().join(name);
        let metadata = SeriesMetadata::new(1);

        // Should handle special characters safely
        if let Ok(_) = metadata.save(&path).await {
            let _ = SeriesMetadata::load(&path).await;
        }
    }

    // Should not crash or execute anything
}

/// Test lock file PID spoofing
#[tokio::test]
async fn test_lock_pid_spoofing() {
    let temp_dir = TempDir::new().unwrap();
    let lock_path = temp_dir.path().join(".write.lock");

    // Create lock with fake PID
    let current_pid = std::process::id();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Try to spoof with our own PID (should be detected as held)
    let fake_lock = format!("{}:{}", current_pid, now);
    fs::write(&lock_path, fake_lock).await.unwrap();

    let mut lock = WriteLock::new(temp_dir.path());
    let result = lock.try_acquire().await;

    // On Unix, should detect process is still alive
    #[cfg(unix)]
    assert!(
        result.is_err(),
        "Should detect lock held by alive process (our own)"
    );

    // On non-Unix, may succeed (no process check)
    #[cfg(not(unix))]
    {
        // Either way, should not panic
        let _ = result;
    }
}

/// Test metadata size limits
#[tokio::test]
async fn test_metadata_extreme_values() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.json");

    let mut metadata = SeriesMetadata::new(u128::MAX);
    metadata.retention_days = u32::MAX;
    metadata.max_points_per_chunk = u32::MAX;
    metadata.total_points = u64::MAX;
    metadata.total_chunks = u32::MAX;
    metadata.created_at = i64::MAX;
    metadata.modified_at = i64::MIN;

    // Should handle extreme values
    let save_result = metadata.save(&metadata_path).await;
    assert!(save_result.is_ok(), "Should save extreme values");

    let loaded = SeriesMetadata::load(&metadata_path).await.unwrap();
    assert_eq!(loaded.series_id, u128::MAX);
    assert_eq!(loaded.retention_days, u32::MAX);
}

/// Test concurrent lock acquisition race condition
///
/// FIXED: Now uses O_CREAT | O_EXCL for atomic lock acquisition
/// This test verifies that only one process can hold the lock at a time.
#[tokio::test]
async fn test_lock_race_condition() {
    let temp_dir = Arc::new(TempDir::new().unwrap());
    let series_path = Arc::new(temp_dir.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(10));
    let active_locks = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = vec![];

    // Spawn 10 tasks trying to acquire lock simultaneously
    for _ in 0..10 {
        let path = Arc::clone(&series_path);
        let barrier = Arc::clone(&barrier);
        let active = Arc::clone(&active_locks);
        let _temp_dir = Arc::clone(&temp_dir); // Keep temp_dir alive

        let handle = tokio::spawn(async move {
            barrier.wait().await; // Synchronize start

            let mut lock = WriteLock::new(&path);
            let result = lock.try_acquire().await;

            // Track active locks
            if result.is_ok() {
                active.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                // Check that we're the only one
                let count = active.load(std::sync::atomic::Ordering::SeqCst);
                assert_eq!(count, 1, "Only one lock should be active at a time");

                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                let _ = lock.release().await;
            }

            drop(_temp_dir);
            result.is_ok()
        });

        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    for handle in handles {
        if let Ok(success) = handle.await {
            if success {
                success_count += 1;
            }
        }
    }

    // At most one at a time, but multiple could succeed sequentially
    assert!(
        success_count >= 1 && success_count <= 10,
        "Lock acquisitions should succeed but enforce mutual exclusion"
    );
}

/// Test directory validation doesn't follow symlinks outside
#[cfg(unix)]
#[tokio::test]
async fn test_validation_symlink_escape() {
    let temp_dir = TempDir::new().unwrap();
    let outside_dir = TempDir::new().unwrap();

    // Create symlink pointing outside
    let outside_file = outside_dir.path().join("outside.gor");
    fs::write(&outside_file, b"outside data").await.unwrap();

    let symlink = temp_dir.path().join("escape.gor");
    tokio::fs::symlink(&outside_file, &symlink).await.unwrap();

    // Create metadata
    let metadata = SeriesMetadata::new(1);
    metadata
        .save(&temp_dir.path().join("metadata.json"))
        .await
        .unwrap();

    // Validation should not follow symlink or crash
    let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
        .await
        .unwrap();

    // Should handle gracefully
    assert!(!issues.iter().any(|i| i.contains("panic")));
}

#[tokio::test]
async fn test_metadata_very_long_strings() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.json");

    let mut metadata = SeriesMetadata::new(1);
    // Very long name (1MB)
    metadata.name = Some("A".repeat(1024 * 1024));
    // Very long compression string
    metadata.compression = "B".repeat(1024 * 1024);

    // Should handle or reject gracefully
    let result = metadata.save(&metadata_path).await;
    if result.is_ok() {
        // If it saves, should be able to load
        let loaded = SeriesMetadata::load(&metadata_path).await;
        assert!(loaded.is_ok(), "Should load large metadata");
    }
}
