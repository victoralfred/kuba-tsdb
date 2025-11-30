//! Integration tests for enabled dead code features
//!
//! This module tests the previously dead code that has been wired into
//! the system as part of the dead code implementation effort.
//!
//! ## Features Tested:
//! - Feature 4: Chunk lifecycle metrics (age, utilization, capacity)
//! - Feature 5: Subscription cleanup with age
//! - Feature 3: Metrics histogram reset

use gorilla_tsdb::query::subscription::{SubscriptionConfig, SubscriptionManager};
use gorilla_tsdb::redis::metrics::{MetricsConfig, RedisMetrics};
use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
use std::time::Duration;

// ============================================================================
// Feature 4: Chunk Lifecycle Metrics Tests
// ============================================================================

#[test]
fn test_active_chunk_age() {
    // Create a chunk and verify age tracking works
    let config = SealConfig {
        max_points: 1000,
        max_duration_ms: 3600000,
        max_size_bytes: 1048576,
    };

    let chunk = ActiveChunk::new(42, 500, config);

    // Age should be very small immediately after creation
    let age = chunk.age();
    assert!(
        age < Duration::from_secs(1),
        "Chunk age should be < 1 second, got {:?}",
        age
    );

    // Sleep a bit and check age increases
    std::thread::sleep(Duration::from_millis(50));
    let age_after = chunk.age();
    assert!(
        age_after >= Duration::from_millis(50),
        "Chunk age should be >= 50ms, got {:?}",
        age_after
    );
}

#[test]
fn test_active_chunk_utilization() {
    let config = SealConfig {
        max_points: 1000,
        max_duration_ms: 3600000,
        max_size_bytes: 1048576,
    };

    // Create chunk with capacity of 100
    let chunk = ActiveChunk::new(42, 100, config);

    // Initially empty, utilization should be 0
    assert_eq!(
        chunk.utilization(),
        0.0,
        "Empty chunk should have 0 utilization"
    );

    // Verify capacity accessor
    assert_eq!(chunk.capacity(), 100, "Capacity should be 100");
}

#[test]
fn test_active_chunk_zero_capacity() {
    let config = SealConfig::default();

    // Edge case: zero capacity should return 0 utilization (not panic)
    let chunk = ActiveChunk::new(42, 0, config);
    assert_eq!(
        chunk.utilization(),
        0.0,
        "Zero capacity should return 0 utilization"
    );
}

// ============================================================================
// Feature 5: Subscription Cleanup Tests
// ============================================================================

#[test]
fn test_subscription_cleanup_stale_basic() {
    let config = SubscriptionConfig::default();
    let manager = SubscriptionManager::new(config);

    // Subscribe to a series
    let _rx = manager.subscribe(1);
    assert_eq!(
        manager.active_series_count(),
        1,
        "Should have 1 active series"
    );

    // Drop the receiver, making the subscription stale
    drop(_rx);

    // Cleanup should remove it
    manager.cleanup_stale();
    assert_eq!(
        manager.active_series_count(),
        0,
        "Stale subscription should be cleaned up"
    );
}

#[test]
fn test_subscription_cleanup_with_age() {
    let config = SubscriptionConfig::default();
    let manager = SubscriptionManager::new(config);

    // Subscribe and immediately drop
    let _rx = manager.subscribe(1);
    drop(_rx);

    // With a very long max_age, cleanup should keep it
    let removed = manager.cleanup_stale_with_age(Duration::from_secs(3600));
    assert_eq!(
        removed, 0,
        "Young subscription should not be removed with long max_age"
    );

    // Sleep briefly then cleanup with very short max_age
    std::thread::sleep(Duration::from_millis(10));
    let removed = manager.cleanup_stale_with_age(Duration::from_millis(5));
    assert_eq!(
        removed, 1,
        "Old subscription should be removed with short max_age"
    );
}

// ============================================================================
// Feature 3: Metrics Histogram Reset Tests
// ============================================================================

#[test]
fn test_redis_metrics_reset_histograms() {
    let config = MetricsConfig {
        detailed_metrics: true,
        histogram_buckets: 10,
        max_latency_us: 1_000_000,
        rate_window_secs: 60,
    };

    let metrics = RedisMetrics::new(config);

    // Record some operations
    metrics.record_operation("query", 100, true);
    metrics.record_operation("query", 200, true);
    metrics.record_operation("insert", 50, true);

    // Get stats before reset
    let stats_before = metrics.stats();
    assert_eq!(stats_before.total_operations, 3, "Should have 3 operations");

    // Reset histograms (keeps counts, clears latency distribution)
    metrics.reset_histograms();

    // Total operations should remain
    let stats_after = metrics.stats();
    assert_eq!(
        stats_after.total_operations, 3,
        "Total operations should remain after histogram reset"
    );
}

#[test]
fn test_redis_metrics_full_reset() {
    let config = MetricsConfig::default();
    let metrics = RedisMetrics::new(config);

    // Record some operations
    metrics.record_operation("query", 100, true);
    metrics.record_operation("query", 200, false);

    let stats_before = metrics.stats();
    assert_eq!(stats_before.total_operations, 2);
    assert_eq!(stats_before.total_errors, 1);

    // Full reset clears everything
    metrics.reset();

    let stats_after = metrics.stats();
    assert_eq!(
        stats_after.total_operations, 0,
        "Operations should be cleared after reset"
    );
    assert_eq!(
        stats_after.total_errors, 0,
        "Errors should be cleared after reset"
    );
}

// ============================================================================
// Notes on Other Features
// ============================================================================

// Feature 2 (Cache System): The cache-first lookup is wired into
// filter_by_pattern in redis/index.rs. It's tested via the existing
// LocalCache unit tests in that module.

// Feature 1 (Connection Pool): The pool() accessor on PooledConnection
// is available for use by connection handling code. It's tested via
// the existing connection pool tests.

// Feature 6 (SIMD/Async): filter_batch_simd and load_current_chunk are
// internal methods tested via the query operator unit tests.

// Feature 7 (UDP Batching): run_batched is tested via the UDP listener
// integration tests in the network module.
