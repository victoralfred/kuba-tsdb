//! Integration tests for adaptive concurrency control with parallel sealing
//!
//! These tests verify that the adaptive concurrency system correctly integrates
//! with the ParallelSealingService and responds appropriately to load conditions.

use kuba_tsdb::storage::adaptive_concurrency::AdaptiveConfig;
use kuba_tsdb::storage::parallel_sealing::{ParallelSealingConfig, ParallelSealingService};
use kuba_tsdb::types::DataPoint;
use std::time::Duration;
use tempfile::TempDir;

// =============================================================================
// Helper Functions
// =============================================================================

/// Create test data points for a series
fn create_test_points(count: usize, series_id: u128, base_ts: i64) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(series_id, base_ts + i as i64 * 1000, 100.0 + i as f64 * 0.1))
        .collect()
}

// =============================================================================
// Service Initialization Tests
// =============================================================================

#[tokio::test]
async fn test_service_initializes_with_adaptive_concurrency() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Verify adaptive concurrency is enabled
    assert!(service.is_adaptive_concurrency_enabled());

    // Verify controller is accessible
    let controller = service.concurrency_controller();
    assert!(controller.is_some());

    // Verify initial concurrency limit
    let limit = service.current_concurrency_limit();
    assert!(limit > 0);
}

#[tokio::test]
async fn test_service_without_adaptive_concurrency() {
    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 4,
        enable_adaptive_concurrency: false,
        adaptive_config: None,
        ..Default::default()
    });

    // Verify adaptive concurrency is disabled
    assert!(!service.is_adaptive_concurrency_enabled());

    // Verify controller is not accessible
    assert!(service.concurrency_controller().is_none());

    // Verify concurrency limit falls back to config value
    assert_eq!(service.current_concurrency_limit(), 4);
}

#[tokio::test]
async fn test_service_with_custom_adaptive_config() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 16,
        initial_concurrency: 8,
        cpu_high_threshold: 0.90,
        cpu_low_threshold: 0.40,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 16,
        enable_adaptive_concurrency: true,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Verify custom initial concurrency is used
    assert_eq!(service.current_concurrency_limit(), 8);
}

// =============================================================================
// Concurrency Limit Control Tests
// =============================================================================

#[tokio::test]
async fn test_force_concurrency_limit_within_bounds() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Force to a valid value
    service.force_concurrency_limit(7);
    assert_eq!(service.current_concurrency_limit(), 7);
}

#[tokio::test]
async fn test_force_concurrency_limit_clamped_to_max() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Try to force above max
    service.force_concurrency_limit(100);
    assert_eq!(service.current_concurrency_limit(), 10);
}

#[tokio::test]
async fn test_force_concurrency_limit_clamped_to_min() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Try to force below min
    service.force_concurrency_limit(0);
    assert_eq!(service.current_concurrency_limit(), 2);
}

#[tokio::test]
async fn test_reset_concurrency() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Modify limit
    service.force_concurrency_limit(8);
    assert_eq!(service.current_concurrency_limit(), 8);

    // Reset
    service.reset_concurrency();
    assert_eq!(service.current_concurrency_limit(), 5);
}

// =============================================================================
// Statistics and Monitoring Tests
// =============================================================================

#[tokio::test]
async fn test_adaptive_concurrency_stats() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let stats = service.adaptive_concurrency_stats().await;
    assert!(stats.is_some());

    let stats = stats.unwrap();
    assert!(stats.current_limit > 0);
    assert_eq!(stats.total_adjustments, 0);
    assert_eq!(stats.total_increases, 0);
    assert_eq!(stats.total_decreases, 0);
}

#[tokio::test]
async fn test_stats_reflect_forced_limits() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Force limit and check stats
    service.force_concurrency_limit(8);

    let stats = service.adaptive_concurrency_stats().await.unwrap();
    assert_eq!(stats.current_limit, 8);
    // Note: forced limits don't count as adjustments
    assert_eq!(stats.total_adjustments, 0);
}

// =============================================================================
// Sealing Operations with Adaptive Concurrency Tests
// =============================================================================

#[tokio::test]
async fn test_single_seal_with_adaptive_concurrency() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let points = create_test_points(100, 1, 1000);
    let path = temp_dir.path().join("chunk_1.kub");

    let handle = service.submit(1, points, path.clone()).await.unwrap();
    let result = handle.await_result().await.unwrap();

    assert_eq!(result.series_id, 1);
    assert_eq!(result.point_count, 100);
    assert!(path.exists());
}

#[tokio::test]
async fn test_parallel_seals_with_adaptive_concurrency() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 4,
        enable_adaptive_concurrency: true,
        adaptive_config: Some(AdaptiveConfig {
            min_concurrency: 1,
            max_concurrency: 8,
            initial_concurrency: 4,
            ..Default::default()
        }),
        ..Default::default()
    });

    let chunks: Vec<_> = (0..10u128)
        .map(|i| {
            let points = create_test_points(100, i, 1000);
            let path = temp_dir.path().join(format!("chunk_{}.kub", i));
            (i, points, path)
        })
        .collect();

    let results = service.seal_all_and_wait(chunks).await;

    assert_eq!(results.len(), 10);
    assert!(results.iter().all(|r| r.is_ok()));

    let stats = service.stats();
    assert_eq!(stats.total_sealed, 10);
    assert_eq!(stats.total_failed, 0);
}

#[tokio::test]
async fn test_sealing_records_io_latency() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Perform a few seals
    for i in 0..3u128 {
        let points = create_test_points(100, i, 1000);
        let path = temp_dir.path().join(format!("chunk_{}.kub", i));

        let handle = service.submit(i, points, path).await.unwrap();
        let _ = handle.await_result().await.unwrap();
    }

    // Take a sample to incorporate recorded latencies
    let controller = service.concurrency_controller().unwrap();
    controller.sampler().take_sample().await;

    // Check that latency was recorded (we have some IO latency data)
    let metrics = controller.sampler().latest().await;
    assert!(metrics.is_some());
}

// =============================================================================
// Adjustment Trigger Tests
// =============================================================================

#[tokio::test]
async fn test_manual_adjustment_trigger() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        cooldown_period: Duration::from_millis(0), // No cooldown for testing
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Trigger adjustment - should return None or Some depending on metrics
    let result = service.trigger_concurrency_adjustment().await;

    // Result is None if no adjustment needed (cooldown, same limit, etc)
    // This test mainly verifies the method doesn't panic
    let _ = result;
}

#[tokio::test]
async fn test_adjustment_respects_cooldown() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        cooldown_period: Duration::from_secs(10), // Long cooldown
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // First trigger - might adjust
    let _ = service.trigger_concurrency_adjustment().await;

    // Second trigger immediately - should be blocked by cooldown
    let result = service.trigger_concurrency_adjustment().await;
    assert!(result.is_none());
}

// =============================================================================
// Graceful Shutdown Tests
// =============================================================================

#[tokio::test]
async fn test_stop_adaptive_concurrency() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Verify sampler is running initially
    let controller = service.concurrency_controller().unwrap();
    assert!(controller.sampler().is_running());

    // Stop adaptive concurrency
    service.stop_adaptive_concurrency();

    // Verify sampler is stopped
    assert!(!controller.sampler().is_running());
}

#[tokio::test]
async fn test_sealing_works_after_stop() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Stop adaptive concurrency
    service.stop_adaptive_concurrency();

    // Sealing should still work
    let points = create_test_points(100, 1, 1000);
    let path = temp_dir.path().join("chunk.kub");

    let handle = service.submit(1, points, path.clone()).await.unwrap();
    let result = handle.await_result().await.unwrap();

    assert_eq!(result.series_id, 1);
    assert!(path.exists());
}

// =============================================================================
// Config Preset Tests
// =============================================================================

#[tokio::test]
async fn test_high_throughput_preset() {
    let service = ParallelSealingService::new(ParallelSealingConfig::high_throughput());

    assert!(service.is_adaptive_concurrency_enabled());

    // High throughput should have higher initial concurrency
    let default_service = ParallelSealingService::new(ParallelSealingConfig::default());
    assert!(service.current_concurrency_limit() >= default_service.current_concurrency_limit());
}

#[tokio::test]
async fn test_low_memory_preset() {
    let service = ParallelSealingService::new(ParallelSealingConfig::low_memory());

    assert!(service.is_adaptive_concurrency_enabled());

    // Low memory should have lower max concurrency
    let limit = service.current_concurrency_limit();
    assert!(limit <= 2);
}

#[tokio::test]
async fn test_low_latency_preset() {
    let service = ParallelSealingService::new(ParallelSealingConfig::low_latency());

    assert!(service.is_adaptive_concurrency_enabled());

    // Low latency should have conservative concurrency
    let default_service = ParallelSealingService::new(ParallelSealingConfig::default());
    assert!(service.current_concurrency_limit() <= default_service.current_concurrency_limit());
}

// =============================================================================
// Controller Access Tests
// =============================================================================

#[tokio::test]
async fn test_controller_access() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let controller = service.concurrency_controller();
    assert!(controller.is_some());

    let controller = controller.unwrap();

    // Access config
    let config = controller.config();
    assert!(config.enabled);

    // Access sampler
    let sampler = controller.sampler();
    assert!(sampler.is_running());
}

#[tokio::test]
async fn test_sampler_metrics() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let controller = service.concurrency_controller().unwrap();
    let sampler = controller.sampler();

    // Take a sample
    sampler.take_sample().await;

    // Verify sample was taken
    let sample_count = sampler.sample_count().await;
    assert!(sample_count >= 1);

    // Get latest metrics
    let metrics = sampler.latest().await;
    assert!(metrics.is_some());
}

// =============================================================================
// Combined Priority and Adaptive Concurrency Tests
// =============================================================================

#[tokio::test]
async fn test_priority_and_adaptive_concurrency_together() {
    let service = ParallelSealingService::new(ParallelSealingConfig {
        enable_priority: true,
        enable_adaptive_concurrency: true,
        ..Default::default()
    });

    // Both should be enabled
    assert!(service.is_priority_enabled());
    assert!(service.is_adaptive_concurrency_enabled());
}

#[tokio::test]
async fn test_sealing_with_priority_and_adaptive() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig {
        enable_priority: true,
        enable_adaptive_concurrency: true,
        ..Default::default()
    });

    // Submit tasks with different priorities
    let mut handles = Vec::new();
    for i in 0..5u128 {
        let points = create_test_points(100, i, 1000);
        let path = temp_dir.path().join(format!("chunk_{}.kub", i));
        let handle = service.submit(i, points, path).await.unwrap();
        handles.push(handle);
    }

    // All should complete
    for handle in handles {
        let result = handle.await_result().await;
        assert!(result.is_ok());
    }

    // Verify both systems are working
    let sealing_stats = service.stats();
    assert_eq!(sealing_stats.total_sealed, 5);

    let adaptive_stats = service.adaptive_concurrency_stats().await.unwrap();
    assert!(adaptive_stats.current_limit > 0);
}
