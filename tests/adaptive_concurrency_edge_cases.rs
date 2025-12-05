//! Edge case tests for adaptive concurrency control
//!
//! These tests verify boundary conditions, error handling, and stress scenarios
//! for the adaptive concurrency system.

use kuba_tsdb::storage::adaptive_concurrency::{
    AdaptiveConfig, AdjustmentDirection, ConcurrencyController, LoadSampler, SystemMetrics,
};
use kuba_tsdb::storage::parallel_sealing::{ParallelSealingConfig, ParallelSealingService};
use kuba_tsdb::types::DataPoint;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_points(count: usize, series_id: u128, base_ts: i64) -> Vec<DataPoint> {
    (0..count)
        .map(|i| DataPoint::new(series_id, base_ts + i as i64 * 1000, 100.0 + i as f64 * 0.1))
        .collect()
}

// =============================================================================
// Configuration Boundary Tests
// =============================================================================

#[test]
fn test_config_min_equals_max_concurrency() {
    // When min equals max, there's no room for adjustment
    let config = AdaptiveConfig {
        min_concurrency: 4,
        max_concurrency: 4,
        initial_concurrency: 4,
        ..Default::default()
    };
    assert!(config.validate().is_ok());

    let controller = ConcurrencyController::new(config);
    assert_eq!(controller.current_limit(), 4);

    // Force should still clamp correctly
    controller.force_limit(10);
    assert_eq!(controller.current_limit(), 4);

    controller.force_limit(1);
    assert_eq!(controller.current_limit(), 4);
}

#[test]
fn test_config_initial_at_min() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 1,
        ..Default::default()
    };
    assert!(config.validate().is_ok());

    let controller = ConcurrencyController::new(config);
    assert_eq!(controller.current_limit(), 1);
}

#[test]
fn test_config_initial_at_max() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 10,
        ..Default::default()
    };
    assert!(config.validate().is_ok());

    let controller = ConcurrencyController::new(config);
    assert_eq!(controller.current_limit(), 10);
}

#[test]
fn test_config_cpu_threshold_at_boundary() {
    // CPU thresholds at edges of valid range
    let config = AdaptiveConfig {
        cpu_high_threshold: 1.0,
        cpu_low_threshold: 0.0,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_cpu_threshold_equal_invalid() {
    // CPU thresholds cannot be equal
    let config = AdaptiveConfig {
        cpu_high_threshold: 0.5,
        cpu_low_threshold: 0.5,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn test_config_memory_threshold_at_boundary() {
    // Memory threshold at edges
    let config1 = AdaptiveConfig {
        memory_high_threshold: 1.0,
        ..Default::default()
    };
    assert!(config1.validate().is_ok());

    let config2 = AdaptiveConfig {
        memory_high_threshold: 0.0,
        ..Default::default()
    };
    assert!(config2.validate().is_ok());
}

#[test]
fn test_config_zero_cooldown() {
    // Zero cooldown is valid
    let config = AdaptiveConfig {
        cooldown_period: Duration::ZERO,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_large_sample_window() {
    // Very large sample window
    let config = AdaptiveConfig {
        sample_window: 1000,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_large_adjustment_step() {
    // Large adjustment step
    let config = AdaptiveConfig {
        adjustment_step: 100,
        min_concurrency: 1,
        max_concurrency: 100,
        initial_concurrency: 50,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

// =============================================================================
// Controller Edge Cases
// =============================================================================

#[test]
fn test_controller_force_limit_exact_bounds() {
    let config = AdaptiveConfig {
        min_concurrency: 2,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let controller = ConcurrencyController::new(config);

    // Force exactly to min
    controller.force_limit(2);
    assert_eq!(controller.current_limit(), 2);

    // Force exactly to max
    controller.force_limit(10);
    assert_eq!(controller.current_limit(), 10);
}

#[test]
fn test_controller_force_limit_zero() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let controller = ConcurrencyController::new(config);

    // Force to 0 should clamp to min
    controller.force_limit(0);
    assert_eq!(controller.current_limit(), 1);
}

#[test]
fn test_controller_force_limit_very_large() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let controller = ConcurrencyController::new(config);

    // Force to very large value should clamp to max
    controller.force_limit(usize::MAX);
    assert_eq!(controller.current_limit(), 10);
}

#[test]
fn test_controller_multiple_resets() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let controller = ConcurrencyController::new(config);

    // Multiple resets should all work
    for _ in 0..10 {
        controller.force_limit(8);
        controller.reset();
        assert_eq!(controller.current_limit(), 5);
    }
}

#[tokio::test]
async fn test_controller_disabled_adjust() {
    let config = AdaptiveConfig {
        enabled: false,
        ..Default::default()
    };

    let controller = ConcurrencyController::new(config);

    // Adjust should return None when disabled
    let result = controller.adjust().await;
    assert!(result.is_none());
}

// =============================================================================
// System Metrics Edge Cases
// =============================================================================

#[test]
fn test_metrics_all_zero() {
    let config = AdaptiveConfig::default();
    let metrics = SystemMetrics::default();

    // All zero metrics should not indicate high load
    assert!(!metrics.is_high_load(&config));
}

#[test]
fn test_metrics_at_threshold() {
    let config = AdaptiveConfig {
        cpu_high_threshold: 0.85,
        memory_high_threshold: 0.80,
        io_latency_threshold_ms: 100,
        ..Default::default()
    };

    // Exactly at CPU threshold - uses > not >=, so exactly at threshold is NOT high load
    let metrics_at_cpu = SystemMetrics {
        cpu_usage: 0.85,
        memory_usage: 0.5,
        io_latency_ms: 50.0,
        ..Default::default()
    };
    assert!(!metrics_at_cpu.is_high_load(&config));

    // Just above CPU threshold - this IS high load
    let metrics_cpu_above = SystemMetrics {
        cpu_usage: 0.86,
        memory_usage: 0.5,
        io_latency_ms: 50.0,
        ..Default::default()
    };
    assert!(metrics_cpu_above.is_high_load(&config));

    // Just below CPU threshold
    let metrics_cpu_below = SystemMetrics {
        cpu_usage: 0.84,
        memory_usage: 0.5,
        io_latency_ms: 50.0,
        ..Default::default()
    };
    assert!(!metrics_cpu_below.is_high_load(&config));
}

#[test]
fn test_metrics_extreme_values() {
    let config = AdaptiveConfig::default();

    // Extreme high values
    let high_metrics = SystemMetrics {
        cpu_usage: 1.0,
        memory_usage: 1.0,
        io_latency_ms: f64::MAX,
        ..Default::default()
    };
    assert!(high_metrics.is_high_load(&config));
}

#[test]
fn test_metrics_is_low_load_boundary() {
    let config = AdaptiveConfig {
        cpu_low_threshold: 0.5,
        memory_high_threshold: 0.8,
        io_latency_threshold_ms: 100,
        ..Default::default()
    };

    // Just under low threshold
    let low_metrics = SystemMetrics {
        cpu_usage: 0.49,
        memory_usage: 0.39,  // Below memory_high_threshold * 0.5
        io_latency_ms: 49.0, // Below io_latency_threshold * 0.5
        ..Default::default()
    };
    assert!(low_metrics.is_low_load(&config));

    // At low threshold
    let at_threshold = SystemMetrics {
        cpu_usage: 0.5,
        memory_usage: 0.4,
        io_latency_ms: 50.0,
        ..Default::default()
    };
    assert!(!at_threshold.is_low_load(&config));
}

// =============================================================================
// Load Sampler Edge Cases
// =============================================================================

#[tokio::test]
async fn test_sampler_empty_averaged() {
    let config = AdaptiveConfig::default();
    let sampler = LoadSampler::new(config);

    // Averaged on empty should return default
    let avg = sampler.averaged().await;
    assert_eq!(avg.cpu_usage, 0.0);
    assert_eq!(avg.memory_usage, 0.0);
}

#[tokio::test]
async fn test_sampler_single_sample_averaged() {
    let config = AdaptiveConfig {
        sample_window: 5,
        ..Default::default()
    };
    let sampler = LoadSampler::new(config);

    sampler.take_sample().await;

    let avg = sampler.averaged().await;
    assert!(avg.sampled_at.is_some());
}

#[tokio::test]
async fn test_sampler_window_overflow() {
    let config = AdaptiveConfig {
        sample_window: 3,
        ..Default::default()
    };
    let sampler = LoadSampler::new(config);

    // Take more samples than window size
    for _ in 0..10 {
        sampler.take_sample().await;
    }

    // Should only have window size samples
    assert_eq!(sampler.sample_count().await, 3);
}

#[tokio::test]
async fn test_sampler_io_latency_reset() {
    let config = AdaptiveConfig::default();
    let sampler = LoadSampler::new(config);

    // Record some latency
    sampler.record_io_latency(Duration::from_millis(100));
    sampler.record_io_latency(Duration::from_millis(200));

    // Take sample - should reset counters
    sampler.take_sample().await;

    // Take another sample - should have 0 latency now
    sampler.take_sample().await;

    let latest = sampler.latest().await.unwrap();
    assert_eq!(latest.io_latency_ms, 0.0);
}

#[tokio::test]
async fn test_sampler_clear_multiple() {
    let config = AdaptiveConfig::default();
    let sampler = LoadSampler::new(config);

    sampler.take_sample().await;
    sampler.take_sample().await;
    sampler.clear().await;

    // Multiple clears should be safe
    sampler.clear().await;
    sampler.clear().await;

    assert_eq!(sampler.sample_count().await, 0);
}

#[tokio::test]
async fn test_sampler_start_stop_restart() {
    let config = AdaptiveConfig {
        adjustment_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let sampler = Arc::new(LoadSampler::new(config));

    // Start
    sampler.start();
    assert!(sampler.is_running());

    // Stop
    sampler.stop();

    // Wait a bit for the task to stop
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!sampler.is_running());

    // Restart
    sampler.start();
    assert!(sampler.is_running());

    // Cleanup
    sampler.stop();
}

// =============================================================================
// Parallel Sealing Service Edge Cases
// =============================================================================

#[tokio::test]
async fn test_service_with_min_one_concurrency() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 1,
        initial_concurrency: 1,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 1,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    assert_eq!(service.current_concurrency_limit(), 1);

    // Force should not change anything
    service.force_concurrency_limit(10);
    assert_eq!(service.current_concurrency_limit(), 1);
}

#[tokio::test]
async fn test_service_sealing_with_min_concurrency() {
    let temp_dir = TempDir::new().unwrap();
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 1,
        initial_concurrency: 1,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 1,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Should still work with only 1 concurrent seal
    let chunks: Vec<_> = (0..5u128)
        .map(|i| {
            let points = create_test_points(50, i, 1000);
            let path = temp_dir.path().join(format!("chunk_{}.kub", i));
            (i, points, path)
        })
        .collect();

    let results = service.seal_all_and_wait(chunks).await;

    assert_eq!(results.len(), 5);
    assert!(results.iter().all(|r| r.is_ok()));
}

#[tokio::test]
async fn test_service_rapid_force_limit_changes() {
    let adaptive_config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 10,
        initial_concurrency: 5,
        ..Default::default()
    };

    let service = ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 10,
        adaptive_config: Some(adaptive_config),
        ..Default::default()
    });

    // Rapidly change limits
    for i in 1..=10 {
        service.force_concurrency_limit(i);
        assert_eq!(service.current_concurrency_limit(), i);
    }
}

#[tokio::test]
async fn test_service_stop_before_any_sealing() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Stop before any sealing
    service.stop_adaptive_concurrency();

    // Service should still be usable
    let temp_dir = TempDir::new().unwrap();
    let points = create_test_points(50, 1, 1000);
    let path = temp_dir.path().join("chunk.kub");

    let handle = service.submit(1, points, path.clone()).await.unwrap();
    let result = handle.await_result().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_service_multiple_stops() {
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Multiple stops should be safe
    service.stop_adaptive_concurrency();
    service.stop_adaptive_concurrency();
    service.stop_adaptive_concurrency();

    // Service should still work
    assert!(!service
        .concurrency_controller()
        .unwrap()
        .sampler()
        .is_running());
}

// =============================================================================
// Adjustment Direction Tests
// =============================================================================

#[test]
fn test_adjustment_direction_variants() {
    assert_eq!(AdjustmentDirection::Increase, AdjustmentDirection::Increase);
    assert_eq!(AdjustmentDirection::Decrease, AdjustmentDirection::Decrease);
    assert_eq!(AdjustmentDirection::None, AdjustmentDirection::None);

    assert_ne!(AdjustmentDirection::Increase, AdjustmentDirection::Decrease);
    assert_ne!(AdjustmentDirection::Increase, AdjustmentDirection::None);
    assert_ne!(AdjustmentDirection::Decrease, AdjustmentDirection::None);
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

#[tokio::test]
async fn test_controller_concurrent_force_limit() {
    let config = AdaptiveConfig {
        min_concurrency: 1,
        max_concurrency: 100,
        initial_concurrency: 50,
        ..Default::default()
    };

    let controller = Arc::new(ConcurrencyController::new(config));

    // Spawn multiple tasks that force limits concurrently
    let mut handles = Vec::new();
    for i in 1..=100 {
        let ctrl = Arc::clone(&controller);
        handles.push(tokio::spawn(async move {
            ctrl.force_limit(i);
        }));
    }

    // Wait for all
    for handle in handles {
        handle.await.unwrap();
    }

    // Limit should be valid
    let limit = controller.current_limit();
    assert!((1..=100).contains(&limit));
}

#[tokio::test]
async fn test_sampler_concurrent_samples() {
    let config = AdaptiveConfig {
        sample_window: 10,
        ..Default::default()
    };

    let sampler = Arc::new(LoadSampler::new(config));

    // Spawn multiple tasks that take samples concurrently
    let mut handles = Vec::new();
    for _ in 0..50 {
        let s = Arc::clone(&sampler);
        handles.push(tokio::spawn(async move {
            s.take_sample().await;
        }));
    }

    // Wait for all
    for handle in handles {
        handle.await.unwrap();
    }

    // Sample count should be at most window size
    assert!(sampler.sample_count().await <= 10);
}

// =============================================================================
// Config Preset Edge Cases
// =============================================================================

#[tokio::test]
async fn test_preset_configs_validate() {
    // All presets should produce valid configs
    let high = AdaptiveConfig::high_throughput();
    assert!(high.validate().is_ok());

    let low_latency = AdaptiveConfig::low_latency();
    assert!(low_latency.validate().is_ok());

    let low_memory = AdaptiveConfig::low_memory();
    assert!(low_memory.validate().is_ok());
}

#[tokio::test]
async fn test_sealing_config_presets_with_adaptive() {
    // High throughput
    let high = ParallelSealingConfig::high_throughput();
    assert!(high.enable_adaptive_concurrency);
    assert!(high.adaptive_config.is_some());

    // Low memory
    let low = ParallelSealingConfig::low_memory();
    assert!(low.enable_adaptive_concurrency);
    assert!(low.adaptive_config.is_some());

    // Low latency
    let latency = ParallelSealingConfig::low_latency();
    assert!(latency.enable_adaptive_concurrency);
    assert!(latency.adaptive_config.is_some());
}
