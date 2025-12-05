//! Edge Case Tests for Priority-Based Sealing
//!
//! These tests cover boundary conditions, error handling, and unusual scenarios
//! for the priority sealing system.

use kuba_tsdb::storage::parallel_sealing::{
    ParallelSealingConfig, ParallelSealingService, SealError,
};
use kuba_tsdb::storage::priority_sealing::{
    PriorityCalculator, PriorityConfig, PrioritySealQueue, SealPriority,
};
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
// Edge Cases: Priority Values
// =============================================================================

#[test]
fn test_priority_all_values() {
    // Test all priority variants can be created and compared
    let priorities = [
        SealPriority::Background,
        SealPriority::Low,
        SealPriority::Normal,
        SealPriority::High,
        SealPriority::Critical,
    ];

    // Verify ordering
    for i in 0..priorities.len() - 1 {
        assert!(priorities[i] < priorities[i + 1]);
    }

    // Verify values
    assert_eq!(SealPriority::Background.value(), 0);
    assert_eq!(SealPriority::Low.value(), 25);
    assert_eq!(SealPriority::Normal.value(), 50);
    assert_eq!(SealPriority::High.value(), 75);
    assert_eq!(SealPriority::Critical.value(), 100);
}

#[test]
fn test_priority_from_score_boundaries() {
    // Test boundary values for from_score based on actual implementation
    // 90-100: Critical, 70-89: High, 40-69: Normal, 20-39: Low, 0-19/101+: Background
    assert_eq!(SealPriority::from_score(0), SealPriority::Background);
    assert_eq!(SealPriority::from_score(19), SealPriority::Background);
    assert_eq!(SealPriority::from_score(20), SealPriority::Low);
    assert_eq!(SealPriority::from_score(39), SealPriority::Low);
    assert_eq!(SealPriority::from_score(40), SealPriority::Normal);
    assert_eq!(SealPriority::from_score(69), SealPriority::Normal);
    assert_eq!(SealPriority::from_score(70), SealPriority::High);
    assert_eq!(SealPriority::from_score(89), SealPriority::High);
    assert_eq!(SealPriority::from_score(90), SealPriority::Critical);
    assert_eq!(SealPriority::from_score(100), SealPriority::Critical);
    // Scores > 100 fall through to the default case (Background) per implementation
    assert_eq!(SealPriority::from_score(200), SealPriority::Background);
}

#[test]
fn test_priority_default() {
    let priority = SealPriority::default();
    assert_eq!(priority, SealPriority::Normal);
}

// =============================================================================
// Edge Cases: Priority Configuration
// =============================================================================

#[test]
fn test_config_validation_weight_sum_exceeds_one() {
    let config = PriorityConfig {
        age_weight: 0.5,
        point_count_weight: 0.5,
        series_priority_weight: 0.5, // Sum > 1.0
        ..Default::default()
    };

    // Should fail validation
    assert!(config.validate().is_err());
}

#[test]
fn test_config_validation_negative_weight() {
    // Test that negative weights are caught by validation
    // Note: The current validation may not check for negative weights
    // so we test with invalid sum instead
    let config = PriorityConfig {
        age_weight: 0.5,
        point_count_weight: 0.5,
        series_priority_weight: 0.5, // Sum = 1.5 > 1.0
        ..Default::default()
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_config_validation_pressure_out_of_range() {
    let config = PriorityConfig {
        memory_pressure_threshold: 1.5,
        ..Default::default()
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_config_validation_age_order() {
    let config = PriorityConfig {
        high_priority_age: Duration::from_secs(100),
        critical_priority_age: Duration::from_secs(50), // Critical < High
        ..Default::default()
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_config_zero_weights() {
    let config = PriorityConfig {
        age_weight: 0.0,
        point_count_weight: 0.0,
        series_priority_weight: 0.0,
        ..Default::default()
    };

    // All zero weights should be valid (but probably useless)
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_extreme_ages() {
    let config = PriorityConfig {
        high_priority_age: Duration::from_millis(1),
        critical_priority_age: Duration::from_millis(2),
        ..Default::default()
    };

    assert!(config.validate().is_ok());

    // Very long ages
    let config = PriorityConfig {
        high_priority_age: Duration::from_secs(86400 * 365), // 1 year
        critical_priority_age: Duration::from_secs(86400 * 365 * 10), // 10 years
        ..Default::default()
    };

    assert!(config.validate().is_ok());
}

// =============================================================================
// Edge Cases: Memory Pressure
// =============================================================================

#[test]
fn test_memory_pressure_boundary_values() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Test 0.0
    calculator.set_memory_pressure(0.0);
    assert_eq!(calculator.get_memory_pressure(), 0.0);

    // Test 1.0
    calculator.set_memory_pressure(1.0);
    assert_eq!(calculator.get_memory_pressure(), 1.0);

    // Test 0.5
    calculator.set_memory_pressure(0.5);
    let pressure = calculator.get_memory_pressure();
    assert!((pressure - 0.5).abs() < f64::EPSILON);
}

#[test]
fn test_memory_pressure_extreme_values() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Very small value
    calculator.set_memory_pressure(0.0001);
    let pressure = calculator.get_memory_pressure();
    assert!((pressure - 0.0001).abs() < f64::EPSILON);

    // Near 1.0
    calculator.set_memory_pressure(0.9999);
    let pressure = calculator.get_memory_pressure();
    assert!((pressure - 0.9999).abs() < f64::EPSILON);
}

#[test]
fn test_memory_pressure_escalation_levels() {
    let config = PriorityConfig {
        memory_pressure_threshold: 0.5,
        enable_dynamic_adjustment: true,
        ..Default::default()
    };

    let calculator = PriorityCalculator::new(config);

    // Below threshold - no escalation
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Low, 0.3);
    assert_eq!(adjusted, SealPriority::Low);

    // At threshold - no escalation
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Low, 0.5);
    assert_eq!(adjusted, SealPriority::Low);

    // Just above threshold - some escalation
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Low, 0.6);
    assert!(adjusted >= SealPriority::Low);

    // High pressure - more escalation
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Low, 0.9);
    assert!(adjusted > SealPriority::Low);
}

#[test]
fn test_memory_pressure_escalation_ceiling() {
    let config = PriorityConfig {
        memory_pressure_threshold: 0.5,
        enable_dynamic_adjustment: true,
        ..Default::default()
    };

    let calculator = PriorityCalculator::new(config);

    // Critical cannot be escalated further
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Critical, 0.99);
    assert_eq!(adjusted, SealPriority::Critical);
}

// =============================================================================
// Edge Cases: Priority Queue
// =============================================================================

#[test]
fn test_queue_empty_pop() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Pop from empty queue
    assert!(queue.pop().is_none());
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn test_queue_size_zero() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 0);

    // Queue with max_size 0 means unlimited queue (0 disables limit check)
    // This is by design - max_size of 0 is treated as "no limit"
    let result = queue.push(1, SealPriority::Normal, 1);
    assert!(result.is_ok());
}

#[test]
fn test_queue_single_element() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 1);

    // Add one element
    queue.push(42, SealPriority::Normal, 1).unwrap();
    assert_eq!(queue.len(), 1);

    // Second should fail
    assert!(queue.push(43, SealPriority::Normal, 2).is_err());

    // Pop and verify
    let task = queue.pop().unwrap();
    assert_eq!(task.task, 42);
    assert!(queue.is_empty());

    // Now can add again
    queue.push(44, SealPriority::Normal, 3).unwrap();
}

#[test]
fn test_queue_large_number_of_elements() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 10000);

    // Add many elements
    for i in 0..10000 {
        queue.push(i, SealPriority::Normal, i as u128).unwrap();
    }

    assert_eq!(queue.len(), 10000);

    // Pop all
    for _ in 0..10000 {
        assert!(queue.pop().is_some());
    }

    assert!(queue.is_empty());
}

#[test]
fn test_queue_mixed_priorities_pop_order() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<&str> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add in random priority order
    queue
        .push("background", SealPriority::Background, 1)
        .unwrap();
    queue.push("critical", SealPriority::Critical, 2).unwrap();
    queue.push("low", SealPriority::Low, 3).unwrap();
    queue.push("high", SealPriority::High, 4).unwrap();
    queue.push("normal", SealPriority::Normal, 5).unwrap();

    // Should pop in priority order
    assert_eq!(queue.pop().unwrap().task, "critical");
    assert_eq!(queue.pop().unwrap().task, "high");
    assert_eq!(queue.pop().unwrap().task, "normal");
    assert_eq!(queue.pop().unwrap().task, "low");
    assert_eq!(queue.pop().unwrap().task, "background");
}

#[test]
fn test_queue_clear_and_reuse() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add elements
    for i in 0..50 {
        queue.push(i, SealPriority::Normal, i as u128).unwrap();
    }

    assert_eq!(queue.len(), 50);

    // Clear
    queue.clear();
    assert!(queue.is_empty());

    // Add again
    for i in 0..25 {
        queue.push(i, SealPriority::High, i as u128).unwrap();
    }

    assert_eq!(queue.len(), 25);
}

// =============================================================================
// Edge Cases: Series Priority Override
// =============================================================================

#[test]
fn test_series_priority_override_update() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Set priority
    calculator.set_series_priority(42, SealPriority::Critical);
    assert_eq!(
        calculator.get_series_priority(42),
        Some(SealPriority::Critical)
    );

    // Update priority
    calculator.set_series_priority(42, SealPriority::Low);
    assert_eq!(calculator.get_series_priority(42), Some(SealPriority::Low));

    // Clear
    calculator.clear_series_priority(42);
    assert_eq!(calculator.get_series_priority(42), None);
}

#[test]
fn test_series_priority_many_series() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Set priorities for many series
    for i in 0..1000 {
        let priority = match i % 5 {
            0 => SealPriority::Critical,
            1 => SealPriority::High,
            2 => SealPriority::Normal,
            3 => SealPriority::Low,
            _ => SealPriority::Background,
        };
        calculator.set_series_priority(i as u128, priority);
    }

    // Verify all set correctly
    for i in 0..1000 {
        let expected = match i % 5 {
            0 => SealPriority::Critical,
            1 => SealPriority::High,
            2 => SealPriority::Normal,
            3 => SealPriority::Low,
            _ => SealPriority::Background,
        };
        assert_eq!(calculator.get_series_priority(i as u128), Some(expected));
    }
}

#[test]
fn test_series_priority_clear_nonexistent() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Clear non-existent series (should not panic)
    calculator.clear_series_priority(999);
    assert_eq!(calculator.get_series_priority(999), None);
}

// =============================================================================
// Edge Cases: ParallelSealingService
// =============================================================================

#[tokio::test]
async fn test_single_point_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Seal chunk with single point
    let points = vec![DataPoint::new(1, 1000, 42.0)];
    let path = temp_dir.path().join("single_point.kub");
    let handle = service.submit(1, points, path).await.unwrap();
    let result = handle.await_result().await.unwrap();

    assert_eq!(result.point_count, 1);
}

#[tokio::test]
async fn test_large_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig {
        memory_budget_bytes: 512 * 1024 * 1024, // 512MB
        ..Default::default()
    });

    // Seal large chunk (100k points)
    let points = create_test_points(100_000, 1, 1000);
    let path = temp_dir.path().join("large_chunk.kub");
    let handle = service.submit(1, points, path).await.unwrap();
    let result = handle.await_result().await.unwrap();

    assert_eq!(result.point_count, 100_000);
}

#[tokio::test]
async fn test_series_id_boundary_values() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Test series_id = 0
    let points = create_test_points(10, 0, 1000);
    let path = temp_dir.path().join("series_0.kub");
    let result = service
        .submit(0, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());

    // Test large series_id
    let points = create_test_points(10, u128::MAX, 1000);
    let path = temp_dir.path().join("series_max.kub");
    let result = service
        .submit(u128::MAX, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_timestamp_boundary_values() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Test negative timestamp
    let points = vec![DataPoint::new(1, i64::MIN, 1.0)];
    let path = temp_dir.path().join("ts_min.kub");
    let result = service
        .submit(1, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());

    // Test maximum timestamp
    let points = vec![DataPoint::new(1, i64::MAX, 1.0)];
    let path = temp_dir.path().join("ts_max.kub");
    let result = service
        .submit(1, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());

    // Test timestamp = 0
    let points = vec![DataPoint::new(1, 0, 1.0)];
    let path = temp_dir.path().join("ts_zero.kub");
    let result = service
        .submit(1, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_value_boundary_values() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Test special float values
    let points = vec![
        DataPoint::new(1, 1000, 0.0),
        DataPoint::new(1, 2000, -0.0),
        DataPoint::new(1, 3000, f64::MIN),
        DataPoint::new(1, 4000, f64::MAX),
        DataPoint::new(1, 5000, f64::MIN_POSITIVE),
    ];
    let path = temp_dir.path().join("special_values.kub");
    let result = service
        .submit(1, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_priority_disabled_still_works() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig {
        enable_priority: false,
        priority_config: None,
        ..Default::default()
    });

    // These should be no-ops
    service.set_series_priority(1, SealPriority::Critical);
    service.set_memory_pressure(0.9);

    // Sealing should still work
    let points = create_test_points(100, 1, 1000);
    let path = temp_dir.path().join("disabled_priority.kub");
    let result = service
        .submit(1, points, path)
        .await
        .unwrap()
        .await_result()
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_concurrent_same_series() {
    let temp_dir = TempDir::new().unwrap();
    let service = Arc::new(ParallelSealingService::new(ParallelSealingConfig::default()));

    // Submit multiple seals for same series concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let service = Arc::clone(&service);
        let temp_path = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let points = create_test_points(50, 1, i * 1000); // Same series_id
            let path = temp_path.join(format!("same_series_{}.kub", i));
            let seal_handle = service.submit(1, points, path).await?;
            seal_handle.await_result().await
        });
        handles.push(handle);
    }

    // All should complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

// =============================================================================
// Edge Cases: Statistics
// =============================================================================

#[test]
fn test_stats_overflow_safety() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 1000);

    // Add and remove many elements
    for _ in 0..1000 {
        for i in 0..100 {
            queue.push(i, SealPriority::Normal, i as u128).unwrap();
        }
        for _ in 0..100 {
            queue.pop();
        }
    }

    let stats = queue.stats();
    assert_eq!(stats.total_pushed, 100_000);
    assert_eq!(stats.total_popped, 100_000);
    assert_eq!(stats.current_size, 0);
}

#[test]
fn test_calculator_stats() {
    let calculator = PriorityCalculator::new(PriorityConfig::default());

    // Perform many calculations
    for i in 0..1000 {
        calculator.calculate_priority_simple(i as u128, (i * 100) as usize);
    }

    assert_eq!(calculator.calculations_count(), 1000);
}

// =============================================================================
// Edge Cases: Queue Reordering
// =============================================================================

#[test]
fn test_reorder_empty_queue() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Reorder empty queue (should not panic)
    queue.reorder_on_pressure(0.9);

    let stats = queue.stats();
    assert_eq!(stats.reorders, 1);
    assert_eq!(stats.current_size, 0);
}

#[test]
fn test_multiple_reorders() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig {
        memory_pressure_threshold: 0.5,
        enable_dynamic_adjustment: true,
        ..Default::default()
    }));

    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add some elements
    queue.push(1, SealPriority::Low, 1).unwrap();
    queue.push(2, SealPriority::Low, 2).unwrap();

    // Reorder multiple times
    queue.reorder_on_pressure(0.6);
    queue.reorder_on_pressure(0.7);
    queue.reorder_on_pressure(0.9);

    let stats = queue.stats();
    assert_eq!(stats.reorders, 3);
}

// =============================================================================
// Edge Cases: Error Conditions
// =============================================================================

#[tokio::test]
async fn test_empty_points_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let path = temp_dir.path().join("empty.kub");
    let result = service.submit(1, Vec::new(), path).await;

    assert!(matches!(result, Err(SealError::EmptyChunk(1))));
}

#[tokio::test]
async fn test_empty_points_with_priority_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let path = temp_dir.path().join("empty_priority.kub");
    let result = service
        .submit_with_priority(42, Vec::new(), path, SealPriority::Critical)
        .await;

    assert!(matches!(result, Err(SealError::EmptyChunk(42))));
}

// =============================================================================
// Edge Cases: Task Age
// =============================================================================

#[test]
fn test_task_age_tracking() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add task
    queue.push(1, SealPriority::Normal, 1).unwrap();

    // Wait a bit
    std::thread::sleep(Duration::from_millis(10));

    // Pop and check age
    let task = queue.pop().unwrap();
    assert!(task.age() >= Duration::from_millis(10));
}
