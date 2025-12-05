//! Integration tests for Priority-Based Sealing
//!
//! These tests verify that the priority sealing system integrates correctly
//! with the parallel sealing service and the broader storage system.

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
// Integration Tests: ParallelSealingService with Priority
// =============================================================================

#[tokio::test]
async fn test_parallel_sealing_with_priority_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let config = ParallelSealingConfig {
        enable_priority: true,
        max_concurrent_seals: 4,
        ..Default::default()
    };

    let service = ParallelSealingService::new(config);
    assert!(service.is_priority_enabled());

    // Submit multiple tasks with different priorities
    let mut handles = Vec::new();
    for i in 0..5 {
        let points = create_test_points(100, i, 1000);
        let path = temp_dir.path().join(format!("chunk_{}.kub", i));
        let handle = service.submit(i, points, path).await.unwrap();
        handles.push(handle);
    }

    // All should complete successfully
    for handle in handles {
        let result = handle.await_result().await;
        assert!(result.is_ok(), "Seal should succeed: {:?}", result);
    }
}

#[tokio::test]
async fn test_parallel_sealing_with_priority_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let config = ParallelSealingConfig {
        enable_priority: false,
        priority_config: None,
        max_concurrent_seals: 4,
        ..Default::default()
    };

    let service = ParallelSealingService::new(config);
    assert!(!service.is_priority_enabled());

    // Submit tasks - should still work without priority
    let points = create_test_points(100, 1, 1000);
    let path = temp_dir.path().join("chunk.kub");
    let handle = service.submit(1, points, path).await.unwrap();
    let result = handle.await_result().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_series_priority_override_affects_calculation() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Set high priority for series 1
    service.set_series_priority(1, SealPriority::Critical);

    // Submit task for series 1
    let points = create_test_points(50, 1, 1000);
    let path = temp_dir.path().join("chunk_critical.kub");
    let handle = service.submit(1, points, path).await.unwrap();
    let result = handle.await_result().await.unwrap();

    assert_eq!(result.series_id, 1);

    // Verify priority was set in calculator
    let calculator = service.priority_calculator().unwrap();
    assert_eq!(
        calculator.get_series_priority(1),
        Some(SealPriority::Critical)
    );
}

#[tokio::test]
async fn test_memory_pressure_updates() {
    let service = ParallelSealingService::new(ParallelSealingConfig {
        memory_budget_bytes: 1024 * 1024, // 1MB
        ..Default::default()
    });

    // Set various memory pressure levels
    service.set_memory_pressure(0.0);
    assert_eq!(
        service.priority_calculator().unwrap().get_memory_pressure(),
        0.0
    );

    service.set_memory_pressure(0.5);
    let pressure = service.priority_calculator().unwrap().get_memory_pressure();
    assert!((pressure - 0.5).abs() < 0.001);

    service.set_memory_pressure(1.0);
    let pressure = service.priority_calculator().unwrap().get_memory_pressure();
    assert!((pressure - 1.0).abs() < 0.001);
}

#[tokio::test]
async fn test_explicit_priority_submission() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Submit with explicit Background priority
    let points = create_test_points(100, 1, 1000);
    let path = temp_dir.path().join("background_chunk.kub");
    let handle = service
        .submit_with_priority(1, points, path, SealPriority::Background)
        .await
        .unwrap();

    let result = handle.await_result().await;
    assert!(result.is_ok());

    // Submit with explicit Critical priority
    let points = create_test_points(100, 2, 1000);
    let path = temp_dir.path().join("critical_chunk.kub");
    let handle = service
        .submit_with_priority(2, points, path, SealPriority::Critical)
        .await
        .unwrap();

    let result = handle.await_result().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_multiple_series_different_priorities() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Set different priorities for different series
    service.set_series_priority(1, SealPriority::Critical);
    service.set_series_priority(2, SealPriority::High);
    service.set_series_priority(3, SealPriority::Normal);
    service.set_series_priority(4, SealPriority::Low);
    service.set_series_priority(5, SealPriority::Background);

    // Submit all at once
    let mut handles = Vec::new();
    for series_id in 1..=5 {
        let points = create_test_points(50, series_id, 1000);
        let path = temp_dir.path().join(format!("series_{}.kub", series_id));
        let handle = service.submit(series_id, points, path).await.unwrap();
        handles.push((series_id, handle));
    }

    // All should complete
    for (series_id, handle) in handles {
        let result = handle.await_result().await;
        assert!(result.is_ok(), "Series {} should complete", series_id);
    }
}

// =============================================================================
// Integration Tests: PrioritySealQueue
// =============================================================================

#[test]
fn test_priority_queue_integration_with_calculator() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Push items with different priorities (data, priority, series_id)
    queue.push(1, SealPriority::Low, 100).unwrap();
    queue.push(2, SealPriority::Critical, 101).unwrap();
    queue.push(3, SealPriority::Normal, 102).unwrap();

    // Pop should return highest priority first
    let task = queue.pop().unwrap();
    assert_eq!(task.task, 2); // Critical

    let task = queue.pop().unwrap();
    assert_eq!(task.task, 3); // Normal

    let task = queue.pop().unwrap();
    assert_eq!(task.task, 1); // Low
}

#[test]
fn test_priority_queue_with_series_override() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));

    // Set series 42 as critical priority
    calculator.set_series_priority(42, SealPriority::Critical);

    let queue: PrioritySealQueue<(u128, &str)> =
        PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add tasks
    queue
        .push((1, "normal_chunk"), SealPriority::Normal, 1)
        .unwrap();
    queue
        .push((42, "critical_series_chunk"), SealPriority::Normal, 42)
        .unwrap();
    queue
        .push((2, "another_normal"), SealPriority::Normal, 2)
        .unwrap();

    // The queue stores priority at push time, so series override
    // affects future calculations, not already-queued items
    let stats = queue.stats();
    assert_eq!(stats.current_size, 3);
}

#[test]
fn test_priority_queue_fifo_within_same_priority() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Push multiple items with same priority
    queue.push(1, SealPriority::Normal, 1).unwrap();
    queue.push(2, SealPriority::Normal, 2).unwrap();
    queue.push(3, SealPriority::Normal, 3).unwrap();

    // Should pop in FIFO order
    assert_eq!(queue.pop().unwrap().task, 1);
    assert_eq!(queue.pop().unwrap().task, 2);
    assert_eq!(queue.pop().unwrap().task, 3);
}

// =============================================================================
// Integration Tests: Priority Configuration
// =============================================================================

#[test]
fn test_priority_config_presets() {
    // Default config
    let default = PriorityConfig::default();
    assert!(default.validate().is_ok());

    // High throughput config
    let high_throughput = PriorityConfig::high_throughput();
    assert!(high_throughput.validate().is_ok());

    // Low latency config
    let low_latency = PriorityConfig::low_latency();
    assert!(low_latency.validate().is_ok());
}

#[test]
fn test_priority_config_custom() {
    let config = PriorityConfig {
        age_weight: 0.3,
        point_count_weight: 0.4,
        series_priority_weight: 0.3,
        high_priority_age: Duration::from_secs(60),
        critical_priority_age: Duration::from_secs(120),
        high_point_count_threshold: 5000,
        memory_pressure_threshold: 0.75,
        enable_dynamic_adjustment: true,
    };

    assert!(config.validate().is_ok());

    let calculator = PriorityCalculator::new(config);
    // Verify calculator was created successfully
    let _ = calculator.get_memory_pressure(); // Should not panic
}

// =============================================================================
// Integration Tests: Memory Pressure Handling
// =============================================================================

#[tokio::test]
async fn test_high_memory_pressure_priority_escalation() {
    let config = PriorityConfig {
        memory_pressure_threshold: 0.5,
        enable_dynamic_adjustment: true,
        ..Default::default()
    };

    let calculator = Arc::new(PriorityCalculator::new(config));

    // Set high memory pressure
    calculator.set_memory_pressure(0.9);

    // With high memory pressure, even low-priority tasks should be escalated
    let adjusted = calculator.adjust_for_memory_pressure(SealPriority::Low, 0.9);
    // Low + high pressure should escalate to at least Normal
    assert!(adjusted >= SealPriority::Normal);
}

#[tokio::test]
async fn test_auto_memory_pressure_update() {
    let service = ParallelSealingService::new(ParallelSealingConfig {
        memory_budget_bytes: 10000,
        ..Default::default()
    });

    // Initially, pressure should be 0
    service.update_memory_pressure();
    let pressure = service.priority_calculator().unwrap().get_memory_pressure();
    assert_eq!(pressure, 0.0);
}

// =============================================================================
// Integration Tests: Concurrent Access
// =============================================================================

#[tokio::test]
async fn test_concurrent_priority_updates() {
    let service = Arc::new(ParallelSealingService::new(ParallelSealingConfig::default()));

    let mut handles = Vec::new();
    for i in 0..10 {
        let service = Arc::clone(&service);
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                let series_id = (i * 100 + j) as u128;
                if j % 2 == 0 {
                    service.set_series_priority(series_id, SealPriority::High);
                } else {
                    service.set_series_priority(series_id, SealPriority::Low);
                }
            }
        });
        handles.push(handle);
    }

    // All should complete without panic
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_seal_submissions() {
    let temp_dir = TempDir::new().unwrap();
    let service = Arc::new(ParallelSealingService::new(ParallelSealingConfig {
        max_concurrent_seals: 8,
        ..Default::default()
    }));

    let mut task_handles = Vec::new();
    for i in 0..20 {
        let service = Arc::clone(&service);
        let temp_path = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let points = create_test_points(50, i as u128, 1000 + i * 1000);
            let path = temp_path.join(format!("concurrent_{}.kub", i));
            let seal_handle = service.submit(i as u128, points, path).await?;
            seal_handle.await_result().await
        });
        task_handles.push(handle);
    }

    // All should complete
    let mut success = 0;
    for handle in task_handles {
        if let Ok(Ok(_)) = handle.await {
            success += 1;
        }
    }
    assert_eq!(success, 20);
}

// =============================================================================
// Integration Tests: Error Handling
// =============================================================================

#[tokio::test]
async fn test_empty_chunk_with_priority() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    let path = temp_dir.path().join("empty.kub");
    let result = service
        .submit_with_priority(1, Vec::new(), path, SealPriority::Critical)
        .await;

    assert!(matches!(result, Err(SealError::EmptyChunk(1))));
}

#[tokio::test]
async fn test_priority_queue_full_error() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 3);

    // Fill the queue
    queue.push(1, SealPriority::Normal, 1).unwrap();
    queue.push(2, SealPriority::Normal, 2).unwrap();
    queue.push(3, SealPriority::Normal, 3).unwrap();

    // Next push should fail
    let result = queue.push(4, SealPriority::Normal, 4);
    assert!(result.is_err());
}

// =============================================================================
// Integration Tests: Stats and Monitoring
// =============================================================================

#[tokio::test]
async fn test_priority_stats_accumulation() {
    let temp_dir = TempDir::new().unwrap();
    let service = ParallelSealingService::new(ParallelSealingConfig::default());

    // Submit and complete several tasks
    for i in 0..5 {
        let points = create_test_points(100, i, 1000);
        let path = temp_dir.path().join(format!("stats_{}.kub", i));
        let handle = service.submit(i, points, path).await.unwrap();
        handle.await_result().await.unwrap();
    }

    // Check stats
    let stats = service.stats();
    assert_eq!(stats.total_sealed, 5);
    assert_eq!(stats.total_failed, 0);
    assert!(stats.total_bytes_original > 0);
    assert!(stats.compression_ratio > 1.0);
}

#[test]
fn test_priority_queue_stats() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig::default()));
    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add various tasks
    queue.push(1, SealPriority::Critical, 1).unwrap();
    queue.push(2, SealPriority::High, 2).unwrap();
    queue.push(3, SealPriority::Normal, 3).unwrap();

    let stats = queue.stats();
    assert_eq!(stats.current_size, 3);
    assert_eq!(stats.total_pushed, 3);
    assert_eq!(stats.total_popped, 0);

    // Pop one
    queue.pop();
    let stats = queue.stats();
    assert_eq!(stats.current_size, 2);
    assert_eq!(stats.total_popped, 1);
}

// =============================================================================
// Integration Tests: Queue Reordering
// =============================================================================

#[test]
fn test_queue_reorder_on_pressure() {
    let calculator = Arc::new(PriorityCalculator::new(PriorityConfig {
        memory_pressure_threshold: 0.5,
        enable_dynamic_adjustment: true,
        ..Default::default()
    }));

    let queue: PrioritySealQueue<u32> = PrioritySealQueue::new(Arc::clone(&calculator), 100);

    // Add some low priority tasks
    queue.push(1, SealPriority::Low, 1).unwrap();
    queue.push(2, SealPriority::Background, 2).unwrap();

    // Trigger reorder with high pressure
    queue.reorder_on_pressure(0.9);

    let stats = queue.stats();
    assert_eq!(stats.reorders, 1);
    assert_eq!(stats.current_size, 2);
}

// =============================================================================
// Integration Tests: Lifecycle
// =============================================================================

#[tokio::test]
async fn test_service_create_submit_complete_cycle() {
    let temp_dir = TempDir::new().unwrap();

    // Create service
    let service = ParallelSealingService::new(ParallelSealingConfig::default());
    assert!(service.is_priority_enabled());

    // Set some priority overrides
    service.set_series_priority(1, SealPriority::Critical);
    service.set_series_priority(2, SealPriority::Background);

    // Submit multiple tasks
    let mut handles = Vec::new();
    for i in 1..=5 {
        let points = create_test_points(100, i, 1000);
        let path = temp_dir.path().join(format!("lifecycle_{}.kub", i));
        handles.push(service.submit(i, points, path).await.unwrap());
    }

    // Wait for all to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await_result().await.unwrap());
    }

    // Verify results
    assert_eq!(results.len(), 5);
    for result in results {
        assert!(result.point_count > 0);
        assert!(result.compression_ratio > 0.0);
    }

    // Check final stats
    let stats = service.stats();
    assert_eq!(stats.total_sealed, 5);
    assert_eq!(stats.active_seals, 0);
}
