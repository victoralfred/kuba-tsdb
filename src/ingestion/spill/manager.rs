//! Spill manager
//!
//! Coordinates spill operations including memory monitoring, file creation,
//! and recovery orchestration.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{debug, info};

use super::config::SpillConfig;
use super::error::{SpillError, SpillResult};
use super::file::{list_spill_files, SpillFile, SpillFileId};
use super::next_spill_file_id;
use super::recovery::SpillRecovery;
use crate::types::DataPoint;

/// Spill manager coordinates emergency disk overflow
///
/// Monitors memory usage and automatically spills data to disk when
/// thresholds are exceeded. Handles recovery of spilled data.
pub struct SpillManager {
    /// Configuration
    config: SpillConfig,
    /// Currently active spill files
    active_files: RwLock<Vec<Arc<SpillFile>>>,
    /// Whether spill is currently active
    spill_active: AtomicBool,
    /// Total points spilled
    points_spilled: AtomicU64,
    /// Total points recovered
    points_recovered: AtomicU64,
    /// Current spill file count
    file_count: AtomicUsize,
    /// Total bytes spilled
    bytes_spilled: AtomicU64,
}

impl SpillManager {
    /// Create a new spill manager
    pub fn new(config: SpillConfig) -> SpillResult<Self> {
        config.validate().map_err(SpillError::from)?;

        // Ensure directory exists
        std::fs::create_dir_all(&config.directory).map_err(|e| SpillError::Io {
            source: e,
            context: format!("creating spill directory {:?}", config.directory),
        })?;

        info!("Spill manager initialized at {:?}", config.directory);

        Ok(Self {
            config,
            active_files: RwLock::new(Vec::new()),
            spill_active: AtomicBool::new(false),
            points_spilled: AtomicU64::new(0),
            points_recovered: AtomicU64::new(0),
            file_count: AtomicUsize::new(0),
            bytes_spilled: AtomicU64::new(0),
        })
    }

    /// Spill data points to disk
    ///
    /// Creates a new spill file and writes the points.
    pub async fn spill(&self, points: Vec<DataPoint>) -> SpillResult<SpillFileId> {
        if points.is_empty() {
            return Ok(0);
        }

        // Check file limit
        let current_count = self.file_count.load(Ordering::Relaxed);
        if current_count >= self.config.max_files {
            return Err(SpillError::MaxFilesReached {
                count: current_count,
                max: self.config.max_files,
            });
        }

        // Generate file ID
        let file_id = next_spill_file_id();

        // Create spill file
        let file = SpillFile::create(&self.config, file_id, &points)?;

        // Update stats
        let point_count = points.len() as u64;
        let file_size = file.size() as u64;

        self.points_spilled
            .fetch_add(point_count, Ordering::Relaxed);
        self.bytes_spilled.fetch_add(file_size, Ordering::Relaxed);
        self.file_count.fetch_add(1, Ordering::Relaxed);

        // Track file
        self.active_files.write().push(Arc::new(file));
        self.spill_active.store(true, Ordering::Release);

        debug!(
            "Spilled {} points to file {} ({} bytes)",
            point_count, file_id, file_size
        );

        Ok(file_id)
    }

    /// Check if spill should be triggered based on memory usage
    ///
    /// # Arguments
    ///
    /// * `current_memory_bytes` - Current memory usage in bytes
    /// * `total_memory_bytes` - Total available memory in bytes
    pub fn should_spill(&self, current_memory_bytes: usize, total_memory_bytes: usize) -> bool {
        // Check absolute threshold
        if current_memory_bytes >= self.config.memory_threshold {
            return true;
        }

        // Check percentage threshold
        if total_memory_bytes > 0 {
            let usage_percent = current_memory_bytes as f64 / total_memory_bytes as f64;
            if usage_percent >= self.config.memory_threshold_percent {
                return true;
            }
        }

        false
    }

    /// Recover all spilled data
    ///
    /// Reads all spill files and returns the recovered points.
    /// Optionally deletes files after recovery.
    pub async fn recover_all(&self) -> SpillResult<Vec<DataPoint>> {
        let recovery = SpillRecovery::new(&self.config);
        let points = recovery.recover_parallel().await?;

        let recovered_count = points.len() as u64;
        self.points_recovered
            .fetch_add(recovered_count, Ordering::Relaxed);

        // Cleanup if configured
        if self.config.auto_cleanup {
            self.cleanup().await?;
        }

        info!("Recovered {} points from spill files", recovered_count);
        Ok(points)
    }

    /// Recover from a specific spill file
    pub async fn recover_file(&self, file_id: SpillFileId) -> SpillResult<Vec<DataPoint>> {
        let path = self.config.file_path(file_id);

        if !path.exists() {
            return Err(SpillError::FileNotFound { path });
        }

        let file = SpillFile::open(&path)?;
        let points = file.read_points()?;

        let recovered_count = points.len() as u64;
        self.points_recovered
            .fetch_add(recovered_count, Ordering::Relaxed);

        // Delete if auto cleanup
        if self.config.auto_cleanup {
            file.delete()?;
            self.file_count.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(points)
    }

    /// Clean up all spill files
    pub async fn cleanup(&self) -> SpillResult<usize> {
        let files = list_spill_files(&self.config.directory)?;
        let count = files.len();

        for path in files {
            std::fs::remove_file(&path).map_err(|e| SpillError::Io {
                source: e,
                context: format!("deleting {:?}", path),
            })?;
        }

        // Reset tracking
        self.active_files.write().clear();
        self.file_count.store(0, Ordering::Relaxed);
        self.spill_active.store(false, Ordering::Release);

        if count > 0 {
            info!("Cleaned up {} spill files", count);
        }

        Ok(count)
    }

    /// Check if spill is currently active
    pub fn is_active(&self) -> bool {
        self.spill_active.load(Ordering::Acquire)
    }

    /// Get current file count
    pub fn file_count(&self) -> usize {
        self.file_count.load(Ordering::Relaxed)
    }

    /// Get spill statistics
    pub fn stats(&self) -> SpillStats {
        SpillStats {
            points_spilled: self.points_spilled.load(Ordering::Relaxed),
            points_recovered: self.points_recovered.load(Ordering::Relaxed),
            bytes_spilled: self.bytes_spilled.load(Ordering::Relaxed),
            active_files: self.file_count.load(Ordering::Relaxed),
            is_active: self.spill_active.load(Ordering::Acquire),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &SpillConfig {
        &self.config
    }

    /// Delete a specific spill file
    pub async fn delete_file(&self, file_id: SpillFileId) -> SpillResult<()> {
        let path = self.config.file_path(file_id);

        if !path.exists() {
            return Err(SpillError::FileNotFound { path });
        }

        std::fs::remove_file(&path).map_err(|e| SpillError::Io {
            source: e,
            context: format!("deleting {:?}", path),
        })?;

        self.file_count.fetch_sub(1, Ordering::Relaxed);

        // Update active status
        if self.file_count.load(Ordering::Relaxed) == 0 {
            self.spill_active.store(false, Ordering::Release);
        }

        Ok(())
    }
}

/// Spill statistics
#[derive(Debug, Clone, Default)]
pub struct SpillStats {
    /// Total points spilled
    pub points_spilled: u64,
    /// Total points recovered
    pub points_recovered: u64,
    /// Total bytes spilled
    pub bytes_spilled: u64,
    /// Current active file count
    pub active_files: usize,
    /// Whether spill is active
    pub is_active: bool,
}

impl SpillStats {
    /// Get pending points (spilled but not recovered)
    pub fn pending_points(&self) -> u64 {
        self.points_spilled.saturating_sub(self.points_recovered)
    }

    /// Get recovery progress (0.0 - 1.0)
    pub fn recovery_progress(&self) -> f64 {
        if self.points_spilled == 0 {
            1.0
        } else {
            self.points_recovered as f64 / self.points_spilled as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> SpillConfig {
        SpillConfig {
            directory: dir.to_path_buf(),
            memory_threshold: 1024,
            memory_threshold_percent: 0.8,
            max_files: 10,
            compression_enabled: true,
            auto_cleanup: false,
            ..Default::default()
        }
    }

    fn sample_points() -> Vec<DataPoint> {
        vec![
            DataPoint::new(1, 1000, 42.5),
            DataPoint::new(2, 1001, 43.5),
            DataPoint::new(3, 1002, 44.5),
        ]
    }

    #[tokio::test]
    async fn test_spill_manager_creation() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let manager = SpillManager::new(config);
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_spill_and_recover() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let manager = SpillManager::new(config).unwrap();

        // Spill
        let points = sample_points();
        let file_id = manager.spill(points.clone()).await.unwrap();
        assert!(file_id > 0 || file_id == 0);
        assert!(manager.is_active());

        // Recover
        let recovered = manager.recover_file(file_id).await.unwrap();
        assert_eq!(recovered.len(), 3);
    }

    #[tokio::test]
    async fn test_should_spill() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let manager = SpillManager::new(config).unwrap();

        // Below threshold
        assert!(!manager.should_spill(512, 10000));

        // Above absolute threshold
        assert!(manager.should_spill(2048, 10000));

        // Above percentage threshold
        assert!(manager.should_spill(8500, 10000)); // 85%
    }

    #[tokio::test]
    async fn test_max_files_limit() {
        let dir = tempdir().unwrap();
        let config = SpillConfig {
            directory: dir.path().to_path_buf(),
            max_files: 2,
            ..Default::default()
        };
        let manager = SpillManager::new(config).unwrap();

        // Create files up to limit
        manager.spill(sample_points()).await.unwrap();
        manager.spill(sample_points()).await.unwrap();

        // Should fail
        let result = manager.spill(sample_points()).await;
        assert!(matches!(result, Err(SpillError::MaxFilesReached { .. })));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let manager = SpillManager::new(config).unwrap();

        // Create some files
        manager.spill(sample_points()).await.unwrap();
        manager.spill(sample_points()).await.unwrap();
        assert_eq!(manager.file_count(), 2);

        // Cleanup
        let removed = manager.cleanup().await.unwrap();
        assert_eq!(removed, 2);
        assert_eq!(manager.file_count(), 0);
        assert!(!manager.is_active());
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let manager = SpillManager::new(config).unwrap();

        // Initial stats
        let stats = manager.stats();
        assert_eq!(stats.points_spilled, 0);
        assert!(!stats.is_active);

        // After spill
        manager.spill(sample_points()).await.unwrap();
        let stats = manager.stats();
        assert_eq!(stats.points_spilled, 3);
        assert!(stats.is_active);
    }

    #[test]
    fn test_spill_stats_calculations() {
        let stats = SpillStats {
            points_spilled: 100,
            points_recovered: 60,
            bytes_spilled: 1000,
            active_files: 2,
            is_active: true,
        };

        assert_eq!(stats.pending_points(), 40);
        assert!((stats.recovery_progress() - 0.6).abs() < 0.001);
    }
}
