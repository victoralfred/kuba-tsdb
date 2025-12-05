//! Spill recovery module
//!
//! Handles recovery of data from spill files with support for
//! parallel recovery for improved performance.

use std::sync::Arc;

use parking_lot::Mutex;
use tracing::{debug, warn};

use super::config::SpillConfig;
use super::error::{SpillError, SpillResult};
use super::file::{list_spill_files, SpillFile};
use crate::types::DataPoint;

/// Spill recovery handler
///
/// Recovers data from spill files with configurable parallelism.
pub struct SpillRecovery {
    /// Configuration
    config: SpillConfig,
}

impl SpillRecovery {
    /// Create a new recovery handler
    pub fn new(config: &SpillConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Recover all data from spill files sequentially
    pub async fn recover_all(&self) -> SpillResult<Vec<DataPoint>> {
        let file_paths = list_spill_files(&self.config.directory)?;

        if file_paths.is_empty() {
            debug!("No spill files found for recovery");
            return Ok(Vec::new());
        }

        debug!("Found {} spill files for recovery", file_paths.len());

        let mut all_points = Vec::new();

        for path in &file_paths {
            match SpillFile::open(path) {
                Ok(file) => match file.read_points() {
                    Ok(points) => {
                        debug!("Recovered {} points from {:?}", points.len(), path);
                        all_points.extend(points);
                    },
                    Err(e) => {
                        warn!("Error reading spill file {:?}: {}", path, e);
                    },
                },
                Err(e) => {
                    warn!("Error opening spill file {:?}: {}", path, e);
                },
            }
        }

        debug!(
            "Sequential recovery complete: {} points from {} files",
            all_points.len(),
            file_paths.len()
        );

        Ok(all_points)
    }

    /// Recover all data using parallel processing
    pub async fn recover_parallel(&self) -> SpillResult<Vec<DataPoint>> {
        let file_paths = list_spill_files(&self.config.directory)?;

        if file_paths.is_empty() {
            return Ok(Vec::new());
        }

        let parallelism = self.config.recovery_parallelism;
        debug!(
            "Starting parallel recovery of {} files with {} workers",
            file_paths.len(),
            parallelism
        );

        let results: Arc<Mutex<Vec<Vec<DataPoint>>>> = Arc::new(Mutex::new(Vec::new()));
        let errors: Arc<Mutex<Vec<SpillError>>> = Arc::new(Mutex::new(Vec::new()));

        // Create chunks for parallel processing
        let chunk_size = file_paths.len().div_ceil(parallelism);
        let chunks: Vec<_> = file_paths.chunks(chunk_size).collect();

        let mut handles = Vec::new();

        for chunk in chunks {
            let paths: Vec<_> = chunk.to_vec();
            let results_clone = Arc::clone(&results);
            let errors_clone = Arc::clone(&errors);

            let handle = tokio::spawn(async move {
                for path in paths {
                    match SpillFile::open(&path) {
                        Ok(file) => match file.read_points() {
                            Ok(points) => {
                                results_clone.lock().push(points);
                            },
                            Err(e) => {
                                errors_clone.lock().push(e);
                            },
                        },
                        Err(e) => {
                            errors_clone.lock().push(e);
                        },
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            if let Err(e) = handle.await {
                warn!("Recovery task panicked: {}", e);
            }
        }

        // Collect results
        let all_results = Arc::try_unwrap(results).unwrap().into_inner();
        let all_errors = Arc::try_unwrap(errors).unwrap().into_inner();

        if !all_errors.is_empty() {
            warn!(
                "Parallel recovery completed with {} errors",
                all_errors.len()
            );
        }

        // Flatten results
        let points: Vec<_> = all_results.into_iter().flatten().collect();

        debug!("Parallel recovery complete: {} points", points.len());

        Ok(points)
    }

    /// Validate all spill files without recovering data
    pub async fn validate(&self) -> SpillResult<ValidationReport> {
        let file_paths = list_spill_files(&self.config.directory)?;

        let mut report = ValidationReport {
            total_files: file_paths.len(),
            valid_files: 0,
            corrupted_files: 0,
            total_points: 0,
            file_reports: Vec::new(),
        };

        for path in &file_paths {
            let file_report = match SpillFile::open(path) {
                Ok(file) => {
                    // Try reading points to verify integrity
                    match file.read_points() {
                        Ok(points) => {
                            report.valid_files += 1;
                            report.total_points += points.len();
                            FileReport {
                                path: path.clone(),
                                is_valid: true,
                                point_count: points.len(),
                                size: file.size(),
                                error: None,
                            }
                        },
                        Err(e) => {
                            report.corrupted_files += 1;
                            FileReport {
                                path: path.clone(),
                                is_valid: false,
                                point_count: 0,
                                size: file.size(),
                                error: Some(e.to_string()),
                            }
                        },
                    }
                },
                Err(e) => {
                    report.corrupted_files += 1;
                    FileReport {
                        path: path.clone(),
                        is_valid: false,
                        point_count: 0,
                        size: 0,
                        error: Some(e.to_string()),
                    }
                },
            };

            report.file_reports.push(file_report);
        }

        Ok(report)
    }
}

/// Validation report for spill files
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Total files checked
    pub total_files: usize,
    /// Valid files
    pub valid_files: usize,
    /// Corrupted files
    pub corrupted_files: usize,
    /// Total recoverable points
    pub total_points: usize,
    /// Per-file reports
    pub file_reports: Vec<FileReport>,
}

impl ValidationReport {
    /// Check if all files are valid
    pub fn is_healthy(&self) -> bool {
        self.corrupted_files == 0
    }

    /// Get corruption rate
    pub fn corruption_rate(&self) -> f64 {
        if self.total_files == 0 {
            0.0
        } else {
            self.corrupted_files as f64 / self.total_files as f64
        }
    }
}

/// Per-file validation report
#[derive(Debug, Clone)]
pub struct FileReport {
    /// File path
    pub path: std::path::PathBuf,
    /// Whether file is valid
    pub is_valid: bool,
    /// Number of points in file
    pub point_count: usize,
    /// File size in bytes
    pub size: usize,
    /// Error message if corrupted
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> SpillConfig {
        SpillConfig {
            directory: dir.to_path_buf(),
            recovery_parallelism: 2,
            ..Default::default()
        }
    }

    fn sample_points() -> Vec<DataPoint> {
        vec![DataPoint::new(1, 1000, 42.5), DataPoint::new(2, 1001, 43.5)]
    }

    #[tokio::test]
    async fn test_recovery_empty() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let recovery = SpillRecovery::new(&config);

        let points = recovery.recover_all().await.unwrap();
        assert!(points.is_empty());
    }

    #[tokio::test]
    async fn test_sequential_recovery() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create some spill files
        SpillFile::create(&config, 1, &sample_points()).unwrap();
        SpillFile::create(&config, 2, &sample_points()).unwrap();

        let recovery = SpillRecovery::new(&config);
        let points = recovery.recover_all().await.unwrap();

        assert_eq!(points.len(), 4); // 2 points * 2 files
    }

    #[tokio::test]
    async fn test_parallel_recovery() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create multiple spill files
        for i in 1..=4 {
            SpillFile::create(&config, i, &sample_points()).unwrap();
        }

        let recovery = SpillRecovery::new(&config);
        let points = recovery.recover_parallel().await.unwrap();

        assert_eq!(points.len(), 8); // 2 points * 4 files
    }

    #[tokio::test]
    async fn test_validation() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create valid spill files
        SpillFile::create(&config, 1, &sample_points()).unwrap();
        SpillFile::create(&config, 2, &sample_points()).unwrap();

        let recovery = SpillRecovery::new(&config);
        let report = recovery.validate().await.unwrap();

        assert!(report.is_healthy());
        assert_eq!(report.valid_files, 2);
        assert_eq!(report.total_points, 4);
    }

    #[test]
    fn test_validation_report() {
        let report = ValidationReport {
            total_files: 10,
            valid_files: 8,
            corrupted_files: 2,
            total_points: 1000,
            file_reports: Vec::new(),
        };

        assert!(!report.is_healthy());
        assert!((report.corruption_rate() - 0.2).abs() < 0.001);
    }
}
