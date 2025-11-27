//! WAL recovery module
//!
//! Handles recovery of data from WAL segments after a crash or restart.
//! Supports both sequential and parallel recovery modes.

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use super::config::WalConfig;
use super::error::{WalError, WalResult};
use super::record::{RecordType, WalRecord};
use super::segment::{list_segments, WalSegment};
use crate::types::DataPoint;

/// WAL recovery handler
///
/// Scans WAL segments and recovers data points from valid records.
pub struct WalRecovery {
    /// Configuration
    config: WalConfig,
    /// Recovery statistics
    stats: RecoveryStats,
}

impl WalRecovery {
    /// Create a new recovery handler
    pub fn new(config: &WalConfig) -> Self {
        Self {
            config: config.clone(),
            stats: RecoveryStats::default(),
        }
    }

    /// Recover all data from WAL segments
    ///
    /// Scans all segments in order and returns recovered data points.
    pub async fn recover_all(&self) -> WalResult<Vec<DataPoint>> {
        let segment_paths = list_segments(&self.config.directory)?;

        if segment_paths.is_empty() {
            info!("No WAL segments found for recovery");
            return Ok(Vec::new());
        }

        info!("Found {} WAL segments for recovery", segment_paths.len());

        let mut all_points = Vec::new();

        for path in &segment_paths {
            match self.recover_segment(path).await {
                Ok(points) => {
                    debug!("Recovered {} points from segment {:?}", points.len(), path);
                    all_points.extend(points);
                }
                Err(e) => {
                    warn!("Error recovering segment {:?}: {}", path, e);
                    // Continue with other segments
                }
            }
        }

        info!(
            "Recovery complete: {} total points from {} segments",
            all_points.len(),
            segment_paths.len()
        );

        Ok(all_points)
    }

    /// Recover data from a single segment
    pub async fn recover_segment(&self, path: &Path) -> WalResult<Vec<DataPoint>> {
        let segment = WalSegment::open(path).await?;
        let records = segment.read_all_records()?;

        let mut points = Vec::new();
        let mut multi_part_buffer: Option<Vec<u8>> = None;

        for record in records {
            // Verify checksum
            if !record.verify_checksum() {
                warn!("Checksum mismatch in segment {:?}, skipping record", path);
                continue;
            }

            // Handle multi-part records
            match record.record_type {
                RecordType::Full => {
                    // Complete record
                    if let Some(buffer) = multi_part_buffer.take() {
                        warn!(
                            "Incomplete multi-part record discarded ({} bytes)",
                            buffer.len()
                        );
                    }
                    match record.get_points() {
                        Ok(pts) => points.extend(pts),
                        Err(e) => {
                            warn!("Failed to decode record: {}", e);
                        }
                    }
                }
                RecordType::First => {
                    // Start of multi-part
                    if multi_part_buffer.is_some() {
                        warn!("Incomplete multi-part record discarded");
                    }
                    multi_part_buffer = Some(record.payload.clone());
                }
                RecordType::Middle => {
                    // Continue multi-part
                    if let Some(ref mut buffer) = multi_part_buffer {
                        buffer.extend_from_slice(&record.payload);
                    } else {
                        warn!("Orphaned middle record");
                    }
                }
                RecordType::Last => {
                    // End of multi-part
                    if let Some(mut buffer) = multi_part_buffer.take() {
                        buffer.extend_from_slice(&record.payload);
                        match WalRecord::deserialize_points(&buffer) {
                            Ok(pts) => points.extend(pts),
                            Err(e) => {
                                warn!("Failed to decode multi-part record: {}", e);
                            }
                        }
                    } else {
                        warn!("Orphaned last record");
                    }
                }
            }
        }

        Ok(points)
    }

    /// Recover using parallel segment processing
    ///
    /// Processes multiple segments concurrently for faster recovery.
    pub async fn recover_parallel(&self, parallelism: usize) -> WalResult<Vec<DataPoint>> {
        let segment_paths = list_segments(&self.config.directory)?;

        if segment_paths.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "Starting parallel recovery of {} segments with parallelism {}",
            segment_paths.len(),
            parallelism
        );

        let results: Arc<Mutex<Vec<Vec<DataPoint>>>> = Arc::new(Mutex::new(Vec::new()));
        let errors: Arc<Mutex<Vec<WalError>>> = Arc::new(Mutex::new(Vec::new()));

        // Create chunks for parallel processing
        let chunk_size = segment_paths.len().div_ceil(parallelism);
        let chunks: Vec<_> = segment_paths.chunks(chunk_size).collect();

        let mut handles = Vec::new();

        for chunk in chunks {
            let paths: Vec<_> = chunk.to_vec();
            let results_clone = Arc::clone(&results);
            let errors_clone = Arc::clone(&errors);
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                let recovery = WalRecovery::new(&config);
                for path in paths {
                    match recovery.recover_segment(&path).await {
                        Ok(points) => {
                            results_clone.lock().push(points);
                        }
                        Err(e) => {
                            errors_clone.lock().push(e);
                        }
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
            warn!("Recovery completed with {} errors", all_errors.len());
        }

        // Flatten results
        let points: Vec<_> = all_results.into_iter().flatten().collect();

        info!("Parallel recovery complete: {} points", points.len());

        Ok(points)
    }

    /// Get recovery statistics
    pub fn stats(&self) -> &RecoveryStats {
        &self.stats
    }

    /// Validate WAL integrity without recovering data
    pub async fn validate(&self) -> WalResult<ValidationReport> {
        let segment_paths = list_segments(&self.config.directory)?;

        let mut report = ValidationReport {
            total_segments: segment_paths.len(),
            valid_segments: 0,
            corrupted_segments: 0,
            total_records: 0,
            valid_records: 0,
            corrupted_records: 0,
            segment_reports: Vec::new(),
        };

        for path in &segment_paths {
            let segment_report = self.validate_segment(path).await;

            if segment_report.is_valid {
                report.valid_segments += 1;
            } else {
                report.corrupted_segments += 1;
            }

            report.total_records += segment_report.total_records;
            report.valid_records += segment_report.valid_records;
            report.corrupted_records += segment_report.corrupted_records;
            report.segment_reports.push(segment_report);
        }

        Ok(report)
    }

    /// Validate a single segment
    async fn validate_segment(&self, path: &Path) -> SegmentReport {
        let segment_id = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        let mut report = SegmentReport {
            segment_id,
            path: path.to_path_buf(),
            is_valid: true,
            total_records: 0,
            valid_records: 0,
            corrupted_records: 0,
            errors: Vec::new(),
        };

        let segment = match WalSegment::open(path).await {
            Ok(s) => s,
            Err(e) => {
                report.is_valid = false;
                report.errors.push(format!("Failed to open: {}", e));
                return report;
            }
        };

        let records = match segment.read_all_records() {
            Ok(r) => r,
            Err(e) => {
                report.is_valid = false;
                report.errors.push(format!("Failed to read: {}", e));
                return report;
            }
        };

        report.total_records = records.len();

        for (i, record) in records.iter().enumerate() {
            if record.verify_checksum() {
                report.valid_records += 1;
            } else {
                report.corrupted_records += 1;
                report
                    .errors
                    .push(format!("Record {} has invalid checksum", i));
                report.is_valid = false;
            }
        }

        report
    }
}

/// Recovery statistics
#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    /// Segments processed
    pub segments_processed: usize,
    /// Records recovered
    pub records_recovered: usize,
    /// Records skipped due to errors
    pub records_skipped: usize,
    /// Points recovered
    pub points_recovered: usize,
    /// Recovery duration in milliseconds
    pub duration_ms: u64,
}

/// Validation report for WAL integrity check
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Total segments checked
    pub total_segments: usize,
    /// Valid segments
    pub valid_segments: usize,
    /// Corrupted segments
    pub corrupted_segments: usize,
    /// Total records across all segments
    pub total_records: usize,
    /// Valid records
    pub valid_records: usize,
    /// Corrupted records
    pub corrupted_records: usize,
    /// Per-segment reports
    pub segment_reports: Vec<SegmentReport>,
}

impl ValidationReport {
    /// Check if all segments are valid
    pub fn is_healthy(&self) -> bool {
        self.corrupted_segments == 0 && self.corrupted_records == 0
    }

    /// Get corruption percentage
    pub fn corruption_rate(&self) -> f64 {
        if self.total_records == 0 {
            0.0
        } else {
            self.corrupted_records as f64 / self.total_records as f64
        }
    }
}

/// Per-segment validation report
#[derive(Debug, Clone)]
pub struct SegmentReport {
    /// Segment identifier
    pub segment_id: String,
    /// Segment path
    pub path: std::path::PathBuf,
    /// Whether segment is valid
    pub is_valid: bool,
    /// Total records in segment
    pub total_records: usize,
    /// Valid records
    pub valid_records: usize,
    /// Corrupted records
    pub corrupted_records: usize,
    /// Error messages
    pub errors: Vec<String>,
}

/// Recovery mode options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryMode {
    /// Stop on first error
    Strict,
    /// Skip corrupted records and continue
    #[default]
    BestEffort,
    /// Repair what can be repaired
    Repair,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::wal::SyncMode;
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> WalConfig {
        WalConfig {
            directory: dir.to_path_buf(),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Immediate,
            write_buffer_size: 1000,
            max_segments: 10,
        }
    }

    #[tokio::test]
    async fn test_recovery_empty() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let recovery = WalRecovery::new(&config);

        let points = recovery.recover_all().await.unwrap();
        assert!(points.is_empty());
    }

    #[tokio::test]
    async fn test_recovery_single_segment() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create a segment with data
        let segment = WalSegment::create(dir.path(), 1, 1024 * 1024)
            .await
            .unwrap();
        let record =
            WalRecord::from_points(&[DataPoint::new(1, 1000, 42.5), DataPoint::new(2, 1001, 43.5)]);
        segment.write_record(&record, 0).unwrap();
        segment.sync().await.unwrap();
        segment.seal().await.unwrap();

        // Recover
        let recovery = WalRecovery::new(&config);
        let points = recovery.recover_all().await.unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].series_id, 1);
        assert_eq!(points[1].series_id, 2);
    }

    #[tokio::test]
    async fn test_recovery_multiple_segments() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create multiple segments
        for seg_id in 1..=3 {
            let segment = WalSegment::create(dir.path(), seg_id, 1024 * 1024)
                .await
                .unwrap();
            let record = WalRecord::from_points(&[DataPoint::new(
                seg_id as u128,
                1000 + seg_id as i64,
                42.5,
            )]);
            segment.write_record(&record, seg_id - 1).unwrap();
            segment.sync().await.unwrap();
            segment.seal().await.unwrap();
        }

        // Recover
        let recovery = WalRecovery::new(&config);
        let points = recovery.recover_all().await.unwrap();

        assert_eq!(points.len(), 3);
    }

    #[tokio::test]
    async fn test_validation() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create a valid segment
        let segment = WalSegment::create(dir.path(), 1, 1024 * 1024)
            .await
            .unwrap();
        let record = WalRecord::from_points(&[DataPoint::new(1, 1000, 42.5)]);
        segment.write_record(&record, 0).unwrap();
        segment.sync().await.unwrap();
        segment.seal().await.unwrap();

        // Validate
        let recovery = WalRecovery::new(&config);
        let report = recovery.validate().await.unwrap();

        assert!(report.is_healthy());
        assert_eq!(report.total_segments, 1);
        assert_eq!(report.valid_segments, 1);
        assert_eq!(report.corrupted_segments, 0);
    }

    #[tokio::test]
    async fn test_parallel_recovery() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // Create multiple segments
        for seg_id in 1..=4 {
            let segment = WalSegment::create(dir.path(), seg_id, 1024 * 1024)
                .await
                .unwrap();
            let record = WalRecord::from_points(&[DataPoint::new(
                seg_id as u128,
                1000 + seg_id as i64,
                42.5,
            )]);
            segment.write_record(&record, seg_id - 1).unwrap();
            segment.sync().await.unwrap();
            segment.seal().await.unwrap();
        }

        // Parallel recovery
        let recovery = WalRecovery::new(&config);
        let points = recovery.recover_parallel(2).await.unwrap();

        assert_eq!(points.len(), 4);
    }

    #[test]
    fn test_recovery_mode_default() {
        assert_eq!(RecoveryMode::default(), RecoveryMode::BestEffort);
    }
}
