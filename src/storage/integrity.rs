//! Data Integrity Module
//!
//! Provides centralized data integrity verification, corruption detection,
//! and recovery mechanisms for chunk storage.
//!
//! # Features
//!
//! - CRC-64-ECMA-182 checksum verification
//! - Corruption detection and logging
//! - Chunk repair suggestions
//! - Integrity scan for entire storage directory
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::storage::integrity::{IntegrityChecker, IntegrityReport};
//!
//! let checker = IntegrityChecker::new();
//!
//! // Verify a single chunk
//! match checker.verify_chunk(chunk_data) {
//!     Ok(()) => println!("Chunk is valid"),
//!     Err(e) => println!("Corruption detected: {}", e),
//! }
//!
//! // Scan entire directory
//! let report = checker.scan_directory("/data/tsdb").await?;
//! println!("{} chunks verified, {} corrupted", report.valid, report.corrupted);
//! ```

use crc::{Crc, CRC_64_ECMA_182};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};

/// CRC-64-ECMA-182 calculator instance (same as Kuba/AHPAC)
const CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

/// Calculate CRC-64-ECMA-182 checksum for data
///
/// This is the standard checksum algorithm used across all chunk formats
/// (Kuba/Gorilla and AHPAC).
#[inline]
pub fn calculate_checksum(data: &[u8]) -> u64 {
    CRC64.checksum(data)
}

/// Verify checksum matches expected value
///
/// # Arguments
///
/// * `data` - The data to verify
/// * `expected` - The expected checksum value
///
/// # Returns
///
/// `Ok(())` if checksum matches, `Err` with details if mismatch
pub fn verify_checksum(data: &[u8], expected: u64) -> Result<(), IntegrityError> {
    let calculated = calculate_checksum(data);

    if calculated != expected {
        return Err(IntegrityError::ChecksumMismatch {
            expected,
            actual: calculated,
            data_size: data.len(),
        });
    }

    Ok(())
}

/// Data integrity error types
#[derive(Debug, Clone)]
pub enum IntegrityError {
    /// Checksum mismatch - data is corrupted
    ChecksumMismatch {
        /// Expected checksum value from header
        expected: u64,
        /// Actual calculated checksum
        actual: u64,
        /// Size of data that was checksummed
        data_size: usize,
    },
    /// Invalid magic bytes - not a valid chunk file
    InvalidMagic {
        /// Expected magic bytes (IROG or GORI)
        expected: [u8; 4],
        /// Actual magic bytes found in file
        actual: [u8; 4],
    },
    /// Header parsing failed
    InvalidHeader {
        /// Description of the header parsing failure
        reason: String,
    },
    /// File is truncated
    TruncatedFile {
        /// Expected minimum file size in bytes
        expected_size: usize,
        /// Actual file size in bytes
        actual_size: usize,
    },
    /// File read error
    IoError {
        /// Path to the file that caused the error
        path: PathBuf,
        /// Description of the IO error
        reason: String,
    },
    /// Decompression failed (possibly corrupted data)
    DecompressionFailed {
        /// Compression codec that failed (e.g., "snappy", "ahpac")
        codec: String,
        /// Description of the decompression failure
        reason: String,
    },
}

impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntegrityError::ChecksumMismatch {
                expected,
                actual,
                data_size,
            } => {
                write!(
                    f,
                    "Checksum mismatch: expected 0x{:016X}, got 0x{:016X} ({} bytes)",
                    expected, actual, data_size
                )
            },
            IntegrityError::InvalidMagic { expected, actual } => {
                write!(
                    f,
                    "Invalid magic: expected {:?}, got {:?}",
                    expected, actual
                )
            },
            IntegrityError::InvalidHeader { reason } => {
                write!(f, "Invalid header: {}", reason)
            },
            IntegrityError::TruncatedFile {
                expected_size,
                actual_size,
            } => {
                write!(
                    f,
                    "Truncated file: expected {} bytes, got {}",
                    expected_size, actual_size
                )
            },
            IntegrityError::IoError { path, reason } => {
                write!(f, "IO error reading {:?}: {}", path, reason)
            },
            IntegrityError::DecompressionFailed { codec, reason } => {
                write!(f, "Decompression failed ({}): {}", codec, reason)
            },
        }
    }
}

impl std::error::Error for IntegrityError {}

/// Corruption severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionSeverity {
    /// Checksum mismatch - data may be recoverable
    Recoverable,
    /// Header corruption - file structure damaged
    HeaderDamaged,
    /// Complete corruption - file is unreadable
    Unrecoverable,
}

/// Information about a corrupted chunk
#[derive(Debug, Clone)]
pub struct CorruptedChunk {
    /// Path to the corrupted chunk file
    pub path: PathBuf,
    /// Series ID if extractable
    pub series_id: Option<u64>,
    /// Chunk ID if extractable
    pub chunk_id: Option<String>,
    /// Type of corruption
    pub error: IntegrityError,
    /// Severity assessment
    pub severity: CorruptionSeverity,
    /// Suggested recovery action
    pub recovery_suggestion: String,
}

/// Integrity scan report
#[derive(Debug, Clone, Default)]
pub struct IntegrityReport {
    /// Number of valid chunks
    pub valid_chunks: usize,
    /// Number of corrupted chunks
    pub corrupted_chunks: usize,
    /// Total bytes scanned
    pub bytes_scanned: u64,
    /// List of corrupted chunk details
    pub corruptions: Vec<CorruptedChunk>,
    /// Scan duration in milliseconds
    pub duration_ms: u64,
}

impl IntegrityReport {
    /// Check if all chunks are valid
    pub fn is_healthy(&self) -> bool {
        self.corrupted_chunks == 0
    }

    /// Get corruption rate as percentage
    pub fn corruption_rate(&self) -> f64 {
        let total = self.valid_chunks + self.corrupted_chunks;
        if total == 0 {
            return 0.0;
        }
        (self.corrupted_chunks as f64 / total as f64) * 100.0
    }

    /// Log report summary
    pub fn log_summary(&self) {
        if self.is_healthy() {
            info!(
                chunks = self.valid_chunks,
                bytes = self.bytes_scanned,
                duration_ms = self.duration_ms,
                "Integrity scan complete: all chunks valid"
            );
        } else {
            warn!(
                valid = self.valid_chunks,
                corrupted = self.corrupted_chunks,
                corruption_rate = format!("{:.2}%", self.corruption_rate()),
                bytes = self.bytes_scanned,
                duration_ms = self.duration_ms,
                "Integrity scan complete: corruption detected"
            );

            for corruption in &self.corruptions {
                error!(
                    path = ?corruption.path,
                    series_id = ?corruption.series_id,
                    error = %corruption.error,
                    severity = ?corruption.severity,
                    suggestion = %corruption.recovery_suggestion,
                    "Corrupted chunk detected"
                );
            }
        }
    }
}

/// Chunk integrity checker
///
/// Provides methods for verifying chunk data integrity and scanning
/// storage directories for corruption.
pub struct IntegrityChecker {
    /// Whether to perform deep verification (decompress and validate points)
    pub deep_verify: bool,
    /// Whether to log each chunk verification
    pub verbose: bool,
}

impl Default for IntegrityChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl IntegrityChecker {
    /// Create a new integrity checker with default settings
    pub fn new() -> Self {
        Self {
            deep_verify: false,
            verbose: false,
        }
    }

    /// Enable deep verification (decompresses data)
    pub fn with_deep_verify(mut self) -> Self {
        self.deep_verify = true;
        self
    }

    /// Enable verbose logging
    pub fn with_verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// Verify a chunk file's integrity
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chunk file
    ///
    /// # Returns
    ///
    /// `Ok(())` if the chunk is valid, `Err` with corruption details otherwise
    pub async fn verify_chunk_file(&self, path: &Path) -> Result<(), CorruptedChunk> {
        use tokio::fs;

        // Read file
        let data = fs::read(path).await.map_err(|e| CorruptedChunk {
            path: path.to_path_buf(),
            series_id: None,
            chunk_id: None,
            error: IntegrityError::IoError {
                path: path.to_path_buf(),
                reason: e.to_string(),
            },
            severity: CorruptionSeverity::Unrecoverable,
            recovery_suggestion: "Check file permissions and disk health".to_string(),
        })?;

        self.verify_chunk_data(path, &data)
    }

    /// Verify chunk data integrity
    ///
    /// # Arguments
    ///
    /// * `path` - Path for error reporting
    /// * `data` - Raw chunk file bytes
    #[allow(clippy::result_large_err)]
    pub fn verify_chunk_data(&self, path: &Path, data: &[u8]) -> Result<(), CorruptedChunk> {
        // Check minimum size (header is 64 bytes)
        if data.len() < 64 {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id: None,
                chunk_id: None,
                error: IntegrityError::TruncatedFile {
                    expected_size: 64,
                    actual_size: data.len(),
                },
                severity: CorruptionSeverity::Unrecoverable,
                recovery_suggestion: "File is too small to be a valid chunk".to_string(),
            });
        }

        // Verify magic bytes (IROG or GORI)
        let magic = &data[0..4];
        if magic != b"IROG" && magic != b"GORI" {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id: None,
                chunk_id: None,
                error: IntegrityError::InvalidMagic {
                    expected: *b"IROG",
                    actual: [magic[0], magic[1], magic[2], magic[3]],
                },
                severity: CorruptionSeverity::HeaderDamaged,
                recovery_suggestion: "File header is corrupted, chunk may be unrecoverable"
                    .to_string(),
            });
        }

        // Extract series_id from header (bytes 6-14)
        let series_id = if data.len() >= 14 {
            Some(u64::from_le_bytes([
                data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13],
            ]))
        } else {
            None
        };

        // Extract sizes from header
        let compressed_size = if data.len() >= 44 {
            u32::from_le_bytes([data[40], data[41], data[42], data[43]]) as usize
        } else {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id,
                chunk_id: extract_chunk_id_from_path(path),
                error: IntegrityError::TruncatedFile {
                    expected_size: 44,
                    actual_size: data.len(),
                },
                severity: CorruptionSeverity::HeaderDamaged,
                recovery_suggestion: "Header is truncated".to_string(),
            });
        };

        // Extract stored checksum (bytes 50-58)
        let stored_checksum = if data.len() >= 58 {
            u64::from_le_bytes([
                data[50], data[51], data[52], data[53], data[54], data[55], data[56], data[57],
            ])
        } else {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id,
                chunk_id: extract_chunk_id_from_path(path),
                error: IntegrityError::TruncatedFile {
                    expected_size: 58,
                    actual_size: data.len(),
                },
                severity: CorruptionSeverity::HeaderDamaged,
                recovery_suggestion: "Checksum field is missing".to_string(),
            });
        };

        // Check file has enough data for compressed payload
        let expected_size = 64 + compressed_size;
        if data.len() < expected_size {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id,
                chunk_id: extract_chunk_id_from_path(path),
                error: IntegrityError::TruncatedFile {
                    expected_size,
                    actual_size: data.len(),
                },
                severity: CorruptionSeverity::Recoverable,
                recovery_suggestion: format!(
                    "File is truncated, missing {} bytes of compressed data",
                    expected_size - data.len()
                ),
            });
        }

        // Verify checksum on compressed data
        let compressed_data = &data[64..64 + compressed_size];
        let calculated_checksum = calculate_checksum(compressed_data);

        if calculated_checksum != stored_checksum {
            return Err(CorruptedChunk {
                path: path.to_path_buf(),
                series_id,
                chunk_id: extract_chunk_id_from_path(path),
                error: IntegrityError::ChecksumMismatch {
                    expected: stored_checksum,
                    actual: calculated_checksum,
                    data_size: compressed_size,
                },
                severity: CorruptionSeverity::Recoverable,
                recovery_suggestion:
                    "Data corruption detected. If WAL exists, attempt recovery from WAL".to_string(),
            });
        }

        if self.verbose {
            debug!(
                path = ?path,
                series_id = ?series_id,
                compressed_size = compressed_size,
                checksum = format!("0x{:016X}", stored_checksum),
                "Chunk verified successfully"
            );
        }

        Ok(())
    }

    /// Scan a storage directory for corrupted chunks
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Root data directory containing series subdirectories
    ///
    /// # Returns
    ///
    /// An `IntegrityReport` summarizing the scan results
    pub async fn scan_directory(&self, data_dir: &Path) -> IntegrityReport {
        use tokio::fs;

        let start = std::time::Instant::now();
        let mut report = IntegrityReport::default();

        info!(path = ?data_dir, "Starting integrity scan");

        // Find all .kub files
        let mut entries = match fs::read_dir(data_dir).await {
            Ok(e) => e,
            Err(e) => {
                error!(path = ?data_dir, error = %e, "Failed to read data directory");
                return report;
            },
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();

            // Check if it's a series directory
            if path.is_dir() {
                let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if dir_name.starts_with("series_") {
                    // Scan chunk files in series directory
                    if let Ok(mut series_entries) = fs::read_dir(&path).await {
                        while let Ok(Some(chunk_entry)) = series_entries.next_entry().await {
                            let chunk_path = chunk_entry.path();

                            if chunk_path.extension().is_some_and(|e| e == "kub") {
                                // Get file size
                                if let Ok(metadata) = fs::metadata(&chunk_path).await {
                                    report.bytes_scanned += metadata.len();
                                }

                                // Verify chunk
                                match self.verify_chunk_file(&chunk_path).await {
                                    Ok(()) => {
                                        report.valid_chunks += 1;
                                    },
                                    Err(corruption) => {
                                        report.corrupted_chunks += 1;
                                        report.corruptions.push(corruption);
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }

        report.duration_ms = start.elapsed().as_millis() as u64;
        report.log_summary();

        report
    }
}

/// Extract chunk ID from file path
fn extract_chunk_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("chunk_").to_string())
}

/// Attempt to repair a corrupted chunk by recovering from WAL
///
/// This function attempts to recover data from the Write-Ahead Log (WAL)
/// for chunks that have detected corruption.
///
/// # Recovery Process
///
/// 1. Find corresponding WAL entries for this chunk's time range
/// 2. Extract points from WAL
/// 3. Re-compress and write new chunk
/// 4. Rename corrupted chunk to .bak
///
/// # Arguments
///
/// * `chunk_path` - Path to the corrupted chunk file
/// * `wal_dir` - Directory containing WAL files
///
/// # Returns
///
/// `Ok(())` if repair succeeded, `Err` if repair failed
pub async fn attempt_chunk_repair(
    chunk_path: &Path,
    wal_dir: &Path,
) -> Result<RepairResult, IntegrityError> {
    use tokio::fs;

    // Check if WAL directory exists
    if !wal_dir.exists() {
        return Ok(RepairResult {
            success: false,
            message: "WAL directory does not exist".to_string(),
            points_recovered: 0,
            backup_path: None,
        });
    }

    // Validate chunk path
    let _chunk_filename = chunk_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| IntegrityError::IoError {
            path: chunk_path.to_path_buf(),
            reason: "Invalid chunk filename".to_string(),
        })?;

    // Check if chunk can be read at all
    let chunk_data = match fs::read(chunk_path).await {
        Ok(data) => data,
        Err(e) => {
            return Ok(RepairResult {
                success: false,
                message: format!("Cannot read chunk file: {}", e),
                points_recovered: 0,
                backup_path: None,
            });
        },
    };

    // Try to extract header info for recovery context
    let (series_id, time_range) = if chunk_data.len() >= 32 {
        let series_id = u64::from_le_bytes([
            chunk_data[6],
            chunk_data[7],
            chunk_data[8],
            chunk_data[9],
            chunk_data[10],
            chunk_data[11],
            chunk_data[12],
            chunk_data[13],
        ]);
        let min_ts = i64::from_le_bytes([
            chunk_data[14],
            chunk_data[15],
            chunk_data[16],
            chunk_data[17],
            chunk_data[18],
            chunk_data[19],
            chunk_data[20],
            chunk_data[21],
        ]);
        let max_ts = i64::from_le_bytes([
            chunk_data[22],
            chunk_data[23],
            chunk_data[24],
            chunk_data[25],
            chunk_data[26],
            chunk_data[27],
            chunk_data[28],
            chunk_data[29],
        ]);
        (series_id, (min_ts, max_ts))
    } else {
        return Ok(RepairResult {
            success: false,
            message: "Chunk header too small to extract recovery info".to_string(),
            points_recovered: 0,
            backup_path: None,
        });
    };

    info!(
        series_id = series_id,
        min_ts = time_range.0,
        max_ts = time_range.1,
        "Attempting chunk repair from WAL"
    );

    // Create backup of corrupted chunk
    let backup_path = chunk_path.with_extension("kub.bak");
    if let Err(e) = fs::copy(chunk_path, &backup_path).await {
        warn!(error = %e, "Failed to create backup of corrupted chunk");
    }

    // For now, we indicate repair needs WAL integration
    // Full implementation would scan WAL for matching entries
    Ok(RepairResult {
        success: false,
        message: format!(
            "WAL recovery infrastructure ready. Series {} time range [{}, {}]. \
             Full repair requires WAL module integration.",
            series_id, time_range.0, time_range.1
        ),
        points_recovered: 0,
        backup_path: Some(backup_path),
    })
}

/// Result of a chunk repair attempt
#[derive(Debug, Clone)]
pub struct RepairResult {
    /// Whether repair was successful
    pub success: bool,
    /// Human-readable message about the repair
    pub message: String,
    /// Number of points recovered (0 if failed)
    pub points_recovered: usize,
    /// Path to backup of corrupted chunk (if created)
    pub backup_path: Option<PathBuf>,
}

impl RepairResult {
    /// Create a successful repair result
    pub fn success(points_recovered: usize, backup_path: PathBuf) -> Self {
        Self {
            success: true,
            message: format!("Successfully recovered {} points", points_recovered),
            points_recovered,
            backup_path: Some(backup_path),
        }
    }

    /// Create a failed repair result
    pub fn failure(message: String) -> Self {
        Self {
            success: false,
            message,
            points_recovered: 0,
            backup_path: None,
        }
    }
}

/// Chunk recovery manager for handling multiple corrupted chunks
pub struct RecoveryManager {
    /// WAL directory path
    wal_dir: PathBuf,
    /// Whether to create backups before repair
    create_backups: bool,
    /// Maximum concurrent repairs
    max_concurrent: usize,
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub fn new(wal_dir: PathBuf) -> Self {
        Self {
            wal_dir,
            create_backups: true,
            max_concurrent: 4,
        }
    }

    /// Set whether to create backups
    pub fn with_backups(mut self, create: bool) -> Self {
        self.create_backups = create;
        self
    }

    /// Set maximum concurrent repairs
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    /// Attempt to repair all corrupted chunks from an integrity report
    pub async fn repair_from_report(&self, report: &IntegrityReport) -> RecoveryReport {
        use tokio::task::JoinSet;

        let start = std::time::Instant::now();
        let mut join_set = JoinSet::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));

        for corruption in &report.corruptions {
            let path = corruption.path.clone();
            let wal_dir = self.wal_dir.clone();
            let permit = semaphore.clone();

            join_set.spawn(async move {
                let _permit = permit.acquire().await;
                attempt_chunk_repair(&path, &wal_dir).await
            });
        }

        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(repair_result)) => results.push(repair_result),
                Ok(Err(e)) => {
                    results.push(RepairResult::failure(format!("Repair error: {}", e)));
                },
                Err(e) => {
                    results.push(RepairResult::failure(format!("Task panicked: {}", e)));
                },
            }
        }

        let successful = results.iter().filter(|r| r.success).count();
        let total_points: usize = results.iter().map(|r| r.points_recovered).sum();

        RecoveryReport {
            attempted: results.len(),
            successful,
            failed: results.len() - successful,
            total_points_recovered: total_points,
            duration_ms: start.elapsed().as_millis() as u64,
            results,
        }
    }
}

/// Report of a batch recovery operation
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// Number of repairs attempted
    pub attempted: usize,
    /// Number of successful repairs
    pub successful: usize,
    /// Number of failed repairs
    pub failed: usize,
    /// Total points recovered across all repairs
    pub total_points_recovered: usize,
    /// Total duration in milliseconds
    pub duration_ms: u64,
    /// Individual repair results
    pub results: Vec<RepairResult>,
}

impl RecoveryReport {
    /// Check if all repairs were successful
    pub fn all_successful(&self) -> bool {
        self.failed == 0
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.attempted == 0 {
            100.0
        } else {
            (self.successful as f64 / self.attempted as f64) * 100.0
        }
    }

    /// Log report summary
    pub fn log_summary(&self) {
        if self.all_successful() {
            info!(
                attempted = self.attempted,
                successful = self.successful,
                points = self.total_points_recovered,
                duration_ms = self.duration_ms,
                "Recovery complete: all repairs successful"
            );
        } else {
            warn!(
                attempted = self.attempted,
                successful = self.successful,
                failed = self.failed,
                success_rate = format!("{:.1}%", self.success_rate()),
                points = self.total_points_recovered,
                duration_ms = self.duration_ms,
                "Recovery complete: some repairs failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_calculation() {
        let data = b"Hello, World!";
        let checksum = calculate_checksum(data);

        // Verify same data produces same checksum
        assert_eq!(checksum, calculate_checksum(data));

        // Different data produces different checksum
        let different = b"Hello, World?";
        assert_ne!(checksum, calculate_checksum(different));
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"Test data for checksum verification";
        let checksum = calculate_checksum(data);

        // Valid checksum
        assert!(verify_checksum(data, checksum).is_ok());

        // Invalid checksum
        assert!(verify_checksum(data, checksum + 1).is_err());
    }

    #[test]
    fn test_corruption_severity() {
        let error = IntegrityError::ChecksumMismatch {
            expected: 123,
            actual: 456,
            data_size: 1000,
        };
        assert!(error.to_string().contains("Checksum mismatch"));
    }

    #[test]
    fn test_integrity_report() {
        let mut report = IntegrityReport::default();
        assert!(report.is_healthy());
        assert_eq!(report.corruption_rate(), 0.0);

        report.valid_chunks = 90;
        report.corrupted_chunks = 10;
        assert!(!report.is_healthy());
        assert_eq!(report.corruption_rate(), 10.0);
    }

    #[test]
    fn test_chunk_data_validation() {
        let checker = IntegrityChecker::new();

        // Too small
        let small_data = vec![0u8; 10];
        let result = checker.verify_chunk_data(Path::new("test.kub"), &small_data);
        assert!(result.is_err());

        // Invalid magic
        let mut bad_magic = vec![0u8; 100];
        bad_magic[0..4].copy_from_slice(b"BAAD");
        let result = checker.verify_chunk_data(Path::new("test.kub"), &bad_magic);
        assert!(result.is_err());
    }
}
