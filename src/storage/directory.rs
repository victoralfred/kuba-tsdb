//! Directory management utilities for storage layer
//!
//! Provides utilities for managing series directories, metadata files,
//! write locks, and cleanup operations.
use crate::error::StorageError;
use crate::types::SeriesId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Series metadata stored in metadata.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesMetadata {
    /// Series identifier
    pub series_id: SeriesId,

    /// Series name (optional)
    pub name: Option<String>,

    /// Retention policy in days (0 = infinite)
    pub retention_days: u32,

    /// Maximum points per chunk
    pub max_points_per_chunk: u32,

    /// Compression algorithm
    pub compression: String,

    /// Creation timestamp (unix milliseconds)
    pub created_at: i64,

    /// Last modified timestamp (unix milliseconds)
    pub modified_at: i64,

    /// Total point count (approximate)
    pub total_points: u64,

    /// Total chunks
    pub total_chunks: u32,
}

impl SeriesMetadata {
    /// Create new series metadata with defaults
    pub fn new(series_id: SeriesId) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            series_id,
            name: None,
            retention_days: 0, // Infinite retention by default
            max_points_per_chunk: 10_000,
            compression: "gorilla".to_string(),
            created_at: now,
            modified_at: now,
            total_points: 0,
            total_chunks: 0,
        }
    }

    /// Load metadata from file
    pub async fn load(path: &Path) -> Result<Self, StorageError> {
        let contents = fs::read_to_string(path).await?;
        serde_json::from_str(&contents).map_err(|e| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse metadata: {}", e),
            ))
        })
    }

    /// Save metadata to file (atomic write)
    ///
    /// Uses a unique temporary file per operation to avoid race conditions
    /// when multiple concurrent saves target the same path.
    pub async fn save(&self, path: &Path) -> Result<(), StorageError> {
        let contents = serde_json::to_string_pretty(self).map_err(|e| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize metadata: {}", e),
            ))
        })?;

        // Atomic write: write to unique temp file, then rename
        // Use process ID and random suffix to ensure uniqueness across concurrent saves
        let temp_filename = format!(
            ".metadata.{}.{}.tmp",
            std::process::id(),
            rand::random::<u32>()
        );
        let temp_path = path.parent().map_or_else(
            || std::path::PathBuf::from(&temp_filename),
            |p| p.join(&temp_filename),
        );

        fs::write(&temp_path, contents).await?;

        // Rename is atomic on POSIX systems - last writer wins
        let rename_result = fs::rename(&temp_path, path).await;

        // Clean up temp file if rename failed (best effort)
        if rename_result.is_err() {
            let _ = fs::remove_file(&temp_path).await;
        }

        rename_result?;
        Ok(())
    }

    /// Update modification timestamp
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
    }
}

/// Configuration for write lock behavior
#[derive(Debug, Clone)]
pub struct WriteLockConfig {
    /// Timeout in milliseconds before considering a lock stale
    pub stale_timeout_ms: i64,
}

impl Default for WriteLockConfig {
    fn default() -> Self {
        Self {
            // 5 minutes default, configurable via environment variable
            stale_timeout_ms: std::env::var("TSDB_LOCK_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300_000),
        }
    }
}

/// Write lock for preventing concurrent writes to a series
///
/// Uses atomic file creation (O_EXCL) to prevent TOCTOU race conditions.
/// On Unix systems, also performs process liveness checks for stale lock detection.
pub struct WriteLock {
    path: PathBuf,
    held: bool,
    config: WriteLockConfig,
    #[cfg(unix)]
    lock_file: Option<std::fs::File>,
}

impl WriteLock {
    /// Create a new write lock with default configuration
    pub fn new(series_path: &Path) -> Self {
        Self::with_config(series_path, WriteLockConfig::default())
    }

    /// Create a new write lock with custom configuration
    pub fn with_config(series_path: &Path, config: WriteLockConfig) -> Self {
        Self {
            path: series_path.join(".write.lock"),
            held: false,
            config,
            #[cfg(unix)]
            lock_file: None,
        }
    }

    /// Try to acquire the write lock atomically
    ///
    /// Uses O_CREAT | O_EXCL for atomic lock acquisition, preventing TOCTOU races.
    /// Returns error if lock is already held by another process.
    pub async fn try_acquire(&mut self) -> Result<(), StorageError> {
        use std::time::Instant;

        let start = Instant::now();

        if self.held {
            return Ok(()); // Already held
        }

        // First, check for stale locks and clean them up
        if let Err(e) = self.cleanup_stale_lock().await {
            // Log but don't fail on cleanup errors
            eprintln!("Warning: Failed to check for stale locks: {}", e);
        }

        // Try to create lock file atomically using O_EXCL
        let pid = std::process::id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let lock_content = format!("{}:{}", pid, now);

        let acquire_start = Instant::now();

        // Use std::fs for synchronous O_EXCL operation (more reliable than tokio)
        #[cfg(unix)]
        let result = {
            use std::os::unix::fs::OpenOptionsExt;

            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true) // O_CREAT | O_EXCL - atomic!
                .mode(0o644)
                .open(&self.path)
            {
                Ok(mut file) => {
                    // Successfully created lock file atomically
                    use std::io::Write;
                    file.write_all(lock_content.as_bytes())
                        .map_err(StorageError::Io)?;
                    file.sync_all().map_err(StorageError::Io)?;

                    self.lock_file = Some(file);
                    self.held = true;
                    Ok(())
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Lock already held - record contention
                    crate::metrics::LOCK_WAIT_DURATION
                        .with_label_values(&["write_lock_contention"])
                        .observe(acquire_start.elapsed().as_secs_f64());

                    Err(StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Write lock is already held by another process",
                    )))
                }
                Err(e) => Err(StorageError::Io(e)),
            }
        };

        #[cfg(not(unix))]
        let result = {
            // Non-Unix systems: use create_new (still atomic)
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true) // Atomic on most platforms
                .open(&self.path)
                .await
            {
                Ok(mut file) => {
                    file.write_all(lock_content.as_bytes()).await?;
                    file.sync_all().await?;
                    self.held = true;
                    Ok(())
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Lock already held - record contention
                    crate::metrics::LOCK_WAIT_DURATION
                        .with_label_values(&["write_lock_contention"])
                        .observe(acquire_start.elapsed().as_secs_f64());

                    Err(StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "Write lock is already held by another process",
                    )))
                }
                Err(e) => Err(StorageError::Io(e)),
            }
        };

        // Record total acquisition time
        if result.is_ok() {
            crate::metrics::LOCK_WAIT_DURATION
                .with_label_values(&["write_lock_acquired"])
                .observe(start.elapsed().as_secs_f64());
        }

        result
    }

    /// Check for and clean up stale locks
    async fn cleanup_stale_lock(&self) -> Result<(), StorageError> {
        // Check if lock file exists
        if !self.path.exists() {
            return Ok(());
        }

        // Try to read lock file
        let contents = match fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(_) => return Ok(()), // Can't read, leave it alone
        };

        // Parse PID and timestamp
        let (pid_str, timestamp_str) = match contents.split_once(':') {
            Some(parts) => parts,
            None => return Ok(()), // Malformed, can't validate
        };

        let (pid, timestamp) = match (pid_str.parse::<u32>(), timestamp_str.parse::<i64>()) {
            (Ok(p), Ok(t)) => (p, t),
            _ => return Ok(()), // Can't parse, leave it alone
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Check if lock is stale (configurable timeout)
        if now - timestamp > self.config.stale_timeout_ms {
            // Stale lock, remove it
            let _ = fs::remove_file(&self.path).await;
            return Ok(());
        }

        // On Unix, check if process is still alive
        #[cfg(unix)]
        {
            use std::process::Command;
            let is_alive = Command::new("ps")
                .arg("-p")
                .arg(pid.to_string())
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);

            if !is_alive {
                // Process is dead, remove stale lock
                let _ = fs::remove_file(&self.path).await;
            }
        }

        Ok(())
    }

    /// Release the write lock
    pub async fn release(&mut self) -> Result<(), StorageError> {
        if !self.held {
            return Ok(());
        }

        #[cfg(unix)]
        {
            // Close file handle first
            self.lock_file = None;
        }

        fs::remove_file(&self.path).await?;
        self.held = false;
        Ok(())
    }

    /// Check if lock is held
    pub fn is_held(&self) -> bool {
        self.held
    }

    /// Get lock configuration
    pub fn config(&self) -> &WriteLockConfig {
        &self.config
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        if self.held {
            #[cfg(unix)]
            {
                // Close file handle
                self.lock_file = None;
            }

            // Best effort cleanup
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Directory cleanup operations
pub struct DirectoryMaintenance;

impl DirectoryMaintenance {
    /// Clean up old chunks based on retention policy
    ///
    /// Removes chunks older than the retention period specified in metadata
    pub async fn cleanup_old_chunks(
        series_path: &Path,
        metadata: &SeriesMetadata,
    ) -> Result<usize, StorageError> {
        if metadata.retention_days == 0 {
            return Ok(0); // Infinite retention
        }

        let retention_ms = (metadata.retention_days as i64) * 24 * 60 * 60 * 1000;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let cutoff = now - retention_ms;

        let mut removed_count = 0;
        let mut entries = fs::read_dir(series_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Only process .gor files
            if path.extension().and_then(|e| e.to_str()) != Some("gor") {
                continue;
            }

            // Check file creation time
            if let Ok(file_metadata) = fs::metadata(&path).await {
                if let Ok(created) = file_metadata.created() {
                    if let Ok(created_duration) = created.duration_since(std::time::UNIX_EPOCH) {
                        let created_ms = created_duration.as_millis() as i64;
                        if created_ms < cutoff {
                            // Remove old chunk file
                            fs::remove_file(&path).await?;
                            removed_count += 1;

                            // Also remove associated .snappy file if exists
                            let snappy_path = path.with_extension("snappy");
                            if snappy_path.exists() {
                                let _ = fs::remove_file(&snappy_path).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Validate directory structure
    ///
    /// Checks for corrupted files, orphaned files, and missing metadata
    pub async fn validate_directory(series_path: &Path) -> Result<Vec<String>, StorageError> {
        let mut issues = Vec::new();

        // Check if directory exists
        if !series_path.exists() {
            issues.push(format!(
                "Series directory does not exist: {:?}",
                series_path
            ));
            return Ok(issues);
        }

        // Check for metadata.json
        let metadata_path = series_path.join("metadata.json");
        if !metadata_path.exists() {
            issues.push("Missing metadata.json file".to_string());
        }

        // Scan for chunk files
        let mut entries = fs::read_dir(series_path).await?;
        let mut chunk_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                chunk_count += 1;

                // Try to read header
                if let Ok(mut file) = fs::File::open(&path).await {
                    let mut header_bytes = [0u8; 64];
                    if file.read_exact(&mut header_bytes).await.is_err() {
                        issues.push(format!("Corrupted chunk file: {:?}", path));
                    }
                }
            }
        }

        if chunk_count == 0 {
            issues.push("No chunk files found in directory".to_string());
        }

        Ok(issues)
    }

    /// Remove empty series directories
    pub async fn cleanup_empty_directories(base_path: &Path) -> Result<usize, StorageError> {
        let mut removed_count = 0;
        let mut entries = fs::read_dir(base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            // Check if directory name starts with "series_"
            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                if dir_name.starts_with("series_") {
                    // Check if directory is empty or only contains lock/metadata files
                    if Self::is_series_directory_empty(&path).await? {
                        fs::remove_dir_all(&path).await?;
                        removed_count += 1;
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Check if a series directory is empty (no chunk files)
    async fn is_series_directory_empty(series_path: &Path) -> Result<bool, StorageError> {
        let mut entries = fs::read_dir(series_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_name = path.file_name().and_then(|n| n.to_str());

            // Ignore lock and metadata files
            if let Some(name) = file_name {
                if name == ".write.lock" || name == "metadata.json" {
                    continue;
                }
            }

            // If we find any other file, directory is not empty
            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_series_metadata_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("metadata.json");

        let mut metadata = SeriesMetadata::new(123);
        metadata.name = Some("test_series".to_string());
        metadata.retention_days = 30;
        metadata.total_points = 1000;

        // Save
        metadata.save(&metadata_path).await.unwrap();

        // Load
        let loaded = SeriesMetadata::load(&metadata_path).await.unwrap();

        assert_eq!(loaded.series_id, 123);
        assert_eq!(loaded.name, Some("test_series".to_string()));
        assert_eq!(loaded.retention_days, 30);
        assert_eq!(loaded.total_points, 1000);
    }

    #[tokio::test]
    async fn test_write_lock_acquire_release() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock = WriteLock::new(temp_dir.path());

        // Acquire lock
        lock.try_acquire().await.unwrap();
        assert!(lock.is_held());
        assert!(temp_dir.path().join(".write.lock").exists());

        // Release lock
        lock.release().await.unwrap();
        assert!(!lock.is_held());
        assert!(!temp_dir.path().join(".write.lock").exists());
    }

    #[tokio::test]
    async fn test_write_lock_prevents_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock1 = WriteLock::new(temp_dir.path());
        let mut lock2 = WriteLock::new(temp_dir.path());

        // First lock acquires successfully
        lock1.try_acquire().await.unwrap();

        // Second lock should fail
        let result = lock2.try_acquire().await;
        assert!(result.is_err());

        // After releasing first lock, second should succeed
        lock1.release().await.unwrap();
        lock2.try_acquire().await.unwrap();
        lock2.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_old_chunks() {
        use std::time::Duration;
        use tokio::time::sleep;

        let temp_dir = TempDir::new().unwrap();

        // Create some chunk files
        fs::write(temp_dir.path().join("chunk_old.gor"), b"old")
            .await
            .unwrap();
        sleep(Duration::from_millis(10)).await;
        fs::write(temp_dir.path().join("chunk_new.gor"), b"new")
            .await
            .unwrap();

        // Create metadata with 0 retention (should not delete anything)
        let metadata = SeriesMetadata::new(1);
        let removed = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata)
            .await
            .unwrap();
        assert_eq!(removed, 0);

        // Both files should still exist
        assert!(temp_dir.path().join("chunk_old.gor").exists());
        assert!(temp_dir.path().join("chunk_new.gor").exists());
    }

    #[tokio::test]
    async fn test_validate_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Empty directory should have issues
        let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
            .await
            .unwrap();
        assert!(!issues.is_empty());

        // Add metadata
        let metadata = SeriesMetadata::new(1);
        metadata
            .save(&temp_dir.path().join("metadata.json"))
            .await
            .unwrap();

        // Still should have issues (no chunks)
        let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
            .await
            .unwrap();
        assert!(issues.iter().any(|i| i.contains("No chunk files")));
    }
}
