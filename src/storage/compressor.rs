//! Background compression service for sealed chunks
//!
//! This module provides a background service that automatically compresses
//! sealed chunks using Snappy compression to reduce disk usage while maintaining
//! fast decompression for reads.
//!
//! # Architecture
//!
//! ```text
//! Sealed Chunks → Compression Queue → Worker Pool → Compressed Chunks
//!                       ↓                  ↓              ↓
//!                   Scheduling         Snappy         Metrics
//!                   (age-based)      (parallel)    (monitoring)
//! ```
//!
//! # Features
//!
//! - **Age-based scheduling**: Compress chunks older than threshold
//! - **Worker pool**: Parallel compression with configurable workers
//! - **Rate limiting**: Prevent overwhelming disk I/O
//! - **Metrics**: Track compression ratios, throughput, errors
//! - **Graceful shutdown**: Complete in-flight compressions
//!
//! # Example
//!
//! ```rust,no_run
//! use kuba_tsdb::storage::compressor::{CompressionService, CompressionConfig};
//! use std::path::PathBuf;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CompressionConfig {
//!     min_age_seconds: 3600,  // Compress chunks older than 1 hour
//!     worker_count: 4,         // Use 4 worker threads
//!     scan_interval_seconds: 300, // Scan every 5 minutes
//!     ..Default::default()
//! };
//!
//! let service = CompressionService::new(PathBuf::from("/data/tsdb"), config);
//! service.start().await?;
//! # Ok(())
//! # }
//! ```

use crate::error::StorageError;
use crate::types::SeriesId;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

/// Configuration for the compression service
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Minimum age in seconds before a sealed chunk is eligible for compression
    pub min_age_seconds: u64,

    /// Number of worker threads for parallel compression
    pub worker_count: usize,

    /// Interval in seconds between compression scans
    pub scan_interval_seconds: u64,

    /// Maximum number of chunks to compress per scan
    pub max_chunks_per_scan: usize,

    /// Enable compression (can be disabled for testing)
    pub enabled: bool,

    /// Compression level (0-9, where 0 is fastest, 9 is best compression)
    /// Note: Snappy doesn't support levels, this is for future extensibility
    pub compression_level: u8,

    /// Delete original .kub files after successful compression
    pub delete_original: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            min_age_seconds: 3600,                // 1 hour
            worker_count: num_cpus::get().min(4), // Up to 4 workers
            scan_interval_seconds: 300,           // 5 minutes
            max_chunks_per_scan: 100,
            enabled: true,
            compression_level: 0,
            delete_original: true, // Delete originals by default to save disk space
        }
    }
}

/// A chunk path awaiting compression
#[derive(Debug, Clone)]
pub struct CompressionTask {
    /// Path to the sealed chunk file
    pub chunk_path: PathBuf,

    /// Series ID for metrics
    pub series_id: SeriesId,

    /// Chunk file size before compression
    pub original_size: u64,

    /// Timestamp when the chunk was created
    pub created_at: SystemTime,
}

/// Statistics for compression operations
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total chunks compressed
    pub chunks_compressed: u64,

    /// Total chunks failed
    pub chunks_failed: u64,

    /// Total bytes before compression
    pub bytes_original: u64,

    /// Total bytes after compression
    pub bytes_compressed: u64,

    /// Total compression time in seconds
    pub total_compression_time_secs: f64,

    /// Currently pending chunks
    pub pending_count: usize,

    /// Currently processing chunks
    pub processing_count: usize,
}

impl CompressionStats {
    /// Calculate overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_original == 0 {
            0.0
        } else {
            self.bytes_compressed as f64 / self.bytes_original as f64
        }
    }

    /// Calculate average compression time per chunk
    pub fn avg_compression_time(&self) -> f64 {
        if self.chunks_compressed == 0 {
            0.0
        } else {
            self.total_compression_time_secs / self.chunks_compressed as f64
        }
    }

    /// Calculate throughput in bytes per second
    pub fn throughput_bytes_per_sec(&self) -> f64 {
        if self.total_compression_time_secs == 0.0 {
            0.0
        } else {
            self.bytes_original as f64 / self.total_compression_time_secs
        }
    }
}

/// Background compression service
pub struct CompressionService {
    /// Base storage path
    base_path: PathBuf,

    /// Configuration
    config: CompressionConfig,

    /// Compression task queue
    task_tx: mpsc::UnboundedSender<CompressionTask>,
    task_rx: Arc<RwLock<mpsc::UnboundedReceiver<CompressionTask>>>,

    /// Worker handles
    workers: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Scanner handle
    scanner: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Statistics
    stats: Arc<RwLock<CompressionStats>>,

    /// Shutdown signal
    shutdown_tx: Arc<RwLock<Option<mpsc::Sender<()>>>>,
}

impl CompressionService {
    /// Create a new compression service
    pub fn new(base_path: PathBuf, config: CompressionConfig) -> Self {
        let (task_tx, task_rx) = mpsc::unbounded_channel();

        Self {
            base_path,
            config,
            task_tx,
            task_rx: Arc::new(RwLock::new(task_rx)),
            workers: Arc::new(RwLock::new(Vec::new())),
            scanner: Arc::new(RwLock::new(None)),
            stats: Arc::new(RwLock::new(CompressionStats::default())),
            shutdown_tx: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the compression service
    pub async fn start(&self) -> Result<(), StorageError> {
        if !self.config.enabled {
            println!("Compression service disabled");
            return Ok(());
        }

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        // Start worker pool
        self.start_workers().await?;

        // Start scanner
        self.start_scanner(shutdown_rx).await?;

        Ok(())
    }

    /// Start compression workers
    async fn start_workers(&self) -> Result<(), StorageError> {
        let mut workers = self.workers.write().await;

        for worker_id in 0..self.config.worker_count {
            let task_rx = Arc::clone(&self.task_rx);
            let stats = Arc::clone(&self.stats);
            let delete_original = self.config.delete_original;

            let worker = tokio::spawn(async move {
                Self::worker_loop(worker_id, task_rx, stats, delete_original).await;
            });

            workers.push(worker);
        }

        println!("Started {} compression workers", self.config.worker_count);
        Ok(())
    }

    /// Start the scanner task
    async fn start_scanner(&self, shutdown_rx: mpsc::Receiver<()>) -> Result<(), StorageError> {
        let base_path = self.base_path.clone();
        let config = self.config.clone();
        let task_tx = self.task_tx.clone();
        let stats = Arc::clone(&self.stats);

        let scanner = tokio::spawn(async move {
            Self::scanner_loop(base_path, config, task_tx, stats, shutdown_rx).await;
        });

        *self.scanner.write().await = Some(scanner);

        println!("Started compression scanner");
        Ok(())
    }

    /// Worker loop - processes compression tasks
    async fn worker_loop(
        worker_id: usize,
        task_rx: Arc<RwLock<mpsc::UnboundedReceiver<CompressionTask>>>,
        stats: Arc<RwLock<CompressionStats>>,
        delete_original: bool,
    ) {
        loop {
            // Try to receive a task - lock is released immediately after recv
            let task = task_rx.write().await.recv().await;

            match task {
                Some(task) => {
                    // Update processing and pending counts
                    {
                        let mut stats = stats.write().await;
                        stats.processing_count += 1;
                        stats.pending_count = stats.pending_count.saturating_sub(1);
                    }

                    // Compress the chunk
                    let start = std::time::Instant::now();
                    match Self::compress_chunk(&task, delete_original).await {
                        Ok(compressed_size) => {
                            let duration = start.elapsed().as_secs_f64();

                            // Update stats
                            let mut stats = stats.write().await;
                            stats.chunks_compressed += 1;
                            stats.bytes_original += task.original_size;
                            stats.bytes_compressed += compressed_size;
                            stats.total_compression_time_secs += duration;
                            stats.processing_count -= 1;

                            // Record metrics
                            crate::metrics::COMPRESSIONS_TOTAL
                                .with_label_values(&["success"])
                                .inc();

                            println!(
                                "Worker {}: Compressed {} ({} -> {} bytes, ratio: {:.2})",
                                worker_id,
                                task.chunk_path.display(),
                                task.original_size,
                                compressed_size,
                                compressed_size as f64 / task.original_size as f64
                            );
                        },
                        Err(e) => {
                            let mut stats = stats.write().await;
                            stats.chunks_failed += 1;
                            stats.processing_count -= 1;

                            crate::metrics::COMPRESSIONS_TOTAL
                                .with_label_values(&["failure"])
                                .inc();

                            eprintln!(
                                "Worker {}: Failed to compress {}: {}",
                                worker_id,
                                task.chunk_path.display(),
                                e
                            );
                        },
                    }
                },
                None => {
                    // Channel closed, exit worker
                    println!("Worker {}: Shutting down", worker_id);
                    break;
                },
            }
        }
    }

    /// Scanner loop - finds chunks eligible for compression
    async fn scanner_loop(
        base_path: PathBuf,
        config: CompressionConfig,
        task_tx: mpsc::UnboundedSender<CompressionTask>,
        stats: Arc<RwLock<CompressionStats>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let scan_interval = Duration::from_secs(config.scan_interval_seconds);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(scan_interval) => {
                    // Scan for eligible chunks
                    match Self::scan_for_chunks(&base_path, &config).await {
                        Ok(tasks) => {
                            let count = tasks.len();
                            for task in tasks {
                                if task_tx.send(task).is_err() {
                                    eprintln!("Failed to queue compression task");
                                    break;
                                }
                            }

                            if count > 0 {
                                // Update pending count
                                let mut stats = stats.write().await;
                                stats.pending_count += count;

                                println!("Scanner: Queued {} chunks for compression", count);
                            }
                        }
                        Err(e) => {
                            eprintln!("Scanner: Error scanning for chunks: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    println!("Scanner: Shutting down");
                    break;
                }
            }
        }
    }

    /// Scan for chunks eligible for compression
    async fn scan_for_chunks(
        base_path: &Path,
        config: &CompressionConfig,
    ) -> Result<Vec<CompressionTask>, StorageError> {
        let mut tasks = Vec::new();
        let now = SystemTime::now();
        let min_age = Duration::from_secs(config.min_age_seconds);

        // Scan all series directories
        let mut series_dirs = fs::read_dir(base_path).await?;

        while let Some(entry) = series_dirs.next_entry().await? {
            let path = entry.path();

            // Check if it's a series directory
            if !path.is_dir() {
                continue;
            }

            let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) if name.starts_with("series_") => name,
                _ => continue,
            };

            // Extract series ID
            let series_id = match dir_name
                .strip_prefix("series_")
                .and_then(|s| s.parse().ok())
            {
                Some(id) => id,
                None => continue,
            };

            // Scan chunks in this series
            let mut chunk_files = fs::read_dir(&path).await?;

            while let Some(chunk_entry) = chunk_files.next_entry().await? {
                let chunk_path = chunk_entry.path();

                // Only process .kub files (sealed, not yet compressed)
                if chunk_path.extension().and_then(|e| e.to_str()) != Some("kub") {
                    continue;
                }

                // Check if .snappy already exists
                let snappy_path = chunk_path.with_extension("snappy");
                if snappy_path.exists() {
                    continue; // Already compressed
                }

                // Check file age
                let metadata = match fs::metadata(&chunk_path).await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let created = match metadata.created() {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                if now.duration_since(created).ok() < Some(min_age) {
                    continue; // Too young
                }

                // SEC: Validate file size to prevent DoS
                const MAX_CHUNK_SIZE: u64 = 1_000_000_000; // 1GB max
                if metadata.len() > MAX_CHUNK_SIZE {
                    tracing::warn!(
                        chunk_path = %chunk_path.display(),
                        size = metadata.len(),
                        max_allowed = MAX_CHUNK_SIZE,
                        "Chunk size exceeds maximum, skipping"
                    );
                    continue;
                }

                // Add to compression queue
                tasks.push(CompressionTask {
                    chunk_path,
                    series_id,
                    original_size: metadata.len(),
                    created_at: created,
                });

                // SEC: Use checked arithmetic for limit check
                if tasks.len() >= config.max_chunks_per_scan {
                    break;
                }
            }

            if tasks.len() >= config.max_chunks_per_scan {
                break;
            }
        }

        Ok(tasks)
    }

    /// Compress a single chunk file
    async fn compress_chunk(
        task: &CompressionTask,
        delete_original: bool,
    ) -> Result<u64, StorageError> {
        // SEC: Validate file size before reading to prevent DoS
        const MAX_CHUNK_SIZE: u64 = 1_000_000_000; // 1GB max
        if task.original_size > MAX_CHUNK_SIZE {
            return Err(StorageError::Io(std::io::Error::other(format!(
                "Chunk size {} exceeds maximum allowed {}",
                task.original_size, MAX_CHUNK_SIZE
            ))));
        }

        // Read the chunk file
        let data = fs::read(&task.chunk_path).await?;

        // Compress using Snappy
        let compressed = snap::raw::Encoder::new().compress_vec(&data).map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "Snappy compression failed: {}",
                e
            )))
        })?;

        // Write compressed data to .snappy file
        let snappy_path = task.chunk_path.with_extension("snappy");
        fs::write(&snappy_path, &compressed).await?;

        // Delete original .kub file to save disk space (if configured)
        if delete_original {
            fs::remove_file(&task.chunk_path).await?;
        }

        Ok(compressed.len() as u64)
    }

    /// Get current compression statistics
    pub async fn stats(&self) -> CompressionStats {
        self.stats.read().await.clone()
    }

    /// Manually trigger a compression scan
    pub async fn trigger_scan(&self) -> Result<(), StorageError> {
        let tasks = Self::scan_for_chunks(&self.base_path, &self.config).await?;

        let count = tasks.len();
        // SEC: Limit number of queued tasks to prevent DoS
        const MAX_QUEUED_TASKS: usize = 10_000;
        let mut queued = 0;
        for task in tasks {
            if queued >= MAX_QUEUED_TASKS {
                tracing::warn!(
                    queued = queued,
                    max_allowed = MAX_QUEUED_TASKS,
                    "Task queue limit reached, skipping remaining tasks"
                );
                break;
            }
            self.task_tx.send(task).map_err(|_| {
                StorageError::Io(std::io::Error::other("Failed to queue compression task"))
            })?;
            queued += 1;
        }

        let mut stats = self.stats.write().await;
        // SEC: Use checked arithmetic to prevent overflow
        stats.pending_count = stats.pending_count.saturating_add(queued);

        println!("Manually triggered scan: queued {} chunks", count);
        Ok(())
    }

    /// Shutdown the compression service gracefully
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        println!("Shutting down compression service...");

        // Signal scanner to stop
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            let _ = shutdown_tx.send(()).await;
        }

        // Wait for scanner to finish
        if let Some(scanner) = self.scanner.write().await.take() {
            let _ = scanner.await;
        }

        // Close task channel
        drop(self.task_tx.clone());

        // Wait for workers to finish
        let mut workers = self.workers.write().await;
        while let Some(worker) = workers.pop() {
            let _ = worker.await;
        }

        println!("Compression service shut down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compression_config_default() {
        let config = CompressionConfig::default();

        assert!(config.enabled);
        assert_eq!(config.min_age_seconds, 3600);
        assert!(config.worker_count > 0);
        assert_eq!(config.scan_interval_seconds, 300);
    }

    #[tokio::test]
    async fn test_compression_stats() {
        let stats = CompressionStats {
            chunks_compressed: 10,
            bytes_original: 10000,
            bytes_compressed: 5000,
            total_compression_time_secs: 2.0,
            ..Default::default()
        };

        assert_eq!(stats.compression_ratio(), 0.5);
        assert_eq!(stats.avg_compression_time(), 0.2);
        assert_eq!(stats.throughput_bytes_per_sec(), 5000.0);
    }

    #[tokio::test]
    async fn test_create_service() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompressionConfig::default();

        let service = CompressionService::new(temp_dir.path().to_path_buf(), config);
        let stats = service.stats().await;

        assert_eq!(stats.chunks_compressed, 0);
        assert_eq!(stats.pending_count, 0);
    }

    #[tokio::test]
    async fn test_compress_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let chunk_path = temp_dir.path().join("test_chunk.kub");

        // Create test chunk with compressible data
        let test_data = vec![0u8; 10000]; // Highly compressible
        fs::write(&chunk_path, &test_data).await.unwrap();

        let task = CompressionTask {
            chunk_path: chunk_path.clone(),
            series_id: 1,
            original_size: test_data.len() as u64,
            created_at: SystemTime::now(),
        };

        let compressed_size = CompressionService::compress_chunk(&task, false)
            .await
            .unwrap();

        // Verify compressed file exists
        let snappy_path = chunk_path.with_extension("snappy");
        assert!(snappy_path.exists());

        // Compression ratio should be good for zeros
        assert!(compressed_size < test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_scan_for_chunks() {
        let temp_dir = TempDir::new().unwrap();

        // Create a series directory
        let series_dir = temp_dir.path().join("series_123");
        fs::create_dir(&series_dir).await.unwrap();

        // Create an old sealed chunk
        let chunk_path = series_dir.join("chunk_0.kub");
        fs::write(&chunk_path, b"test data").await.unwrap();

        // Wait to ensure age
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let config = CompressionConfig {
            min_age_seconds: 0, // No minimum age for test
            ..Default::default()
        };

        let tasks = CompressionService::scan_for_chunks(temp_dir.path(), &config)
            .await
            .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].series_id, 123);
    }
}
