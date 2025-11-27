//! Async Ingestion Pipeline for high-throughput data ingestion
//!
//! This module provides a lock-free, async ingestion pipeline capable of
//! processing >2M points/second with automatic batching and backpressure.
//!
//! # Architecture
//!
//! ```text
//! [Input] → [Batcher] → [Buffer Manager] → [Writer] → [Storage]
//!              ↓              ↓               ↓
//!          [Metrics]     [Backpressure]   [Metrics]
//! ```
//!
//! # Components
//!
//! - **Batch**: Point batching with configurable size and timeout
//! - **Buffer**: Per-series write buffers with out-of-order handling
//! - **Writer**: Parallel write workers with compression
//! - **Backpressure**: Flow control and memory management
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::ingestion::{IngestionPipeline, IngestionConfig};
//! use gorilla_tsdb::types::DataPoint;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create pipeline with default config
//! let config = IngestionConfig::default();
//! let pipeline = IngestionPipeline::new(config).await?;
//!
//! // Ingest points
//! let point = DataPoint::new(1, 1700000000000, 42.5);
//! pipeline.ingest(point).await?;
//!
//! // Ingest batch for higher throughput
//! let points = vec![
//!     DataPoint::new(1, 1700000000001, 42.6),
//!     DataPoint::new(1, 1700000000002, 42.7),
//! ];
//! pipeline.ingest_batch(points).await?;
//!
//! // Graceful shutdown
//! pipeline.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Performance Targets
//!
//! - Single-thread: >2M points/second
//! - Multi-thread: >10M points/second
//! - P99 latency: <1ms
//! - Zero data loss under normal conditions

pub mod backpressure;
pub mod batch;
pub mod buffer;
pub mod metrics;
pub mod writer;

pub use backpressure::{BackpressureConfig, BackpressureController, BackpressureStrategy};
pub use batch::{PointBatch, BatchConfig, Batcher};
pub use buffer::{BufferConfig, SeriesBuffer, WriteBufferManager};
pub use metrics::IngestionMetrics;
pub use writer::{WriterConfig, WriteWorker, ParallelWriter};

use crate::error::IngestionError;
use crate::types::DataPoint;

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Configuration for the ingestion pipeline
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Batch configuration
    pub batch: BatchConfig,
    /// Buffer configuration
    pub buffer: BufferConfig,
    /// Writer configuration
    pub writer: WriterConfig,
    /// Backpressure configuration
    pub backpressure: BackpressureConfig,
    /// Channel buffer size between pipeline stages
    pub channel_buffer_size: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            buffer: BufferConfig::default(),
            writer: WriterConfig::default(),
            backpressure: BackpressureConfig::default(),
            channel_buffer_size: 10_000,
        }
    }
}

impl IngestionConfig {
    /// Create a new ingestion config builder
    pub fn builder() -> IngestionConfigBuilder {
        IngestionConfigBuilder::default()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.channel_buffer_size == 0 {
            return Err("channel_buffer_size must be > 0".to_string());
        }
        self.batch.validate()?;
        self.buffer.validate()?;
        self.writer.validate()?;
        self.backpressure.validate()?;
        Ok(())
    }
}

/// Builder for IngestionConfig
#[derive(Debug, Default)]
pub struct IngestionConfigBuilder {
    config: IngestionConfig,
}

impl IngestionConfigBuilder {
    /// Set batch configuration
    pub fn batch(mut self, config: BatchConfig) -> Self {
        self.config.batch = config;
        self
    }

    /// Set buffer configuration
    pub fn buffer(mut self, config: BufferConfig) -> Self {
        self.config.buffer = config;
        self
    }

    /// Set writer configuration
    pub fn writer(mut self, config: WriterConfig) -> Self {
        self.config.writer = config;
        self
    }

    /// Set backpressure configuration
    pub fn backpressure(mut self, config: BackpressureConfig) -> Self {
        self.config.backpressure = config;
        self
    }

    /// Set channel buffer size
    pub fn channel_buffer_size(mut self, size: usize) -> Self {
        self.config.channel_buffer_size = size;
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<IngestionConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }
}

/// High-throughput ingestion pipeline
///
/// Coordinates batching, buffering, and writing of data points
/// with automatic backpressure management.
pub struct IngestionPipeline {
    /// Batcher for incoming points
    batcher: Arc<Batcher>,
    /// Write buffer manager
    buffer_manager: Arc<WriteBufferManager>,
    /// Parallel writer
    writer: Arc<ParallelWriter>,
    /// Backpressure controller
    backpressure: Arc<BackpressureController>,
    /// Ingestion metrics
    metrics: Arc<IngestionMetrics>,
    /// Shutdown signal sender
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    /// Configuration (stored for potential reconfiguration)
    #[allow(dead_code)]
    config: IngestionConfig,
}

impl IngestionPipeline {
    /// Create a new ingestion pipeline
    ///
    /// # Arguments
    ///
    /// * `config` - Pipeline configuration
    ///
    /// # Returns
    ///
    /// Result containing the pipeline or an error
    pub async fn new(config: IngestionConfig) -> Result<Self, IngestionError> {
        config.validate().map_err(IngestionError::ConfigError)?;

        let metrics = Arc::new(IngestionMetrics::new());
        let backpressure = Arc::new(BackpressureController::new(
            config.backpressure.clone(),
            Arc::clone(&metrics),
        ));

        // Create channel for batch -> buffer communication
        let (batch_tx, batch_rx) = mpsc::channel(config.channel_buffer_size);

        // Create channel for buffer -> writer communication
        let (write_tx, write_rx) = mpsc::channel(config.channel_buffer_size);

        let batcher = Arc::new(Batcher::new(
            config.batch.clone(),
            batch_tx,
            Arc::clone(&metrics),
        ));

        let buffer_manager = Arc::new(WriteBufferManager::new(
            config.buffer.clone(),
            batch_rx,
            write_tx,
            Arc::clone(&backpressure),
            Arc::clone(&metrics),
        ));

        let writer = Arc::new(ParallelWriter::new(
            config.writer.clone(),
            write_rx,
            Arc::clone(&metrics),
        ));

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        info!("Ingestion pipeline initialized with {} writer workers", config.writer.num_workers);

        Ok(Self {
            batcher,
            buffer_manager,
            writer,
            backpressure,
            metrics,
            shutdown_tx: Some(shutdown_tx),
            config,
        })
    }

    /// Start the pipeline background tasks
    ///
    /// This spawns the buffer flush and writer tasks.
    pub async fn start(&self) -> Result<(), IngestionError> {
        // Start buffer manager flush task
        let buffer_manager = Arc::clone(&self.buffer_manager);
        let shutdown_rx = self.shutdown_tx.as_ref()
            .map(|tx| tx.subscribe())
            .ok_or_else(|| IngestionError::ShutdownError("Pipeline already shut down".to_string()))?;

        tokio::spawn(async move {
            buffer_manager.run(shutdown_rx).await;
        });

        // Start parallel writer
        let writer = Arc::clone(&self.writer);
        let shutdown_rx = self.shutdown_tx.as_ref()
            .map(|tx| tx.subscribe())
            .ok_or_else(|| IngestionError::ShutdownError("Pipeline already shut down".to_string()))?;

        tokio::spawn(async move {
            writer.run(shutdown_rx).await;
        });

        info!("Ingestion pipeline started");
        Ok(())
    }

    /// Ingest a single data point
    ///
    /// # Arguments
    ///
    /// * `point` - The data point to ingest
    ///
    /// # Returns
    ///
    /// Result indicating success or backpressure/error
    pub async fn ingest(&self, point: DataPoint) -> Result<(), IngestionError> {
        // Check backpressure before accepting
        if self.backpressure.should_reject() {
            self.metrics.record_rejected(1);
            return Err(IngestionError::Backpressure(
                "Pipeline under pressure, try again later".to_string()
            ));
        }

        self.metrics.record_received(1);
        self.batcher.add_point(point).await?;
        Ok(())
    }

    /// Ingest a batch of data points
    ///
    /// More efficient than ingesting points one at a time.
    ///
    /// # Arguments
    ///
    /// * `points` - Vector of data points to ingest
    ///
    /// # Returns
    ///
    /// Result indicating success or backpressure/error
    pub async fn ingest_batch(&self, points: Vec<DataPoint>) -> Result<(), IngestionError> {
        if points.is_empty() {
            return Ok(());
        }

        // Check backpressure before accepting
        if self.backpressure.should_reject() {
            self.metrics.record_rejected(points.len() as u64);
            return Err(IngestionError::Backpressure(
                "Pipeline under pressure, try again later".to_string()
            ));
        }

        let count = points.len() as u64;
        self.metrics.record_received(count);
        self.batcher.add_batch(points).await?;
        Ok(())
    }

    /// Flush all pending data
    ///
    /// Forces all buffered data to be written to storage.
    pub async fn flush(&self) -> Result<(), IngestionError> {
        debug!("Flushing ingestion pipeline");
        self.batcher.flush().await?;
        self.buffer_manager.flush_all().await?;
        Ok(())
    }

    /// Get current pipeline statistics
    pub fn stats(&self) -> PipelineStats {
        PipelineStats {
            points_received: self.metrics.points_received(),
            points_written: self.metrics.points_written(),
            points_rejected: self.metrics.points_rejected(),
            batches_processed: self.metrics.batches_processed(),
            buffer_size: self.buffer_manager.total_buffered(),
            active_series: self.buffer_manager.active_series_count(),
            backpressure_active: self.backpressure.is_active(),
            memory_used_bytes: self.buffer_manager.memory_used(),
        }
    }

    /// Gracefully shutdown the pipeline
    ///
    /// Flushes all pending data and stops background tasks.
    pub async fn shutdown(mut self) -> Result<(), IngestionError> {
        info!("Shutting down ingestion pipeline");

        // Flush remaining data
        if let Err(e) = self.flush().await {
            warn!("Error flushing during shutdown: {}", e);
        }

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        info!("Ingestion pipeline shutdown complete");
        Ok(())
    }
}

/// Pipeline statistics snapshot
#[derive(Debug, Clone)]
pub struct PipelineStats {
    /// Total points received
    pub points_received: u64,
    /// Total points written to storage
    pub points_written: u64,
    /// Total points rejected due to backpressure
    pub points_rejected: u64,
    /// Total batches processed
    pub batches_processed: u64,
    /// Current buffer size (points)
    pub buffer_size: usize,
    /// Number of active series in buffer
    pub active_series: usize,
    /// Whether backpressure is currently active
    pub backpressure_active: bool,
    /// Memory used by buffers in bytes
    pub memory_used_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingestion_config_default() {
        let config = IngestionConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.channel_buffer_size > 0);
    }

    #[test]
    fn test_ingestion_config_builder() {
        let config = IngestionConfig::builder()
            .channel_buffer_size(5000)
            .build();

        assert!(config.is_ok());
        assert_eq!(config.unwrap().channel_buffer_size, 5000);
    }

    #[test]
    fn test_ingestion_config_validation() {
        let mut config = IngestionConfig::default();
        config.channel_buffer_size = 0;
        assert!(config.validate().is_err());
    }
}
