//! WAL writer with batch coalescing
//!
//! Handles batched writes to WAL segments with configurable sync modes.
//! Coalesces multiple write requests into fewer disk operations for
//! improved throughput.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::debug;

use super::config::{SyncMode, WalConfig};
use super::error::{WalError, WalResult};
use super::record::WalRecord;
use super::segment::WalSegment;
use super::SequenceNumber;
use crate::types::DataPoint;

/// Write request for WAL operations
#[derive(Debug)]
pub struct WriteRequest {
    /// Sequence number for this batch
    pub sequence: SequenceNumber,
    /// Points to write
    pub points: Vec<DataPoint>,
}

/// WAL writer handles batched writes to segments
///
/// Receives write requests through a channel and coalesces them
/// into efficient disk writes based on the configured sync mode.
pub struct WalWriter {
    /// Configuration
    config: WalConfig,
    /// Channel for receiving write requests
    receiver: mpsc::Receiver<WriteRequest>,
    /// Current active segment
    active_segment: Option<Arc<WalSegment>>,
    /// Pending writes awaiting sync
    pending_writes: Vec<(WalRecord, SequenceNumber)>,
    /// Last sync time
    last_sync: Instant,
    /// Writes since last sync
    writes_since_sync: usize,
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(config: WalConfig, receiver: mpsc::Receiver<WriteRequest>) -> Self {
        Self {
            config,
            receiver,
            active_segment: None,
            pending_writes: Vec::new(),
            last_sync: Instant::now(),
            writes_since_sync: 0,
        }
    }

    /// Run the writer task
    ///
    /// Processes write requests until the channel is closed.
    pub async fn run(mut self) -> WalResult<()> {
        debug!("WAL writer started");

        // Create initial segment
        self.rotate_segment().await?;

        loop {
            // Calculate timeout based on sync mode
            let timeout = self.sync_timeout();

            tokio::select! {
                // Receive write request
                request = self.receiver.recv() => {
                    match request {
                        Some(req) => {
                            self.handle_write(req).await?;
                        }
                        None => {
                            // Channel closed, flush and exit
                            self.flush().await?;
                            break;
                        }
                    }
                }

                // Periodic sync timeout
                _ = tokio::time::sleep(timeout) => {
                    if !self.pending_writes.is_empty() {
                        self.flush().await?;
                    }
                }
            }
        }

        debug!("WAL writer stopped");
        Ok(())
    }

    /// Handle a write request
    async fn handle_write(&mut self, request: WriteRequest) -> WalResult<()> {
        let record = WalRecord::from_points(&request.points);

        // Check if we need to rotate
        let segment = self.active_segment.as_ref().ok_or_else(|| WalError::Io {
            source: std::io::Error::new(std::io::ErrorKind::NotConnected, "no active segment"),
            context: "writer not initialized".to_string(),
        })?;

        if !segment.has_capacity(record.disk_size()) {
            self.rotate_segment().await?;
        }

        // Queue the write
        self.pending_writes.push((record, request.sequence));
        self.writes_since_sync += 1;

        // Check if we should sync
        if self.should_sync() {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush pending writes to disk
    async fn flush(&mut self) -> WalResult<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }

        // Take pending writes to avoid borrow issues
        let writes = std::mem::take(&mut self.pending_writes);

        // Write all pending records
        for (record, sequence) in writes {
            // Get current segment
            let segment = self.active_segment.as_ref().ok_or_else(|| WalError::Io {
                source: std::io::Error::new(std::io::ErrorKind::NotConnected, "no active segment"),
                context: "flushing without segment".to_string(),
            })?;

            // Check capacity and rotate if needed
            if !segment.has_capacity(record.disk_size()) {
                self.rotate_segment().await?;
            }

            // Write to current segment
            let segment = self.active_segment.as_ref().unwrap();
            segment.write_record(&record, sequence)?;
        }

        // Sync based on mode
        if let Some(seg) = &self.active_segment {
            seg.sync().await?;
        }

        self.last_sync = Instant::now();
        self.writes_since_sync = 0;

        Ok(())
    }

    /// Rotate to a new segment
    async fn rotate_segment(&mut self) -> WalResult<()> {
        // Seal current segment if exists
        if let Some(segment) = self.active_segment.take() {
            segment.seal().await?;
        }

        // Determine new segment ID
        let new_id = self.next_segment_id();

        // Create new segment
        let new_segment = Arc::new(
            WalSegment::create(&self.config.directory, new_id, self.config.segment_size).await?,
        );

        self.active_segment = Some(new_segment);
        debug!("Rotated to segment {}", new_id);

        Ok(())
    }

    /// Get next segment ID
    fn next_segment_id(&self) -> u64 {
        self.active_segment
            .as_ref()
            .map(|s| s.id() + 1)
            .unwrap_or(1)
    }

    /// Check if we should sync based on sync mode
    fn should_sync(&self) -> bool {
        match self.config.sync_mode {
            SyncMode::Immediate => true,
            SyncMode::BatchSize(n) => self.writes_since_sync >= n,
            SyncMode::Interval(ms) => self.last_sync.elapsed() >= Duration::from_millis(ms),
            SyncMode::None => false,
        }
    }

    /// Get sync timeout duration
    fn sync_timeout(&self) -> Duration {
        match self.config.sync_mode {
            SyncMode::Interval(ms) => {
                let elapsed = self.last_sync.elapsed();
                let target = Duration::from_millis(ms);
                if elapsed >= target {
                    Duration::from_millis(1)
                } else {
                    target - elapsed
                }
            }
            _ => Duration::from_secs(1), // Default timeout
        }
    }
}

/// Batched writer for high-throughput scenarios
///
/// Collects points into batches before writing to WAL for
/// improved performance under heavy load.
pub struct BatchedWalWriter {
    /// Sender for write requests
    sender: mpsc::Sender<WriteRequest>,
    /// Current batch of points
    batch: Vec<DataPoint>,
    /// Batch size threshold
    batch_size: usize,
    /// Last flush time
    last_flush: Instant,
    /// Flush interval
    flush_interval: Duration,
    /// Next sequence number
    next_sequence: SequenceNumber,
}

impl BatchedWalWriter {
    /// Create a new batched writer
    pub fn new(
        sender: mpsc::Sender<WriteRequest>,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        Self {
            sender,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            last_flush: Instant::now(),
            flush_interval,
            next_sequence: 0,
        }
    }

    /// Add a point to the batch
    pub async fn write(&mut self, point: DataPoint) -> WalResult<()> {
        self.batch.push(point);

        if self.should_flush() {
            self.flush().await?;
        }

        Ok(())
    }

    /// Add multiple points to the batch
    pub async fn write_batch(&mut self, points: Vec<DataPoint>) -> WalResult<()> {
        self.batch.extend(points);

        if self.should_flush() {
            self.flush().await?;
        }

        Ok(())
    }

    /// Check if we should flush
    fn should_flush(&self) -> bool {
        self.batch.len() >= self.batch_size || self.last_flush.elapsed() >= self.flush_interval
    }

    /// Flush the current batch
    pub async fn flush(&mut self) -> WalResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let points = std::mem::take(&mut self.batch);
        self.batch = Vec::with_capacity(self.batch_size);

        let sequence = self.next_sequence;
        self.next_sequence += 1;

        let request = WriteRequest { sequence, points };

        self.sender
            .send(request)
            .await
            .map_err(|_| WalError::ChannelClosed)?;
        self.last_flush = Instant::now();

        Ok(())
    }
}

/// Write coalescer for grouping writes
///
/// Combines multiple small writes into larger batches
/// to reduce syscall overhead.
pub struct WriteCoalescer {
    /// Buffer for coalescing
    buffer: Vec<DataPoint>,
    /// Maximum buffer size
    max_size: usize,
    /// Callback for flushing
    on_flush: Box<dyn FnMut(Vec<DataPoint>) + Send>,
}

impl WriteCoalescer {
    /// Create a new coalescer
    pub fn new<F>(max_size: usize, on_flush: F) -> Self
    where
        F: FnMut(Vec<DataPoint>) + Send + 'static,
    {
        Self {
            buffer: Vec::with_capacity(max_size),
            max_size,
            on_flush: Box::new(on_flush),
        }
    }

    /// Add points to the coalescer
    pub fn add(&mut self, points: &[DataPoint]) {
        self.buffer.extend_from_slice(points);

        if self.buffer.len() >= self.max_size {
            self.flush();
        }
    }

    /// Flush the buffer
    pub fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let points = std::mem::take(&mut self.buffer);
            self.buffer = Vec::with_capacity(self.max_size);
            (self.on_flush)(points);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_writer_basic() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let (tx, rx) = mpsc::channel(100);

        let writer = WalWriter::new(config, rx);

        // Send a write request
        let points = vec![DataPoint::new(1, 1000, 42.5)];
        tx.send(WriteRequest {
            sequence: 0,
            points,
        })
        .await
        .unwrap();

        // Close channel to trigger shutdown
        drop(tx);

        // Run writer (will process request and exit)
        let result = writer.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_batched_writer() {
        let (_tx, rx) = mpsc::channel::<WriteRequest>(100);
        let (batch_tx, _batch_rx) = mpsc::channel(100);

        let mut writer = BatchedWalWriter::new(
            batch_tx,
            10, // batch size
            Duration::from_millis(100),
        );

        // Add points below threshold
        for i in 0..5 {
            writer
                .write(DataPoint::new(1, 1000 + i, 42.5))
                .await
                .unwrap();
        }

        // Should not have flushed yet
        assert_eq!(writer.batch.len(), 5);

        // Add more to trigger flush
        for i in 0..5 {
            writer
                .write(DataPoint::new(1, 2000 + i, 43.5))
                .await
                .unwrap();
        }

        // Should have flushed
        assert!(writer.batch.len() < 10);

        drop(rx);
    }

    #[test]
    fn test_write_coalescer() {
        use std::sync::Arc;
        use std::sync::Mutex;

        let flushed: Arc<Mutex<Vec<Vec<DataPoint>>>> = Arc::new(Mutex::new(Vec::new()));
        let flushed_clone = Arc::clone(&flushed);

        let mut coalescer = WriteCoalescer::new(5, move |points| {
            flushed_clone.lock().unwrap().push(points);
        });

        // Add points
        coalescer.add(&[DataPoint::new(1, 1000, 42.5)]);
        coalescer.add(&[DataPoint::new(1, 1001, 42.6)]);

        // Not flushed yet
        assert!(flushed.lock().unwrap().is_empty());

        // Add more to trigger flush
        coalescer.add(&[
            DataPoint::new(1, 1002, 42.7),
            DataPoint::new(1, 1003, 42.8),
            DataPoint::new(1, 1004, 42.9),
        ]);

        // Should have flushed
        assert!(!flushed.lock().unwrap().is_empty());
    }

    #[test]
    fn test_sync_mode_should_sync() {
        let dir = tempdir().unwrap();

        let config = WalConfig {
            sync_mode: SyncMode::Immediate,
            ..test_config(dir.path())
        };
        let (_, rx) = mpsc::channel(1);
        let writer = WalWriter::new(config, rx);
        assert!(writer.should_sync());

        let config = WalConfig {
            sync_mode: SyncMode::BatchSize(10),
            ..test_config(dir.path())
        };
        let (_, rx) = mpsc::channel(1);
        let writer = WalWriter::new(config, rx);
        assert!(!writer.should_sync()); // No writes yet

        let config = WalConfig {
            sync_mode: SyncMode::None,
            ..test_config(dir.path())
        };
        let (_, rx) = mpsc::channel(1);
        let writer = WalWriter::new(config, rx);
        assert!(!writer.should_sync());
    }
}
