//! Write buffer management for crash recovery
//!
//! Implements a Redis-backed write buffer that:
//! - Buffers incoming points before they are persisted to chunks
//! - Enables crash recovery by replaying buffered points
//! - Provides automatic flushing with configurable intervals
//! - Handles buffer overflow gracefully
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::{RedisConfig, RedisPool};
//! use gorilla_tsdb::redis::buffer::{WriteBuffer, BufferConfig};
//! use gorilla_tsdb::types::DataPoint;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = Arc::new(RedisPool::new(RedisConfig::default()).await?);
//! let buffer = WriteBuffer::new(pool, BufferConfig::default());
//!
//! // Buffer some points (series_id=1, timestamp=1000, value=42.0)
//! let point = DataPoint::new(1, 1000, 42.0);
//! buffer.append(1, vec![point]).await?;
//!
//! // Flush when ready
//! let points = buffer.flush(1).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::IndexError;
use crate::types::{DataPoint, SeriesId};

use super::connection::RedisPool;
use super::scripts::LuaScripts;

use parking_lot::RwLock;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Key prefix for write buffer
const KEY_BUFFER_PREFIX: &str = "ts:series:";
const KEY_BUFFER_SUFFIX: &str = ":buffer";

/// Configuration for write buffer
#[derive(Clone, Debug)]
pub struct BufferConfig {
    /// Maximum size of buffer per series (number of batches)
    pub max_buffer_size: usize,

    /// Maximum age of buffered points before forcing flush (in milliseconds)
    pub max_buffer_age_ms: u64,

    /// Automatic flush interval (in milliseconds)
    pub auto_flush_interval_ms: u64,

    /// Maximum batch size for buffered points
    pub max_batch_size: usize,

    /// Enable automatic background flushing
    pub auto_flush_enabled: bool,

    /// Timeout for buffer operations (in milliseconds)
    pub operation_timeout_ms: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 100,
            max_buffer_age_ms: 60_000,     // 1 minute
            auto_flush_interval_ms: 5_000, // 5 seconds
            max_batch_size: 1000,
            auto_flush_enabled: true,
            operation_timeout_ms: 5_000,
        }
    }
}

/// Serialized batch of points for Redis storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BufferedBatch {
    /// The data points in this batch
    points: Vec<BufferedPoint>,
    /// When this batch was created (Unix timestamp in milliseconds)
    created_at: i64,
    /// Sequence number for ordering
    sequence: u64,
}

/// A single buffered point
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BufferedPoint {
    /// Series ID this point belongs to
    series_id: SeriesId,
    /// Timestamp in milliseconds
    timestamp: i64,
    /// Value of the data point
    value: f64,
}

impl BufferedPoint {
    /// Create a buffered point from a data point
    fn from_data_point(point: &DataPoint) -> Self {
        Self {
            series_id: point.series_id,
            timestamp: point.timestamp,
            value: point.value,
        }
    }

    /// Convert back to a DataPoint
    fn to_data_point(&self) -> DataPoint {
        DataPoint::new(self.series_id, self.timestamp, self.value)
    }
}

/// Local buffer state for a series
struct SeriesBufferState {
    /// Last flush time
    last_flush: Instant,
    /// Pending batch count
    pending_batches: u64,
    /// Total points buffered
    total_points: u64,
}

impl Default for SeriesBufferState {
    fn default() -> Self {
        Self {
            last_flush: Instant::now(),
            pending_batches: 0,
            total_points: 0,
        }
    }
}

/// Write buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// Total points buffered since startup
    pub total_points_buffered: u64,
    /// Total batches buffered
    pub total_batches_buffered: u64,
    /// Total points flushed
    pub total_points_flushed: u64,
    /// Total flush operations
    pub total_flushes: u64,
    /// Number of buffer overflow events
    pub buffer_overflows: u64,
    /// Number of series with active buffers
    pub active_series: usize,
}

/// Write buffer for crash recovery
///
/// Stores incoming data points in Redis before they are persisted
/// to storage. This allows recovery of recent writes after a crash.
pub struct WriteBuffer {
    /// Redis connection pool
    pool: Arc<RedisPool>,

    /// Lua scripts for atomic operations
    scripts: Arc<LuaScripts>,

    /// Configuration
    config: BufferConfig,

    /// Local state per series
    series_state: RwLock<HashMap<SeriesId, SeriesBufferState>>,

    /// Global sequence counter for ordering
    sequence_counter: AtomicU64,

    /// Statistics: points buffered
    points_buffered: AtomicU64,

    /// Statistics: batches buffered
    batches_buffered: AtomicU64,

    /// Statistics: points flushed
    points_flushed: AtomicU64,

    /// Statistics: flush count
    flush_count: AtomicU64,

    /// Statistics: overflow count
    overflow_count: AtomicU64,

    /// Notification for flush completion
    flush_notify: Arc<Notify>,
}

impl WriteBuffer {
    /// Create a new write buffer
    ///
    /// # Arguments
    ///
    /// * `pool` - Redis connection pool
    /// * `config` - Buffer configuration
    pub fn new(pool: Arc<RedisPool>, config: BufferConfig) -> Self {
        Self {
            pool,
            scripts: Arc::new(LuaScripts::new()),
            config,
            series_state: RwLock::new(HashMap::new()),
            sequence_counter: AtomicU64::new(0),
            points_buffered: AtomicU64::new(0),
            batches_buffered: AtomicU64::new(0),
            points_flushed: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            overflow_count: AtomicU64::new(0),
            flush_notify: Arc::new(Notify::new()),
        }
    }

    /// Get the Redis key for a series buffer
    fn buffer_key(series_id: SeriesId) -> String {
        format!("{}{}{}", KEY_BUFFER_PREFIX, series_id, KEY_BUFFER_SUFFIX)
    }

    /// Append points to the buffer
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series to buffer points for
    /// * `points` - Data points to buffer
    ///
    /// # Returns
    ///
    /// Ok(true) if points were buffered successfully,
    /// Ok(false) if buffer is full (overflow)
    pub async fn append(
        &self,
        series_id: SeriesId,
        points: Vec<DataPoint>,
    ) -> Result<bool, IndexError> {
        if points.is_empty() {
            return Ok(true);
        }

        let points_count = points.len();

        // Check buffer limit
        {
            let state = self.series_state.read();
            if let Some(s) = state.get(&series_id) {
                if s.pending_batches >= self.config.max_buffer_size as u64 {
                    self.overflow_count.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Buffer overflow for series {}: {} batches pending",
                        series_id, s.pending_batches
                    );
                    return Ok(false);
                }
            }
        }

        // Create batch
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let batch = BufferedBatch {
            points: points.iter().map(BufferedPoint::from_data_point).collect(),
            created_at: chrono::Utc::now().timestamp_millis(),
            sequence,
        };

        let batch_json = serde_json::to_string(&batch)
            .map_err(|e| IndexError::SerializationError(e.to_string()))?;

        // Push to Redis
        let script = self.scripts.buffer_points();
        let buffer_key = Self::buffer_key(series_id);
        let meta_key = format!("{}{}:meta", KEY_BUFFER_PREFIX, series_id);
        let current_time = chrono::Utc::now().timestamp_millis();
        let max_size = self.config.max_buffer_size;

        let result: i32 = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let buffer_key = buffer_key.clone();
                let meta_key = meta_key.clone();
                let batch_json = batch_json.clone();
                async move {
                    script
                        .key(buffer_key)
                        .key(meta_key)
                        .arg(batch_json)
                        .arg(current_time)
                        .arg(max_size)
                        .invoke_async(&mut conn)
                        .await
                }
            })
            .await?;

        if result == 1 {
            // Update local state
            {
                let mut state = self.series_state.write();
                let series_state = state.entry(series_id).or_default();
                series_state.pending_batches += 1;
                series_state.total_points += points_count as u64;
            }

            self.points_buffered
                .fetch_add(points_count as u64, Ordering::Relaxed);
            self.batches_buffered.fetch_add(1, Ordering::Relaxed);

            debug!(
                "Buffered {} points for series {} (seq: {})",
                points_count, series_id, sequence
            );
            Ok(true)
        } else {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            warn!("Buffer full for series {}", series_id);
            Ok(false)
        }
    }

    /// Flush all buffered points for a series
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series to flush
    ///
    /// # Returns
    ///
    /// All buffered points for the series, in order
    pub async fn flush(&self, series_id: SeriesId) -> Result<Vec<DataPoint>, IndexError> {
        let script = self.scripts.flush_buffer();
        let buffer_key = Self::buffer_key(series_id);

        let batches_json: Vec<String> = self
            .pool
            .execute(|mut conn| {
                let script = script.clone();
                let buffer_key = buffer_key.clone();
                async move { script.key(buffer_key).invoke_async(&mut conn).await }
            })
            .await?;

        let mut all_points = Vec::new();
        let mut batches: Vec<BufferedBatch> = Vec::new();

        // Deserialize all batches
        for batch_json in batches_json {
            match serde_json::from_str::<BufferedBatch>(&batch_json) {
                Ok(batch) => batches.push(batch),
                Err(e) => {
                    warn!("Failed to deserialize buffer batch: {}", e);
                }
            }
        }

        // Sort by sequence to ensure order
        batches.sort_by_key(|b| b.sequence);

        // Extract all points
        for batch in batches {
            for bp in batch.points {
                all_points.push(bp.to_data_point());
            }
        }

        let points_count = all_points.len();

        // Update local state
        {
            let mut state = self.series_state.write();
            if let Some(s) = state.get_mut(&series_id) {
                s.last_flush = Instant::now();
                s.pending_batches = 0;
            }
        }

        self.points_flushed
            .fetch_add(points_count as u64, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        self.flush_notify.notify_waiters();

        info!("Flushed {} points for series {}", points_count, series_id);

        Ok(all_points)
    }

    /// Check buffer size for a series
    ///
    /// Returns the number of batches currently buffered.
    pub async fn buffer_size(&self, series_id: SeriesId) -> Result<usize, IndexError> {
        let buffer_key = Self::buffer_key(series_id);

        let size: i64 = self
            .pool
            .execute(|mut conn| {
                let buffer_key = buffer_key.clone();
                async move { conn.llen(buffer_key).await }
            })
            .await?;

        Ok(size as usize)
    }

    /// Check if a series needs flushing
    ///
    /// Returns true if the buffer is above threshold or too old.
    pub fn needs_flush(&self, series_id: SeriesId) -> bool {
        let state = self.series_state.read();

        if let Some(s) = state.get(&series_id) {
            // Check batch count
            if s.pending_batches >= self.config.max_buffer_size as u64 / 2 {
                return true;
            }

            // Check age
            if s.last_flush.elapsed().as_millis() > self.config.max_buffer_age_ms as u128 {
                return true;
            }
        }

        false
    }

    /// Get all series with pending buffers
    pub fn pending_series(&self) -> Vec<SeriesId> {
        let state = self.series_state.read();
        state
            .iter()
            .filter(|(_, s)| s.pending_batches > 0)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Flush all series that need flushing
    ///
    /// Returns the number of series flushed.
    pub async fn flush_all_pending(&self) -> Result<usize, IndexError> {
        let pending = self.pending_series();
        let mut flushed = 0;

        for series_id in pending {
            if self.needs_flush(series_id) {
                let _ = self.flush(series_id).await?;
                flushed += 1;
            }
        }

        Ok(flushed)
    }

    /// Recover buffered points for a series
    ///
    /// Used during startup to recover points that weren't persisted before shutdown.
    pub async fn recover(&self, series_id: SeriesId) -> Result<Vec<DataPoint>, IndexError> {
        // Recovery is same as flush - get all buffered points
        self.flush(series_id).await
    }

    /// Get buffer statistics
    pub fn stats(&self) -> BufferStats {
        let state = self.series_state.read();
        let active_series = state.values().filter(|s| s.pending_batches > 0).count();

        BufferStats {
            total_points_buffered: self.points_buffered.load(Ordering::Relaxed),
            total_batches_buffered: self.batches_buffered.load(Ordering::Relaxed),
            total_points_flushed: self.points_flushed.load(Ordering::Relaxed),
            total_flushes: self.flush_count.load(Ordering::Relaxed),
            buffer_overflows: self.overflow_count.load(Ordering::Relaxed),
            active_series,
        }
    }

    /// Wait for the next flush operation
    ///
    /// Useful for testing or synchronization.
    pub async fn wait_for_flush(&self) {
        self.flush_notify.notified().await;
    }

    /// Get the configuration
    pub fn config(&self) -> &BufferConfig {
        &self.config
    }
}

/// Background flush task
///
/// Runs in a separate task to periodically flush old buffers.
pub struct BufferFlushTask {
    /// The write buffer to flush
    buffer: Arc<WriteBuffer>,

    /// Shutdown signal
    shutdown: Arc<Notify>,
}

impl BufferFlushTask {
    /// Create a new flush task
    pub fn new(buffer: Arc<WriteBuffer>) -> Self {
        Self {
            buffer,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Run the flush task
    ///
    /// This method runs until shutdown is signaled.
    pub async fn run(&self) {
        let interval = Duration::from_millis(self.buffer.config.auto_flush_interval_ms);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    match self.buffer.flush_all_pending().await {
                        Ok(count) if count > 0 => {
                            debug!("Auto-flushed {} series", count);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Auto-flush error: {}", e);
                        }
                    }
                }
                _ = self.shutdown.notified() => {
                    info!("Buffer flush task shutting down");
                    break;
                }
            }
        }
    }

    /// Signal the task to shut down
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.max_buffer_size, 100);
        assert_eq!(config.auto_flush_interval_ms, 5_000);
        assert!(config.auto_flush_enabled);
    }

    #[test]
    fn test_buffer_key_generation() {
        assert_eq!(WriteBuffer::buffer_key(123), "ts:series:123:buffer");
    }

    #[test]
    fn test_buffered_point_conversion() {
        let dp = DataPoint::new(1, 1000, 42.5);
        let bp = BufferedPoint::from_data_point(&dp);

        assert_eq!(bp.series_id, 1);
        assert_eq!(bp.timestamp, 1000);
        assert!((bp.value - 42.5).abs() < f64::EPSILON);

        let dp2 = bp.to_data_point();
        assert_eq!(dp2.series_id, 1);
        assert_eq!(dp2.timestamp, 1000);
        assert!((dp2.value - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_buffered_batch_serialization() {
        let batch = BufferedBatch {
            points: vec![
                BufferedPoint {
                    series_id: 1,
                    timestamp: 1000,
                    value: 42.0,
                },
                BufferedPoint {
                    series_id: 1,
                    timestamp: 2000,
                    value: 43.0,
                },
            ],
            created_at: 1000000,
            sequence: 1,
        };

        let json = serde_json::to_string(&batch).unwrap();
        let deserialized: BufferedBatch = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.points.len(), 2);
        assert_eq!(deserialized.sequence, 1);
        assert_eq!(deserialized.points[0].timestamp, 1000);
        assert_eq!(deserialized.points[0].series_id, 1);
    }

    #[test]
    fn test_series_buffer_state_default() {
        let state = SeriesBufferState::default();
        assert_eq!(state.pending_batches, 0);
        assert_eq!(state.total_points, 0);
    }

    #[test]
    fn test_buffer_stats_initialization() {
        let stats = BufferStats {
            total_points_buffered: 100,
            total_batches_buffered: 10,
            total_points_flushed: 50,
            total_flushes: 5,
            buffer_overflows: 0,
            active_series: 2,
        };

        assert_eq!(stats.total_points_buffered, 100);
        assert_eq!(stats.total_flushes, 5);
        assert_eq!(stats.active_series, 2);
    }
}
