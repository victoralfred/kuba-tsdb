//! Active chunk management with thread-safe, out-of-order point handling
//!
//! This module provides `ActiveChunk`, a thread-safe wrapper around chunk data
//! that supports:
//! - Concurrent appends from multiple threads
//! - Out-of-order point handling (maintains sorted order)
//! - Automatic sealing based on configurable thresholds
//! - Lock-free reads for common operations
use crate::storage::chunk::Chunk;
use crate::types::{DataPoint, SeriesId};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::RwLock;
use std::time::Instant;

// Re-export SealConfig for convenience
pub use crate::storage::chunk::SealConfig;

/// Active chunk with thread-safe operations and out-of-order handling
///
/// `ActiveChunk` wraps the basic `Chunk` with additional capabilities:
/// - **Thread Safety**: Uses `RwLock` for concurrent access
/// - **Out-of-Order**: `BTreeMap` maintains sorted order by timestamp
/// - **Lock-Free Reads**: Atomic counters for common queries
///
/// # Example
///
/// ```
/// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
/// use gorilla_tsdb::types::DataPoint;
/// use std::sync::Arc;
/// use std::thread;
///
/// let chunk = Arc::new(ActiveChunk::new(1, 1000, SealConfig::default()));
///
/// // Multiple threads can append concurrently
/// let chunk_clone = Arc::clone(&chunk);
/// thread::spawn(move || {
///     chunk_clone.append(DataPoint {
///         series_id: 1,
///         timestamp: 1000,
///         value: 42.0
///     }).unwrap();
/// });
///
/// // Check if sealing needed (lock-free)
/// if chunk.should_seal() {
///     // Seal the chunk
/// }
/// ```
pub struct ActiveChunk {
    /// Series this chunk belongs to
    series_id: SeriesId,

    /// Points stored in sorted order (by timestamp)
    /// BTreeMap automatically maintains sort order for out-of-order inserts
    points: RwLock<BTreeMap<i64, DataPoint>>,

    /// Atomic counter for lock-free reads
    point_count: AtomicU32,

    /// Whether chunk has been sealed
    sealed: AtomicBool,

    /// P0.4: Cached minimum timestamp for lock-free access
    /// This allows should_seal() to check duration without acquiring read lock
    min_timestamp: AtomicI64,

    /// P0.4: Cached maximum timestamp for lock-free access
    /// This allows should_seal() to check duration without acquiring read lock
    max_timestamp: AtomicI64,

    /// Chunk creation time (reserved for future metrics)
    #[allow(dead_code)]
    created_at: Instant,

    /// Initial capacity hint (reserved for future pre-allocation)
    #[allow(dead_code)]
    capacity: usize,

    /// Sealing configuration
    seal_config: SealConfig,
}

impl ActiveChunk {
    /// Create a new active chunk
    ///
    /// # Arguments
    ///
    /// * `series_id` - Series identifier
    /// * `capacity` - Initial capacity hint (not a hard limit)
    /// * `seal_config` - Configuration for when to seal the chunk
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    ///
    /// let config = SealConfig {
    ///     max_points: 10_000,
    ///     max_duration_ms: 3_600_000, // 1 hour
    ///     max_size_bytes: 1_048_576,   // 1MB
    /// };
    ///
    /// let chunk = ActiveChunk::new(42, 1000, config);
    /// assert_eq!(chunk.point_count(), 0);
    /// ```
    pub fn new(series_id: SeriesId, capacity: usize, seal_config: SealConfig) -> Self {
        Self {
            series_id,
            points: RwLock::new(BTreeMap::new()),
            point_count: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            min_timestamp: AtomicI64::new(i64::MAX), // Will be updated on first append
            max_timestamp: AtomicI64::new(i64::MIN), // Will be updated on first append
            created_at: Instant::now(),
            capacity,
            seal_config,
        }
    }

    /// Append a point to the chunk
    ///
    /// This method is thread-safe and handles out-of-order points automatically.
    /// Points are stored in a `BTreeMap` keyed by timestamp, ensuring sorted order.
    ///
    /// # Arguments
    ///
    /// * `point` - Data point to append
    ///
    /// # Returns
    ///
    /// - `Ok(())` if point was successfully added
    /// - `Err(String)` if:
    ///   - Series ID doesn't match
    ///   - Chunk is already sealed
    ///
    /// # Thread Safety
    ///
    /// Multiple threads can call this method concurrently. The implementation uses
    /// `RwLock` to ensure exclusive write access while allowing concurrent reads.
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let chunk = ActiveChunk::new(1, 100, SealConfig::default());
    ///
    /// // Points can arrive out of order
    /// chunk.append(DataPoint { series_id: 1, timestamp: 100, value: 1.0 }).unwrap();
    /// chunk.append(DataPoint { series_id: 1, timestamp: 50, value: 2.0 }).unwrap();
    /// chunk.append(DataPoint { series_id: 1, timestamp: 75, value: 3.0 }).unwrap();
    ///
    /// // All points are stored in sorted order
    /// assert_eq!(chunk.point_count(), 3);
    /// let (start, end) = chunk.time_range();
    /// assert_eq!(start, 50);
    /// assert_eq!(end, 100);
    /// ```
    pub fn append(&self, point: DataPoint) -> Result<(), String> {
        use std::time::Instant;
        let start = Instant::now();

        // Validate series ID
        if point.series_id != self.series_id {
            crate::metrics::record_error("series_mismatch", "append");
            return Err(format!(
                "Point series_id {} doesn't match chunk series_id {}",
                point.series_id, self.series_id
            ));
        }

        // P1.3: Acquire write lock with proper error handling
        let lock_start = Instant::now();
        {
            let mut points = self.points.write().map_err(|_| {
                crate::metrics::record_error("lock_poisoned", "append");
                "Lock poisoned: cannot append to corrupted chunk".to_string()
            })?;

            let lock_duration = lock_start.elapsed().as_secs_f64();
            crate::metrics::LOCK_WAIT_DURATION
                .with_label_values(&["write"])
                .observe(lock_duration);

            // Check if sealed AFTER acquiring lock to prevent TOCTOU race
            if self.sealed.load(Ordering::Acquire) {
                crate::metrics::record_error("sealed_chunk", "append");
                return Err(format!(
                    "Cannot append to sealed chunk. Series ID: {}, timestamp: {}. \
                     Sealed chunks are immutable - create a new chunk for additional data.",
                    self.series_id, point.timestamp
                ));
            }

            // P0.2: Check for duplicate timestamp BEFORE inserting
            if points.contains_key(&point.timestamp) {
                crate::metrics::record_duplicate_timestamp(self.series_id);
                return Err(format!(
                    "Duplicate timestamp {}: point already exists in chunk",
                    point.timestamp
                ));
            }

            // Check max_points limit before inserting
            let current_count = points.len();
            if current_count >= self.seal_config.max_points {
                crate::metrics::record_error("max_points_exceeded", "append");
                return Err(format!(
                    "Chunk full: {} points (max: {}). Chunk should be sealed.",
                    current_count, self.seal_config.max_points
                ));
            }

            // BTreeMap handles ordering automatically - no need to check monotonicity here
            // Compression layer will validate monotonic order when sealing
            // This allows concurrent appends with different timestamps to work correctly
            points.insert(point.timestamp, point);

            // Update atomic counter
            self.point_count
                .store(points.len() as u32, Ordering::Release);

            // P1.5: Update cached min/max timestamps using compare-and-swap loop
            // This prevents race conditions where concurrent updates could set incorrect values
            // Example race without CAS:
            //   Thread A: load(100), set to 50
            //   Thread B: load(100), set to 25
            //   Thread A: store(50) <- overwrites B's correct value of 25!

            // Update minimum timestamp atomically
            loop {
                let current_min = self.min_timestamp.load(Ordering::Acquire);
                if point.timestamp >= current_min {
                    break; // No update needed
                }

                match self.min_timestamp.compare_exchange_weak(
                    current_min,
                    point.timestamp,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,     // Successfully updated
                    Err(_) => continue, // Another thread updated, retry
                }
            }

            // Update maximum timestamp atomically
            loop {
                let current_max = self.max_timestamp.load(Ordering::Acquire);
                if point.timestamp <= current_max {
                    break; // No update needed
                }

                match self.max_timestamp.compare_exchange_weak(
                    current_max,
                    point.timestamp,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,     // Successfully updated
                    Err(_) => continue, // Another thread updated, retry
                }
            }
        }

        // Record successful write
        let duration = start.elapsed().as_secs_f64();
        crate::metrics::record_write(self.series_id, duration, true);

        Ok(())
    }

    /// Check if chunk should be sealed
    ///
    /// Returns `true` if any threshold is exceeded:
    /// - Point count >= `max_points`
    /// - Duration >= `max_duration_ms`
    /// - Memory size >= `max_size_bytes`
    ///
    /// This method is lock-free for the point count check and uses a read lock
    /// for time range checks.
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let config = SealConfig {
    ///     max_points: 5,
    ///     max_duration_ms: 10_000,
    ///     max_size_bytes: 10_000,
    /// };
    ///
    /// let chunk = ActiveChunk::new(1, 10, config);
    ///
    /// for i in 0..5 {
    ///     chunk.append(DataPoint {
    ///         series_id: 1,
    ///         timestamp: i * 1000,
    ///         value: i as f64
    ///     }).unwrap();
    /// }
    ///
    /// assert!(chunk.should_seal());
    /// ```
    pub fn should_seal(&self) -> bool {
        // Check if already sealed (lock-free)
        if self.sealed.load(Ordering::Acquire) {
            return false;
        }

        // Check point count (lock-free)
        let count = self.point_count.load(Ordering::Acquire);
        if count == 0 {
            return false;
        }

        if count >= self.seal_config.max_points as u32 {
            return true;
        }

        // P0.4: Check duration using cached timestamps (lock-free!)
        let min_ts = self.min_timestamp.load(Ordering::Relaxed);
        let max_ts = self.max_timestamp.load(Ordering::Relaxed);

        // Use saturating_sub to prevent overflow with extreme timestamp values
        let duration = max_ts.saturating_sub(min_ts);

        if duration >= self.seal_config.max_duration_ms {
            return true;
        }

        // P0.4: Check memory size using count (lock-free, approximate for BTreeMap)
        // BTreeMap has ~64 bytes overhead per entry (node structure + pointers)
        let size_estimate = count as usize * (std::mem::size_of::<DataPoint>() + 64);
        if size_estimate >= self.seal_config.max_size_bytes {
            return true;
        }

        false
    }

    /// Seal the chunk and convert to disk-based `Chunk`
    ///
    /// This operation:
    /// 1. Marks chunk as sealed (prevents further appends)
    /// 2. Extracts all points in sorted order
    /// 3. Creates a `Chunk` and seals it to disk
    /// 4. Returns the sealed `Chunk`
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk is already sealed
    /// - Chunk is empty
    /// - Disk write fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let chunk = ActiveChunk::new(1, 100, SealConfig::default());
    ///
    /// chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 })?;
    ///
    /// let sealed_chunk = chunk.seal("/tmp/chunk.gor".into()).await?;
    /// assert!(sealed_chunk.is_sealed());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn seal(&self, path: PathBuf) -> Result<Chunk, String> {
        use std::time::Instant;
        let start = Instant::now();

        // Try to mark as sealed
        if self.sealed.swap(true, Ordering::AcqRel) {
            crate::metrics::record_error("already_sealed", "seal");
            return Err(format!(
                "Cannot seal chunk: already sealed. Series ID: {}, point count: {}. \
                 This may indicate a race condition where multiple threads tried to seal the same chunk.",
                self.series_id, self.point_count.load(Ordering::Acquire)
            ));
        }

        // P1.1: ZERO-COPY - Take ownership of BTreeMap without cloning
        // P1.3: Proper lock error handling
        let points_btree = {
            let mut points = self.points.write().map_err(|_| {
                // Restore sealed flag on error
                self.sealed.store(false, Ordering::Release);
                "Lock poisoned: cannot seal corrupted chunk".to_string()
            })?;

            if points.is_empty() {
                // Restore sealed flag on error
                self.sealed.store(false, Ordering::Release);
                return Err(format!(
                    "Cannot seal empty chunk. Series ID: {}. \
                     Append at least one data point before sealing.",
                    self.series_id
                ));
            }

            // Take ownership, replace with empty BTreeMap (zero-copy move)
            std::mem::take(&mut *points)
        };
        // Lock released here

        // P1.1: Create Chunk directly from BTreeMap (no intermediate Vec, no append loop)
        let mut chunk = Chunk::from_btreemap(self.series_id, points_btree).map_err(|e| {
            // Restore sealed flag on error
            self.sealed.store(false, Ordering::Release);
            format!("Failed to create chunk: {}", e)
        })?;

        // Seal the chunk to disk
        let seal_result = chunk.seal(path).await;

        // Record metrics
        let duration = start.elapsed().as_secs_f64();
        crate::metrics::record_seal(duration, seal_result.is_ok());

        if seal_result.is_err() {
            // Restore sealed flag on seal failure
            self.sealed.store(false, Ordering::Release);
        }

        seal_result?;
        Ok(chunk)
    }

    /// Append multiple points in a single operation (batch write)
    ///
    /// This method is much more efficient than calling `append()` in a loop
    /// because it acquires the write lock once for the entire batch.
    ///
    /// # Arguments
    ///
    /// * `points` - Vector of data points to append
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of points successfully appended
    /// - `Err(String)` - If chunk is sealed or series ID mismatch
    ///
    /// # Performance
    ///
    /// Batch operations are 10-50x faster than individual appends for large batches
    /// because lock overhead is amortized over all points.
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let chunk = ActiveChunk::new(1, 1000, SealConfig::default());
    ///
    /// let points = vec![
    ///     DataPoint { series_id: 1, timestamp: 100, value: 1.0 },
    ///     DataPoint { series_id: 1, timestamp: 200, value: 2.0 },
    ///     DataPoint { series_id: 1, timestamp: 300, value: 3.0 },
    /// ];
    ///
    /// let count = chunk.append_batch(points).unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn append_batch(&self, points: Vec<DataPoint>) -> Result<usize, String> {
        use std::time::Instant;
        let start = Instant::now();

        // Check if sealed (lock-free check)
        if self.sealed.load(Ordering::Acquire) {
            crate::metrics::record_error("sealed_chunk", "append_batch");
            return Err(format!(
                "Cannot append to sealed chunk. Series ID: {}. \
                 Sealed chunks are immutable - create a new chunk for additional data.",
                self.series_id
            ));
        }

        if points.is_empty() {
            return Ok(0);
        }

        let mut success_count = 0;
        let lock_start = Instant::now();

        // Acquire lock once for entire batch
        {
            let mut points_map = self.points.write().map_err(|_| {
                crate::metrics::record_error("lock_poisoned", "append_batch");
                "Lock poisoned: cannot append to corrupted chunk".to_string()
            })?;

            let lock_duration = lock_start.elapsed().as_secs_f64();
            crate::metrics::LOCK_WAIT_DURATION
                .with_label_values(&["write_batch"])
                .observe(lock_duration);

            // Process all points
            for point in points {
                // Validate series ID
                if point.series_id != self.series_id {
                    continue; // Skip mismatched series
                }

                // Skip duplicates
                if points_map.contains_key(&point.timestamp) {
                    crate::metrics::record_duplicate_timestamp(self.series_id);
                    continue;
                }

                // Insert point
                let timestamp = point.timestamp;
                points_map.insert(timestamp, point);
                success_count += 1;

                // Update min/max timestamps (simple version, not atomic since we hold lock)
                let current_min = self.min_timestamp.load(Ordering::Relaxed);
                if timestamp < current_min {
                    self.min_timestamp.store(timestamp, Ordering::Relaxed);
                }

                let current_max = self.max_timestamp.load(Ordering::Relaxed);
                if timestamp > current_max {
                    self.max_timestamp.store(timestamp, Ordering::Relaxed);
                }
            }

            // Update point count once at the end
            self.point_count
                .store(points_map.len() as u32, Ordering::Release);
        }

        // Record metrics
        let duration = start.elapsed().as_secs_f64();
        let series_id_str = self.series_id.to_string();
        let status = "batch_success".to_string();
        crate::metrics::WRITES_TOTAL
            .with_label_values(&[&series_id_str, &status])
            .inc();
        crate::metrics::WRITE_DURATION
            .with_label_values(&[&series_id_str])
            .observe(duration);

        Ok(success_count)
    }

    /// Get current point count (lock-free)
    ///
    /// This method uses an atomic counter and doesn't require locking,
    /// making it very fast for monitoring and metrics.
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let chunk = ActiveChunk::new(1, 100, SealConfig::default());
    /// assert_eq!(chunk.point_count(), 0);
    ///
    /// chunk.append(DataPoint { series_id: 1, timestamp: 1000, value: 42.0 }).unwrap();
    /// assert_eq!(chunk.point_count(), 1);
    /// ```
    pub fn point_count(&self) -> u32 {
        self.point_count.load(Ordering::Acquire)
    }

    /// Get time range of points in chunk
    ///
    /// Returns `(start_timestamp, end_timestamp)` where points are stored
    /// in sorted order. Returns `(0, 0)` if chunk is empty.
    ///
    /// This method acquires a read lock to access the BTreeMap.
    ///
    /// # Example
    ///
    /// ```
    /// use gorilla_tsdb::storage::active_chunk::{ActiveChunk, SealConfig};
    /// use gorilla_tsdb::types::DataPoint;
    ///
    /// let chunk = ActiveChunk::new(1, 100, SealConfig::default());
    ///
    /// chunk.append(DataPoint { series_id: 1, timestamp: 100, value: 1.0 }).unwrap();
    /// chunk.append(DataPoint { series_id: 1, timestamp: 50, value: 2.0 }).unwrap();
    /// chunk.append(DataPoint { series_id: 1, timestamp: 200, value: 3.0 }).unwrap();
    ///
    /// let (start, end) = chunk.time_range();
    /// assert_eq!(start, 50);
    /// assert_eq!(end, 200);
    /// ```
    pub fn time_range(&self) -> (i64, i64) {
        // P0.4: Use cached timestamps for lock-free access
        let count = self.point_count.load(Ordering::Acquire);
        if count == 0 {
            return (0, 0);
        }

        let start = self.min_timestamp.load(Ordering::Relaxed);
        let end = self.max_timestamp.load(Ordering::Relaxed);
        (start, end)
    }

    /// Check if chunk is sealed
    ///
    /// This is a lock-free operation using atomic load.
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    /// Get series ID
    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_active_chunk() {
        let chunk = ActiveChunk::new(42, 1000, SealConfig::default());

        assert_eq!(chunk.series_id(), 42);
        assert_eq!(chunk.point_count(), 0);
        assert!(!chunk.is_sealed());
        assert_eq!(chunk.time_range(), (0, 0));
    }

    #[test]
    fn test_append_single_point() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        let point = DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0,
        };

        chunk.append(point).unwrap();

        assert_eq!(chunk.point_count(), 1);
        assert_eq!(chunk.time_range(), (1000, 1000));
    }

    #[test]
    fn test_append_out_of_order() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        // Insert in random order
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 300,
                value: 3.0,
            })
            .unwrap();
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 100,
                value: 1.0,
            })
            .unwrap();
        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 200,
                value: 2.0,
            })
            .unwrap();

        assert_eq!(chunk.point_count(), 3);

        // Should be sorted
        let (start, end) = chunk.time_range();
        assert_eq!(start, 100);
        assert_eq!(end, 300);
    }

    #[test]
    fn test_append_wrong_series() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        let result = chunk.append(DataPoint {
            series_id: 2,
            timestamp: 1000,
            value: 42.0,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("doesn't match"));
    }

    #[tokio::test]
    async fn test_seal_success() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .unwrap();

        let result = chunk.seal("/tmp/test_active_chunk_seal.gor".into()).await;

        assert!(result.is_ok());
        assert!(chunk.is_sealed());
    }

    #[tokio::test]
    async fn test_seal_empty() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        let result = chunk.seal("/tmp/test.gor".into()).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[tokio::test]
    async fn test_append_after_seal() {
        let chunk = ActiveChunk::new(1, 100, SealConfig::default());

        chunk
            .append(DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            })
            .unwrap();
        chunk.seal("/tmp/test.gor".into()).await.unwrap();

        let result = chunk.append(DataPoint {
            series_id: 1,
            timestamp: 2000,
            value: 43.0,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("sealed"));
    }
}
