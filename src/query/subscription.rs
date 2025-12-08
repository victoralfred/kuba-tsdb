//! Live Query Subscription Manager
//!
//! Provides real-time streaming of query results to subscribers using
//! tokio broadcast channels. Supports tail mode (raw points) and
//! windowed aggregation mode.
//!
//! # Architecture
//!
//! ```text
//! Data Ingestion
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ SubscriptionManager │
//! │  ┌───────────────┐  │
//! │  │ Subscribers   │  │
//! │  │ (by series)   │  │
//! │  └───────────────┘  │
//! └──────────┬──────────┘
//!            │ broadcast
//!            ▼
//! ┌──────────┬──────────┐
//! │ Sub 1    │ Sub 2    │  ... (concurrent subscribers)
//! └──────────┴──────────┘
//! ```
//!
//! # Usage
//!
//! ```rust
//! use kuba_tsdb::query::subscription::{SubscriptionManager, SubscriptionConfig};
//! use kuba_tsdb::types::DataPoint;
//!
//! let config = SubscriptionConfig::default();
//! let manager = SubscriptionManager::new(config);
//!
//! // Subscribe to series updates
//! let _rx = manager.subscribe(1);
//!
//! // When data arrives, notify subscribers
//! let point = DataPoint::new(1, 1000, 42.0);
//! manager.notify(1, point);
//!
//! // Check stats - subscriptions_created tracks total subscriptions made
//! let stats = manager.stats();
//! assert_eq!(stats.subscriptions_created, 1);
//! ```

use crate::query::ast::{Aggregation, StreamMode, StreamQuery};
use crate::query::result::{ResultRow, StreamingResult};
use crate::types::{DataPoint, SeriesId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the subscription manager
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Maximum number of subscribers per series (default: 1000)
    pub max_subscribers_per_series: usize,

    /// Buffer size for broadcast channels (default: 256)
    pub channel_buffer_size: usize,

    /// Maximum total subscriptions (default: 100,000)
    pub max_total_subscriptions: usize,

    /// Window duration for aggregated streams (default: 10 seconds)
    pub default_window_duration: Duration,

    /// Enable subscription (default: true)
    pub enabled: bool,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_subscribers_per_series: 1000,
            channel_buffer_size: 256,
            max_total_subscriptions: 100_000,
            default_window_duration: Duration::from_secs(10),
            enabled: true,
        }
    }
}

impl SubscriptionConfig {
    /// Create config with custom buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.channel_buffer_size = size;
        self
    }

    /// Set window duration for aggregated streams
    pub fn with_window_duration(mut self, duration: Duration) -> Self {
        self.default_window_duration = duration;
        self
    }

    /// Disable subscriptions
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

// ============================================================================
// Subscription Update
// ============================================================================

/// An update sent to subscribers
#[derive(Debug, Clone)]
pub struct SubscriptionUpdate {
    /// Series ID that was updated
    pub series_id: SeriesId,

    /// The data point or aggregated result
    pub data: UpdateData,

    /// Monotonic sequence number for ordering
    pub sequence: u64,

    /// Timestamp when update was generated
    pub timestamp_ns: i64,
}

/// Types of data in an update
#[derive(Debug, Clone)]
pub enum UpdateData {
    /// Single raw data point (Tail mode)
    Point(DataPoint),

    /// Batch of points (batch notification)
    Points(Vec<DataPoint>),

    /// Windowed aggregation result (Window mode)
    WindowResult {
        /// Window start timestamp
        window_start: i64,
        /// Window end timestamp
        window_end: i64,
        /// Aggregated value
        value: f64,
    },
}

impl SubscriptionUpdate {
    /// Create a point update
    pub fn point(series_id: SeriesId, point: DataPoint, sequence: u64) -> Self {
        Self {
            series_id,
            data: UpdateData::Point(point),
            sequence,
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        }
    }

    /// Create a batch update
    pub fn points(series_id: SeriesId, points: Vec<DataPoint>, sequence: u64) -> Self {
        Self {
            series_id,
            data: UpdateData::Points(points),
            sequence,
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        }
    }

    /// Create a window result update
    pub fn window_result(
        series_id: SeriesId,
        window_start: i64,
        window_end: i64,
        value: f64,
        sequence: u64,
    ) -> Self {
        Self {
            series_id,
            data: UpdateData::WindowResult {
                window_start,
                window_end,
                value,
            },
            sequence,
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        }
    }

    /// Convert to streaming result for client delivery
    pub fn to_streaming_result(&self, stream_id: &str) -> StreamingResult {
        let rows = match &self.data {
            UpdateData::Point(p) => {
                vec![ResultRow::new(p.timestamp, p.value).with_series(p.series_id)]
            },
            UpdateData::Points(pts) => pts
                .iter()
                .map(|p| ResultRow::new(p.timestamp, p.value).with_series(p.series_id))
                .collect(),
            UpdateData::WindowResult {
                window_start,
                value,
                ..
            } => vec![ResultRow::new(*window_start, *value).with_series(self.series_id)],
        };

        StreamingResult::new(stream_id, self.sequence, rows)
    }
}

// ============================================================================
// Series Subscription (internal)
// ============================================================================

/// Manages subscriptions for a single series
struct SeriesSubscription {
    /// Broadcast sender for this series
    sender: broadcast::Sender<SubscriptionUpdate>,

    /// Number of active receivers
    receiver_count: AtomicU64,

    /// Sequence counter for ordering
    sequence: AtomicU64,

    /// When subscription was created (for stale cleanup policies)
    created_at: Instant,
}

impl SeriesSubscription {
    /// Create a new series subscription
    fn new(buffer_size: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer_size);
        Self {
            sender,
            receiver_count: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Get a new receiver (subscriber)
    fn subscribe(&self) -> broadcast::Receiver<SubscriptionUpdate> {
        self.receiver_count.fetch_add(1, Ordering::Relaxed);
        self.sender.subscribe()
    }

    /// Send an update to all subscribers
    fn notify(
        &self,
        mut update: SubscriptionUpdate,
    ) -> Result<usize, broadcast::error::SendError<SubscriptionUpdate>> {
        // Assign sequence number
        update.sequence = self.sequence.fetch_add(1, Ordering::Relaxed);

        // Send to all subscribers (returns number of receivers)
        self.sender.send(update)
    }

    /// Get current subscriber count
    fn subscriber_count(&self) -> u64 {
        // Note: receiver_count is approximate since we don't decrement on drop
        // Use sender.receiver_count() for accurate count
        self.sender.receiver_count() as u64
    }

    /// Get age of this subscription (for stale cleanup policies)
    ///
    /// Returns the duration since this subscription was created.
    /// Used by cleanup logic to identify stale subscriptions.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

// ============================================================================
// Subscription Manager
// ============================================================================

/// Manager for live query subscriptions
///
/// Thread-safe manager that handles subscriptions across multiple series
/// and broadcasts updates to all subscribers of a series.
pub struct SubscriptionManager {
    /// Configuration
    config: SubscriptionConfig,

    /// Subscriptions by series ID
    subscriptions: RwLock<HashMap<SeriesId, SeriesSubscription>>,

    /// Statistics
    stats: SubscriptionStats,
}

/// Statistics for subscription manager
#[derive(Debug, Default)]
pub struct SubscriptionStats {
    /// Total subscriptions created
    pub subscriptions_created: AtomicU64,

    /// Total updates sent
    pub updates_sent: AtomicU64,

    /// Total updates dropped (no subscribers)
    pub updates_dropped: AtomicU64,

    /// Current active series with subscribers
    pub active_series: AtomicU64,
}

impl SubscriptionManager {
    /// Create a new subscription manager with default config
    pub fn new(config: SubscriptionConfig) -> Self {
        Self {
            config,
            subscriptions: RwLock::new(HashMap::new()),
            stats: SubscriptionStats::default(),
        }
    }

    /// Subscribe to updates for a series
    ///
    /// Returns a receiver that will receive updates when data is written
    /// to the specified series.
    pub fn subscribe(
        &self,
        series_id: SeriesId,
    ) -> Option<broadcast::Receiver<SubscriptionUpdate>> {
        if !self.config.enabled {
            return None;
        }

        let mut subs = self.subscriptions.write();

        // Check if we need to create a new subscription
        let sub = subs.entry(series_id).or_insert_with(|| {
            self.stats.active_series.fetch_add(1, Ordering::Relaxed);
            SeriesSubscription::new(self.config.channel_buffer_size)
        });

        // SEC: Check subscriber limit and total subscription limit
        // SEC: Cap max_subscribers_per_series to prevent DoS
        const MAX_SUBSCRIBERS_PER_SERIES: usize = 10_000; // 10K max
        let max_per_series = self
            .config
            .max_subscribers_per_series
            .min(MAX_SUBSCRIBERS_PER_SERIES);

        if sub.subscriber_count() >= max_per_series as u64 {
            return None;
        }

        // SEC: Check total subscription limit
        let total_subs = self.stats.subscriptions_created.load(Ordering::Relaxed);
        const MAX_TOTAL_SUBSCRIPTIONS: u64 = 1_000_000; // 1M max
        if total_subs >= MAX_TOTAL_SUBSCRIPTIONS {
            return None;
        }

        self.stats
            .subscriptions_created
            .fetch_add(1, Ordering::Relaxed);
        Some(sub.subscribe())
    }

    /// Subscribe to a stream query
    ///
    /// Creates a subscription based on the stream query's selector and mode.
    pub fn subscribe_query(&self, query: &StreamQuery) -> Option<QuerySubscription> {
        if !self.config.enabled {
            return None;
        }

        // Currently only supports single series subscriptions
        let series_id = query.selector.series_id?;
        let receiver = self.subscribe(series_id)?;

        Some(QuerySubscription {
            stream_id: format!("stream_{}", series_id),
            series_id,
            mode: query.mode,
            aggregation: query.aggregation.clone(),
            receiver,
        })
    }

    /// Notify subscribers of a new data point
    ///
    /// Call this when data is ingested to push updates to subscribers.
    pub fn notify(&self, series_id: SeriesId, point: DataPoint) {
        if !self.config.enabled {
            return;
        }

        let subs = self.subscriptions.read();
        if let Some(sub) = subs.get(&series_id) {
            let update = SubscriptionUpdate::point(series_id, point, 0);
            match sub.notify(update) {
                Ok(count) => {
                    self.stats
                        .updates_sent
                        .fetch_add(count as u64, Ordering::Relaxed);
                },
                Err(_) => {
                    self.stats.updates_dropped.fetch_add(1, Ordering::Relaxed);
                },
            }
        }
    }

    /// Notify subscribers of multiple data points
    pub fn notify_batch(&self, series_id: SeriesId, points: Vec<DataPoint>) {
        if !self.config.enabled || points.is_empty() {
            return;
        }

        let subs = self.subscriptions.read();
        if let Some(sub) = subs.get(&series_id) {
            let update = SubscriptionUpdate::points(series_id, points, 0);
            match sub.notify(update) {
                Ok(count) => {
                    self.stats
                        .updates_sent
                        .fetch_add(count as u64, Ordering::Relaxed);
                },
                Err(_) => {
                    self.stats.updates_dropped.fetch_add(1, Ordering::Relaxed);
                },
            }
        }
    }

    /// Notify subscribers of a window aggregation result
    pub fn notify_window(
        &self,
        series_id: SeriesId,
        window_start: i64,
        window_end: i64,
        value: f64,
    ) {
        if !self.config.enabled {
            return;
        }

        let subs = self.subscriptions.read();
        if let Some(sub) = subs.get(&series_id) {
            let update =
                SubscriptionUpdate::window_result(series_id, window_start, window_end, value, 0);
            match sub.notify(update) {
                Ok(count) => {
                    self.stats
                        .updates_sent
                        .fetch_add(count as u64, Ordering::Relaxed);
                },
                Err(_) => {
                    self.stats.updates_dropped.fetch_add(1, Ordering::Relaxed);
                },
            }
        }
    }

    /// Check if a series has any active subscribers
    pub fn has_subscribers(&self, series_id: SeriesId) -> bool {
        let subs = self.subscriptions.read();
        subs.get(&series_id)
            .map(|s| s.subscriber_count() > 0)
            .unwrap_or(false)
    }

    /// Get subscriber count for a series
    pub fn subscriber_count(&self, series_id: SeriesId) -> u64 {
        let subs = self.subscriptions.read();
        subs.get(&series_id)
            .map(|s| s.subscriber_count())
            .unwrap_or(0)
    }

    /// Get total number of series with subscriptions
    pub fn active_series_count(&self) -> usize {
        self.subscriptions.read().len()
    }

    /// Clean up stale subscriptions (series with no subscribers)
    ///
    /// Removes subscriptions that have no active subscribers.
    /// For age-based cleanup, use `cleanup_stale_with_age`.
    pub fn cleanup_stale(&self) {
        let mut subs = self.subscriptions.write();
        let before = subs.len();
        subs.retain(|_, sub| sub.subscriber_count() > 0);
        let removed = before - subs.len();
        if removed > 0 {
            self.stats
                .active_series
                .fetch_sub(removed as u64, Ordering::Relaxed);
        }
    }

    /// Clean up stale subscriptions with age check
    ///
    /// Removes subscriptions that have no active subscribers AND are older
    /// than the specified max_age. This prevents premature cleanup of
    /// newly created subscriptions that haven't received subscribers yet.
    ///
    /// Returns the number of subscriptions removed.
    pub fn cleanup_stale_with_age(&self, max_age: Duration) -> usize {
        let mut subs = self.subscriptions.write();
        let before = subs.len();

        subs.retain(|_, sub| {
            // Keep if has subscribers OR is younger than max_age
            sub.subscriber_count() > 0 || sub.age() < max_age
        });

        let removed = before - subs.len();
        if removed > 0 {
            self.stats
                .active_series
                .fetch_sub(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> SubscriptionStatsSnapshot {
        SubscriptionStatsSnapshot {
            subscriptions_created: self.stats.subscriptions_created.load(Ordering::Relaxed),
            updates_sent: self.stats.updates_sent.load(Ordering::Relaxed),
            updates_dropped: self.stats.updates_dropped.load(Ordering::Relaxed),
            active_series: self.subscriptions.read().len() as u64,
        }
    }
}

/// Snapshot of subscription statistics
#[derive(Debug, Clone)]
pub struct SubscriptionStatsSnapshot {
    /// Total subscriptions created
    pub subscriptions_created: u64,
    /// Total updates sent to subscribers
    pub updates_sent: u64,
    /// Updates dropped (no subscribers or channel full)
    pub updates_dropped: u64,
    /// Current active series with subscriptions
    pub active_series: u64,
}

// ============================================================================
// Query Subscription Handle
// ============================================================================

/// A subscription handle for a stream query
///
/// Wraps the broadcast receiver with query-specific metadata.
pub struct QuerySubscription {
    /// Unique stream identifier
    pub stream_id: String,

    /// Series being subscribed to
    pub series_id: SeriesId,

    /// Stream mode (Tail, Window, Changes)
    pub mode: StreamMode,

    /// Optional aggregation for windowed mode
    pub aggregation: Option<Aggregation>,

    /// The underlying receiver
    receiver: broadcast::Receiver<SubscriptionUpdate>,
}

impl QuerySubscription {
    /// Receive the next update (async)
    ///
    /// Returns None if the channel is closed.
    pub async fn recv(&mut self) -> Option<SubscriptionUpdate> {
        self.receiver.recv().await.ok()
    }

    /// Try to receive an update without blocking
    pub fn try_recv(&mut self) -> Option<SubscriptionUpdate> {
        self.receiver.try_recv().ok()
    }

    /// Get the stream ID
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }
}

// ============================================================================
// Shared Type Alias
// ============================================================================

/// Thread-safe shared subscription manager
pub type SharedSubscriptionManager = Arc<SubscriptionManager>;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_config_defaults() {
        let config = SubscriptionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.channel_buffer_size, 256);
    }

    #[test]
    fn test_subscription_manager_basic() {
        let config = SubscriptionConfig::default();
        let manager = SubscriptionManager::new(config);

        // Subscribe to series 1
        let rx = manager.subscribe(1);
        assert!(rx.is_some());
        assert!(manager.has_subscribers(1));
        assert_eq!(manager.subscriber_count(1), 1);
    }

    #[test]
    fn test_subscription_disabled() {
        let config = SubscriptionConfig::default().disabled();
        let manager = SubscriptionManager::new(config);

        // Subscribe should fail when disabled
        let rx = manager.subscribe(1);
        assert!(rx.is_none());
    }

    #[test]
    fn test_notify_no_subscribers() {
        let config = SubscriptionConfig::default();
        let manager = SubscriptionManager::new(config);

        // Notify without subscribers should not panic
        let point = DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0,
        };
        manager.notify(1, point);

        let stats = manager.stats();
        assert_eq!(stats.updates_sent, 0);
    }

    #[tokio::test]
    async fn test_subscribe_and_receive() {
        let config = SubscriptionConfig::default();
        let manager = Arc::new(SubscriptionManager::new(config));

        let mut rx = manager.subscribe(1).unwrap();

        // Notify in a separate task
        let mgr = manager.clone();
        tokio::spawn(async move {
            let point = DataPoint {
                series_id: 1,
                timestamp: 1000,
                value: 42.0,
            };
            mgr.notify(1, point);
        });

        // Receive the update
        let update = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("receive failed");

        assert_eq!(update.series_id, 1);
        match update.data {
            UpdateData::Point(p) => {
                assert_eq!(p.value, 42.0);
            },
            _ => panic!("Expected point update"),
        }
    }

    #[test]
    fn test_cleanup_stale() {
        let config = SubscriptionConfig::default();
        let manager = SubscriptionManager::new(config);

        // Subscribe and immediately drop the receiver
        {
            let _rx = manager.subscribe(1);
        }

        assert_eq!(manager.active_series_count(), 1);

        // Cleanup should remove series with no subscribers
        manager.cleanup_stale();

        // Note: cleanup might not remove immediately as broadcast tracks receivers internally
        // This tests the cleanup logic runs without error
    }

    #[test]
    fn test_subscription_update_to_streaming_result() {
        let point = DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.0,
        };
        let update = SubscriptionUpdate::point(1, point, 5);
        let result = update.to_streaming_result("test_stream");

        assert_eq!(result.stream_id, "test_stream");
        assert_eq!(result.sequence, 5);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].value, 42.0);
    }

    #[test]
    fn test_stats_snapshot() {
        let config = SubscriptionConfig::default();
        let manager = SubscriptionManager::new(config);

        let _rx = manager.subscribe(1);

        let stats = manager.stats();
        assert_eq!(stats.subscriptions_created, 1);
        assert_eq!(stats.active_series, 1);
    }
}
