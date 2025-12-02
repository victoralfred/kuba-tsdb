//! Redis Pub/Sub for Cache Invalidation
//!
//! Provides event-driven cache invalidation across multiple application nodes.
//! When data is written to a series, an invalidation event is published so all
//! nodes can clear their local caches for that series.
//!
//! # Architecture
//!
//! ```text
//! Node A (writes data)          Node B (caches query)          Node C (caches query)
//!        │                              │                              │
//!        │ write(series_123)            │                              │
//!        │                              │                              │
//!        ├──── PUBLISH ────────────────►│                              │
//!        │  tsdb:invalidate:series      │ invalidate_series(123)       │
//!        │                              │                              │
//!        ├──── PUBLISH ─────────────────┼─────────────────────────────►│
//!        │  tsdb:invalidate:series      │                              │ invalidate_series(123)
//!        │                              │                              │
//! ```
//!
//! # Channel Names
//!
//! - `tsdb:invalidate:series` - Published when series data changes
//! - `tsdb:invalidate:chunk` - Published when a chunk is sealed
//! - `tsdb:invalidate:metadata` - Published when series metadata changes
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::redis::pubsub::{InvalidationPublisher, InvalidationSubscriber};
//!
//! // Publisher (on write path)
//! let publisher = InvalidationPublisher::new("redis://localhost:6379").await?;
//! publisher.publish_series_write(series_id, "cpu.usage", &["host:server1"]).await?;
//!
//! // Subscriber (on startup)
//! let subscriber = InvalidationSubscriber::new("redis://localhost:6379").await?;
//! let mut rx = subscriber.subscribe();
//! tokio::spawn(async move {
//!     while let Ok(event) = rx.recv().await {
//!         match event {
//!             InvalidationEvent::SeriesWrite { series_id, .. } => {
//!                 query_cache.invalidate_series(series_id);
//!             }
//!             _ => {}
//!         }
//!     }
//! });
//! ```

use futures::StreamExt;
use redis::{AsyncCommands, Client, RedisError};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

// ============================================================================
// Channel Names
// ============================================================================

/// Pub/Sub channel names for cache invalidation
pub mod channels {
    /// Channel for series data changes (writes)
    pub const INVALIDATE_SERIES: &str = "tsdb:invalidate:series";
    /// Channel for chunk seal events
    pub const INVALIDATE_CHUNK: &str = "tsdb:invalidate:chunk";
    /// Channel for metadata updates
    pub const INVALIDATE_METADATA: &str = "tsdb:invalidate:metadata";
}

// ============================================================================
// Invalidation Events
// ============================================================================

/// Helper module for u128 serialization as string (JSON doesn't support u128 natively)
mod u128_as_string {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Types of invalidation events that can be published/received
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum InvalidationEvent {
    /// Series data was written - invalidate query cache for this series
    SeriesWrite {
        /// The series that was modified (serialized as string for JSON compatibility)
        #[serde(with = "u128_as_string")]
        series_id: u128,
        /// Metric name for logging/debugging
        metric: String,
        /// Tags as "key:value" strings for logging/debugging
        tags: Vec<String>,
    },

    /// A chunk was sealed - may need to refresh chunk index cache
    ChunkSealed {
        /// The series containing the sealed chunk (serialized as string for JSON compatibility)
        #[serde(with = "u128_as_string")]
        series_id: u128,
        /// The chunk ID that was sealed
        chunk_id: String,
        /// Start timestamp of the chunk
        start_ts: i64,
        /// End timestamp of the chunk
        end_ts: i64,
    },

    /// Series metadata was updated
    MetadataUpdate {
        /// The series whose metadata changed (serialized as string for JSON compatibility)
        #[serde(with = "u128_as_string")]
        series_id: u128,
        /// Which field was updated (e.g., "tags", "retention")
        field: String,
    },
}

// ============================================================================
// Publisher
// ============================================================================

/// Publisher for invalidation events
///
/// Use this on the write path to notify other nodes about data changes.
/// Thread-safe and can be cloned for use across multiple tasks.
#[derive(Clone)]
pub struct InvalidationPublisher {
    /// Redis client for publishing
    client: Client,
    /// Statistics
    stats: Arc<PublisherStats>,
}

/// Statistics for the publisher
#[derive(Default)]
pub struct PublisherStats {
    /// Total events published
    pub published: AtomicU64,
    /// Total publish failures
    pub failures: AtomicU64,
}

impl InvalidationPublisher {
    /// Create a new publisher connected to Redis
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., "redis://localhost:6379")
    ///
    /// # Errors
    /// Returns error if unable to create Redis client
    pub fn new(redis_url: &str) -> Result<Self, RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self {
            client,
            stats: Arc::new(PublisherStats::default()),
        })
    }

    /// Publish a series write event
    ///
    /// Call this after successfully writing data to a series.
    /// Other nodes will invalidate their query caches for this series.
    ///
    /// # Arguments
    /// * `series_id` - The series that was written to
    /// * `metric` - Metric name (for logging)
    /// * `tags` - Tag strings (for logging)
    pub async fn publish_series_write(
        &self,
        series_id: u128,
        metric: &str,
        tags: &[String],
    ) -> Result<(), RedisError> {
        let event = InvalidationEvent::SeriesWrite {
            series_id,
            metric: metric.to_string(),
            tags: tags.to_vec(),
        };
        self.publish(channels::INVALIDATE_SERIES, &event).await
    }

    /// Publish a chunk sealed event
    ///
    /// Call this when a chunk is sealed and written to disk.
    /// Other nodes may need to refresh their chunk index caches.
    ///
    /// # Arguments
    /// * `series_id` - The series containing the chunk
    /// * `chunk_id` - The sealed chunk's ID
    /// * `start_ts` - Chunk start timestamp
    /// * `end_ts` - Chunk end timestamp
    pub async fn publish_chunk_sealed(
        &self,
        series_id: u128,
        chunk_id: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<(), RedisError> {
        let event = InvalidationEvent::ChunkSealed {
            series_id,
            chunk_id: chunk_id.to_string(),
            start_ts,
            end_ts,
        };
        self.publish(channels::INVALIDATE_CHUNK, &event).await
    }

    /// Publish a metadata update event
    ///
    /// Call this when series metadata is modified (e.g., tags changed).
    ///
    /// # Arguments
    /// * `series_id` - The series whose metadata changed
    /// * `field` - The field that was updated
    pub async fn publish_metadata_update(
        &self,
        series_id: u128,
        field: &str,
    ) -> Result<(), RedisError> {
        let event = InvalidationEvent::MetadataUpdate {
            series_id,
            field: field.to_string(),
        };
        self.publish(channels::INVALIDATE_METADATA, &event).await
    }

    /// Internal method to publish an event to a channel
    async fn publish(&self, channel: &str, event: &InvalidationEvent) -> Result<(), RedisError> {
        let payload = match serde_json::to_string(event) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "Failed to serialize invalidation event");
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
                return Err(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Serialization failed",
                )));
            }
        };

        let mut conn = self.client.get_multiplexed_async_connection().await?;

        match conn.publish::<_, _, i64>(channel, &payload).await {
            Ok(receivers) => {
                self.stats.published.fetch_add(1, Ordering::Relaxed);
                debug!(
                    channel = channel,
                    receivers = receivers,
                    "Published invalidation event"
                );
                Ok(())
            }
            Err(e) => {
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, channel = channel, "Failed to publish invalidation event");
                Err(e)
            }
        }
    }

    /// Get publisher statistics
    pub fn stats(&self) -> (u64, u64) {
        (
            self.stats.published.load(Ordering::Relaxed),
            self.stats.failures.load(Ordering::Relaxed),
        )
    }
}

// ============================================================================
// Subscriber
// ============================================================================

/// Configuration for the invalidation subscriber
#[derive(Clone, Debug)]
pub struct SubscriberConfig {
    /// Redis URL
    pub redis_url: String,
    /// Size of the broadcast channel buffer
    pub channel_buffer_size: usize,
    /// Whether to subscribe to series invalidation events
    pub subscribe_series: bool,
    /// Whether to subscribe to chunk invalidation events
    pub subscribe_chunks: bool,
    /// Whether to subscribe to metadata invalidation events
    pub subscribe_metadata: bool,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            channel_buffer_size: 1000,
            subscribe_series: true,
            subscribe_chunks: false, // Usually not needed
            subscribe_metadata: true,
        }
    }
}

impl SubscriberConfig {
    /// Create config with Redis URL
    pub fn with_url(url: impl Into<String>) -> Self {
        Self {
            redis_url: url.into(),
            ..Default::default()
        }
    }
}

/// Subscriber for invalidation events
///
/// Listens to Redis Pub/Sub channels and broadcasts events to local receivers.
/// Run the `listen()` method in a background task.
pub struct InvalidationSubscriber {
    /// Configuration
    config: SubscriberConfig,
    /// Broadcast sender for distributing events to local handlers
    tx: broadcast::Sender<InvalidationEvent>,
    /// Statistics
    stats: Arc<SubscriberStats>,
}

/// Statistics for the subscriber
#[derive(Default)]
pub struct SubscriberStats {
    /// Total events received
    pub received: AtomicU64,
    /// Total events that failed to parse
    pub parse_errors: AtomicU64,
    /// Number of reconnection attempts
    pub reconnects: AtomicU64,
}

impl InvalidationSubscriber {
    /// Create a new subscriber
    ///
    /// # Arguments
    /// * `config` - Subscriber configuration
    ///
    /// # Returns
    /// A new subscriber instance. Call `subscribe()` to get a receiver,
    /// then spawn `listen()` in a background task.
    pub fn new(config: SubscriberConfig) -> Self {
        let (tx, _) = broadcast::channel(config.channel_buffer_size);
        Self {
            config,
            tx,
            stats: Arc::new(SubscriberStats::default()),
        }
    }

    /// Get a receiver for invalidation events
    ///
    /// Can be called multiple times to get multiple receivers.
    /// Each receiver gets a copy of every event.
    pub fn subscribe(&self) -> broadcast::Receiver<InvalidationEvent> {
        self.tx.subscribe()
    }

    /// Get subscriber statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.received.load(Ordering::Relaxed),
            self.stats.parse_errors.load(Ordering::Relaxed),
            self.stats.reconnects.load(Ordering::Relaxed),
        )
    }

    /// Run the subscription loop
    ///
    /// This method blocks and listens for Pub/Sub messages.
    /// Should be spawned in a background task.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let subscriber = InvalidationSubscriber::new(config);
    /// let mut rx = subscriber.subscribe();
    ///
    /// // Spawn the listener
    /// tokio::spawn(async move {
    ///     if let Err(e) = subscriber.listen().await {
    ///         error!("Subscriber error: {}", e);
    ///     }
    /// });
    ///
    /// // Handle events
    /// while let Ok(event) = rx.recv().await {
    ///     // Process event
    /// }
    /// ```
    pub async fn listen(&self) -> Result<(), RedisError> {
        loop {
            match self.connect_and_listen().await {
                Ok(()) => {
                    // Normal shutdown
                    info!("Invalidation subscriber shutting down");
                    break Ok(());
                }
                Err(e) => {
                    self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, "Pub/Sub connection lost, reconnecting in 1s...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Internal method to connect and listen
    #[allow(deprecated)] // get_async_connection is required for PubSub
    async fn connect_and_listen(&self) -> Result<(), RedisError> {
        let client = Client::open(self.config.redis_url.as_str())?;
        // Note: We must use get_async_connection (not multiplexed) for PubSub
        // because PubSub requires a dedicated connection
        let conn = client.get_async_connection().await?;
        let mut pubsub = conn.into_pubsub();

        // Subscribe to configured channels
        if self.config.subscribe_series {
            pubsub.subscribe(channels::INVALIDATE_SERIES).await?;
            debug!("Subscribed to {}", channels::INVALIDATE_SERIES);
        }
        if self.config.subscribe_chunks {
            pubsub.subscribe(channels::INVALIDATE_CHUNK).await?;
            debug!("Subscribed to {}", channels::INVALIDATE_CHUNK);
        }
        if self.config.subscribe_metadata {
            pubsub.subscribe(channels::INVALIDATE_METADATA).await?;
            debug!("Subscribed to {}", channels::INVALIDATE_METADATA);
        }

        info!("Invalidation subscriber connected and listening");

        // Message processing loop
        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload()?;
            let channel: String = msg.get_channel_name().to_string();

            match serde_json::from_str::<InvalidationEvent>(&payload) {
                Ok(event) => {
                    self.stats.received.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        channel = channel,
                        event = ?event,
                        "Received invalidation event"
                    );

                    // Broadcast to all local receivers
                    // Ignore errors (no receivers is OK)
                    let _ = self.tx.send(event);
                }
                Err(e) => {
                    self.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        error = %e,
                        channel = channel,
                        payload = payload,
                        "Failed to parse invalidation event"
                    );
                }
            }
        }

        Ok(())
    }
}

// ============================================================================
// Helper function for integration
// ============================================================================

use crate::query::SharedQueryCache;

/// Set up cache invalidation listener
///
/// Creates a subscriber and spawns a background task to handle invalidation events.
/// Returns the subscriber handle for statistics access.
///
/// # Arguments
/// * `redis_url` - Redis connection URL
/// * `query_cache` - Shared query cache to invalidate
///
/// # Example
///
/// ```rust,ignore
/// let subscriber = setup_cache_invalidation("redis://localhost:6379", query_cache.clone()).await?;
/// ```
pub async fn setup_cache_invalidation(
    redis_url: &str,
    query_cache: SharedQueryCache,
) -> Result<Arc<InvalidationSubscriber>, RedisError> {
    let config = SubscriberConfig::with_url(redis_url);
    let subscriber = Arc::new(InvalidationSubscriber::new(config));

    // Get a receiver before spawning
    let mut rx = subscriber.subscribe();
    let subscriber_clone = subscriber.clone();

    // Spawn the listener task
    tokio::spawn(async move {
        if let Err(e) = subscriber_clone.listen().await {
            error!(error = %e, "Invalidation subscriber error");
        }
    });

    // Spawn the event handler task
    let cache = query_cache;
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match event {
                InvalidationEvent::SeriesWrite { series_id, .. } => {
                    cache.invalidate_series(series_id);
                    debug!(
                        series_id = series_id,
                        "Invalidated cache for series (via Pub/Sub)"
                    );
                }
                InvalidationEvent::ChunkSealed { series_id, .. } => {
                    // Could also invalidate chunk cache here if needed
                    cache.invalidate_series(series_id);
                }
                InvalidationEvent::MetadataUpdate { series_id, .. } => {
                    cache.invalidate_series(series_id);
                }
            }
        }
    });

    info!("Cache invalidation via Pub/Sub initialized");
    Ok(subscriber)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let event = InvalidationEvent::SeriesWrite {
            series_id: 12345,
            metric: "cpu.usage".to_string(),
            tags: vec!["host:server1".to_string()],
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: InvalidationEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, parsed);
    }

    #[test]
    fn test_chunk_sealed_serialization() {
        let event = InvalidationEvent::ChunkSealed {
            series_id: 67890,
            chunk_id: "chunk_001".to_string(),
            start_ts: 1000,
            end_ts: 2000,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("ChunkSealed"));
        assert!(json.contains("67890"));
    }

    #[test]
    fn test_metadata_update_serialization() {
        let event = InvalidationEvent::MetadataUpdate {
            series_id: 11111,
            field: "retention".to_string(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: InvalidationEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, parsed);
    }

    #[test]
    fn test_subscriber_config_defaults() {
        let config = SubscriberConfig::default();
        assert!(config.subscribe_series);
        assert!(!config.subscribe_chunks);
        assert!(config.subscribe_metadata);
        assert_eq!(config.channel_buffer_size, 1000);
    }

    #[test]
    fn test_subscriber_config_with_url() {
        let config = SubscriberConfig::with_url("redis://custom:6379");
        assert_eq!(config.redis_url, "redis://custom:6379");
    }
}
