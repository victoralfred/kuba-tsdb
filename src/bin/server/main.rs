//! Gorilla TSDB HTTP Server
//!
//! This binary provides a complete HTTP server for the Gorilla time-series database.
//! It exposes REST endpoints for data ingestion, querying, and administration.
//!
//! # Endpoints
//!
//! ## Write
//! - `POST /api/v1/write` - Write data points
//!
//! ## Query
//! - `GET /api/v1/query` - Query data points
//! - `POST /api/v1/query/sql` - Execute SQL/PromQL queries
//!
//! ## Series Management
//! - `POST /api/v1/series` - Register a new series
//! - `GET /api/v1/series/find` - Find series by metric and tags
//!
//! ## Admin
//! - `GET /health` - Health check
//! - `GET /metrics` - Prometheus metrics
//! - `GET /api/v1/stats` - Database statistics
//!
//! # Configuration
//!
//! The server reads configuration from:
//! 1. `TSDB_CONFIG` environment variable (path to TOML file)
//! 2. `./application.toml` in current directory
//! 3. `./tsdb.toml` (legacy)
//! 4. Default configuration

// Allow manual modulo checks since is_multiple_of is unstable on stable Rust
#![allow(clippy::manual_is_multiple_of)]

mod aggregation;
mod config;
mod handlers;
mod query_router;
mod types;

use axum::{
    http::{HeaderValue, Method},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use config::{load_config_with_app, ServerConfig};
use gorilla_tsdb::{
    compression::gorilla::GorillaCompressor,
    config::ApplicationConfig,
    engine::{DatabaseConfig, TimeSeriesDBBuilder},
    query::{
        subscription::{SubscriptionConfig, SubscriptionManager},
        CacheConfig as QueryCacheConfig, QueryCache,
    },
    redis::{
        setup_cache_invalidation, InvalidationPublisher, RedisConfig as RedisPoolConfig,
        RedisTimeIndex,
    },
    storage::LocalDiskEngine,
};
use handlers::AppState;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::signal;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

// =============================================================================
// Router and Server Setup
// =============================================================================

/// Build CORS layer from configuration
fn build_cors_layer(cors_origins: &[String]) -> CorsLayer {
    if cors_origins.is_empty() {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(Any)
    } else {
        let origins: Vec<HeaderValue> =
            cors_origins.iter().filter_map(|o| o.parse().ok()).collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(Any)
    }
}

/// Build the application router
fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and metrics
        .route("/health", get(handlers::health))
        .route("/metrics", get(handlers::metrics))
        // Write API
        .route("/api/v1/write", post(handlers::write_points))
        // Query API
        .route("/api/v1/query", get(handlers::query_points))
        .route(
            "/api/v1/query/sql",
            post(handlers::execute_sql_promql_query),
        )
        // Series management
        .route("/api/v1/series", post(handlers::register_series))
        .route("/api/v1/series/find", get(handlers::find_series))
        // Stats
        .route("/api/v1/stats", get(handlers::get_stats))
        // State and CORS
        .with_state(state.clone())
        .layer(build_cors_layer(&state.config.cors_allowed_origins))
}

/// Graceful shutdown signal handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown");
}

/// Initialize the database
async fn init_database(
    config: &ServerConfig,
    app_config: &ApplicationConfig,
) -> Result<(gorilla_tsdb::engine::TimeSeriesDB, Arc<LocalDiskEngine>), Box<dyn std::error::Error>>
{
    std::fs::create_dir_all(&config.data_dir)?;

    let storage = Arc::new(LocalDiskEngine::new(config.data_dir.clone())?);
    storage.load_index().await?;

    let series_ids = storage.get_all_series_ids();
    if !series_ids.is_empty() {
        info!("Found {} series on disk to index", series_ids.len());
    }

    let series_metadata = storage.get_all_series_metadata();
    if !series_metadata.is_empty() {
        info!(
            "Loaded {} series metadata entries from disk",
            series_metadata.len()
        );
    }

    let compressor = GorillaCompressor::new();

    let redis_url = if app_config.redis.enabled {
        Some(app_config.redis.url.clone())
    } else {
        None
    };

    let db_config = DatabaseConfig {
        data_dir: config.data_dir.clone(),
        redis_url: redis_url.clone(),
        max_chunk_size: config.max_chunk_size,
        retention_days: config.retention_days,
        custom_options: HashMap::new(),
    };

    let storage_dyn: Arc<dyn gorilla_tsdb::engine::traits::StorageEngine + Send + Sync> =
        storage.clone();

    let db = if app_config.redis.enabled {
        info!("Connecting to Redis at {}...", app_config.redis.url);

        // Build RedisConfig using the builder pattern
        let redis_config = RedisPoolConfig::with_url(&app_config.redis.url)
            .pool_size(app_config.redis.pool_size as u32)
            .connection_timeout(Duration::from_secs(
                app_config.redis.connection_timeout_secs,
            ))
            .command_timeout(Duration::from_secs(app_config.redis.command_timeout_secs))
            .tls(app_config.redis.tls_enabled);

        let redis_index = RedisTimeIndex::new(redis_config).await?;
        info!("Connected to Redis successfully");

        // Re-register series metadata using the register_series trait method
        for (series_id, metadata) in &series_metadata {
            use gorilla_tsdb::engine::traits::TimeIndex;
            let index_metadata = gorilla_tsdb::engine::traits::SeriesMetadata {
                metric_name: metadata.metric_name.clone(),
                tags: metadata.tags.clone(),
                created_at: Utc::now().timestamp_millis(),
                retention_days: metadata.retention_days,
            };
            if let Err(e) = redis_index
                .register_series(*series_id, index_metadata)
                .await
            {
                tracing::warn!(series_id = *series_id, error = %e, "Failed to register series in Redis");
            }
        }

        TimeSeriesDBBuilder::new()
            .with_config(db_config)
            .with_storage_arc(storage_dyn)
            .with_index(redis_index)
            .with_compressor(compressor)
            .build()
            .await?
    } else {
        info!("Redis disabled, using in-memory index");

        let in_memory_index = gorilla_tsdb::engine::InMemoryTimeIndex::new();
        // Re-register series metadata using the register_series trait method
        for (series_id, metadata) in &series_metadata {
            use gorilla_tsdb::engine::traits::TimeIndex;
            let index_metadata = gorilla_tsdb::engine::traits::SeriesMetadata {
                metric_name: metadata.metric_name.clone(),
                tags: metadata.tags.clone(),
                created_at: Utc::now().timestamp_millis(),
                retention_days: metadata.retention_days,
            };
            if let Err(e) = in_memory_index
                .register_series(*series_id, index_metadata)
                .await
            {
                tracing::warn!(series_id = *series_id, error = %e, "Failed to register series in memory index");
            }
        }

        TimeSeriesDBBuilder::new()
            .with_config(db_config)
            .with_storage_arc(storage_dyn)
            .with_index(in_memory_index)
            .with_compressor(compressor)
            .build()
            .await?
    };

    Ok((db, storage))
}

// =============================================================================
// Main Entry Point
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let (config, app_config) = load_config_with_app();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&app_config.server.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!(
        "Starting Gorilla TSDB Server v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!(
        "Configuration: listen_addr={}, data_dir={:?}",
        config.listen_addr, config.data_dir
    );

    // Initialize database
    let (db, storage) = init_database(&config, &app_config).await?;
    info!("Database initialized successfully");

    // Create subscription manager with configured settings
    let subscription_config = SubscriptionConfig {
        max_subscribers_per_series: 1000,
        channel_buffer_size: 256,
        max_total_subscriptions: 100_000,
        default_window_duration: Duration::from_secs(10),
        enabled: true,
    };
    let subscriptions = Arc::new(SubscriptionManager::new(subscription_config));

    // Create query result cache with default configuration
    // 128 MB max size, 10K max entries, 60s TTL
    let query_cache_config = QueryCacheConfig::default();
    let query_cache = Arc::new(QueryCache::new(query_cache_config));
    info!("Query result cache initialized (128 MB max, 60s TTL)");

    // Set up Redis Pub/Sub cache invalidation if Redis is enabled
    // This enables cross-node cache coherence in multi-instance deployments
    let invalidation_publisher = if app_config.redis.enabled {
        // Set up the subscriber (listens for invalidation events from other nodes)
        match setup_cache_invalidation(&app_config.redis.url, query_cache.clone()).await {
            Ok(_subscriber) => {
                info!("Redis Pub/Sub cache invalidation subscriber enabled");
            }
            Err(e) => {
                // Non-fatal: local invalidation still works, just no cross-node sync
                tracing::warn!(
                    error = %e,
                    "Failed to set up Redis Pub/Sub subscriber, continuing with local-only invalidation"
                );
            }
        }

        // Set up the publisher (publishes invalidation events on writes)
        match InvalidationPublisher::new(&app_config.redis.url) {
            Ok(publisher) => {
                info!("Redis Pub/Sub cache invalidation publisher enabled");
                Some(publisher)
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to create Redis Pub/Sub publisher, writes will not notify other nodes"
                );
                None
            }
        }
    } else {
        None
    };

    // Create app state
    let state = Arc::new(AppState {
        db,
        storage,
        config: config.clone(),
        subscriptions,
        query_cache,
        invalidation_publisher,
    });

    // Build router
    let app = build_router(state);

    // Parse listen address
    let addr: SocketAddr = config.listen_addr.parse()?;
    info!("Server listening on http://{}", addr);

    // Start server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shutdown complete");
    Ok(())
}
