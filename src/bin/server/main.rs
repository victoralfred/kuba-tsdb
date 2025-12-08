//! Kuba TSDB HTTP Server
//!
//! This binary provides a complete HTTP server for the Kuba time-series database.
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
//! - `POST /api/v1/integrity/scan` - Scan storage for corrupted chunks
//! - `GET /api/v1/compression/stats` - Compression performance metrics
//!
//! # CLI Commands
//!
//! - `start` - Start the HTTP server (default if no command specified)
//! - `check-config` - Validate configuration file
//! - `stats` - Show database statistics without starting server
//! - `compact` - Run compaction on data directory
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

use clap::{Parser, Subcommand};

use axum::{
    http::{HeaderValue, Method},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use config::{load_config_with_app, ServerConfig};
use handlers::AppState;
use kuba_tsdb::{
    cache::{setup_cache_invalidation, InvalidationPublisher, QueryCache, QueryCacheConfig},
    compression::AhpacCompressor,
    config::ApplicationConfig,
    engine::{DatabaseConfig, TimeSeriesDBBuilder},
    query::subscription::{SubscriptionConfig, SubscriptionManager},
    redis::{RedisConfig as RedisPoolConfig, RedisTimeIndex},
    storage::{BackgroundSealingConfig, LocalDiskEngine},
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::signal;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, info, warn};

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
        // Unified cache stats (Phase 4)
        .route("/api/v1/cache/stats", get(handlers::get_cache_stats))
        // Integrity scan
        .route("/api/v1/integrity/scan", post(handlers::integrity_scan))
        // Index rebuild (for recovering from corruption)
        .route("/api/v1/index/rebuild", post(handlers::rebuild_index))
        // Compression metrics
        .route(
            "/api/v1/compression/stats",
            get(handlers::get_compression_stats),
        )
        // State and CORS
        .with_state(state.clone())
        .layer(build_cors_layer(&state.config.cors_allowed_origins))
}

/// Graceful shutdown signal handler
///
/// EDGE-010: Handles signal registration failures gracefully by logging
/// a warning and waiting indefinitely (server must be killed forcefully).
/// This is preferable to panicking during startup.
async fn shutdown_signal() {
    let ctrl_c = async {
        match signal::ctrl_c().await {
            Ok(()) => {},
            Err(e) => {
                warn!(
                    error = %e,
                    "Ctrl+C handler installation failed - graceful shutdown unavailable"
                );
                std::future::pending::<()>().await;
            },
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            },
            Err(e) => {
                warn!(
                    error = %e,
                    "SIGTERM handler installation failed - SIGTERM shutdown unavailable"
                );
                std::future::pending::<()>().await;
            },
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown");
}

/// Flush all active database buffers during shutdown
///
/// This ensures no data is lost when the server is stopped gracefully.
/// Buffered points in active chunks are sealed and written to disk.
async fn flush_database_on_shutdown(state: &Arc<AppState>) {
    info!("Flushing active database buffers before shutdown...");

    let start = std::time::Instant::now();

    match state.db.flush_all().await {
        Ok(chunk_ids) => {
            let elapsed = start.elapsed();
            info!(
                chunks_flushed = chunk_ids.len(),
                elapsed_ms = elapsed.as_millis(),
                "Successfully flushed all active buffers"
            );
        },
        Err(e) => {
            warn!(
                error = %e,
                "Failed to flush some buffers during shutdown - data may be lost"
            );
        },
    }
}

/// Initialize the database
async fn init_database(
    config: &ServerConfig,
    app_config: &ApplicationConfig,
) -> Result<(kuba_tsdb::engine::TimeSeriesDB, Arc<LocalDiskEngine>), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&config.data_dir)?;

    let storage = Arc::new(LocalDiskEngine::new(config.data_dir.clone())?);
    storage.load_index().await?;

    let series_ids = storage.get_all_series_ids();
    if !series_ids.is_empty() {
        debug!("Found {} series on disk to index", series_ids.len());
    }

    let series_metadata = storage.get_all_series_metadata();
    if !series_metadata.is_empty() {
        debug!(
            "Loaded {} series metadata entries from disk",
            series_metadata.len()
        );
    }

    // Create AHPAC compressor with Neural strategy for adaptive ML-based codec selection
    let compressor = AhpacCompressor::new();

    let redis_url = if app_config.redis.enabled {
        Some(app_config.redis.url.clone())
    } else {
        None
    };

    // Use Neural compression strategy for online adaptive learning
    // This enables the ML-based codec selection that learns from compression feedback
    let db_config = DatabaseConfig {
        data_dir: config.data_dir.clone(),
        redis_url: redis_url.clone(),
        max_chunk_size: config.max_chunk_size,
        retention_days: config.retention_days,
        custom_options: HashMap::new(),
        compression_strategy: kuba_tsdb::ahpac::SelectionStrategy::Neural,
    };

    let storage_dyn: Arc<dyn kuba_tsdb::engine::traits::StorageEngine + Send + Sync> =
        storage.clone();

    let db = if app_config.redis.enabled {
        // SEC-004: Use sanitized URL in logs to prevent credential leakage
        let sanitized_url = kuba_tsdb::redis::util::sanitize_url(&app_config.redis.url);
        debug!("Connecting to Redis at {}...", sanitized_url);

        // Build RedisConfig using the builder pattern
        let redis_config = RedisPoolConfig::with_url(&app_config.redis.url)
            .pool_size(app_config.redis.pool_size as u32)
            .connection_timeout(Duration::from_secs(
                app_config.redis.connection_timeout_secs,
            ))
            .command_timeout(Duration::from_secs(app_config.redis.command_timeout_secs))
            .tls(app_config.redis.tls_enabled);

        let redis_index = RedisTimeIndex::new(redis_config).await?;
        debug!("Connected to Redis successfully");

        // Re-register series metadata using the register_series trait method
        for (series_id, metadata) in &series_metadata {
            use kuba_tsdb::engine::traits::TimeIndex;
            let index_metadata = kuba_tsdb::engine::traits::SeriesMetadata {
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
            .with_background_sealing_config(BackgroundSealingConfig::default())
            .build()
            .await?
    } else {
        debug!("Redis disabled, using in-memory index");

        let in_memory_index = kuba_tsdb::engine::InMemoryTimeIndex::new();
        // Re-register series metadata using the register_series trait method
        for (series_id, metadata) in &series_metadata {
            use kuba_tsdb::engine::traits::TimeIndex;
            let index_metadata = kuba_tsdb::engine::traits::SeriesMetadata {
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
            .with_background_sealing_config(BackgroundSealingConfig::default())
            .build()
            .await?
    };

    Ok((db, storage))
}

// =============================================================================
// CLI Definition
// =============================================================================

/// Kuba TSDB - High-performance time-series database
#[derive(Parser)]
#[command(name = "kuba-tsdb")]
#[command(author = "Victor Oseghale")]
#[command(version)]
#[command(about = "High-performance time-series database with adaptive compression", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to configuration file (overrides TSDB_CONFIG env var)
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Override listen address (e.g., 0.0.0.0:8080)
    #[arg(short, long, global = true)]
    listen: Option<String>,

    /// Override data directory path
    #[arg(short, long, global = true)]
    data_dir: Option<std::path::PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server (default)
    Start,

    /// Validate configuration file without starting the server
    CheckConfig,

    /// Show database statistics without starting the server
    Stats {
        /// Output format (text, json)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Run compaction on the data directory
    Compact {
        /// Dry run - show what would be compacted without actually doing it
        #[arg(long)]
        dry_run: bool,
    },

    /// Show compression statistics
    CompressionStats,
}

// =============================================================================
// CLI Command Handlers
// =============================================================================

/// Validate configuration and print summary
async fn cmd_check_config(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Apply config path override if specified
    if let Some(config_path) = &cli.config {
        std::env::set_var("TSDB_CONFIG", config_path);
    }

    let (config, app_config) = config::load_config_with_app();

    println!("Configuration is valid!");
    println!();
    println!("Server Settings:");
    println!("  Listen address: {}", config.listen_addr);
    println!("  Data directory: {:?}", config.data_dir);
    println!("  Max chunk size: {} bytes", config.max_chunk_size);
    println!();
    println!("Redis Settings:");
    println!("  Enabled: {}", app_config.redis.enabled);
    if app_config.redis.enabled {
        // Sanitize URL to hide credentials
        let sanitized = kuba_tsdb::redis::util::sanitize_url(&app_config.redis.url);
        println!("  URL: {}", sanitized);
        println!("  Pool size: {}", app_config.redis.pool_size);
    }
    println!();
    println!("Monitoring:");
    println!(
        "  Prometheus enabled: {}",
        app_config.monitoring.prometheus_enabled
    );
    println!("  Log level: {}", app_config.server.log_level);

    Ok(())
}

/// Show database statistics without starting the server
async fn cmd_stats(cli: &Cli, format: &str) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(config_path) = &cli.config {
        std::env::set_var("TSDB_CONFIG", config_path);
    }

    let (mut config, app_config) = config::load_config_with_app();

    // Apply CLI overrides
    if let Some(data_dir) = &cli.data_dir {
        config.data_dir = data_dir.clone();
    }

    // Initialize storage to read stats
    let storage = kuba_tsdb::storage::LocalDiskEngine::new(config.data_dir.clone())?;
    storage.load_index().await?;

    let series_ids = storage.get_all_series_ids();
    let series_metadata = storage.get_all_series_metadata();

    // Calculate storage size
    let storage_size = calculate_dir_size(&config.data_dir)?;

    if format == "json" {
        let stats = serde_json::json!({
            "data_dir": config.data_dir,
            "series_count": series_ids.len(),
            "series_with_metadata": series_metadata.len(),
            "storage_size_bytes": storage_size,
            "storage_size_human": format_bytes(storage_size),
            "redis_enabled": app_config.redis.enabled,
        });
        println!("{}", serde_json::to_string_pretty(&stats)?);
    } else {
        println!("Kuba TSDB Statistics");
        println!("====================");
        println!();
        println!("Data directory: {:?}", config.data_dir);
        println!("Series count: {}", series_ids.len());
        println!("Series with metadata: {}", series_metadata.len());
        println!("Storage size: {}", format_bytes(storage_size));
        println!("Redis enabled: {}", app_config.redis.enabled);
    }

    Ok(())
}

/// Run compaction on the data directory
async fn cmd_compact(cli: &Cli, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(config_path) = &cli.config {
        std::env::set_var("TSDB_CONFIG", config_path);
    }

    let (mut config, _) = config::load_config_with_app();

    if let Some(data_dir) = &cli.data_dir {
        config.data_dir = data_dir.clone();
    }

    println!("Compacting data directory: {:?}", config.data_dir);

    if dry_run {
        println!("[DRY RUN] Would compact the following:");
        // List files that would be compacted
        let storage = kuba_tsdb::storage::LocalDiskEngine::new(config.data_dir.clone())?;
        storage.load_index().await?;
        let series_ids = storage.get_all_series_ids();
        println!("  Series to process: {}", series_ids.len());
        println!("[DRY RUN] No changes made.");
        return Ok(());
    }

    // Create storage and run maintenance
    let storage = kuba_tsdb::storage::LocalDiskEngine::new(config.data_dir.clone())?;
    storage.load_index().await?;

    println!("Running maintenance...");
    let start = std::time::Instant::now();

    use kuba_tsdb::engine::traits::StorageEngine;
    match storage.maintenance().await {
        Ok(report) => {
            println!("Compaction complete in {:?}", start.elapsed());
            println!("  Chunks compacted: {}", report.chunks_compacted);
            println!("  Chunks deleted: {}", report.chunks_deleted);
            println!("  Bytes freed: {}", format_bytes(report.bytes_freed));
        },
        Err(e) => {
            eprintln!("Compaction failed: {}", e);
            return Err(e.into());
        },
    }

    Ok(())
}

/// Show compression statistics
async fn cmd_compression_stats(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(config_path) = &cli.config {
        std::env::set_var("TSDB_CONFIG", config_path);
    }

    let (mut config, _) = config::load_config_with_app();

    if let Some(data_dir) = &cli.data_dir {
        config.data_dir = data_dir.clone();
    }

    // Get compression metrics
    let metrics = kuba_tsdb::compression::global_metrics();
    let snapshot = metrics.snapshot();
    let total = &snapshot.total;

    println!("Compression Statistics");
    println!("======================");
    println!();
    println!("Uptime: {} seconds", snapshot.uptime_secs);
    println!();
    println!("Totals:");
    println!("  Compressions: {}", total.compress_count);
    println!("  Decompressions: {}", total.decompress_count);
    println!("  Original bytes: {}", format_bytes(total.original_bytes));
    println!(
        "  Compressed bytes: {}",
        format_bytes(total.compressed_bytes)
    );
    println!("  Points compressed: {}", total.points_compressed);
    println!();
    println!("Performance:");
    println!(
        "  Compression ratio: {:.2}:1",
        snapshot.overall_compression_ratio
    );
    println!("  Bits per sample: {:.2}", snapshot.overall_bits_per_sample);
    if total.compress_count > 0 {
        let avg_compress_us = total.compress_time_us / total.compress_count;
        println!("  Avg compression time: {} µs", avg_compress_us);
    }
    if total.decompress_count > 0 {
        let avg_decompress_us = total.decompress_time_us / total.decompress_count;
        println!("  Avg decompression time: {} µs", avg_decompress_us);
    }

    // Show per-codec breakdown if there are multiple codecs
    if !snapshot.per_codec.is_empty() {
        println!();
        println!("Per-Codec Breakdown:");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.compress_count > 0 {
                let ratio = if metrics.compressed_bytes > 0 {
                    metrics.original_bytes as f64 / metrics.compressed_bytes as f64
                } else {
                    0.0
                };
                println!(
                    "  {}: {} compressions, {:.2}:1 ratio",
                    codec, metrics.compress_count, ratio
                );
            }
        }
    }

    Ok(())
}

/// Calculate total size of a directory
fn calculate_dir_size(path: &std::path::Path) -> Result<u64, std::io::Error> {
    let mut total = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total += calculate_dir_size(&path)?;
            } else {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

/// Format bytes into human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

// =============================================================================
// Main Entry Point
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Route to appropriate command handler
    match &cli.command {
        Some(Commands::CheckConfig) => return cmd_check_config(&cli).await,
        Some(Commands::Stats { format }) => return cmd_stats(&cli, format).await,
        Some(Commands::Compact { dry_run }) => return cmd_compact(&cli, *dry_run).await,
        Some(Commands::CompressionStats) => return cmd_compression_stats(&cli).await,
        Some(Commands::Start) | None => {
            // Continue with server startup below
        },
    }

    // Apply config path override if specified via CLI
    if let Some(config_path) = &cli.config {
        std::env::set_var("TSDB_CONFIG", config_path);
    }

    // Initialize tracing
    let (mut config, app_config) = load_config_with_app();

    // Apply CLI overrides for listen address and data directory
    if let Some(listen) = &cli.listen {
        config.listen_addr = listen.clone();
    }
    if let Some(data_dir) = &cli.data_dir {
        config.data_dir = data_dir.clone();
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&app_config.server.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!("Starting Kuba TSDB Server v{}", env!("CARGO_PKG_VERSION"));
    debug!(
        "Configuration: listen_addr={}, data_dir={:?}",
        config.listen_addr, config.data_dir
    );

    // Initialize database
    let (db, storage) = init_database(&config, &app_config).await?;
    info!("Database initialized successfully");

    // Start background sealing service for automatic chunk persistence
    // This ensures chunks are flushed to disk even if they don't reach the point threshold
    // Default: seal after 500 points or 30 seconds, whichever comes first
    db.start_background_sealing().await;
    info!("Background sealing service started (500 points or 30s threshold)");

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
    debug!("Query result cache initialized (128 MB max, 60s TTL)");

    // Set up Redis Pub/Sub cache invalidation if Redis is enabled
    // This enables cross-node cache coherence in multi-instance deployments
    let invalidation_publisher = if app_config.redis.enabled {
        // Set up the subscriber (listens for invalidation events from other nodes)
        match setup_cache_invalidation(&app_config.redis.url, query_cache.clone()).await {
            Ok(_subscriber) => {
                debug!("Redis Pub/Sub cache invalidation subscriber enabled");
            },
            Err(e) => {
                // Non-fatal: local invalidation still works, just no cross-node sync
                tracing::warn!(
                    error = %e,
                    "Failed to set up Redis Pub/Sub subscriber, continuing with local-only invalidation"
                );
            },
        }

        // Set up the publisher (publishes invalidation events on writes)
        match InvalidationPublisher::new(&app_config.redis.url) {
            Ok(publisher) => {
                debug!("Redis Pub/Sub cache invalidation publisher enabled");
                Some(publisher)
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to create Redis Pub/Sub publisher, writes will not notify other nodes"
                );
                None
            },
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

    // Start background task to log neural predictor learning progress
    // Logs every 5 minutes when there's new learning activity
    let neural_state = state.clone();
    tokio::spawn(async move {
        let mut last_sample_count = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;
            let stats = neural_state.db.neural_predictor().stats();

            // Only log if there's been learning activity since last check
            if stats.sample_count > last_sample_count {
                let new_samples = stats.sample_count - last_sample_count;
                info!(
                    samples = stats.sample_count,
                    new_samples = new_samples,
                    learning_rate = format!("{:.5}", stats.learning_rate),
                    best_codec = ?stats.best_performing_codec(),
                    strategy = ?neural_state.db.compression_strategy(),
                    "Neural predictor learning progress"
                );
                last_sample_count = stats.sample_count;
            }
        }
    });

    // Build router
    let app = build_router(state.clone());

    // Parse listen address
    let addr: SocketAddr = config.listen_addr.parse()?;
    info!("Server listening on http://{}", addr);

    // Start server with ConnectInfo enabled for per-client rate limiting (SEC-003)
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Run the server until shutdown signal
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    // Flush active database buffers before exit
    // This ensures no data is lost when the server stops
    flush_database_on_shutdown(&state).await;

    info!("Server shutdown complete");
    Ok(())
}
