//! HTTP Handlers for the Gorilla TSDB Server
//!
//! This module contains all HTTP endpoint handlers for the REST API.
//!
//! # Query Caching
//!
//! The server integrates a query result cache that:
//! - Caches query results with LRU eviction and TTL expiration
//! - Automatically invalidates cache entries when series data is written
//! - Tracks cache statistics (hits, misses, hit ratio)

use super::aggregation::compute_aggregation;
use super::config::ServerConfig;
use super::query_router;
use super::types::*;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use gorilla_tsdb::cache::InvalidationPublisher;
use gorilla_tsdb::cache::SharedQueryCache;
use gorilla_tsdb::engine::TimeSeriesDB;
use gorilla_tsdb::ingestion::schema::sanitize_tags_for_redis;
use gorilla_tsdb::query::ast::{Query as AstQuery, SelectQuery, SeriesSelector};
use gorilla_tsdb::query::result::QueryResult;
use gorilla_tsdb::query::subscription::SubscriptionManager;
use gorilla_tsdb::storage::LocalDiskEngine;
use gorilla_tsdb::types::{generate_series_id, DataPoint, SeriesId, TagFilter, TimeRange};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, warn};

// =============================================================================
// Application State
// =============================================================================

/// Shared application state containing database, storage, and configuration.
/// The subscriptions field is reserved for future real-time subscription features.
#[allow(dead_code)]
pub struct AppState {
    /// The time-series database instance
    pub db: TimeSeriesDB,
    /// Local disk storage engine
    pub storage: Arc<LocalDiskEngine>,
    /// Server configuration
    pub config: ServerConfig,
    /// Subscription manager for real-time updates
    pub subscriptions: Arc<SubscriptionManager>,
    /// Query result cache for reducing query latency
    pub query_cache: SharedQueryCache,
    /// Optional Pub/Sub publisher for cross-node cache invalidation
    /// Present when Redis is enabled and Pub/Sub setup succeeds
    pub invalidation_publisher: Option<InvalidationPublisher>,
}

// =============================================================================
// Health & Stats Handlers
// =============================================================================

/// Health check endpoint
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Get database statistics including query cache metrics
pub async fn get_stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    let db_stats = state.db.stats();
    let cache_stats = state.query_cache.stats();

    // Calculate hit rates
    let total_index_ops = db_stats.index_cache_hits + db_stats.index_cache_misses;
    let index_hit_rate = if total_index_ops > 0 {
        (db_stats.index_cache_hits as f64 / total_index_ops as f64) * 100.0
    } else {
        0.0
    };

    let query_cache_hits = cache_stats.hits.load(Ordering::Relaxed);
    let query_cache_misses = cache_stats.misses.load(Ordering::Relaxed);
    let total_query_ops = query_cache_hits + query_cache_misses;
    let query_hit_rate = if total_query_ops > 0 {
        (query_cache_hits as f64 / total_query_ops as f64) * 100.0
    } else {
        0.0
    };

    Json(StatsResponse {
        // Storage statistics
        total_chunks: db_stats.total_chunks,
        total_bytes: db_stats.total_bytes,
        total_series: db_stats.total_series,
        write_ops: db_stats.write_ops,
        read_ops: db_stats.read_ops,
        compression_ratio: db_stats.compression_ratio,

        // Index cache statistics
        index_cache_hits: db_stats.index_cache_hits,
        index_cache_misses: db_stats.index_cache_misses,
        index_queries_served: db_stats.index_queries_served,
        index_cache_hit_rate: index_hit_rate,

        // Query result cache statistics
        query_cache_hits,
        query_cache_misses,
        query_cache_entries: state.query_cache.entry_count() as u64,
        query_cache_size_bytes: state.query_cache.size_bytes(),
        query_cache_hit_rate: query_hit_rate,
        query_cache_evictions: cache_stats.evictions.load(Ordering::Relaxed),
        query_cache_invalidations: cache_stats.invalidations.load(Ordering::Relaxed),
    })
}

/// Unified cache statistics endpoint
///
/// Returns comprehensive cache statistics from all cache layers:
/// - Query cache (LRU with TTL for query results)
/// - Storage cache (when available, for chunk data)
///
/// # Response
///
/// ```json
/// {
///   "query": {
///     "entries": 100,
///     "size_bytes": 10240,
///     "hits": 500,
///     "misses": 100,
///     "hit_rate": 0.833,
///     "evictions": 10,
///     "invalidations": 5
///   },
///   "total_memory_bytes": 10240,
///   "overall_hit_rate": 0.833,
///   "total_hits": 500,
///   "total_misses": 100
/// }
/// ```
pub async fn get_cache_stats(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    use gorilla_tsdb::cache::UnifiedCacheManager;

    // Create unified manager (storage cache not yet integrated in AppState)
    let manager = UnifiedCacheManager::new(state.query_cache.clone(), None);
    let stats = manager.stats();

    Json(serde_json::to_value(&stats).unwrap_or(serde_json::json!({
        "error": "Failed to serialize cache stats"
    })))
}

/// Prometheus metrics endpoint with database and query cache statistics
pub async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_stats = state.db.stats();
    let cache_stats = state.query_cache.stats();

    let query_cache_hits = cache_stats.hits.load(Ordering::Relaxed);
    let query_cache_misses = cache_stats.misses.load(Ordering::Relaxed);

    let metrics = format!(
        "# HELP tsdb_total_chunks Total number of chunks\n\
         # TYPE tsdb_total_chunks gauge\n\
         tsdb_total_chunks {}\n\
         # HELP tsdb_total_bytes Total bytes stored\n\
         # TYPE tsdb_total_bytes gauge\n\
         tsdb_total_bytes {}\n\
         # HELP tsdb_total_series Total number of series\n\
         # TYPE tsdb_total_series gauge\n\
         tsdb_total_series {}\n\
         # HELP tsdb_write_ops_total Total write operations\n\
         # TYPE tsdb_write_ops_total counter\n\
         tsdb_write_ops_total {}\n\
         # HELP tsdb_read_ops_total Total read operations\n\
         # TYPE tsdb_read_ops_total counter\n\
         tsdb_read_ops_total {}\n\
         # HELP tsdb_compression_ratio Current compression ratio\n\
         # TYPE tsdb_compression_ratio gauge\n\
         tsdb_compression_ratio {}\n\
         # HELP tsdb_index_cache_hits Index cache hits\n\
         # TYPE tsdb_index_cache_hits counter\n\
         tsdb_index_cache_hits {}\n\
         # HELP tsdb_index_cache_misses Index cache misses\n\
         # TYPE tsdb_index_cache_misses counter\n\
         tsdb_index_cache_misses {}\n\
         # HELP tsdb_index_queries_served Total index queries served\n\
         # TYPE tsdb_index_queries_served counter\n\
         tsdb_index_queries_served {}\n\
         # HELP tsdb_query_cache_hits Query cache hits\n\
         # TYPE tsdb_query_cache_hits counter\n\
         tsdb_query_cache_hits {}\n\
         # HELP tsdb_query_cache_misses Query cache misses\n\
         # TYPE tsdb_query_cache_misses counter\n\
         tsdb_query_cache_misses {}\n\
         # HELP tsdb_query_cache_entries Current number of cached queries\n\
         # TYPE tsdb_query_cache_entries gauge\n\
         tsdb_query_cache_entries {}\n\
         # HELP tsdb_query_cache_size_bytes Current query cache size in bytes\n\
         # TYPE tsdb_query_cache_size_bytes gauge\n\
         tsdb_query_cache_size_bytes {}\n\
         # HELP tsdb_query_cache_evictions Total query cache evictions\n\
         # TYPE tsdb_query_cache_evictions counter\n\
         tsdb_query_cache_evictions {}\n\
         # HELP tsdb_query_cache_invalidations Total query cache invalidations\n\
         # TYPE tsdb_query_cache_invalidations counter\n\
         tsdb_query_cache_invalidations {}\n",
        db_stats.total_chunks,
        db_stats.total_bytes,
        db_stats.total_series,
        db_stats.write_ops,
        db_stats.read_ops,
        db_stats.compression_ratio,
        db_stats.index_cache_hits,
        db_stats.index_cache_misses,
        db_stats.index_queries_served,
        query_cache_hits,
        query_cache_misses,
        state.query_cache.entry_count(),
        state.query_cache.size_bytes(),
        cache_stats.evictions.load(Ordering::Relaxed),
        cache_stats.invalidations.load(Ordering::Relaxed),
    );
    (StatusCode::OK, [("content-type", "text/plain")], metrics)
}

// =============================================================================
// Write Handlers
// =============================================================================

/// Write data points
pub async fn write_points(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WriteRequest>,
) -> impl IntoResponse {
    if req.points.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                series_id: None,
                points_written: 0,
                chunk_id: None,
                error: Some("No points provided".to_string()),
            }),
        );
    }

    if req.points.len() > state.config.max_write_points {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                series_id: None,
                points_written: 0,
                chunk_id: None,
                error: Some(format!(
                    "Too many points: {} exceeds maximum {}",
                    req.points.len(),
                    state.config.max_write_points
                )),
            }),
        );
    }

    // Sanitize tags to prevent Redis key injection attacks.
    // Tags are used in Redis key construction (e.g., ts:tag:{key}:{value}:series),
    // so colons, newlines, and other special characters must be removed/replaced.
    let sanitized_tags = sanitize_tags_for_redis(&req.tags);

    // Determine series ID
    let series_id = match req.series_id {
        Some(id) => id,
        None => {
            match &req.metric {
                Some(metric) => {
                    // Use sanitized tags for series ID generation for consistency
                    let id = generate_series_id(metric, &sanitized_tags);

                    // Auto-register the series with sanitized tags
                    if let Err(e) = state
                        .storage
                        .register_series_metadata(
                            id,
                            metric,
                            sanitized_tags.clone(),
                            state.config.retention_days,
                        )
                        .await
                    {
                        warn!(error = %e, metric = %metric, "Series metadata persistence");
                    }

                    if let Err(e) = state
                        .db
                        .register_series(id, metric, sanitized_tags.clone())
                        .await
                    {
                        warn!(error = %e, metric = %metric, "Series index registration");
                    }

                    id
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(WriteResponse {
                            success: false,
                            series_id: None,
                            points_written: 0,
                            chunk_id: None,
                            error: Some(
                                "Must provide either 'series_id' or 'metric' name".to_string(),
                            ),
                        }),
                    );
                }
            }
        }
    };

    let points: Vec<DataPoint> = req
        .points
        .iter()
        .map(|p| DataPoint::new(series_id, p.timestamp, p.value))
        .collect();

    match state.db.write(series_id, points.clone()).await {
        Ok(chunk_id) => {
            // Invalidate any cached queries for this series to ensure freshness
            state.query_cache.invalidate_series(series_id);

            // Publish invalidation event to Redis for cross-node cache coherence
            // Use sanitized tags for consistency with what was stored
            if let Some(ref publisher) = state.invalidation_publisher {
                let metric = req.metric.as_deref().unwrap_or("unknown");
                let tags: Vec<String> = sanitized_tags
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .collect();

                // Fire-and-forget: don't block the write response on Pub/Sub
                let publisher = publisher.clone();
                let metric = metric.to_string();
                tokio::spawn(async move {
                    if let Err(e) = publisher
                        .publish_series_write(series_id, &metric, &tags)
                        .await
                    {
                        warn!(error = %e, series_id = series_id, "Failed to publish cache invalidation");
                    }
                });
            }

            (
                StatusCode::OK,
                Json(WriteResponse {
                    success: true,
                    series_id: Some(series_id),
                    points_written: points.len(),
                    chunk_id: Some(chunk_id.to_string()),
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, series_id = series_id, "Write failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WriteResponse {
                    success: false,
                    series_id: Some(series_id),
                    points_written: 0,
                    chunk_id: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

// =============================================================================
// Query Handlers
// =============================================================================

/// Query data points (REST API) with result caching
///
/// Results are cached based on (series_id, time_range, limit) for 60 seconds.
/// Cache is automatically invalidated when data is written to the series.
pub async fn query_points(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let time_range = TimeRange::new_unchecked(params.start, params.end);

    // Determine series ID
    let series_id: SeriesId = match params.series_id {
        Some(id_str) => match id_str.parse::<u128>() {
            Ok(id) => id,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(QueryResponse {
                        success: false,
                        series_id: None,
                        points: vec![],
                        aggregation: None,
                        error: Some(format!("Invalid series_id format: {}", id_str)),
                    }),
                );
            }
        },
        None => match &params.metric {
            Some(metric) => {
                let tags: HashMap<String, String> = params
                    .tags
                    .as_ref()
                    .and_then(|t| serde_json::from_str(t).ok())
                    .unwrap_or_default();

                let tag_filter = if tags.is_empty() {
                    TagFilter::All
                } else {
                    TagFilter::Exact(tags.clone())
                };

                match state.db.find_series(metric, &tag_filter).await {
                    Ok(series_ids) if !series_ids.is_empty() => series_ids[0],
                    Ok(_) => {
                        return (
                            StatusCode::NOT_FOUND,
                            Json(QueryResponse {
                                success: false,
                                series_id: None,
                                points: vec![],
                                aggregation: None,
                                error: Some(format!("Series not found for metric: {}", metric)),
                            }),
                        );
                    }
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(QueryResponse {
                                success: false,
                                series_id: None,
                                points: vec![],
                                aggregation: None,
                                error: Some(format!("Series lookup error: {}", e)),
                            }),
                        );
                    }
                }
            }
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(QueryResponse {
                        success: false,
                        series_id: None,
                        points: vec![],
                        aggregation: None,
                        error: Some("Must provide either 'series_id' or 'metric' name".to_string()),
                    }),
                );
            }
        },
    };

    // Build a Query AST for cache key generation
    let select_query =
        SelectQuery::new(SeriesSelector::by_id(series_id), time_range).with_limit(params.limit);
    let cache_query = AstQuery::Select(select_query);

    // Check cache first
    if let Some(cached_result) = state.query_cache.get(&cache_query) {
        tracing::debug!(series_id = series_id, "REST API query cache hit");

        // Convert cached result back to response
        let points: Vec<QueryPoint> = match cached_result.data {
            gorilla_tsdb::query::result::ResultData::Rows(rows) => rows
                .iter()
                .map(|r| QueryPoint {
                    timestamp: r.timestamp,
                    value: r.value,
                    series_id: None,
                    tags: None,
                })
                .collect(),
            _ => vec![],
        };

        // Apply aggregation if requested (on cached data)
        if let Some(ref agg_func) = params.aggregation {
            let data_points: Vec<DataPoint> = points
                .iter()
                .map(|p| DataPoint::new(series_id, p.timestamp, p.value))
                .collect();
            if let Some(agg_value) = compute_aggregation(agg_func, &data_points) {
                return (
                    StatusCode::OK,
                    Json(QueryResponse {
                        success: true,
                        series_id: Some(series_id),
                        points: vec![],
                        aggregation: Some(AggregationResult {
                            function: agg_func.clone(),
                            value: agg_value,
                            point_count: points.len(),
                            groups: None,
                        }),
                        error: None,
                    }),
                );
            }
        }

        return (
            StatusCode::OK,
            Json(QueryResponse {
                success: true,
                series_id: Some(series_id),
                points,
                aggregation: None,
                error: None,
            }),
        );
    }

    // Cache miss - execute query
    match state.db.query(series_id, time_range).await {
        Ok(mut points) => {
            points.sort_by_key(|p| p.timestamp);
            let total_points = points.len();
            points.truncate(params.limit);

            // Cache the result
            let cache_result = QueryResult::from_points(points.clone());
            state.query_cache.put(&cache_query, cache_result);
            tracing::debug!(series_id = series_id, "REST API query cached");

            // Apply aggregation if requested
            if let Some(ref agg_func) = params.aggregation {
                if let Some(agg_value) = compute_aggregation(agg_func, &points) {
                    return (
                        StatusCode::OK,
                        Json(QueryResponse {
                            success: true,
                            series_id: Some(series_id),
                            points: vec![],
                            aggregation: Some(AggregationResult {
                                function: agg_func.clone(),
                                value: agg_value,
                                point_count: total_points,
                                groups: None,
                            }),
                            error: None,
                        }),
                    );
                }
            }

            let response_points: Vec<QueryPoint> = points
                .iter()
                .map(|p| QueryPoint {
                    timestamp: p.timestamp,
                    value: p.value,
                    series_id: None,
                    tags: None,
                })
                .collect();

            (
                StatusCode::OK,
                Json(QueryResponse {
                    success: true,
                    series_id: Some(series_id),
                    points: response_points,
                    aggregation: None,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, series_id = series_id, "Query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryResponse {
                    success: false,
                    series_id: Some(series_id),
                    points: vec![],
                    aggregation: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

// =============================================================================
// SQL/PromQL Query Handler
// =============================================================================

/// Execute SQL or PromQL query with result caching
///
/// SELECT queries are cached based on their AST hash. Cache hits
/// return immediately without database access.
pub async fn execute_sql_promql_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlPromqlRequest>,
) -> impl IntoResponse {
    debug!(query = %req.query, language = %req.language, "Executing SQL/PromQL query");

    // Execute with caching enabled
    match query_router::execute_query_with_cache(
        &state.db,
        &req.query,
        &req.language,
        Some(&state.query_cache),
    )
    .await
    {
        Ok((language, query_type, _points, agg_execution_result, series_data, explain_result)) => {
            let (from_date, to_date) = series_data
                .as_ref()
                .map(|series| {
                    let mut min_ts = i64::MAX;
                    let mut max_ts = i64::MIN;
                    for s in series {
                        for p in &s.pointlist {
                            let ts = p[0] as i64;
                            min_ts = min_ts.min(ts);
                            max_ts = max_ts.max(ts);
                        }
                    }
                    if min_ts == i64::MAX {
                        (None, None)
                    } else {
                        (Some(min_ts), Some(max_ts))
                    }
                })
                .unwrap_or((None, None));

            let agg_result = agg_execution_result.map(|exec_result| {
                let point_count = exec_result
                    .groups
                    .as_ref()
                    .map(|g| g.iter().map(|gr| gr.point_count).sum())
                    .unwrap_or_else(|| {
                        series_data
                            .as_ref()
                            .map(|s| s.iter().map(|sd| sd.length).sum())
                            .unwrap_or(0)
                    });

                SqlAggregationResult {
                    function: exec_result.func_name,
                    value: Some(exec_result.value),
                    point_count,
                    buckets: exec_result.buckets,
                    groups: exec_result.groups,
                    time_aggregation: exec_result.time_aggregation,
                    warnings: exec_result.warnings,
                }
            });

            (
                StatusCode::OK,
                Json(SqlPromqlResponse {
                    status: "ok".to_string(),
                    series: series_data,
                    from_date,
                    to_date,
                    query_type: Some(query_type),
                    language: Some(language.to_string()),
                    aggregation: agg_result,
                    plan: explain_result,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, "Query execution failed");
            (
                StatusCode::BAD_REQUEST,
                Json(SqlPromqlResponse {
                    status: "error".to_string(),
                    series: None,
                    from_date: None,
                    to_date: None,
                    query_type: None,
                    language: None,
                    aggregation: None,
                    plan: None,
                    error: Some(e),
                }),
            )
        }
    }
}

// =============================================================================
// Series Management Handlers
// =============================================================================

/// Register a new series
pub async fn register_series(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeriesRequest>,
) -> impl IntoResponse {
    // Sanitize tags to prevent Redis key injection attacks.
    // Tags are used in Redis key construction (e.g., ts:tag:{key}:{value}:series),
    // so colons, newlines, and other special characters must be removed/replaced.
    let sanitized_tags = sanitize_tags_for_redis(&req.tags);

    // Use sanitized tags for series ID generation for consistency
    let series_id = req
        .series_id
        .unwrap_or_else(|| generate_series_id(&req.metric_name, &sanitized_tags));

    // Persist to storage with sanitized tags
    if let Err(e) = state
        .storage
        .register_series_metadata(
            series_id,
            &req.metric_name,
            sanitized_tags.clone(),
            state.config.retention_days,
        )
        .await
    {
        warn!(error = %e, metric = %req.metric_name, "Series metadata persistence");
    }

    match state
        .db
        .register_series(series_id, &req.metric_name, sanitized_tags)
        .await
    {
        Ok(()) => {
            debug!(series_id = series_id, metric = %req.metric_name, "Series registered");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "series_id": series_id
                })),
            )
        }
        Err(e) => {
            error!(error = %e, "Failed to register series");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "success": false,
                    "error": e.to_string()
                })),
            )
        }
    }
}

/// Find series by metric name and tags
pub async fn find_series(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FindSeriesParams>,
) -> impl IntoResponse {
    let tag_filter = params.tags.map(TagFilter::Exact).unwrap_or(TagFilter::All);

    match state.db.find_series(&params.metric_name, &tag_filter).await {
        Ok(series_ids) => {
            debug!(metric = %params.metric_name, count = series_ids.len(), "Found series");
            (
                StatusCode::OK,
                Json(FindSeriesResponse {
                    success: true,
                    series_ids,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, "Failed to find series");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FindSeriesResponse {
                    success: false,
                    series_ids: vec![],
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}
