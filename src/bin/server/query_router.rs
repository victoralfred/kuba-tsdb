//! Query Router - SQL/PromQL Parsing and Execution
//!
//! This module bridges the gap between the HTTP API and the query engine.
//! It parses SQL/PromQL queries, extracts parameters, and uses TimeSeriesDB
//! methods to retrieve and aggregate data.
//!
//! # Query Result Caching
//!
//! The router integrates with `QueryCache` to cache parsed query results:
//! - SELECT and AGGREGATE queries are cached based on AST hash
//! - Cache hits return results immediately without database access
//! - Cache misses execute the query and populate the cache

use super::aggregation::{
    aggregate_into_buckets, calculate_auto_interval, compute_aggregation_from_ast,
    create_time_aggregation_info, format_interval_ms, validate_interval, DEFAULT_TARGET_POINTS,
};
use super::types::{AggregateExecutionResult, GroupedAggregationResult, SeriesData};
use gorilla_tsdb::engine::TimeSeriesDB;
use gorilla_tsdb::query::ast::{AggregateQuery, DownsampleQuery, LatestQuery, SelectQuery};
use gorilla_tsdb::query::result::{QueryResult, ResultData, ResultRow, SeriesResult};
use gorilla_tsdb::query::SharedQueryCache;
use gorilla_tsdb::query::{
    parse_promql, parse_sql, AggregationFunction as QueryAggFunction, Query as ParsedQuery,
};
use gorilla_tsdb::types::{DataPoint, SeriesId, TimeRange};
use std::collections::HashMap;

/// Detected query language
#[derive(Debug, Clone, Copy)]
pub enum QueryLanguage {
    Sql,
    PromQL,
}

impl std::fmt::Display for QueryLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryLanguage::Sql => write!(f, "sql"),
            QueryLanguage::PromQL => write!(f, "promql"),
        }
    }
}

/// Execute a query string against the database with optional caching
///
/// Auto-detects the query language and routes to appropriate parser.
/// When a cache is provided, SELECT queries will check the cache first
/// and populate it on cache misses.
///
/// # Arguments
/// * `db` - The TimeSeriesDB instance
/// * `query_str` - The SQL or PromQL query string
/// * `language` - Query language hint ("sql", "promql", or "auto")
/// * `cache` - Optional query cache for result caching
///
/// # Returns
/// A tuple containing:
/// - `QueryLanguage`: Detected query language
/// - `String`: Query type (select, aggregate, etc.)
/// - `Vec<DataPoint>`: Raw data points (for backward compatibility)
/// - `Option<AggregateExecutionResult>`: Full aggregation result with time windowing
/// - `Option<Vec<SeriesData>>`: Series-grouped data for SELECT queries
#[allow(dead_code)] // Kept for backward compatibility / non-caching use cases
pub async fn execute_query(
    db: &TimeSeriesDB,
    query_str: &str,
    language: &str,
) -> Result<
    (
        QueryLanguage,
        String,
        Vec<DataPoint>,
        Option<AggregateExecutionResult>,
        Option<Vec<SeriesData>>,
    ),
    String,
> {
    execute_query_with_cache(db, query_str, language, None).await
}

/// Execute a query with optional result caching
///
/// This is the core execution function that supports caching.
/// SELECT queries are cached based on their AST hash.
pub async fn execute_query_with_cache(
    db: &TimeSeriesDB,
    query_str: &str,
    language: &str,
    cache: Option<&SharedQueryCache>,
) -> Result<
    (
        QueryLanguage,
        String,
        Vec<DataPoint>,
        Option<AggregateExecutionResult>,
        Option<Vec<SeriesData>>,
    ),
    String,
> {
    // Detect and parse query
    let (parsed, lang) = match language.to_lowercase().as_str() {
        "sql" => {
            let q = parse_sql(query_str).map_err(|e| format!("SQL parse error: {}", e))?;
            (q, QueryLanguage::Sql)
        }
        "promql" => {
            let q = parse_promql(query_str).map_err(|e| format!("PromQL parse error: {}", e))?;
            (q, QueryLanguage::PromQL)
        }
        _ => {
            // Auto-detect based on query content
            let trimmed = query_str.trim().to_uppercase();
            if trimmed.starts_with("SELECT") || trimmed.starts_with("EXPLAIN") {
                let q = parse_sql(query_str).map_err(|e| format!("SQL parse error: {}", e))?;
                (q, QueryLanguage::Sql)
            } else {
                let q =
                    parse_promql(query_str).map_err(|e| format!("PromQL parse error: {}", e))?;
                (q, QueryLanguage::PromQL)
            }
        }
    };

    // Check cache for SELECT queries
    if let Some(query_cache) = cache {
        if let Some(cached_result) = query_cache.get(&parsed) {
            tracing::debug!("Query cache hit for {:?}", query_str);
            return convert_cached_result(cached_result, lang);
        }
    }

    // Execute based on query type
    let result = match &parsed {
        ParsedQuery::Select(q) => {
            let (lang, qtype, points, series_data) = execute_select(db, q.clone(), lang).await?;

            // Cache the result for SELECT queries
            if let (Some(query_cache), Some(ref sd)) = (cache, &series_data) {
                let query_result = convert_to_query_result(sd);
                query_cache.put(&parsed, query_result);
                tracing::debug!("Cached SELECT query result");
            }

            Ok((lang, qtype, points, None, series_data))
        }
        ParsedQuery::Aggregate(q) => {
            let (lang, qtype, points, agg_result) = execute_aggregate(db, q.clone(), lang).await?;

            // Cache aggregate results (they're expensive to compute)
            if let Some(query_cache) = cache {
                let query_result =
                    QueryResult::from_rows(points.iter().map(|p| ResultRow::from(*p)).collect());
                query_cache.put(&parsed, query_result);
                tracing::debug!("Cached AGGREGATE query result");
            }

            Ok((lang, qtype, points, Some(agg_result), None))
        }
        ParsedQuery::Downsample(q) => {
            let (lang, qtype, points) = execute_downsample(db, q.clone(), lang).await?;

            // Cache downsample results
            if let Some(query_cache) = cache {
                let query_result = QueryResult::from_points(points.clone());
                query_cache.put(&parsed, query_result);
                tracing::debug!("Cached DOWNSAMPLE query result");
            }

            Ok((lang, qtype, points, None, None))
        }
        ParsedQuery::Latest(q) => {
            let (lang, qtype, points) = execute_latest(db, q.clone(), lang).await?;

            // Cache latest results (short TTL would be ideal, but using default)
            if let Some(query_cache) = cache {
                let query_result = QueryResult::from_points(points.clone());
                query_cache.put(&parsed, query_result);
                tracing::debug!("Cached LATEST query result");
            }

            Ok((lang, qtype, points, None, None))
        }
        ParsedQuery::Explain(_inner) => Ok((lang, "explain".to_string(), vec![], None, None)),
        ParsedQuery::Stream(_) => Err(
            "Stream queries are not supported via HTTP. Use WebSocket for streaming.".to_string(),
        ),
    };

    result
}

/// Convert SeriesData to QueryResult for caching
fn convert_to_query_result(series_data: &[SeriesData]) -> QueryResult {
    let series: Vec<SeriesResult> = series_data
        .iter()
        .enumerate()
        .map(|(idx, sd)| {
            let values: Vec<(i64, f64)> =
                sd.pointlist.iter().map(|p| (p[0] as i64, p[1])).collect();

            // Parse tags from tag_set (format: "key:value")
            let tags: HashMap<String, String> = sd
                .tag_set
                .iter()
                .filter_map(|t| {
                    let parts: Vec<&str> = t.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        Some((parts[0].to_string(), parts[1].to_string()))
                    } else {
                        None
                    }
                })
                .collect();

            // Store metric name in tags for reconstruction
            let mut tags_with_metric = tags;
            tags_with_metric.insert("__metric__".to_string(), sd.metric.clone());

            SeriesResult::new(idx as u128)
                .with_tags(tags_with_metric)
                .with_values(values)
        })
        .collect();

    QueryResult::from_series(series)
}

/// Convert cached QueryResult back to the execute_query return format
#[allow(clippy::type_complexity)]
fn convert_cached_result(
    result: QueryResult,
    lang: QueryLanguage,
) -> Result<
    (
        QueryLanguage,
        String,
        Vec<DataPoint>,
        Option<AggregateExecutionResult>,
        Option<Vec<SeriesData>>,
    ),
    String,
> {
    match result.data {
        ResultData::Series(series) => {
            let series_data: Vec<SeriesData> = series
                .into_iter()
                .map(|s| {
                    let pointlist: Vec<[f64; 2]> = s
                        .values
                        .iter()
                        .map(|(ts, val)| [*ts as f64, *val])
                        .collect();
                    let length = pointlist.len();

                    // Extract metric name from special tag
                    let metric = s
                        .tags
                        .get("__metric__")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());

                    // Build tag_set excluding internal __metric__ tag
                    let tag_set: Vec<String> = s
                        .tags
                        .iter()
                        .filter(|(k, _)| *k != "__metric__")
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect();
                    let scope = tag_set.join(",");

                    SeriesData {
                        metric,
                        scope,
                        tag_set,
                        pointlist,
                        length,
                    }
                })
                .collect();

            // Reconstruct DataPoints for backward compatibility
            let points: Vec<DataPoint> = series_data
                .iter()
                .flat_map(|sd| {
                    sd.pointlist
                        .iter()
                        .map(|p| DataPoint::new(0, p[0] as i64, p[1]))
                })
                .collect();

            Ok((lang, "select".to_string(), points, None, Some(series_data)))
        }
        ResultData::Scalar(val) => Ok((
            lang,
            "scalar".to_string(),
            vec![DataPoint::new(0, 0, val)],
            None,
            None,
        )),
        _ => Ok((lang, "unknown".to_string(), vec![], None, None)),
    }
}

/// Execute a SELECT query
async fn execute_select(
    db: &TimeSeriesDB,
    q: SelectQuery,
    lang: QueryLanguage,
) -> Result<
    (
        QueryLanguage,
        String,
        Vec<DataPoint>,
        Option<Vec<SeriesData>>,
    ),
    String,
> {
    // Get series IDs from selector
    let series_ids: Vec<SeriesId> = match q.selector.series_id {
        Some(id) => vec![id],
        None => {
            if let Some(ref measurement) = q.selector.measurement {
                let tag_filter = q.selector.to_tag_filter();
                db.find_series(measurement, &tag_filter)
                    .await
                    .map_err(|e| format!("Series lookup error: {}", e))?
            } else {
                return Err("No series_id or measurement specified in query".to_string());
            }
        }
    };

    if series_ids.is_empty() {
        return Ok((lang, "select".to_string(), vec![], Some(vec![])));
    }

    // Fetch tags for all series in batch
    let series_tags = db
        .get_series_tags_batch(&series_ids)
        .await
        .unwrap_or_default();

    let metric_name = q
        .selector
        .measurement
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    // Query each series separately and build series data
    let mut series_data: Vec<SeriesData> = Vec::new();
    let mut all_points: Vec<DataPoint> = Vec::new();

    for series_id in &series_ids {
        match db.query(*series_id, q.time_range).await {
            Ok(mut points) => {
                points.sort_by_key(|p| p.timestamp);

                let tags = series_tags.get(series_id).cloned().unwrap_or_default();
                let mut tag_set: Vec<String> =
                    tags.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
                tag_set.sort();
                let scope = tag_set.join(",");

                let pointlist: Vec<[f64; 2]> = points
                    .iter()
                    .map(|p| [p.timestamp as f64, p.value])
                    .collect();
                let length = pointlist.len();

                series_data.push(SeriesData {
                    metric: metric_name.clone(),
                    scope,
                    tag_set,
                    pointlist,
                    length,
                });

                all_points.extend(points);
            }
            Err(e) => {
                tracing::warn!("Error querying series {}: {}", series_id, e);
            }
        }
    }

    all_points.sort_by_key(|p| p.timestamp);

    // Apply limit and offset
    let mut result_points = all_points;
    if let Some(offset) = q.offset {
        result_points = result_points.into_iter().skip(offset).collect();
    }
    if let Some(limit) = q.limit {
        result_points = result_points.into_iter().take(limit).collect();
    }

    Ok((lang, "select".to_string(), result_points, Some(series_data)))
}

/// Execute an AGGREGATE query with optional GROUP BY and time windowing support
async fn execute_aggregate(
    db: &TimeSeriesDB,
    q: AggregateQuery,
    lang: QueryLanguage,
) -> Result<
    (
        QueryLanguage,
        String,
        Vec<DataPoint>,
        AggregateExecutionResult,
    ),
    String,
> {
    let func_name = format!("{:?}", q.aggregation.function).to_lowercase();
    let mut warnings: Vec<String> = Vec::new();

    // Get series IDs using selector's tag filter
    let series_ids: Vec<SeriesId> = match q.selector.series_id {
        Some(id) => vec![id],
        None => {
            if let Some(ref measurement) = q.selector.measurement {
                let tag_filter = q.selector.to_tag_filter();
                db.find_series(measurement, &tag_filter)
                    .await
                    .map_err(|e| format!("Series lookup error: {}", e))?
            } else {
                return Err("No series_id or measurement specified".to_string());
            }
        }
    };

    if series_ids.is_empty() {
        return Ok((
            lang,
            "aggregate".to_string(),
            vec![],
            AggregateExecutionResult {
                func_name,
                value: f64::NAN,
                groups: None,
                time_aggregation: None,
                warnings: vec![],
                buckets: None,
            },
        ));
    }

    // Determine time window configuration
    let time_range_ms = q.time_range.end - q.time_range.start;
    let (use_time_window, interval_ms, interval_str, time_mode) =
        if let Some(ref window_spec) = q.aggregation.window {
            let interval_ms = window_spec.duration.as_millis() as i64;
            let interval_str = format_interval_ms(interval_ms);
            if let Some(warning) = validate_interval(interval_ms, time_range_ms) {
                warnings.push(warning);
            }
            (true, interval_ms, interval_str, "custom".to_string())
        } else {
            let (interval_ms, interval_str) =
                calculate_auto_interval(time_range_ms, DEFAULT_TARGET_POINTS);
            (true, interval_ms, interval_str, "auto".to_string())
        };

    let group_by_tags = &q.aggregation.group_by;
    let has_group_by = !group_by_tags.is_empty();

    if has_group_by {
        // GROUP BY execution path with optional time windowing
        let series_tags = db
            .index()
            .get_series_tags_batch(&series_ids)
            .await
            .map_err(|e| format!("Failed to fetch series tags: {}", e))?;

        // Group series by tag values
        let mut groups: HashMap<Vec<(String, String)>, Vec<SeriesId>> = HashMap::new();

        for series_id in &series_ids {
            let tags = series_tags.get(series_id).cloned().unwrap_or_default();
            let mut group_key: Vec<(String, String)> = group_by_tags
                .iter()
                .map(|tag_key| {
                    let value = tags.get(tag_key).cloned().unwrap_or_default();
                    (tag_key.clone(), value)
                })
                .collect();
            group_key.sort();
            groups.entry(group_key).or_default().push(*series_id);
        }

        // Compute aggregation for each group
        let mut grouped_results: Vec<GroupedAggregationResult> = Vec::new();
        let mut total_bucket_count = 0usize;

        for (group_key, group_series_ids) in groups {
            let mut group_points: Vec<DataPoint> = Vec::new();
            for series_id in &group_series_ids {
                match db.query(*series_id, q.time_range).await {
                    Ok(points) => group_points.extend(points),
                    Err(e) => tracing::warn!("Error querying series {}: {}", series_id, e),
                }
            }

            group_points.sort_by_key(|p| p.timestamp);
            let (_, value) = compute_aggregation_from_ast(&q.aggregation.function, &group_points);

            let buckets = if use_time_window && !group_points.is_empty() {
                let b = aggregate_into_buckets(
                    &group_points,
                    interval_ms,
                    q.time_range,
                    &q.aggregation.function,
                );
                total_bucket_count = total_bucket_count.max(b.len());
                Some(b)
            } else {
                None
            };

            let mut tag_set: Vec<String> = group_key
                .iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect();
            tag_set.sort();
            let scope = tag_set.join(",");

            grouped_results.push(GroupedAggregationResult {
                scope,
                tag_set,
                value,
                point_count: group_points.len(),
                buckets,
            });
        }

        grouped_results.sort_by(|a, b| a.scope.cmp(&b.scope));

        // Compute total aggregation
        let total_value = if grouped_results.is_empty() {
            f64::NAN
        } else {
            match q.aggregation.function {
                QueryAggFunction::Sum | QueryAggFunction::Count => {
                    grouped_results.iter().map(|g| g.value).sum()
                }
                _ => {
                    let mut all_points: Vec<DataPoint> = Vec::new();
                    for series_id in &series_ids {
                        if let Ok(points) = db.query(*series_id, q.time_range).await {
                            all_points.extend(points);
                        }
                    }
                    all_points.sort_by_key(|p| p.timestamp);
                    let (_, val) =
                        compute_aggregation_from_ast(&q.aggregation.function, &all_points);
                    val
                }
            }
        };

        let time_aggregation = if use_time_window {
            Some(create_time_aggregation_info(
                time_mode,
                interval_str,
                interval_ms,
                total_bucket_count,
            ))
        } else {
            None
        };

        Ok((
            lang,
            "aggregate".to_string(),
            vec![],
            AggregateExecutionResult {
                func_name,
                value: total_value,
                groups: Some(grouped_results),
                time_aggregation,
                warnings,
                buckets: None,
            },
        ))
    } else {
        // Simple aggregation (no GROUP BY) with optional time windowing
        let mut all_points: Vec<DataPoint> = Vec::new();
        for series_id in &series_ids {
            match db.query(*series_id, q.time_range).await {
                Ok(points) => all_points.extend(points),
                Err(e) => tracing::warn!("Error querying series {}: {}", series_id, e),
            }
        }

        if all_points.is_empty() {
            return Ok((
                lang,
                "aggregate".to_string(),
                vec![],
                AggregateExecutionResult {
                    func_name,
                    value: f64::NAN,
                    groups: None,
                    time_aggregation: None,
                    warnings: vec![],
                    buckets: None,
                },
            ));
        }

        all_points.sort_by_key(|p| p.timestamp);
        let (_, value) = compute_aggregation_from_ast(&q.aggregation.function, &all_points);

        let (buckets, time_aggregation) = if use_time_window {
            let b = aggregate_into_buckets(
                &all_points,
                interval_ms,
                q.time_range,
                &q.aggregation.function,
            );
            let bucket_count = b.len();
            (
                Some(b),
                Some(create_time_aggregation_info(
                    time_mode,
                    interval_str,
                    interval_ms,
                    bucket_count,
                )),
            )
        } else {
            (None, None)
        };

        Ok((
            lang,
            "aggregate".to_string(),
            vec![],
            AggregateExecutionResult {
                func_name,
                value,
                groups: None,
                time_aggregation,
                warnings,
                buckets,
            },
        ))
    }
}

/// Execute a DOWNSAMPLE query
async fn execute_downsample(
    db: &TimeSeriesDB,
    q: DownsampleQuery,
    lang: QueryLanguage,
) -> Result<(QueryLanguage, String, Vec<DataPoint>), String> {
    let series_ids: Vec<SeriesId> = match q.selector.series_id {
        Some(id) => vec![id],
        None => {
            if let Some(ref measurement) = q.selector.measurement {
                let tag_filter = q.selector.to_tag_filter();
                db.find_series(measurement, &tag_filter)
                    .await
                    .map_err(|e| format!("Series lookup error: {}", e))?
            } else {
                return Err("No series_id or measurement specified".to_string());
            }
        }
    };

    if series_ids.is_empty() {
        return Ok((lang, "downsample".to_string(), vec![]));
    }

    let mut all_points: Vec<DataPoint> = Vec::new();
    for series_id in &series_ids {
        match db.query(*series_id, q.time_range).await {
            Ok(points) => all_points.extend(points),
            Err(e) => tracing::warn!("Error querying series {}: {}", series_id, e),
        }
    }

    all_points.sort_by_key(|p| p.timestamp);

    let step = (all_points.len() / q.target_points).max(1);
    let downsampled: Vec<DataPoint> = all_points
        .into_iter()
        .step_by(step)
        .take(q.target_points)
        .collect();

    Ok((lang, "downsample".to_string(), downsampled))
}

/// Execute a LATEST query
async fn execute_latest(
    db: &TimeSeriesDB,
    q: LatestQuery,
    lang: QueryLanguage,
) -> Result<(QueryLanguage, String, Vec<DataPoint>), String> {
    let series_ids: Vec<SeriesId> = match q.selector.series_id {
        Some(id) => vec![id],
        None => {
            if let Some(ref measurement) = q.selector.measurement {
                let tag_filter = q.selector.to_tag_filter();
                db.find_series(measurement, &tag_filter)
                    .await
                    .map_err(|e| format!("Series lookup error: {}", e))?
            } else {
                return Err("No series_id or measurement specified".to_string());
            }
        }
    };

    if series_ids.is_empty() {
        return Ok((lang, "latest".to_string(), vec![]));
    }

    let now = chrono::Utc::now().timestamp_millis();
    let time_range = TimeRange::new_unchecked(0, now);

    let mut all_points: Vec<DataPoint> = Vec::new();
    for series_id in &series_ids {
        match db.query(*series_id, time_range).await {
            Ok(points) => all_points.extend(points),
            Err(e) => tracing::warn!("Error querying series {}: {}", series_id, e),
        }
    }

    all_points.sort_by_key(|p| p.timestamp);

    let latest: Vec<DataPoint> = all_points
        .into_iter()
        .rev()
        .take(q.count)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    Ok((lang, "latest".to_string(), latest))
}
