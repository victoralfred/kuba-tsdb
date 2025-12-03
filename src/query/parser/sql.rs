//! SQL-like Query Parser
//!
//! Parses SQL-like syntax into the Query AST.
//!
//! # Supported Syntax
//!
//! ```sql
//! -- Basic select
//! SELECT * FROM measurement
//!
//! -- With time range
//! SELECT * FROM cpu WHERE time >= now() - 1h AND time < now()
//!
//! -- With tag filters
//! SELECT * FROM cpu WHERE host = 'server01' AND region = 'us-east'
//!
//! -- Aggregations
//! SELECT avg(value), max(value) FROM cpu GROUP BY time(5m)
//!
//! -- Downsampling
//! SELECT * FROM cpu DOWNSAMPLE 1000 USING lttb
//!
//! -- Ordering and limits
//! SELECT * FROM cpu ORDER BY time DESC LIMIT 100 OFFSET 50
//!
//! -- Explain
//! EXPLAIN SELECT * FROM cpu
//! ```

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, opt, value},
    multi::separated_list0,
    sequence::{delimited, preceded},
    IResult, Parser,
};

use crate::query::ast::{
    AggregateQuery, Aggregation, AggregationFunction, DownsampleMethod, DownsampleQuery,
    FillStrategy, LatestQuery, OrderBy, OrderDirection, OrderField, Predicate, PredicateOp,
    PredicateValue, Query, SelectQuery, SeriesSelector, TagMatcher, WindowSpec, WindowType,
};
use crate::query::error::{QueryError, QueryResult};
use crate::types::{current_time_ms, TimeRange};
use std::time::Duration;

/// Parse a SQL-like query string into a Query AST
pub fn parse_sql(input: &str) -> QueryResult<Query> {
    match parse_query_internal(input.trim()) {
        Ok((remaining, query)) => {
            if remaining.trim().is_empty() {
                Ok(query)
            } else {
                Err(QueryError::parse(format!(
                    "Unexpected trailing input: '{}'",
                    remaining.trim()
                )))
            }
        }
        Err(e) => Err(QueryError::parse(format!("Parse error: {:?}", e))),
    }
}

// ============================================================================
// Top-level Parser
// ============================================================================

/// Parse the complete query
fn parse_query_internal(input: &str) -> IResult<&str, Query> {
    alt((parse_explain, parse_select_query)).parse(input)
}

/// Parse EXPLAIN prefix
fn parse_explain(input: &str) -> IResult<&str, Query> {
    let (input, _) = tag_no_case("EXPLAIN").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, inner) = parse_select_query(input)?;
    Ok((input, Query::Explain(Box::new(inner))))
}

/// Parse SELECT query
fn parse_select_query(input: &str) -> IResult<&str, Query> {
    let (input, _) = tag_no_case("SELECT").parse(input)?;
    let (input, _) = multispace1(input)?;

    // Parse column/aggregation list
    let (input, columns) = parse_column_list(input)?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM").parse(input)?;
    let (input, _) = multispace1(input)?;

    // Parse measurement name
    let (input, measurement) = parse_identifier(input)?;

    // Parse optional WHERE clause
    let (input, where_clause) = opt(preceded(
        (multispace1, tag_no_case("WHERE"), multispace1),
        parse_where_clause,
    ))
    .parse(input)?;

    // Parse optional GROUP BY clause
    let (input, group_by) = opt(preceded(
        (
            multispace1,
            tag_no_case("GROUP"),
            multispace1,
            tag_no_case("BY"),
            multispace1,
        ),
        parse_group_by,
    ))
    .parse(input)?;

    // Parse optional DOWNSAMPLE clause
    let (input, downsample) = opt(preceded(
        (multispace1, tag_no_case("DOWNSAMPLE"), multispace1),
        parse_downsample_clause,
    ))
    .parse(input)?;

    // Parse optional ORDER BY clause
    let (input, order_by) = opt(preceded(
        (
            multispace1,
            tag_no_case("ORDER"),
            multispace1,
            tag_no_case("BY"),
            multispace1,
        ),
        parse_order_by,
    ))
    .parse(input)?;

    // Parse optional LIMIT
    let (input, limit) = opt(preceded(
        (multispace1, tag_no_case("LIMIT"), multispace1),
        parse_number,
    ))
    .parse(input)?;

    // Parse optional OFFSET
    let (input, offset) = opt(preceded(
        (multispace1, tag_no_case("OFFSET"), multispace1),
        parse_number,
    ))
    .parse(input)?;

    // Build selector
    let mut selector = SeriesSelector::by_measurement(measurement).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    // Extract time range, tag matchers, and value predicates
    // **FIX**: Tag matchers are now added to the selector for proper index lookup
    let (time_range, predicates) = if let Some((tr, tag_matchers, preds)) = where_clause {
        // Add tag matchers to selector for index-based filtering
        for matcher in tag_matchers {
            selector.tag_filters.push(matcher);
        }
        (tr, preds)
    } else {
        (default_time_range(), vec![])
    };

    // Check for aggregation functions in columns
    if let Some((agg_funcs, _)) = extract_aggregations(&columns) {
        let window = group_by;
        let aggregation = Aggregation {
            function: agg_funcs[0],
            window,
            group_by: vec![],
            fill: FillStrategy::None,
        };

        return Ok((
            input,
            Query::Aggregate(AggregateQuery {
                selector,
                time_range,
                aggregation,
                predicates,
                order_by,
                limit,
            }),
        ));
    }

    // Check for DOWNSAMPLE
    if let Some((target_points, method)) = downsample {
        return Ok((
            input,
            Query::Downsample(DownsampleQuery {
                selector,
                time_range,
                target_points,
                method,
            }),
        ));
    }

    // Check for LATEST
    if columns
        .iter()
        .any(|c| c.to_uppercase() == "LATEST" || c.to_uppercase() == "LAST")
    {
        return Ok((
            input,
            Query::Latest(LatestQuery {
                selector,
                count: limit.unwrap_or(1),
            }),
        ));
    }

    // Default: basic SELECT query
    Ok((
        input,
        Query::Select(SelectQuery {
            selector,
            time_range,
            predicates,
            projections: if columns.iter().any(|c| c == "*") {
                vec![]
            } else {
                columns
            },
            order_by,
            limit,
            offset,
        }),
    ))
}

// ============================================================================
// Column Parsing
// ============================================================================

/// Parse column list
fn parse_column_list(input: &str) -> IResult<&str, Vec<String>> {
    alt((
        map(char('*'), |_| vec!["*".to_string()]),
        separated_list0((multispace0, char(','), multispace0), parse_column_item),
    ))
    .parse(input)
}

/// Parse a single column item
fn parse_column_item(input: &str) -> IResult<&str, String> {
    alt((parse_aggregation_call, map(parse_identifier, String::from))).parse(input)
}

/// Parse aggregation function call like avg(value)
fn parse_aggregation_call(input: &str) -> IResult<&str, String> {
    let (input, func_name) = alt((
        tag_no_case("count"),
        tag_no_case("sum"),
        tag_no_case("avg"),
        tag_no_case("min"),
        tag_no_case("max"),
        tag_no_case("stddev"),
        tag_no_case("variance"),
        tag_no_case("median"),
        tag_no_case("first"),
        tag_no_case("last"),
        tag_no_case("rate"),
        tag_no_case("percentile"),
    ))
    .parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _args) = take_while1(|c: char| c != ')')(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, format!("{}()", func_name.to_uppercase())))
}

/// Extract aggregation functions from column list
fn extract_aggregations(
    columns: &[String],
) -> Option<(Vec<AggregationFunction>, Option<WindowSpec>)> {
    let mut agg_funcs = Vec::new();

    for col in columns {
        let upper = col.to_uppercase();
        if upper.starts_with("COUNT(") {
            agg_funcs.push(AggregationFunction::Count);
        } else if upper.starts_with("SUM(") {
            agg_funcs.push(AggregationFunction::Sum);
        } else if upper.starts_with("AVG(") {
            agg_funcs.push(AggregationFunction::Avg);
        } else if upper.starts_with("MIN(") {
            agg_funcs.push(AggregationFunction::Min);
        } else if upper.starts_with("MAX(") {
            agg_funcs.push(AggregationFunction::Max);
        } else if upper.starts_with("STDDEV(") {
            agg_funcs.push(AggregationFunction::StdDev);
        } else if upper.starts_with("VARIANCE(") {
            agg_funcs.push(AggregationFunction::Variance);
        } else if upper.starts_with("MEDIAN(") {
            agg_funcs.push(AggregationFunction::Median);
        } else if upper.starts_with("FIRST(") {
            agg_funcs.push(AggregationFunction::First);
        } else if upper.starts_with("LAST(") {
            agg_funcs.push(AggregationFunction::Last);
        } else if upper.starts_with("RATE(") {
            agg_funcs.push(AggregationFunction::Rate);
        }
    }

    if agg_funcs.is_empty() {
        None
    } else {
        Some((agg_funcs, None))
    }
}

// ============================================================================
// WHERE Clause Parsing
// ============================================================================

/// Result from parsing WHERE clause
/// Contains time range, tag matchers, and value predicates
type WhereClauseResult = (TimeRange, Vec<TagMatcher>, Vec<Predicate>);

/// Parse WHERE clause
///
/// **FIX**: Now returns tag matchers separately from value predicates
/// so they can be added to the selector for proper index lookup.
fn parse_where_clause(input: &str) -> IResult<&str, WhereClauseResult> {
    let (input, conditions) = separated_list0(
        (multispace1, tag_no_case("AND"), multispace1),
        parse_condition,
    )
    .parse(input)?;

    let mut start_time: Option<i64> = None;
    let mut end_time: Option<i64> = None;
    let mut tag_matchers = Vec::new();
    let mut predicates = Vec::new();

    for cond in conditions {
        match cond {
            Condition::TimeGe(t) => start_time = Some(t),
            Condition::TimeGt(t) => start_time = Some(t + 1),
            Condition::TimeLe(t) => end_time = Some(t),
            Condition::TimeLt(t) => end_time = Some(t - 1),
            // **FIX**: Collect tag matchers separately
            Condition::Tag(matcher) => tag_matchers.push(matcher),
            Condition::Predicate(p) => predicates.push(p),
        }
    }

    let now = current_time_ms();
    let time_range = TimeRange {
        start: start_time.unwrap_or(now - 3_600_000), // Default: 1 hour ago in ms
        end: end_time.unwrap_or(now),
    };

    Ok((input, (time_range, tag_matchers, predicates)))
}

/// Condition from WHERE clause
///
/// Distinguishes between:
/// - Time conditions (for time range filtering)
/// - Tag conditions (for tag-based series filtering via selector)
/// - Value predicates (for post-filtering on data values)
#[derive(Debug)]
enum Condition {
    TimeGe(i64),
    TimeGt(i64),
    TimeLe(i64),
    TimeLt(i64),
    /// Tag condition: becomes TagMatcher in selector.tag_filters
    Tag(TagMatcher),
    /// Value predicate: stored in query.predicates for post-filtering
    Predicate(Predicate),
}

/// Parse a single condition
fn parse_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        parse_time_condition,
        parse_value_predicate,
        parse_tag_condition,
    ))
    .parse(input)
}

/// Parse time condition
fn parse_time_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("time").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = alt((tag(">="), tag(">"), tag("<="), tag("<"), tag("="))).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, time_value) = parse_time_expression(input)?;

    let cond = match op {
        ">=" => Condition::TimeGe(time_value),
        ">" => Condition::TimeGt(time_value),
        "<=" => Condition::TimeLe(time_value),
        "<" => Condition::TimeLt(time_value),
        "=" => Condition::TimeGe(time_value),
        _ => Condition::TimeGe(time_value),
    };

    Ok((input, cond))
}

/// Parse time expression
fn parse_time_expression(input: &str) -> IResult<&str, i64> {
    alt((parse_relative_time, parse_absolute_timestamp)).parse(input)
}

/// Parse relative time like: now() - 1h
fn parse_relative_time(input: &str) -> IResult<&str, i64> {
    let (input, _) = tag_no_case("now()").parse(input)?;
    let (input, offset) = opt(preceded(
        (multispace0, char('-'), multispace0),
        parse_duration,
    ))
    .parse(input)?;

    let now = current_time_ms();
    let result = if let Some(duration) = offset {
        now - (duration.as_millis() as i64)
    } else {
        now
    };

    Ok((input, result))
}

/// Parse duration like: 1h, 5m, 30s
fn parse_duration(input: &str) -> IResult<&str, Duration> {
    let (input, num_str) = digit1(input)?;
    let (input, unit) = alt((
        tag_no_case("ns"),
        tag_no_case("us"),
        tag_no_case("ms"),
        tag_no_case("s"),
        tag_no_case("m"),
        tag_no_case("h"),
        tag_no_case("d"),
        tag_no_case("w"),
    ))
    .parse(input)?;

    let num: u64 = num_str.parse().unwrap_or(0);
    let duration = match unit.to_lowercase().as_str() {
        "ns" => Duration::from_nanos(num),
        "us" => Duration::from_micros(num),
        "ms" => Duration::from_millis(num),
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        "h" => Duration::from_secs(num * 3600),
        "d" => Duration::from_secs(num * 86400),
        "w" => Duration::from_secs(num * 604800),
        _ => Duration::from_secs(0),
    };

    Ok((input, duration))
}

/// Parse absolute timestamp
fn parse_absolute_timestamp(input: &str) -> IResult<&str, i64> {
    let (input, num_str) = digit1(input)?;
    let num: i64 = num_str.parse().unwrap_or(0);
    Ok((input, num))
}

/// Parse value predicate like: value > 100
fn parse_value_predicate(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("value").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = parse_predicate_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, val) = parse_number_f64(input)?;

    Ok((
        input,
        Condition::Predicate(Predicate {
            field: "value".to_string(),
            op,
            value: PredicateValue::Float(val),
        }),
    ))
}

/// Parse predicate operator
fn parse_predicate_op(input: &str) -> IResult<&str, PredicateOp> {
    alt((
        value(PredicateOp::Gte, tag(">=")),
        value(PredicateOp::Lte, tag("<=")),
        value(PredicateOp::Gt, tag(">")),
        value(PredicateOp::Lt, tag("<")),
        value(PredicateOp::Eq, tag("=")),
        value(PredicateOp::Ne, tag("!=")),
        value(PredicateOp::Ne, tag("<>")),
    ))
    .parse(input)
}

/// Parse tag condition
///
/// Tag conditions have string values (quoted) and are used for series filtering.
/// **FIX**: Now returns Condition::Tag with TagMatcher for proper index lookup
/// instead of putting it in predicates where it would be ignored.
///
/// Supports:
/// - `host = 'server01'`  -> TagMatcher::Equals
/// - `host != 'server01'` -> TagMatcher::NotEquals
fn parse_tag_condition(input: &str) -> IResult<&str, Condition> {
    let (input, tag_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;

    // Parse operator (= or !=)
    let (input, op) = alt((tag("!="), tag("<>"), tag("="))).parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, tag_value) = parse_string_literal(input)?;

    // **FIX**: Create TagMatcher instead of Predicate
    let matcher = match op {
        "!=" | "<>" => TagMatcher::NotEquals {
            key: tag_name.to_string(),
            value: tag_value.to_string(),
        },
        _ => TagMatcher::Equals {
            key: tag_name.to_string(),
            value: tag_value.to_string(),
        },
    };

    Ok((input, Condition::Tag(matcher)))
}

// ============================================================================
// GROUP BY Parsing
// ============================================================================

/// Parse GROUP BY clause
fn parse_group_by(input: &str) -> IResult<&str, WindowSpec> {
    let (input, _) = tag_no_case("time").parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, duration) = parse_duration(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        WindowSpec {
            duration,
            window_type: WindowType::Tumbling,
            offset: None,
        },
    ))
}

// ============================================================================
// DOWNSAMPLE Parsing
// ============================================================================

/// Parse DOWNSAMPLE clause
fn parse_downsample_clause(input: &str) -> IResult<&str, (usize, DownsampleMethod)> {
    let (input, target_str) = digit1(input)?;
    let target: usize = target_str.parse().unwrap_or(1000);

    let (input, method) = opt(preceded(
        (multispace1, tag_no_case("USING"), multispace1),
        parse_downsample_method,
    ))
    .parse(input)?;

    Ok((input, (target, method.unwrap_or(DownsampleMethod::Lttb))))
}

/// Parse downsample method
fn parse_downsample_method(input: &str) -> IResult<&str, DownsampleMethod> {
    alt((
        value(DownsampleMethod::Lttb, tag_no_case("lttb")),
        value(DownsampleMethod::M4, tag_no_case("m4")),
        value(DownsampleMethod::Average, tag_no_case("average")),
        value(DownsampleMethod::Average, tag_no_case("avg")),
    ))
    .parse(input)
}

// ============================================================================
// ORDER BY / LIMIT Parsing
// ============================================================================

/// Parse ORDER BY clause
fn parse_order_by(input: &str) -> IResult<&str, OrderBy> {
    let (input, _) = tag_no_case("time").parse(input)?;
    let (input, direction) = opt(preceded(
        multispace1,
        alt((
            value(OrderDirection::Asc, tag_no_case("ASC")),
            value(OrderDirection::Desc, tag_no_case("DESC")),
        )),
    ))
    .parse(input)?;

    Ok((
        input,
        OrderBy {
            field: OrderField::Timestamp,
            direction: direction.unwrap_or(OrderDirection::Asc),
        },
    ))
}

/// Parse LIMIT number
fn parse_number(input: &str) -> IResult<&str, usize> {
    let (input, num_str) = digit1(input)?;
    let num: usize = num_str.parse().unwrap_or(0);
    Ok((input, num))
}

/// Parse floating point number
fn parse_number_f64(input: &str) -> IResult<&str, f64> {
    let (input, neg) = opt(char('-')).parse(input)?;
    let (input, int_part) = digit1(input)?;
    let (input, frac) = opt(preceded(char('.'), digit1)).parse(input)?;

    let mut num_str = String::new();
    if neg.is_some() {
        num_str.push('-');
    }
    num_str.push_str(int_part);
    if let Some(f) = frac {
        num_str.push('.');
        num_str.push_str(f);
    }

    let num: f64 = num_str.parse().unwrap_or(0.0);
    Ok((input, num))
}

// ============================================================================
// Utility Parsers
// ============================================================================

/// Parse identifier
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')(input)
}

/// Parse string literal
fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(char('\''), take_while(|c| c != '\''), char('\'')),
        delimited(char('"'), take_while(|c| c != '"'), char('"')),
    ))
    .parse(input)
}

/// Get default time range (last hour) in milliseconds
fn default_time_range() -> TimeRange {
    let now = current_time_ms();
    TimeRange {
        start: now - 3_600_000, // 1 hour in milliseconds
        end: now,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let result = parse_sql("SELECT * FROM cpu");
        assert!(result.is_ok());
        let query = result.unwrap();
        match query {
            Query::Select(q) => {
                assert!(q.selector.measurement.as_ref().unwrap() == "cpu");
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_select_with_tag_filter() {
        // **NEW TEST**: Verify tag conditions are added to selector
        let result = parse_sql("SELECT * FROM cpu WHERE host = 'server01'");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                // Tag should be in selector.tag_filters, not in predicates
                assert_eq!(q.selector.tag_filters.len(), 1);
                match &q.selector.tag_filters[0] {
                    TagMatcher::Equals { key, value } => {
                        assert_eq!(key, "host");
                        assert_eq!(value, "server01");
                    }
                    _ => panic!("Expected Equals matcher"),
                }
                // Value predicates should be empty (no value > X conditions)
                assert!(q.predicates.is_empty());
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_select_with_multiple_tags() {
        let result = parse_sql("SELECT * FROM cpu WHERE host = 'server01' AND region = 'us-east'");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                assert_eq!(q.selector.tag_filters.len(), 2);
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_select_with_tag_and_value_predicate() {
        // Both tag filter and value predicate
        let result = parse_sql("SELECT * FROM cpu WHERE host = 'server01' AND value > 100");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                // Tag filter in selector
                assert_eq!(q.selector.tag_filters.len(), 1);
                // Value predicate in predicates
                assert_eq!(q.predicates.len(), 1);
                assert!(matches!(q.predicates[0].op, PredicateOp::Gt));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_aggregation_with_tag_filter() {
        // Verify tag filters work in aggregate queries
        let result = parse_sql("SELECT avg(value) FROM cpu WHERE host = 'server01'");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert_eq!(q.selector.tag_filters.len(), 1);
                match &q.selector.tag_filters[0] {
                    TagMatcher::Equals { key, value } => {
                        assert_eq!(key, "host");
                        assert_eq!(value, "server01");
                    }
                    _ => panic!("Expected Equals matcher"),
                }
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_select_with_time_range() {
        let result = parse_sql("SELECT * FROM cpu WHERE time >= now() - 1h");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_aggregation() {
        let result = parse_sql("SELECT avg(value) FROM cpu");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(matches!(q.aggregation.function, AggregationFunction::Avg));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_group_by() {
        let result = parse_sql("SELECT avg(value) FROM cpu GROUP BY time(5m)");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(q.aggregation.window.is_some());
                let window = q.aggregation.window.unwrap();
                assert_eq!(window.duration, Duration::from_secs(300));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_downsample() {
        let result = parse_sql("SELECT * FROM cpu DOWNSAMPLE 1000 USING lttb");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Downsample(q) => {
                assert_eq!(q.target_points, 1000);
                assert!(matches!(q.method, DownsampleMethod::Lttb));
            }
            _ => panic!("Expected Downsample query"),
        }
    }

    #[test]
    fn test_parse_limit_offset() {
        let result = parse_sql("SELECT * FROM cpu LIMIT 100 OFFSET 50");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                assert_eq!(q.limit, Some(100));
                assert_eq!(q.offset, Some(50));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_explain() {
        let result = parse_sql("EXPLAIN SELECT * FROM cpu");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Explain(_) => {}
            _ => panic!("Expected Explain query"),
        }
    }

    #[test]
    fn test_parse_value_predicate() {
        let result = parse_sql("SELECT * FROM cpu WHERE value > 100");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                assert!(!q.predicates.is_empty());
                let pred = &q.predicates[0];
                assert!(matches!(pred.op, PredicateOp::Gt));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_order_by() {
        let result = parse_sql("SELECT * FROM cpu ORDER BY time DESC");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                assert!(q.order_by.is_some());
                assert!(matches!(
                    q.order_by.unwrap().direction,
                    OrderDirection::Desc
                ));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_duration_parsing() {
        let (_, d) = parse_duration("5m").unwrap();
        assert_eq!(d, Duration::from_secs(300));

        let (_, d) = parse_duration("1h").unwrap();
        assert_eq!(d, Duration::from_secs(3600));

        let (_, d) = parse_duration("1d").unwrap();
        assert_eq!(d, Duration::from_secs(86400));
    }
}
