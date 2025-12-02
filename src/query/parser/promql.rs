//! PromQL Query Parser
//!
//! Parses Prometheus Query Language (PromQL) syntax into the Query AST.
//!
//! # Supported Syntax
//!
//! ```promql
//! # Instant vector selector
//! cpu_usage
//! cpu_usage{host="server01"}
//!
//! # Range vector selector
//! cpu_usage[5m]
//! cpu_usage{host="server01"}[1h]
//!
//! # Aggregation functions
//! sum(cpu_usage)
//! avg(cpu_usage{host="server01"})
//! max(cpu_usage) by (host)
//!
//! # Rate functions
//! rate(http_requests_total[5m])
//! increase(http_requests_total[1h])
//! ```

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, digit1, multispace0},
    combinator::{map, opt, value},
    multi::separated_list0,
    sequence::{delimited, preceded},
    IResult, Parser,
};

use crate::query::ast::{
    AggregateQuery, Aggregation, AggregationFunction, FillStrategy, Query, SelectQuery,
    SeriesSelector, TagMatcher, WindowSpec, WindowType,
};
use crate::query::error::{QueryError, QueryResult};
use crate::types::TimeRange;
use std::time::Duration;

// ============================================================================
// Label Match Operators
// ============================================================================

/// PromQL label matching operators
///
/// Supports all four PromQL label matching semantics:
/// - `=`  : Exact string equality
/// - `!=` : String inequality
/// - `=~` : Regex match
/// - `!~` : Regex non-match
#[derive(Debug, Clone, PartialEq)]
pub enum LabelMatchOp {
    /// Exact equality: label="value"
    Equals,
    /// Inequality: label!="value"
    NotEquals,
    /// Regex match: label=~"pattern"
    RegexMatch,
    /// Regex non-match: label!~"pattern"
    RegexNotMatch,
}

/// Parse a PromQL query string into a Query AST
pub fn parse_promql(input: &str) -> QueryResult<Query> {
    match parse_promql_internal(input.trim()) {
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
        Err(e) => Err(QueryError::parse(format!("PromQL parse error: {:?}", e))),
    }
}

// ============================================================================
// Top-level Parser
// ============================================================================

/// Parse complete PromQL expression
fn parse_promql_internal(input: &str) -> IResult<&str, Query> {
    alt((
        parse_aggregation_expr,
        parse_rate_expr,
        parse_vector_selector,
    ))
    .parse(input)
}

// ============================================================================
// Aggregation Functions
// ============================================================================

/// Parse aggregation expression like: avg(cpu_usage{host="server01"})
fn parse_aggregation_expr(input: &str) -> IResult<&str, Query> {
    let (input, func) = parse_agg_function(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse inner vector selector (labels are now included in selector)
    let (input, (selector, range)) = parse_vector_selector_inner(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Parse optional BY clause
    let (input, group_by) = opt(preceded(
        (multispace0, tag_no_case("by"), multispace0),
        parse_label_list,
    ))
    .parse(input)?;

    // Parse optional offset - shifts the time range backward
    let (input, offset) = opt(preceded(
        (multispace1, tag_no_case("offset"), multispace1),
        parse_duration,
    ))
    .parse(input)?;

    // Build time range (using milliseconds to match database timestamp format)
    let now = current_time_millis();

    // Apply offset if present - shifts entire time window backward
    let adjusted_now = if let Some(off) = offset {
        now - (off.as_millis() as i64)
    } else {
        now
    };

    let time_range = if let Some(duration) = range {
        TimeRange {
            start: adjusted_now - (duration.as_millis() as i64),
            end: adjusted_now,
        }
    } else {
        // Default: 1 hour lookback in milliseconds
        TimeRange {
            start: adjusted_now - 3_600_000,
            end: adjusted_now,
        }
    };

    let window = range.map(|d| WindowSpec {
        duration: d,
        window_type: WindowType::Tumbling,
        offset,
    });

    Ok((
        input,
        Query::Aggregate(AggregateQuery {
            selector,
            time_range,
            aggregation: Aggregation {
                function: func,
                window,
                group_by: group_by.unwrap_or_default(),
                fill: FillStrategy::None,
            },
            predicates: vec![],
            order_by: None,
            limit: None,
        }),
    ))
}

/// Parse aggregation function name
/// Supports all 9 core aggregation functions plus aliases for compatibility
fn parse_agg_function(input: &str) -> IResult<&str, AggregationFunction> {
    alt((
        // Core aggregation functions
        value(AggregationFunction::Sum, tag_no_case("sum")),
        value(AggregationFunction::Avg, tag_no_case("avg")),
        value(AggregationFunction::Min, tag_no_case("min")),
        value(AggregationFunction::Max, tag_no_case("max")),
        value(AggregationFunction::Count, tag_no_case("count")),
        value(AggregationFunction::First, tag_no_case("first")),
        value(AggregationFunction::Last, tag_no_case("last")),
        // Statistical functions - stddev and variance with aliases
        value(AggregationFunction::StdDev, tag_no_case("stddev")),
        value(AggregationFunction::Variance, tag_no_case("variance")),
        value(AggregationFunction::Variance, tag_no_case("stdvar")), // PromQL standard alias
        // Additional functions
        value(AggregationFunction::Median, tag_no_case("median")),
        value(
            AggregationFunction::CountDistinct,
            tag_no_case("count_values"),
        ),
    ))
    .parse(input)
}

// ============================================================================
// Rate Functions
// ============================================================================

/// Parse rate expression like: `rate(http_requests_total[5m])`
/// Also supports offset: `rate(http_requests_total[5m]) offset 1h`
fn parse_rate_expr(input: &str) -> IResult<&str, Query> {
    let (input, func) = parse_rate_function(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse inner vector selector (labels are now included in selector)
    let (input, (selector, range)) = parse_vector_selector_inner(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Parse optional offset - shifts the time range backward
    let (input, offset) = opt(preceded(
        (multispace1, tag_no_case("offset"), multispace1),
        parse_duration,
    ))
    .parse(input)?;

    // Range is required for rate functions
    let range = range.ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    // Build time range (using milliseconds to match database timestamp format)
    let now = current_time_millis();

    // Apply offset if present - shifts entire time window backward
    let adjusted_now = if let Some(off) = offset {
        now - (off.as_millis() as i64)
    } else {
        now
    };

    let time_range = TimeRange {
        start: adjusted_now - (range.as_millis() as i64),
        end: adjusted_now,
    };

    let window = WindowSpec {
        duration: range,
        window_type: WindowType::Tumbling,
        offset,
    };

    Ok((
        input,
        Query::Aggregate(AggregateQuery {
            selector,
            time_range,
            aggregation: Aggregation {
                function: func,
                window: Some(window),
                group_by: vec![],
                fill: FillStrategy::None,
            },
            predicates: vec![],
            order_by: None,
            limit: None,
        }),
    ))
}

/// Parse rate function name
fn parse_rate_function(input: &str) -> IResult<&str, AggregationFunction> {
    alt((
        value(AggregationFunction::Rate, tag_no_case("rate")),
        value(AggregationFunction::Increase, tag_no_case("increase")),
        value(AggregationFunction::Delta, tag_no_case("delta")),
        value(AggregationFunction::Delta, tag_no_case("idelta")),
    ))
    .parse(input)
}

// ============================================================================
// Vector Selectors
// ============================================================================

/// Parse vector selector: `metric_name{labels}[range]`
fn parse_vector_selector(input: &str) -> IResult<&str, Query> {
    // Labels are now included in the selector (no longer returned separately)
    let (input, (selector, range)) = parse_vector_selector_inner(input)?;

    // Parse optional offset
    let (input, offset) = opt(preceded(
        (multispace1, tag_no_case("offset"), multispace1),
        parse_duration,
    ))
    .parse(input)?;

    // Build time range (using milliseconds to match database timestamp format)
    let now = current_time_millis();

    // Apply offset if present
    let adjusted_now = if let Some(off) = offset {
        now - (off.as_millis() as i64)
    } else {
        now
    };

    // Build time range
    let time_range = if let Some(duration) = range {
        TimeRange {
            start: adjusted_now - (duration.as_millis() as i64),
            end: adjusted_now,
        }
    } else {
        // Default: 1 hour lookback in milliseconds
        TimeRange {
            start: adjusted_now - 3_600_000,
            end: adjusted_now,
        }
    };

    Ok((
        input,
        Query::Select(SelectQuery {
            selector,
            time_range,
            predicates: vec![],
            projections: vec![],
            order_by: None,
            limit: None,
            offset: None,
        }),
    ))
}

/// Parsed vector selector result type
/// Returns (selector_with_tags, optional_range) - labels are now integrated into selector
type VectorSelectorResult = (SeriesSelector, Option<Duration>);

/// Parse vector selector components
///
/// Parses metric name, label matchers, and optional range.
/// **FIXED**: Labels are now properly added to the SeriesSelector's tag_filters
/// instead of being returned separately and discarded by callers.
#[allow(clippy::type_complexity)]
fn parse_vector_selector_inner(input: &str) -> IResult<&str, VectorSelectorResult> {
    // Parse metric name
    let (input, metric_name) = parse_metric_name(input)?;

    // Parse optional label matchers
    let (input, labels) = opt(parse_label_matchers).parse(input)?;
    let labels = labels.unwrap_or_default();

    // Parse optional range
    let (input, range) = opt(parse_range).parse(input)?;

    // Build selector with tag filters from labels
    let mut selector = SeriesSelector::by_measurement(metric_name).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    // **FIX**: Convert parsed labels to TagMatchers and add to selector
    // Previously, labels were returned but never added to the selector
    for (key, op, value) in labels {
        let matcher = match op {
            LabelMatchOp::Equals => TagMatcher::Equals { key, value },
            LabelMatchOp::NotEquals => TagMatcher::NotEquals { key, value },
            LabelMatchOp::RegexMatch => TagMatcher::Regex {
                key,
                pattern: value,
            },
            LabelMatchOp::RegexNotMatch => TagMatcher::NotRegex {
                key,
                pattern: value,
            },
        };
        selector.tag_filters.push(matcher);
    }

    Ok((input, (selector, range)))
}

/// Parse metric name
fn parse_metric_name(input: &str) -> IResult<&str, &str> {
    // Allow dots (.) in metric names to support Timeseries-style naming (e.g., system.cpu)
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == ':' || c == '.')(input)
}

/// Parse label matchers: {label1="value1", label2!="value2", label3=~"pattern"}
///
/// Parses the full label matcher block including all operators.
/// Returns a list of (key, operator, value) tuples.
fn parse_label_matchers(input: &str) -> IResult<&str, Vec<(String, LabelMatchOp, String)>> {
    delimited(
        (multispace0, char('{')),
        separated_list0((multispace0, char(','), multispace0), parse_label_matcher),
        (multispace0, char('}')),
    )
    .parse(input)
}

/// Parse single label matcher with operator
///
/// Captures the full label matcher including the operator type for proper
/// tag filtering. Supports all four PromQL operators: =, !=, =~, !~
///
/// # Returns
/// Tuple of (label_name, operator, value)
fn parse_label_matcher(input: &str) -> IResult<&str, (String, LabelMatchOp, String)> {
    let (input, _) = multispace0(input)?;
    let (input, label) = parse_label_name(input)?;
    let (input, _) = multispace0(input)?;

    // Parse operator and CAPTURE it (fix for BUG: operator was discarded)
    let (input, op) = alt((
        value(LabelMatchOp::RegexMatch, tag("=~")),
        value(LabelMatchOp::RegexNotMatch, tag("!~")),
        value(LabelMatchOp::NotEquals, tag("!=")),
        value(LabelMatchOp::Equals, tag("=")),
    ))
    .parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, val) = parse_string_value(input)?;

    Ok((input, (label.to_string(), op, val.to_string())))
}

/// Parse label name
fn parse_label_name(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

/// Parse string value
fn parse_string_value(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(char('"'), take_while(|c| c != '"'), char('"')),
        delimited(char('\''), take_while(|c| c != '\''), char('\'')),
    ))
    .parse(input)
}

/// Parse range: `[5m]`
fn parse_range(input: &str) -> IResult<&str, Duration> {
    delimited(
        (multispace0, char('[')),
        parse_duration,
        (multispace0, char(']')),
    )
    .parse(input)
}

/// Parse label list for BY clause
fn parse_label_list(input: &str) -> IResult<&str, Vec<String>> {
    delimited(
        (multispace0, char('(')),
        separated_list0(
            (multispace0, char(','), multispace0),
            map(parse_label_name, String::from),
        ),
        (multispace0, char(')')),
    )
    .parse(input)
}

// ============================================================================
// Duration Parsing
// ============================================================================

/// Parse PromQL duration
fn parse_duration(input: &str) -> IResult<&str, Duration> {
    let (input, num_str) = digit1(input)?;
    let (input, unit) = alt((
        tag("ms"),
        tag("s"),
        tag("m"),
        tag("h"),
        tag("d"),
        tag("w"),
        tag("y"),
    ))
    .parse(input)?;

    let num: u64 = num_str.parse().unwrap_or(0);
    let duration = match unit {
        "ms" => Duration::from_millis(num),
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        "h" => Duration::from_secs(num * 3600),
        "d" => Duration::from_secs(num * 86400),
        "w" => Duration::from_secs(num * 604800),
        "y" => Duration::from_secs(num * 31536000),
        _ => Duration::from_secs(0),
    };

    Ok((input, duration))
}

/// Parse at least one whitespace
fn multispace1(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_whitespace())(input)
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Get current time in milliseconds (matching database timestamp format)
fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::TagMatcher;

    #[test]
    fn test_parse_simple_metric() {
        let result = parse_promql("cpu_usage");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                assert_eq!(q.selector.measurement.as_ref().unwrap(), "cpu_usage");
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let result = parse_promql("cpu_usage{host=\"server01\"}");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                // **NEW TEST**: Verify labels are now added to selector
                assert_eq!(q.selector.tag_filters.len(), 1);
                match &q.selector.tag_filters[0] {
                    TagMatcher::Equals { key, value } => {
                        assert_eq!(key, "host");
                        assert_eq!(value, "server01");
                    }
                    _ => panic!("Expected Equals matcher"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_metric_with_multiple_labels() {
        let result = parse_promql("cpu_usage{host=\"server01\", region=\"us-east\"}");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                // **NEW TEST**: Verify multiple labels are added to selector
                assert_eq!(q.selector.tag_filters.len(), 2);
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_label_operators() {
        // Test all four label operators: =, !=, =~, !~

        // Test equality (=)
        let result = parse_promql("cpu{host=\"server01\"}");
        assert!(result.is_ok());
        if let Query::Select(q) = result.unwrap() {
            assert!(matches!(
                &q.selector.tag_filters[0],
                TagMatcher::Equals { .. }
            ));
        }

        // Test inequality (!=)
        let result = parse_promql("cpu{host!=\"server01\"}");
        assert!(result.is_ok());
        if let Query::Select(q) = result.unwrap() {
            assert!(matches!(
                &q.selector.tag_filters[0],
                TagMatcher::NotEquals { .. }
            ));
        }

        // Test regex match (=~)
        let result = parse_promql("cpu{host=~\"server.*\"}");
        assert!(result.is_ok());
        if let Query::Select(q) = result.unwrap() {
            match &q.selector.tag_filters[0] {
                TagMatcher::Regex { key, pattern } => {
                    assert_eq!(key, "host");
                    assert_eq!(pattern, "server.*");
                }
                _ => panic!("Expected Regex matcher"),
            }
        }

        // Test regex non-match (!~)
        let result = parse_promql("cpu{host!~\"test.*\"}");
        assert!(result.is_ok());
        if let Query::Select(q) = result.unwrap() {
            assert!(matches!(
                &q.selector.tag_filters[0],
                TagMatcher::NotRegex { .. }
            ));
        }
    }

    #[test]
    fn test_aggregation_preserves_labels() {
        // Verify labels are preserved in aggregation queries
        let result = parse_promql("sum(http_requests{status=\"200\", method=\"GET\"})");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert_eq!(q.selector.tag_filters.len(), 2);
                // Verify the first filter
                match &q.selector.tag_filters[0] {
                    TagMatcher::Equals { key, value } => {
                        assert_eq!(key, "status");
                        assert_eq!(value, "200");
                    }
                    _ => panic!("Expected Equals matcher"),
                }
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_range_vector() {
        let result = parse_promql("cpu_usage[5m]");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                // TimeRange uses milliseconds, 5 minutes = 300,000 ms
                let duration = q.time_range.end - q.time_range.start;
                assert!((299_000..=301_000).contains(&duration));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_parse_range_with_labels() {
        let result = parse_promql("http_requests_total{method=\"GET\"}[1h]");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_avg_aggregation() {
        let result = parse_promql("avg(cpu_usage)");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(matches!(q.aggregation.function, AggregationFunction::Avg));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_sum_with_labels() {
        let result = parse_promql("sum(http_requests_total{status=\"200\"})");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(matches!(q.aggregation.function, AggregationFunction::Sum));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_aggregation_with_by() {
        let result = parse_promql("sum(http_requests_total) by (host, method)");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert_eq!(q.aggregation.group_by.len(), 2);
                assert!(q.aggregation.group_by.contains(&"host".to_string()));
                assert!(q.aggregation.group_by.contains(&"method".to_string()));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_rate() {
        let result = parse_promql("rate(http_requests_total[5m])");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(matches!(q.aggregation.function, AggregationFunction::Rate));
                assert!(q.aggregation.window.is_some());
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_increase() {
        let result = parse_promql("increase(http_requests_total[1h])");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Aggregate(q) => {
                assert!(matches!(
                    q.aggregation.function,
                    AggregationFunction::Increase
                ));
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_parse_with_offset() {
        // Test vector selector with offset
        let result = parse_promql("cpu_usage offset 5m");
        assert!(result.is_ok());

        // Verify offset shifts the time range backward
        // Without offset: end ~= now, with 5m offset: end ~= now - 5min
        match result.unwrap() {
            Query::Select(q) => {
                let now = current_time_millis();
                // End should be approximately now - 5min (300,000ms), with some tolerance
                let offset_ms = 5 * 60 * 1000; // 5 minutes in ms
                let expected_end = now - offset_ms;
                // Allow 1 second tolerance for test execution time
                assert!(
                    (q.time_range.end - expected_end).abs() < 1000,
                    "Time range end should be shifted by offset"
                );
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_aggregation_with_offset() {
        // Test aggregation function with offset: avg(cpu[1h]) offset 1d
        let result = parse_promql("avg(cpu_usage[1h]) offset 1d");
        assert!(result.is_ok());

        match result.unwrap() {
            Query::Aggregate(q) => {
                let now = current_time_millis();
                // End should be approximately now - 1day (86,400,000ms)
                let offset_ms = 24 * 60 * 60 * 1000; // 1 day in ms
                let expected_end = now - offset_ms;
                // Allow 1 second tolerance for test execution time
                assert!(
                    (q.time_range.end - expected_end).abs() < 1000,
                    "Aggregation time range end should be shifted by offset"
                );
                // Duration should be 1 hour
                let duration_ms = q.time_range.end - q.time_range.start;
                assert!(
                    (duration_ms - 3_600_000).abs() < 1000,
                    "Duration should be 1 hour"
                );
            }
            _ => panic!("Expected Aggregate query"),
        }
    }

    #[test]
    fn test_rate_with_offset() {
        // Test rate function with offset: rate(requests[5m]) offset 1h
        let result = parse_promql("rate(http_requests_total[5m]) offset 1h");
        assert!(result.is_ok());

        match result.unwrap() {
            Query::Aggregate(q) => {
                let now = current_time_millis();
                // End should be approximately now - 1hour (3,600,000ms)
                let offset_ms = 60 * 60 * 1000; // 1 hour in ms
                let expected_end = now - offset_ms;
                // Allow 1 second tolerance for test execution time
                assert!(
                    (q.time_range.end - expected_end).abs() < 1000,
                    "Rate time range end should be shifted by offset"
                );
                // Duration should be 5 minutes
                let duration_ms = q.time_range.end - q.time_range.start;
                assert!(
                    (duration_ms - 300_000).abs() < 1000,
                    "Duration should be 5 minutes"
                );
            }
            _ => panic!("Expected Aggregate query"),
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

        let (_, d) = parse_duration("1w").unwrap();
        assert_eq!(d, Duration::from_secs(604800));
    }
}
