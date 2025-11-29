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
    SeriesSelector, WindowSpec, WindowType,
};
use crate::query::error::{QueryError, QueryResult};
use crate::types::TimeRange;
use std::time::Duration;

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

    // Parse inner vector selector
    let (input, (selector, range, _labels)) = parse_vector_selector_inner(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Parse optional BY clause
    let (input, group_by) = opt(preceded(
        (multispace0, tag_no_case("by"), multispace0),
        parse_label_list,
    ))
    .parse(input)?;

    // Parse optional offset
    let (input, _offset) = opt(preceded(
        (multispace1, tag_no_case("offset"), multispace1),
        parse_duration,
    ))
    .parse(input)?;

    // Build time range
    let now = current_time_nanos();
    let time_range = if let Some(duration) = range {
        TimeRange {
            start: now - (duration.as_nanos() as i64),
            end: now,
        }
    } else {
        TimeRange {
            start: now - 3_600_000_000_000,
            end: now,
        }
    };

    let window = range.map(|d| WindowSpec {
        duration: d,
        window_type: WindowType::Tumbling,
        offset: None,
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
fn parse_agg_function(input: &str) -> IResult<&str, AggregationFunction> {
    alt((
        value(AggregationFunction::Sum, tag_no_case("sum")),
        value(AggregationFunction::Avg, tag_no_case("avg")),
        value(AggregationFunction::Min, tag_no_case("min")),
        value(AggregationFunction::Max, tag_no_case("max")),
        value(AggregationFunction::Count, tag_no_case("count")),
        value(AggregationFunction::StdDev, tag_no_case("stddev")),
        value(AggregationFunction::Variance, tag_no_case("stdvar")),
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
fn parse_rate_expr(input: &str) -> IResult<&str, Query> {
    let (input, func) = parse_rate_function(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse inner vector selector (must have range)
    let (input, (selector, range, _labels)) = parse_vector_selector_inner(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Range is required for rate functions
    let range = range.ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    let now = current_time_nanos();
    let time_range = TimeRange {
        start: now - (range.as_nanos() as i64),
        end: now,
    };

    let window = WindowSpec {
        duration: range,
        window_type: WindowType::Tumbling,
        offset: None,
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
    let (input, (selector, range, _labels)) = parse_vector_selector_inner(input)?;

    // Parse optional offset
    let (input, offset) = opt(preceded(
        (multispace1, tag_no_case("offset"), multispace1),
        parse_duration,
    ))
    .parse(input)?;

    let now = current_time_nanos();

    // Apply offset if present
    let adjusted_now = if let Some(off) = offset {
        now - (off.as_nanos() as i64)
    } else {
        now
    };

    // Build time range
    let time_range = if let Some(duration) = range {
        TimeRange {
            start: adjusted_now - (duration.as_nanos() as i64),
            end: adjusted_now,
        }
    } else {
        TimeRange {
            start: adjusted_now - 3_600_000_000_000,
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
type VectorSelectorResult = (SeriesSelector, Option<Duration>, Vec<(String, String)>);

/// Parse vector selector components
#[allow(clippy::type_complexity)]
fn parse_vector_selector_inner(input: &str) -> IResult<&str, VectorSelectorResult> {
    // Parse metric name
    let (input, metric_name) = parse_metric_name(input)?;

    // Parse optional label matchers
    let (input, labels) = opt(parse_label_matchers).parse(input)?;
    let labels = labels.unwrap_or_default();

    // Parse optional range
    let (input, range) = opt(parse_range).parse(input)?;

    // Build selector
    let selector = SeriesSelector::by_measurement(metric_name).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    Ok((input, (selector, range, labels)))
}

/// Parse metric name
fn parse_metric_name(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == ':')(input)
}

/// Parse label matchers: {label1="value1", label2="value2"}
fn parse_label_matchers(input: &str) -> IResult<&str, Vec<(String, String)>> {
    delimited(
        (multispace0, char('{')),
        separated_list0((multispace0, char(','), multispace0), parse_label_matcher),
        (multispace0, char('}')),
    )
    .parse(input)
}

/// Parse single label matcher
fn parse_label_matcher(input: &str) -> IResult<&str, (String, String)> {
    let (input, _) = multispace0(input)?;
    let (input, label) = parse_label_name(input)?;
    let (input, _) = multispace0(input)?;

    // Parse operator
    let (input, _op) = alt((tag("=~"), tag("!~"), tag("!="), tag("="))).parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, val) = parse_string_value(input)?;

    Ok((input, (label.to_string(), val.to_string())))
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

/// Get current time in nanoseconds
fn current_time_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_parse_metric_with_multiple_labels() {
        let result = parse_promql("cpu_usage{host=\"server01\", region=\"us-east\"}");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_range_vector() {
        let result = parse_promql("cpu_usage[5m]");
        assert!(result.is_ok());
        match result.unwrap() {
            Query::Select(q) => {
                let duration = q.time_range.end - q.time_range.start;
                assert!((299_000_000_000..=301_000_000_000).contains(&duration));
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
        let result = parse_promql("cpu_usage offset 5m");
        assert!(result.is_ok());
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
