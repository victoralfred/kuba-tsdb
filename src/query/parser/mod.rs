//! Query Parser Module
//!
//! Provides parsers for SQL-like and PromQL query languages.
//! Both parsers convert their respective syntax into the common Query AST.
//!
//! # Supported Languages
//!
//! ## SQL-like Syntax
//! ```sql
//! SELECT * FROM cpu WHERE host = 'server01' AND time >= now() - 1h
//! SELECT avg(value) FROM cpu WHERE time >= now() - 1h GROUP BY time(5m)
//! SELECT * FROM cpu ORDER BY time DESC LIMIT 100
//! ```
//!
//! ## PromQL Syntax
//! ```promql
//! cpu_usage{host="server01"}
//! avg(cpu_usage{host="server01"}[5m])
//! rate(http_requests_total[1m])
//! ```
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::query::parser::{parse_sql, parse_promql, parse_query};
//!
//! // Parse SQL query
//! let query = parse_sql("SELECT * FROM cpu WHERE time >= 0");
//! assert!(query.is_ok());
//!
//! // Parse PromQL query
//! let query = parse_promql("cpu_usage{host=\"server01\"}[5m]");
//! assert!(query.is_ok());
//!
//! // Auto-detect query language
//! let query = parse_query("SELECT * FROM metrics");
//! assert!(query.is_ok());
//! ```

pub mod promql;
pub mod sql;

pub use promql::parse_promql;
pub use sql::parse_sql;

use super::error::QueryResult;

/// Parse a query string, auto-detecting the language
///
/// Tries SQL first (if starts with SELECT/EXPLAIN), then PromQL
pub fn parse_query(input: &str) -> QueryResult<super::Query> {
    let trimmed = input.trim();
    let upper = trimmed.to_uppercase();

    // SQL-like queries start with SELECT or EXPLAIN
    if upper.starts_with("SELECT") || upper.starts_with("EXPLAIN") {
        parse_sql(trimmed)
    } else {
        // Assume PromQL for everything else
        parse_promql(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_detect_sql() {
        let result = parse_query("SELECT * FROM cpu");
        assert!(result.is_ok());
    }

    #[test]
    fn test_auto_detect_promql() {
        let result = parse_query("cpu_usage{host=\"server01\"}");
        assert!(result.is_ok());
    }

    #[test]
    fn test_case_insensitive_sql() {
        let result = parse_query("select * from cpu");
        assert!(result.is_ok());
    }
}
