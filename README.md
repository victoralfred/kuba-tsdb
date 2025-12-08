[![CodeQL Analysis](https://github.com/victoralfred/kuba-tsdb/actions/workflows/codeql.yml/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/codeql.yml) 
[![Rust CI/CD](https://github.com/victoralfred/kuba-tsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/rust.yml) [![Security Audit](https://github.com/victoralfred/kuba-tsdb/actions/workflows/security.yml/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/security.yml) [![CodeQL Analysis](https://github.com/victoralfred/kuba-tsdb/actions/workflows/codeql.yml/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/codeql.yml) [![CodeQL](https://github.com/victoralfred/kuba-tsdb/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/github-code-scanning/codeql) [![AI PR Review](https://github.com/victoralfred/kuba-tsdb/actions/workflows/pr-review.yml/badge.svg)](https://github.com/victoralfred/kuba-tsdb/actions/workflows/pr-review.yml)
# KUBA TSDB

A high-performance time-series database written in Rust, an efficient compression algorithm for 10:1+ compression ratios and sub-millisecond query latencies.

## Features

- **High Performance**: >2M points/second ingestion, sub-millisecond queries
- **SQL & PromQL Support**: Query with familiar syntax
- **Time-Windowed Aggregations**: Auto-interval bucketing for visualization
- **HTTP REST API**: Easy integration with any language/framework
- **Redis Integration**: Scalable indexing with Redis backend

## Installation

### Prerequisites

- Rust 1.70+
- Redis 6.0+ (optional, for distributed indexing)

### Build from Source

```bash
git clone https://github.com/victoralfred/kuba-tsdb.git
cd kube-tsdb/source

# Build release binary
cargo build --release

# Run tests
cargo test

# Start the server
./target/release/tsdb-server
```

### Configuration

Create `application.toml` in the working directory:

```toml
[server]
listen_addr = "0.0.0.0:8090"
data_dir = "./data"
log_level = "info"

[redis]
enabled = false  # Set to true for Redis indexing
url = "redis://localhost:6379"
```

Or use environment variables:

```bash
export TSDB_SERVER_LISTEN_ADDR=0.0.0.0:8090
export TSDB_SERVER_DATA_DIR=/var/lib/tsdb
```

## Quick Start

### 1. Start the Server

```bash
./target/release/tsdb-server
# Server listening on http://0.0.0.0:8090
```

### 2. Write Data

```bash
# Write with metric name and tags (recommended)
curl -X POST http://localhost:8090/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu.usage",
    "tags": {"host": "server1", "region": "us-east"},
    "points": [
      {"timestamp": 1733100000000, "value": 45.5},
      {"timestamp": 1733100001000, "value": 46.2},
      {"timestamp": 1733100002000, "value": 44.8}
    ]
  }'

# Response
{"success":true,"series_id":123456789,"points_written":3}
```

### 3. Query Data

```bash
# Simple query by metric
curl "http://localhost:8090/api/v1/query?metric=cpu.usage&start=1733100000000&end=1733100010000"
```

## Query API

### REST API Query (`GET /api/v1/query`)

```bash
# Basic query by metric name
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&limit=10"

# Query with tag filter
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&tags=%7B%22host%22%3A%22server1%22%7D&start=1733100000000&end=1733200000000"

# Query with AVG aggregation
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&aggregation=avg"
# Response: {"success":true,"series_id":123,"points":[],"aggregation":{"function":"avg","value":45.5,"point_count":100}}

# Query with MIN aggregation
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&aggregation=min"

# Query with MAX aggregation
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&aggregation=max"

# Query with SUM aggregation
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&aggregation=sum"

# Query with COUNT aggregation
curl "http://localhost:8090/api/v1/query?metric=cpu_usage&start=1733100000000&end=1733200000000&aggregation=count"
```

### SQL Queries (`POST /api/v1/query/sql`)

Execute SQL queries via the `/api/v1/query/sql` endpoint:

```bash
# Basic SELECT with time range
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu_usage WHERE time > now() - 2h LIMIT 10", "language": "sql"}'

# SELECT with tag filter
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu_usage WHERE host = '\''server1'\'' LIMIT 10", "language": "sql"}'

# AVG aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT avg(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# MIN/MAX aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT min(value), max(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# SUM aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT sum(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# COUNT aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT count(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# STDDEV aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT stddev(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# PERCENTILE aggregation (95th percentile)
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT percentile(value, 95) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# MEDIAN aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT median(value) FROM cpu_usage WHERE time > now() - 1h", "language": "sql"}'

# Time-bucketed aggregation (5 minute buckets)
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT avg(value) FROM cpu_usage WHERE time > now() - 1h GROUP BY time(5m)", "language": "sql"}'
# Response includes time_aggregation with bucket details
```

### PromQL Queries (`POST /api/v1/query/sql` with `language: "promql"`)

Use Prometheus-compatible query syntax:

```bash
# Instant vector selector
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "cpu_usage{host=\"server1\"}", "language": "promql"}'

# Range vector (last 5 minutes)
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "cpu_usage[5m]", "language": "promql"}'

# AVG aggregation over range
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "avg(cpu_usage[1h])", "language": "promql"}'

# SUM aggregation over range
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "sum(cpu_usage[1h])", "language": "promql"}'

# MIN aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "min(cpu_usage[1h])", "language": "promql"}'

# MAX aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "max(cpu_usage[1h])", "language": "promql"}'

# COUNT aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "count(cpu_usage[1h])", "language": "promql"}'
```

### Supported Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count` | Number of points | `SELECT count(*) FROM metric` |
| `sum` | Sum of values | `SELECT sum(value) FROM metric` |
| `avg` | Average value | `SELECT avg(value) FROM metric` |
| `min` | Minimum value | `SELECT min(value) FROM metric` |
| `max` | Maximum value | `SELECT max(value) FROM metric` |
| `first` | First value in range | `SELECT first(value) FROM metric` |
| `last` | Last value in range | `SELECT last(value) FROM metric` |
| `stddev` | Standard deviation | `SELECT stddev(value) FROM metric` |
| `variance` | Variance | `SELECT variance(value) FROM metric` |
| `median` | Median (p50) | `SELECT median(value) FROM metric` |
| `percentile` | Nth percentile | `SELECT percentile(value, 95) FROM metric` |
| `rate` | Per-second rate | `rate(counter[5m])` |
| `increase` | Counter increase | `increase(counter[1h])` |

### Time Functions

```sql
-- Relative time
SELECT * FROM metric WHERE time > now() - 1h
SELECT * FROM metric WHERE time > now() - 30m
SELECT * FROM metric WHERE time > now() - 7d

-- Time bucketing (auto-interval)
SELECT avg(value) FROM metric GROUP BY time(auto)

-- Fixed interval bucketing
SELECT avg(value) FROM metric GROUP BY time(5m)
SELECT sum(value) FROM metric GROUP BY time(1h)
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |
| `/api/v1/write` | POST | Write data points |
| `/api/v1/query` | GET | Query with parameters |
| `/api/v1/query/sql` | POST | SQL/PromQL queries |
| `/api/v1/series` | POST | Register new series |
| `/api/v1/series/find` | GET | Find series by metric/tags |
| `/api/v1/stats` | GET | Database statistics |

## Response Format

### Query Response

```json
{
  "status": "ok",
  "series": [
    {
      "metric": "cpu.usage",
      "scope": "host:server1,region:us-east",
      "tag_set": ["host:server1", "region:us-east"],
      "pointlist": [[1733100000000, 45.5], [1733100001000, 46.2]],
      "length": 2
    }
  ],
  "from_date": 1733100000000,
  "to_date": 1733103600000,
  "query_type": "select",
  "language": "sql"
}
```

### Aggregation Response

```json
{
  "status": "ok",
  "aggregation": {
    "function": "avg",
    "value": 45.5,
    "point_count": 100,
    "time_aggregation": {
      "mode": "auto",
      "interval": "5m",
      "interval_ms": 300000,
      "bucket_count": 12
    },
    "buckets": [
      {"timestamp": 1733100000000, "value": 44.2},
      {"timestamp": 1733100300000, "value": 46.8}
    ]
  }
}
```

## Library Usage

```rust
use kuba_tsdb::{TimeSeriesDBBuilder, DatabaseConfig, DataPoint};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DatabaseConfig {
        data_dir: "/var/lib/tsdb".into(),
        redis_url: Some("redis://localhost:6379".to_string()),
        ..Default::default()
    };

    let db = TimeSeriesDBBuilder::new()
        .with_config(config)
        .build()
        .await?;

    // Write data
    let point = DataPoint {
        series_id: 12345,
        timestamp: 1700000000000,
        value: 42.5,
    };
    db.write(point).await?;

    // Query data
    let results = db.query(12345, TimeRange::new(
        1700000000000,
        1700000001000
    )).await?;

    Ok(())
}
```

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run benchmarks
cargo bench
```

## Documentation

- [Technical Documentation](TECHNICAL.md) - Architecture, internals, and configuration reference
- [API Documentation](https://docs.rs/kuba-tsdb) - Rust API docs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new features
4. Ensure `cargo clippy` passes
5. Submit a pull request

## License

MIT License

## References

- [Prometheus](https://prometheus.io/) - Query language inspiration
