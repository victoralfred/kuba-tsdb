[![Rust CI/CD](https://github.com/victoralfred/gorilla-tsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/victoralfred/gorilla-tsdb/actions/workflows/rust.yml)

# Gorilla TSDB

A high-performance time-series database written in Rust, featuring Facebook's Gorilla compression algorithm for 10:1+ compression ratios and sub-millisecond query latencies.

## Features

- **High Performance**: >2M points/second ingestion, sub-millisecond queries
- **Efficient Compression**: 10:1+ compression using Gorilla algorithm
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
git clone https://github.com/victoralfred/gorilla-tsdb.git
cd gorilla-tsdb/source

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

### REST API Query

```bash
# Query with aggregation
curl "http://localhost:8090/api/v1/query?\
metric=cpu.usage&\
start=1733100000000&\
end=1733103600000&\
aggregation=avg"
```

### SQL Queries

Execute SQL queries via the `/api/v1/query/sql` endpoint:

```bash
# Basic SELECT
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu.usage WHERE time > now() - 1h"}'

# Aggregation with time bucketing
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT avg(value) FROM cpu.usage WHERE time > now() - 1h GROUP BY time(5m)"}'

# Filter by tags
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu.usage WHERE host = '\''server1'\'' AND time > now() - 1h"}'

# Multiple aggregations
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT min(value), max(value), avg(value) FROM cpu.usage WHERE time > now() - 24h"}'

# Percentiles
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT percentile(value, 95) FROM response_time WHERE time > now() - 1h"}'
```

### PromQL Queries

Use Prometheus-compatible query syntax:

```bash
# Instant vector
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "cpu.usage{host=\"server1\"}", "language": "promql"}'

# Range vector with aggregation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "avg(cpu.usage{region=\"us-east\"}[5m])", "language": "promql"}'

# Rate calculation
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "rate(http_requests_total[5m])", "language": "promql"}'

# Group by labels
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "sum(cpu.usage) by (host)", "language": "promql"}'

# With offset (query historical data)
curl -X POST http://localhost:8090/api/v1/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "avg(cpu.usage[1h]) offset 1d", "language": "promql"}'
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
use gorilla_tsdb::{TimeSeriesDBBuilder, DatabaseConfig, DataPoint};

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
- [API Documentation](https://docs.rs/gorilla-tsdb) - Rust API docs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new features
4. Ensure `cargo clippy` passes
5. Submit a pull request

## License

MIT License

## References

- [Gorilla Paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) - Facebook's original research
- [Prometheus](https://prometheus.io/) - Query language inspiration
