# Gorilla TSDB Deployment Guide

## Staging Deployment

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- At least 4GB RAM available
- 20GB disk space

### Quick Start

```bash
# Build and start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f tsdb

# Stop services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

### Services

| Service | Port | Purpose |
|---------|------|---------|
| Gorilla TSDB | 8080 | Time-series database |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Visualization dashboard |

### Accessing Services

**Gorilla TSDB API**:
```bash
curl http://localhost:8080/health
```

**Prometheus**:
- URL: http://localhost:9090
- Targets: http://localhost:9090/targets

**Grafana**:
- URL: http://localhost:3000
- Username: `admin`
- Password: `changeme` (change this!)

### Configuration

#### Environment Variables

Edit `docker-compose.yml` to configure:

```yaml
environment:
  - RUST_LOG=info              # Logging level: error, warn, info, debug, trace
  - TSDB_DATA_DIR=/data        # Data directory
  - TSDB_PORT=8080             # Server port
  - TSDB_MAX_MEMORY=4GB        # Maximum memory usage
  - TSDB_CHUNK_SIZE=10000      # Points per chunk (max 10M)
  - TSDB_SEAL_DURATION_MS=3600000  # Seal after 1 hour
```

#### Storage Configuration

Data is persisted in Docker volumes:

```bash
# List volumes
docker volume ls | grep gorilla

# Inspect volume
docker volume inspect gorilla-tsdb-staging_tsdb-data

# Backup data
docker run --rm -v gorilla-tsdb-staging_tsdb-data:/data \
  -v $(pwd):/backup alpine tar czf /backup/tsdb-backup.tar.gz /data

# Restore data
docker run --rm -v gorilla-tsdb-staging_tsdb-data:/data \
  -v $(pwd):/backup alpine tar xzf /backup/tsdb-backup.tar.gz -C /
```

### Health Checks

The TSDB container includes automatic health checks:

```bash
# Check health status
docker inspect --format='{{.State.Health.Status}}' gorilla-tsdb-staging

# View health logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' gorilla-tsdb-staging
```

### Monitoring

#### Prometheus Metrics

Access Prometheus at http://localhost:9090 and query:

```promql
# Request rate
rate(http_requests_total[5m])

# Error rate
rate(http_errors_total[5m])

# Memory usage
process_resident_memory_bytes

# Chunk seal rate
rate(tsdb_chunks_sealed_total[5m])

# Active chunks
tsdb_active_chunks_count
```

#### Grafana Dashboards

1. Open Grafana at http://localhost:3000
2. Login with admin/changeme
3. Navigate to Dashboards
4. Import dashboard or create custom panels

Key metrics to monitor:
- Request throughput (writes/sec, reads/sec)
- Latency (p50, p95, p99)
- Error rate
- Memory usage
- Disk usage
- Active chunks count
- Seal operations

### Load Testing

```bash
# Run basic load test
cd tests
cargo test --release test_high_throughput_single_thread -- --ignored --nocapture

# Run concurrent load test
cargo test --release test_concurrent_appends -- --ignored --nocapture

# Custom load test with vegeta
echo "POST http://localhost:8080/write
Content-Type: application/json
@test-data.json" | vegeta attack -rate=1000 -duration=60s | vegeta report
```

### Troubleshooting

#### Container won't start

```bash
# Check logs
docker-compose logs tsdb

# Check disk space
df -h

# Check memory
free -h

# Restart services
docker-compose restart
```

#### High memory usage

```bash
# Check container memory
docker stats gorilla-tsdb-staging

# Reduce TSDB_CHUNK_SIZE in docker-compose.yml
# Reduce TSDB_SEAL_DURATION_MS to seal chunks more frequently
```

#### Data loss after restart

```bash
# Verify volumes are mounted
docker-compose config

# Check volume exists
docker volume inspect gorilla-tsdb-staging_tsdb-data

# Ensure data directory permissions
docker-compose exec tsdb ls -la /data
```

### Upgrading

```bash
# Pull latest changes
git pull origin main

# Rebuild containers
docker-compose build --no-cache

# Restart with new image
docker-compose up -d

# Verify health
docker-compose ps
```

### Security Considerations

1. **Change default Grafana password**:
   - Edit `docker-compose.yml`
   - Set `GF_SECURITY_ADMIN_PASSWORD`

2. **Use TLS in production**:
   - Add reverse proxy (nginx/traefik)
   - Configure SSL certificates
   - Update TSDB to use HTTPS

3. **Network isolation**:
   - Use firewall rules
   - Limit exposed ports
   - Use VPN for admin access

4. **Backup regularly**:
   - Schedule automated backups
   - Test restore procedures
   - Store backups off-site

### Production Deployment

For production deployment:

1. Use orchestration (Kubernetes, Docker Swarm)
2. Configure persistent storage (NFS, Ceph, cloud storage)
3. Set up monitoring alerts
4. Implement backup/restore automation
5. Configure TLS/SSL
6. Set up log aggregation
7. Implement rate limiting
8. Configure authentication/authorization

See `docs/production-deployment.md` for detailed guide.

### Support

- Documentation: `docs/`
- Issues: GitHub Issues
- Logs: `docker-compose logs -f`
