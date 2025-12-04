#!/usr/bin/env python3
"""
Batch metrics submission script for testing chunk compression.

Submits large batches of metrics to the TSDB server using JSON API
to trigger chunk sealing with 1000+ points for optimal AHPAC compression ratios.

Usage:
    python3 scripts/batch_metrics.py --points 1000 --metric cpu
    python3 scripts/batch_metrics.py --points 2000 --metric memory --batch-size 100

JSON API Format:
    POST /api/v1/write
    {
        "metric": "cpu",
        "tags": {"host": "server1", "core": "0"},
        "points": [
            {"timestamp": 1733323272000, "value": 42.5},
            ...
        ]
    }
"""

import argparse
import json
import random
import socket
import sys
import time
from datetime import datetime


def generate_write_request(metric_name: str, tags: dict, points: list) -> dict:
    """Generate a write request in the server's JSON format."""
    return {
        "metric": metric_name,
        "tags": tags,
        "points": points
    }


def generate_point(timestamp_ms: int, value: float) -> dict:
    """Generate a single point for the write request."""
    return {
        "timestamp": timestamp_ms,
        "value": value
    }


def generate_cpu_metrics(count: int, start_ts_ms: int, interval_ms: int = 1000) -> tuple:
    """Generate realistic CPU usage metrics (0-100%). Returns (metric_name, tags, points)."""
    points = []
    base_value = random.uniform(20, 60)

    for i in range(count):
        # Random walk with mean reversion
        delta = random.gauss(0, 5)
        base_value = max(0, min(100, base_value + delta))

        # Add some spikes
        if random.random() < 0.05:
            value = min(100, base_value + random.uniform(20, 40))
        else:
            value = base_value

        points.append(generate_point(
            start_ts_ms + i * interval_ms,
            round(value, 2)
        ))

    return ("cpu", {"host": "server1", "core": "0"}, points)


def generate_memory_metrics(count: int, start_ts_ms: int, interval_ms: int = 1000) -> tuple:
    """Generate realistic memory usage metrics (bytes). Returns (metric_name, tags, points)."""
    points = []
    base_value = random.uniform(4e9, 8e9)  # 4-8 GB

    for i in range(count):
        # Slow drift with occasional jumps
        delta = random.gauss(0, 1e7)
        base_value = max(1e9, min(16e9, base_value + delta))

        # Occasional allocation spikes
        if random.random() < 0.02:
            value = base_value + random.uniform(1e8, 5e8)
        else:
            value = base_value

        points.append(generate_point(
            start_ts_ms + i * interval_ms,
            round(value, 0)
        ))

    return ("memory", {"host": "server1"}, points)


def generate_network_metrics(count: int, start_ts_ms: int, interval_ms: int = 1000) -> tuple:
    """Generate network counter metrics (monotonically increasing). Returns (metric_name, tags, points)."""
    points = []
    counter = random.randint(1000000, 10000000)

    for i in range(count):
        # Counters always increase
        counter += random.randint(1000, 50000)

        points.append(generate_point(
            start_ts_ms + i * interval_ms,
            float(counter)
        ))

    return ("network", {"host": "server1", "interface": "eth0"}, points)


def generate_temperature_metrics(count: int, start_ts_ms: int, interval_ms: int = 1000) -> tuple:
    """Generate temperature sensor metrics (smooth, decimal values). Returns (metric_name, tags, points)."""
    points = []
    base_temp = random.uniform(35, 45)

    for i in range(count):
        # Smooth sinusoidal variation with noise
        time_factor = i / count * 2 * 3.14159
        seasonal = 5 * (0.5 + 0.5 * (1 + random.random()) * 0.5)
        noise = random.gauss(0, 0.5)
        value = base_temp + seasonal * (1 + 0.3 * (time_factor % 1)) + noise

        points.append(generate_point(
            start_ts_ms + i * interval_ms,
            round(value, 1)
        ))

    return ("temperature", {"host": "server1", "sensor": "cpu"}, points)


def generate_request_latency_metrics(count: int, start_ts_ms: int, interval_ms: int = 100) -> tuple:
    """Generate request latency metrics (log-normal distribution). Returns (metric_name, tags, points)."""
    points = []

    for i in range(count):
        # Log-normal distribution for latency
        latency = random.lognormvariate(2, 0.8)  # ~7ms median
        latency = min(latency, 1000)  # Cap at 1 second

        points.append(generate_point(
            start_ts_ms + i * interval_ms,
            round(latency, 2)
        ))

    return ("latency", {"host": "server1", "endpoint": "/api/v1/data"}, points)


def send_http_json(host: str, port: int, metric_name: str, tags: dict, points: list, batch_size: int = 500) -> tuple:
    """Send metrics via HTTP POST using JSON API. Returns (success_count, error_count)."""
    import urllib.request
    import urllib.error

    success = 0
    errors = 0
    url = f"http://{host}:{port}/api/v1/write"

    # Send in batches
    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]

        request_data = {
            "metric": metric_name,
            "tags": tags,
            "points": batch
        }

        try:
            data = json.dumps(request_data).encode('utf-8')
            req = urllib.request.Request(
                url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=60) as response:
                response_body = response.read().decode('utf-8')
                result = json.loads(response_body)

                if result.get('success'):
                    success += result.get('points_written', len(batch))
                else:
                    errors += len(batch)
                    error_msg = result.get('error', 'Unknown error')
                    print(f"Server error: {error_msg}", file=sys.stderr)

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            print(f"HTTP error {e.code}: {error_body}", file=sys.stderr)
            errors += len(batch)

        except urllib.error.URLError as e:
            print(f"Connection error: {e}", file=sys.stderr)
            errors += len(batch)

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            errors += len(batch)

        # Progress indicator
        total_sent = i + len(batch)
        print(f"  Sent {total_sent}/{len(points)} points...", end='\r')

        # Small delay between batches
        if i + batch_size < len(points):
            time.sleep(0.01)

    print()  # Newline after progress
    return success, errors


def main():
    parser = argparse.ArgumentParser(
        description='Submit batch metrics to TSDB for compression testing'
    )
    parser.add_argument(
        '--points', '-n',
        type=int,
        default=1000,
        help='Number of points to generate (default: 1000)'
    )
    parser.add_argument(
        '--metric', '-m',
        choices=['cpu', 'memory', 'network', 'temperature', 'latency'],
        default='cpu',
        help='Type of metric to generate (default: cpu)'
    )
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='TSDB server host (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=8090,
        help='TSDB server port (default: 8090)'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=500,
        help='Batch size for HTTP requests (default: 500)'
    )
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=1000,
        help='Interval between points in ms (default: 1000)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate metrics but do not send'
    )

    args = parser.parse_args()

    # Start timestamp in milliseconds (now minus points * interval to simulate historical data)
    start_ts_ms = int(time.time() * 1000) - (args.points * args.interval)

    # Generate metrics based on type
    generators = {
        'cpu': generate_cpu_metrics,
        'memory': generate_memory_metrics,
        'network': generate_network_metrics,
        'temperature': generate_temperature_metrics,
        'latency': generate_request_latency_metrics,
    }

    gen_func = generators[args.metric]
    metric_name, tags, points = gen_func(args.points, start_ts_ms, args.interval)
    print(f"Generated {len(points)} {args.metric} metrics")

    # Get time range for display
    first_ts_ms = points[0]['timestamp']
    last_ts_ms = points[-1]['timestamp']

    print(f"Total points: {len(points)}")
    print(f"Time range: {datetime.fromtimestamp(first_ts_ms/1000)} - {datetime.fromtimestamp(last_ts_ms/1000)}")
    print(f"Metric: {metric_name}, Tags: {tags}")

    if args.dry_run:
        print("\nDry run - not sending metrics")
        print("\nSample points:")
        for p in points[:5]:
            print(f"  {json.dumps(p)}")
        print("  ...")
        for p in points[-3:]:
            print(f"  {json.dumps(p)}")
        return

    # Send metrics via HTTP JSON API
    print(f"\nSending to http://{args.host}:{args.port}/api/v1/write ...")

    start_time = time.time()

    success, errors = send_http_json(
        args.host, args.port,
        metric_name, tags, points,
        args.batch_size
    )

    elapsed = time.time() - start_time

    print(f"\nResults:")
    print(f"  Sent: {success} points")
    print(f"  Errors: {errors} points")
    print(f"  Time: {elapsed:.2f}s")
    if elapsed > 0:
        print(f"  Rate: {success/elapsed:.0f} points/sec")

    if success > 0:
        print(f"\nChunk should contain ~{success} points for optimal AHPAC compression.")
        print("Check chunk files to verify compression ratios.")


if __name__ == '__main__':
    main()
