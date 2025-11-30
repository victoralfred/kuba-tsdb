#!/bin/bash
# Metrics Collector for Gorilla TSDB
# Collects CPU stats every 10 seconds and pushes to the database

TSDB_URL="${TSDB_URL:-http://localhost:8090}"
INTERVAL="${INTERVAL:-10}"
HOSTNAME=$(hostname)
PID=$$

echo "Starting metrics collector..."
echo "  TSDB URL: $TSDB_URL"
echo "  Interval: ${INTERVAL}s"
echo "  Hostname: $HOSTNAME"
echo "  Collector PID: $PID"
echo ""

# Function to get current timestamp in milliseconds
get_timestamp_ms() {
    echo $(($(date +%s) * 1000))
}

# Function to send a single metric
send_metric() {
    local metric_name="$1"
    local timestamp="$2"
    local value="$3"

    curl -s -X POST "$TSDB_URL/api/v1/write" \
        -H "Content-Type: application/json" \
        -d "{\"metric\":\"$metric_name\",\"tags\":{\"hostname\":\"$HOSTNAME\"},\"points\":[{\"timestamp\":$timestamp,\"value\":$value}]}" > /dev/null
}

# Function to collect and send CPU metrics
collect_cpu_metrics() {
    local timestamp=$(get_timestamp_ms)

    # Read CPU stats from /proc/stat
    read -r cpu user nice system idle iowait irq softirq steal guest guest_nice < /proc/stat

    # Calculate total and usage percentages
    local total=$((user + nice + system + idle + iowait + irq + softirq + steal))
    local idle_pct=$(echo "scale=2; $idle * 100 / $total" | bc)
    local user_pct=$(echo "scale=2; $user * 100 / $total" | bc)
    local system_pct=$(echo "scale=2; $system * 100 / $total" | bc)
    local iowait_pct=$(echo "scale=2; $iowait * 100 / $total" | bc)
    local usage_pct=$(echo "scale=2; 100 - $idle_pct" | bc)

    # Get load averages
    read -r load1 load5 load15 _ < /proc/loadavg

    # Get memory stats
    local mem_total=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    local mem_available=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    local mem_used=$((mem_total - mem_available))
    local mem_used_pct=$(echo "scale=2; $mem_used * 100 / $mem_total" | bc)

    # Send all metrics
    send_metric "cpu_usage" "$timestamp" "$usage_pct"
    send_metric "cpu_user" "$timestamp" "$user_pct"
    send_metric "cpu_system" "$timestamp" "$system_pct"
    send_metric "cpu_iowait" "$timestamp" "$iowait_pct"
    send_metric "load_avg_1m" "$timestamp" "$load1"
    send_metric "load_avg_5m" "$timestamp" "$load5"
    send_metric "load_avg_15m" "$timestamp" "$load15"
    send_metric "memory_used_pct" "$timestamp" "$mem_used_pct"
    send_metric "memory_used_kb" "$timestamp" "$mem_used"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sent metrics - CPU: ${usage_pct}%, Mem: ${mem_used_pct}%, Load: $load1"
}

# Trap SIGINT/SIGTERM for clean shutdown
trap 'echo ""; echo "Stopping metrics collector..."; exit 0' INT TERM

# Main loop
echo "Collecting metrics every ${INTERVAL} seconds. Press Ctrl+C to stop."
echo ""

while true; do
    collect_cpu_metrics
    sleep "$INTERVAL"
done
