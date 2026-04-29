#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT_DIR/.pipeline_logs"
PID_DIR="$ROOT_DIR/.pipeline_pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_FIREHOSE_TOPIC="${KAFKA_FIREHOSE_TOPIC:-bluesky.firehose.raw}"
KAFKA_GETPOSTS_TOPIC="${KAFKA_GETPOSTS_TOPIC:-bluesky.getposts.raw}"

start_proc() {
  local name="$1"
  local cmd="$2"
  local pid_file="$PID_DIR/${name}.pid"
  local log_file="$LOG_DIR/${name}.log"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "[skip] $name already running (pid $(cat "$pid_file"))"
    return
  fi

  echo "[start] $name"
  nohup bash -lc "cd \"$ROOT_DIR\" && $cmd" >"$log_file" 2>&1 &
  echo $! >"$pid_file"
  sleep 1

  if kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "[ok] $name started (pid $(cat "$pid_file")) log=$log_file"
  else
    echo "[err] $name failed to start. Check $log_file"
    exit 1
  fi
}

stop_proc() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"

  if [[ ! -f "$pid_file" ]]; then
    echo "[skip] $name pid file not found"
    return
  fi

  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "[stop] $name (pid $pid)"
    kill "$pid" || true
  else
    echo "[skip] $name pid $pid not running"
  fi
  rm -f "$pid_file"
}

status_proc() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "[up]   $name (pid $(cat "$pid_file"))"
  else
    echo "[down] $name"
  fi
}

ensure_python_kafka() {
  if python3 -c "import kafka" >/dev/null 2>&1; then
    return
  fi
  echo "[deps] Installing kafka-python"
  python3 -m pip install kafka-python
}

start_all() {
  echo "[infra] Starting Kafka via docker compose"
  (cd "$ROOT_DIR" && docker compose -f docker-compose.kafka.yml up -d)

  ensure_python_kafka

  start_proc "firehose" "KAFKA_ENABLED=true KAFKA_BROKERS=$KAFKA_BROKERS KAFKA_FIREHOSE_TOPIC=$KAFKA_FIREHOSE_TOPIC npx ts-node firehose.ts"
  start_proc "getposts" "KAFKA_ENABLED=true KAFKA_BROKERS=$KAFKA_BROKERS KAFKA_GETPOSTS_TOPIC=$KAFKA_GETPOSTS_TOPIC python3 getPosts_streaming.py"

  start_proc "spark_vader" "cd spark_streaming && USE_KAFKA_FIREHOSE_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BROKERS KAFKA_FIREHOSE_TOPIC=$KAFKA_FIREHOSE_TOPIC python3 vaderSentimentTimeSeries.py"
  start_proc "spark_ingestion" "cd spark_streaming && USE_KAFKA_SOURCES=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BROKERS KAFKA_FIREHOSE_TOPIC=$KAFKA_FIREHOSE_TOPIC KAFKA_GETPOSTS_TOPIC=$KAFKA_GETPOSTS_TOPIC python3 ingestionMetricsTimeline.py"
  start_proc "spark_controversial" "cd spark_streaming && USE_KAFKA_GETPOSTS_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BROKERS KAFKA_GETPOSTS_TOPIC=$KAFKA_GETPOSTS_TOPIC python3 controversialTopicsTimeseries.py"
  start_proc "spark_reddit" "cd spark_streaming && USE_KAFKA_GETPOSTS_SOURCE=true SPARK_KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BROKERS KAFKA_GETPOSTS_TOPIC=$KAFKA_GETPOSTS_TOPIC python3 redditCrossoverStats.py"

  echo
  echo "Pipeline started."
  echo "Logs: $LOG_DIR"
}

stop_all() {
  stop_proc "spark_reddit"
  stop_proc "spark_controversial"
  stop_proc "spark_ingestion"
  stop_proc "spark_vader"
  stop_proc "getposts"
  stop_proc "firehose"

  echo "[infra] Stopping Kafka via docker compose"
  (cd "$ROOT_DIR" && docker compose -f docker-compose.kafka.yml down) || true
}

status_all() {
  status_proc "firehose"
  status_proc "getposts"
  status_proc "spark_vader"
  status_proc "spark_ingestion"
  status_proc "spark_controversial"
  status_proc "spark_reddit"
}

usage() {
  cat <<EOF
Usage: $0 {start|stop|restart|status}

start   Start Kafka + publishers + all 4 Spark jobs
stop    Stop all jobs and Kafka
restart Restart everything
status  Show process status from PID files
EOF
}

ACTION="${1:-start}"
case "$ACTION" in
  start) start_all ;;
  stop) stop_all ;;
  restart) stop_all; start_all ;;
  status) status_all ;;
  *) usage; exit 1 ;;
esac
