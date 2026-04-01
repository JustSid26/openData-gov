#!/bin/bash
# ============================================================
# run_hadoop_jobs.sh
# Submits all MapReduce jobs for the Open Data Gov platform
# Supports: --dataset foodstock|rainfall|airquality|all
# ============================================================
set -euo pipefail

# ── Colour helpers ───────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
header(){ echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# ── Parse arguments ──────────────────────────────────────────
DATASET="${1:-all}"
if [[ "$DATASET" == "--dataset" ]]; then
  DATASET="${2:-all}"
fi

if [[ "$DATASET" != "foodstock" && "$DATASET" != "rainfall" && "$DATASET" != "airquality" && "$DATASET" != "all" ]]; then
  echo "Usage: $0 [--dataset] {foodstock|rainfall|airquality|all}"
  exit 1
fi

info "Dataset mode: $DATASET"

# ── Config ───────────────────────────────────────────────────
HDFS_OUTPUT_BASE="hdfs:///output/opendata_gov"

# Resolve the streaming jar
HADOOP_STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
if [[ -z "$HADOOP_STREAMING_JAR" ]]; then
  error "hadoop-streaming jar not found under HADOOP_HOME=$HADOOP_HOME"
fi
info "Using streaming jar: $HADOOP_STREAMING_JAR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPER_DIR="$SCRIPT_DIR/mapper"
REDUCER_DIR="$SCRIPT_DIR/reducer"

# ── Wait for namenode ────────────────────────────────────────
info "Checking namenode safe mode..."
SAFE_MODE_WAIT=0; MAX_WAIT=120
while true; do
  STATUS=$(hdfs dfsadmin -safemode get 2>/dev/null | awk '{print $4}')
  if [[ "$STATUS" == "OFF" ]]; then info "Namenode is out of safe mode."; break; fi
  if (( SAFE_MODE_WAIT >= MAX_WAIT )); then
    warn "Forcing leave safe mode..."
    hdfs dfsadmin -safemode leave; break
  fi
  warn "Namenode in safe mode (${SAFE_MODE_WAIT}s) — waiting..."
  sleep 5; SAFE_MODE_WAIT=$(( SAFE_MODE_WAIT + 5 ))
done

# ═══════════════════════════════════════════════════════════════
#  FOODSTOCK JOBS
# ═══════════════════════════════════════════════════════════════

run_foodstock() {
  header
  info "╔══ FOODSTOCK DATASET ══╗"

  HDFS_INPUT="hdfs:///data/rice-raw-2025.csv"
  LOCAL_DATA="./data/rice-raw-2025.csv"

  [[ -f "$LOCAL_DATA" ]] || error "Local dataset not found: $LOCAL_DATA"
  hdfs dfs -mkdir -p /data
  hdfs dfs -put -f "$LOCAL_DATA" "$HDFS_INPUT"
  info "Uploaded: $HDFS_INPUT"

  # Job 1: Region Stock Aggregation
  info "=== Job: Region Stock Aggregation ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/foodstock/region_stock" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="foodstock_region_stock_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=2 \
      -files "${MAPPER_DIR}/mapper_region_stock.py,${REDUCER_DIR}/reducer_region_stock.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/foodstock/region_stock" \
      -mapper  "python3 mapper_region_stock.py" \
      -reducer "python3 reducer_region_stock.py"

  info "Region stock job complete."
  hdfs dfs -cat "${HDFS_OUTPUT_BASE}/foodstock/region_stock/part-*" | sort -t$'\t' -k2 -rn | head -10

  # Job 2: Daily Drawdown
  info "=== Job: Daily Drawdown Detection ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/foodstock/daily_drawdown" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="foodstock_daily_drawdown_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=1 \
      -files "${MAPPER_DIR}/mapper_daily_drawdown.py,${REDUCER_DIR}/reducer_daily_drawdown.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/foodstock/daily_drawdown" \
      -mapper  "python3 mapper_daily_drawdown.py" \
      -reducer "python3 reducer_daily_drawdown.py"

  info "Daily drawdown job complete."
}

# ═══════════════════════════════════════════════════════════════
#  RAINFALL JOBS
# ═══════════════════════════════════════════════════════════════

run_rainfall() {
  header
  info "╔══ RAINFALL DATASET ══╗"

  HDFS_INPUT="hdfs:///data/rainfall-india-2025.csv"
  LOCAL_DATA="./data/rainfall-india-2025.csv"

  [[ -f "$LOCAL_DATA" ]] || error "Local dataset not found: $LOCAL_DATA"
  hdfs dfs -mkdir -p /data
  hdfs dfs -put -f "$LOCAL_DATA" "$HDFS_INPUT"
  info "Uploaded: $HDFS_INPUT"

  # Job 1: State Rainfall Aggregation
  info "=== Job: State Rainfall Aggregation ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/rainfall/state_agg" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="rainfall_state_agg_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=2 \
      -files "${MAPPER_DIR}/mapper_rainfall_state.py,${REDUCER_DIR}/reducer_rainfall_state.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/rainfall/state_agg" \
      -mapper  "python3 mapper_rainfall_state.py" \
      -reducer "python3 reducer_rainfall_state.py"

  info "State aggregation job complete."
  hdfs dfs -cat "${HDFS_OUTPUT_BASE}/rainfall/state_agg/part-*" | sort -t$'\t' -k2 -rn | head -10

  # Job 2: Anomaly Detection
  info "=== Job: Rainfall Anomaly Detection ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/rainfall/anomaly" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="rainfall_anomaly_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=1 \
      -files "${MAPPER_DIR}/mapper_rainfall_anomaly.py,${REDUCER_DIR}/reducer_rainfall_anomaly.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/rainfall/anomaly" \
      -mapper  "python3 mapper_rainfall_anomaly.py" \
      -reducer "python3 reducer_rainfall_anomaly.py"

  info "Anomaly detection job complete."
  hdfs dfs -cat "${HDFS_OUTPUT_BASE}/rainfall/anomaly/part-*" | grep "HEAVY_RAIN" | head -10
}

# ═══════════════════════════════════════════════════════════════
#  AIR QUALITY JOBS
# ═══════════════════════════════════════════════════════════════

run_airquality() {
  header
  info "╔══ AIR QUALITY DATASET ══╗"

  HDFS_INPUT="hdfs:///data/air-quality-india-2025.csv"
  LOCAL_DATA="./data/air-quality-india-2025.csv"

  [[ -f "$LOCAL_DATA" ]] || error "Local dataset not found: $LOCAL_DATA"
  hdfs dfs -mkdir -p /data
  hdfs dfs -put -f "$LOCAL_DATA" "$HDFS_INPUT"
  info "Uploaded: $HDFS_INPUT"

  # Job 1: City AQI Aggregation
  info "=== Job: City AQI Aggregation ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/airquality/city_agg" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="aqi_city_agg_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=2 \
      -files "${MAPPER_DIR}/mapper_aqi_city.py,${REDUCER_DIR}/reducer_aqi_city.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/airquality/city_agg" \
      -mapper  "python3 mapper_aqi_city.py" \
      -reducer "python3 reducer_aqi_city.py"

  info "City AQI aggregation job complete."
  hdfs dfs -cat "${HDFS_OUTPUT_BASE}/airquality/city_agg/part-*" | sort -t$'\t' -k2 -rn | head -10

  # Job 2: Critical AQI Events
  info "=== Job: Critical AQI Event Detection ==="
  hdfs dfs -rm -r -f "${HDFS_OUTPUT_BASE}/airquality/critical" 2>/dev/null || true

  hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapred.job.name="aqi_critical_2025" \
      -D mapreduce.job.maps=4 \
      -D mapreduce.job.reduces=1 \
      -files "${MAPPER_DIR}/mapper_aqi_critical.py,${REDUCER_DIR}/reducer_aqi_critical.py" \
      -input   "$HDFS_INPUT" \
      -output  "${HDFS_OUTPUT_BASE}/airquality/critical" \
      -mapper  "python3 mapper_aqi_critical.py" \
      -reducer "python3 reducer_aqi_critical.py"

  info "Critical AQI detection job complete."
  hdfs dfs -cat "${HDFS_OUTPUT_BASE}/airquality/critical/part-*" | grep "SEVERE" | head -10
}

# ═══════════════════════════════════════════════════════════════
#  DISPATCH
# ═══════════════════════════════════════════════════════════════

case "$DATASET" in
  foodstock)   run_foodstock ;;
  rainfall)    run_rainfall ;;
  airquality)  run_airquality ;;
  all)
    run_foodstock
    run_rainfall
    run_airquality
    ;;
esac

header
info "=== All requested jobs complete ==="
info "HDFS output base: $HDFS_OUTPUT_BASE"
info "Flask dashboard: http://localhost:5000"