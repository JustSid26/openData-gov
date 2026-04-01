#!/bin/bash
# ============================================================
# sqoop_import_rainfall.sh
# Imports rainfall data from MySQL → HDFS → HBase
# ============================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Config ───────────────────────────────────────────────────
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DB="${MYSQL_DB:-opendata_gov}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-password}"

HDFS_TARGET="hdfs:///data/rainfall/"
HBASE_TABLE="rainfall_data"
HBASE_CF="cf1"

# ── Step 1: Sqoop import to HDFS ─────────────────────────────
info "=== Sqoop Import: Rainfall → HDFS ==="
info "Source: MySQL ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}.rainfall_india_2025"
info "Target: ${HDFS_TARGET}"

sqoop import \
  --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
  --username "${MYSQL_USER}" \
  --password "${MYSQL_PASS}" \
  --table rainfall_india_2025 \
  --target-dir "${HDFS_TARGET}" \
  --fields-terminated-by ',' \
  --delete-target-dir \
  --num-mappers 4 \
  --where "Year = 2025"

info "HDFS import complete. Verifying..."
hdfs dfs -ls "${HDFS_TARGET}"

# ── Step 2: Sqoop import directly to HBase ───────────────────
info ""
info "=== Sqoop Import: Rainfall → HBase ==="

sqoop import \
  --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
  --username "${MYSQL_USER}" \
  --password "${MYSQL_PASS}" \
  --table rainfall_india_2025 \
  --hbase-table "${HBASE_TABLE}" \
  --column-family "${HBASE_CF}" \
  --hbase-row-key id \
  --hbase-create-table \
  --num-mappers 4

info "HBase import complete."
info "Verify: hbase shell → scan '${HBASE_TABLE}', {LIMIT => 5}"

echo ""
info "=== Rainfall Sqoop Pipeline Complete ==="
