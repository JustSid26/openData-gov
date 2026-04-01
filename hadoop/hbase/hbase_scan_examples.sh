#!/bin/bash
# ============================================================
# hbase_scan_examples.sh
# Example HBase shell commands for querying each dataset
# ============================================================

cat <<'HBASE_COMMANDS'
# ============================================================
#  Open Data Gov — HBase Query Examples
#  Run these commands in the HBase Shell
# ============================================================

# ═══════════════════════════════════════════════════════════
#  FOODSTOCK DATA
# ═══════════════════════════════════════════════════════════

# Scan first 10 rows
scan 'foodstock_data', {LIMIT => 10}

# Get a specific row
get 'foodstock_data', 'Punjab_LUDHIANA_2025-03-13'

# Scan all Punjab entries
scan 'foodstock_data', {ROWPREFIXFILTER => 'Punjab_', LIMIT => 20}

# Get only stock values for a region
scan 'foodstock_data', {ROWPREFIXFILTER => 'Haryana_', COLUMNS => ['cf1:stock'], LIMIT => 10}

# Count all rows
count 'foodstock_data', {INTERVAL => 1000}

# Filter: stock > 1000000 (using SingleColumnValueFilter)
scan 'foodstock_data', { \
  FILTER => "SingleColumnValueFilter('cf1', 'stock', >, 'binary:1000000')", \
  LIMIT => 10 \
}

# ═══════════════════════════════════════════════════════════
#  RAINFALL DATA
# ═══════════════════════════════════════════════════════════

# Scan first 10 rows
scan 'rainfall_data', {LIMIT => 10}

# Get a specific district-date entry
get 'rainfall_data', 'Kerala_Kochi_2025-06-15'

# Scan all Kerala entries (monsoon analysis)
scan 'rainfall_data', {ROWPREFIXFILTER => 'Kerala_', LIMIT => 20}

# Heavy rainfall filter (> 50mm)
scan 'rainfall_data', { \
  FILTER => "SingleColumnValueFilter('cf1', 'rainfall_mm', >, 'binary:50.0')", \
  LIMIT => 20 \
}

# Scan by date range prefix (all June 2025 data)
scan 'rainfall_data', { \
  FILTER => "RowFilter(=, 'substring:2025-06')", \
  LIMIT => 20 \
}

# ═══════════════════════════════════════════════════════════
#  AIR QUALITY DATA
# ═══════════════════════════════════════════════════════════

# Scan first 10 rows
scan 'airquality_data', {LIMIT => 10}

# Get a specific station reading
get 'airquality_data', 'Delhi_DEL01_2025-01-15'

# Scan all Delhi stations
scan 'airquality_data', {ROWPREFIXFILTER => 'Delhi_', LIMIT => 20}

# Critical AQI filter (> 300 — Very Poor / Severe)
scan 'airquality_data', { \
  FILTER => "SingleColumnValueFilter('cf2', 'aqi', >, 'binary:300')", \
  LIMIT => 20 \
}

# Get PM2.5 and AQI columns only
scan 'airquality_data', { \
  ROWPREFIXFILTER => 'Mumbai_', \
  COLUMNS => ['cf2:pm25', 'cf2:aqi', 'cf2:aqi_category'], \
  LIMIT => 10 \
}

# Count rows per table
count 'airquality_data', {INTERVAL => 1000}

# ═══════════════════════════════════════════════════════════
#  ADMIN / UTILITY
# ═══════════════════════════════════════════════════════════

# Check table sizes
status 'simple'

# Table region info
describe 'foodstock_data'
describe 'rainfall_data'
describe 'airquality_data'

# Flush memstore to disk
flush 'foodstock_data'
flush 'rainfall_data'
flush 'airquality_data'

HBASE_COMMANDS

echo ""
echo "[INFO] Copy the commands above and paste into: hbase shell"
