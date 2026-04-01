#!/bin/bash
# ============================================================
# hbase_create_tables.sh
# Creates HBase tables for all three datasets
# Run inside: hbase shell < hbase_create_tables.sh
# Or execute line-by-line in HBase shell
# ============================================================

cat <<'HBASE_COMMANDS'
# ============================================================
#  Open Data Gov — HBase Table Definitions
#  Run these commands in the HBase Shell
# ============================================================

# ── Disable & drop existing tables (if re-creating) ─────────

disable 'foodstock_data'
drop 'foodstock_data'
disable 'rainfall_data'
drop 'rainfall_data'
disable 'airquality_data'
drop 'airquality_data'

# ── Create: Foodstock Data ───────────────────────────────────
# Row key: composite  region_district_date (e.g. "Punjab_LUDHIANA_2025-03-13")
# Column family cf1: region, district, date, month, year, commodity, stock
# Column family cf2: total_stock, commodity_stock (aggregated values)

create 'foodstock_data', \
  {NAME => 'cf1', VERSIONS => 3, COMPRESSION => 'SNAPPY', BLOOMFILTER => 'ROW'}, \
  {NAME => 'cf2', VERSIONS => 1, COMPRESSION => 'SNAPPY', TTL => 31536000}

# ── Create: Rainfall Data ────────────────────────────────────
# Row key: composite  state_district_date (e.g. "Kerala_Kochi_2025-06-15")
# Column family cf1: state, district, date, month, year, rainfall_mm, avg_temp, humidity

create 'rainfall_data', \
  {NAME => 'cf1', VERSIONS => 3, COMPRESSION => 'SNAPPY', BLOOMFILTER => 'ROW'}

# ── Create: Air Quality Data ────────────────────────────────
# Row key: composite  city_station_date (e.g. "Delhi_DEL01_2025-01-15")
# Column family cf1: city, state, station_id, date
# Column family cf2: pm25, pm10, no2, so2, co, o3, aqi, aqi_category

create 'airquality_data', \
  {NAME => 'cf1', VERSIONS => 1, COMPRESSION => 'SNAPPY', BLOOMFILTER => 'ROW'}, \
  {NAME => 'cf2', VERSIONS => 3, COMPRESSION => 'SNAPPY', BLOOMFILTER => 'ROW'}

# ── Verify table creation ────────────────────────────────────
list
describe 'foodstock_data'
describe 'rainfall_data'
describe 'airquality_data'

status 'detailed'
HBASE_COMMANDS

echo ""
echo "[INFO] Copy the commands above and paste into: hbase shell"
echo "[INFO] Or run: hbase shell < <(sed -n '/^#/!p' hbase_create_tables.sh)"
