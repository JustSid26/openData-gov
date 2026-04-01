# 🇮🇳 Open Data Gov — India Analytics Platform

A **multi-dataset analytics platform** for exploring India's open government data through interactive dashboards. Users select from three dataset categories — **Foodstock**, **Rainfall**, and **Air Quality** — and receive tailored analytics powered by the **Hadoop ecosystem**.

> Built with Flask · Chart.js · Hadoop MapReduce · Sqoop · HBase · HDFS

![Landing Page](https://img.shields.io/badge/Datasets-3-blue?style=flat-square)
![Records](https://img.shields.io/badge/Records-41%2C000%2B-green?style=flat-square)
![MapReduce](https://img.shields.io/badge/MR%20Jobs-6-orange?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.8%2B-yellow?style=flat-square)

---

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Datasets](#-datasets)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Hadoop Ecosystem](#-hadoop-ecosystem)
  - [MapReduce Jobs](#mapreduce-jobs)
  - [Sqoop Import](#sqoop-import)
  - [HBase Storage](#hbase-storage)
  - [HDFS Commands](#hdfs-commands)
- [API Reference](#-api-reference)
- [Screenshots](#-screenshots)
- [License](#-license)

---

## ✨ Features

- **Dataset Selector** — Choose between Foodstock (Rice-Raw), Rainfall, or Air Quality from a premium landing page
- **Interactive Dashboards** — Each dataset gets its own analytics with KPI cards, time-series charts, regional breakdowns, and more
- **Hadoop MapReduce** — 6 mapper/reducer jobs for distributed data processing (2 per dataset)
- **Sqoop Integration** — Import scripts for MySQL → HDFS and MySQL → HBase data pipelines
- **HBase Schema** — Column families with SNAPPY compression, bloom filters, and TTL policies
- **HDFS Pipeline** — Automated upload, job execution, and output retrieval scripts
- **Pipeline Visualization** — Each dashboard shows the full data flow: CSV/MySQL → Sqoop → HDFS → MapReduce → HBase → Dashboard
- **Dark Theme UI** — Modern glassmorphism design with smooth animations and responsive layout
- **24 API Endpoints** — RESTful JSON APIs for all analytics across all three datasets

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Flask Web App (app.py)                │
│                                                         │
│  ┌──────────────┐ ┌────────────┐ ┌──────────────────┐  │
│  │  Foodstock   │ │  Rainfall  │ │   Air Quality    │  │
│  │  Dashboard   │ │  Dashboard │ │   Dashboard      │  │
│  └──────┬───────┘ └─────┬──────┘ └───────┬──────────┘  │
│         │               │                │              │
│  /api/foodstock/*  /api/rainfall/*  /api/airquality/*   │
└─────────┼───────────────┼────────────────┼──────────────┘
          │               │                │
┌─────────┴───────────────┴────────────────┴──────────────┐
│                  Hadoop Ecosystem Layer                   │
│                                                          │
│  ┌──────────┐  ┌─────────┐  ┌────────┐  ┌───────────┐  │
│  │   HDFS   │◄─│  Sqoop  │─►│ HBase  │  │ MapReduce │  │
│  │ Storage  │  │ Import  │  │ Store  │  │   Jobs    │  │
│  └──────────┘  └─────────┘  └────────┘  └───────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | HTML5, CSS3 (dark theme), JavaScript, Chart.js 4.x |
| **Backend** | Python 3, Flask, Pandas |
| **Big Data** | Hadoop MapReduce (Streaming API), HDFS, Sqoop, HBase |
| **Data** | CSV datasets (govt. open data format) |

---

## 📊 Datasets

| Dataset | File | Records | Coverage |
|---------|------|---------|----------|
| 🌾 **Foodstock (Rice-Raw)** | `data/rice-raw-2025.csv` | 15,575 | 151 districts, 26 states · Jan–May 2025 |
| 🌧️ **Rainfall** | `data/rainfall-india-2025.csv` | 15,659 | 121 districts, 25 states · Jan–Jun 2025 |
| 🏭 **Air Quality** | `data/air-quality-india-2025.csv` | 9,955 | 30 cities, 55 stations · Jan–Jun 2025 |

### Data Schema

**Foodstock:** `Commodity_Id, Total_stock, Commodity_Stock, Code, Date, Individual_Date, District_name, Month, Year, Commodity_name, District_code, Stock`

**Rainfall:** `State, District, Date, Month, Year, Rainfall_mm, Avg_Temp_C, Humidity_pct`

**Air Quality:** `City, State, Station_ID, Date, PM25, PM10, NO2, SO2, CO, O3, AQI, AQI_Category`

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/JustSid26/openData-gov.git
cd openData-gov

# Install dependencies
pip install flask pandas

# (Optional) Generate fresh synthetic datasets
python3 generate_datasets.py

# Run the application
python3 app.py
```

Open **http://localhost:5000** in your browser.

### For Hadoop Cluster Deployment

```bash
# Set environment
export HADOOP_HOME=/path/to/hadoop
export MYSQL_HOST=localhost
export MYSQL_DB=opendata_gov

# Run all MapReduce jobs
./hadoop/run_hadoop_jobs.sh --dataset all

# Or run for a specific dataset
./hadoop/run_hadoop_jobs.sh --dataset rainfall

# Sqoop import (MySQL → HDFS + HBase)
./hadoop/sqoop/sqoop_import_foodstock.sh
./hadoop/sqoop/sqoop_import_rainfall.sh
./hadoop/sqoop/sqoop_import_airquality.sh

# Create HBase tables
hbase shell < hadoop/hbase/hbase_create_tables.sh
```

---

## 📁 Project Structure

```
openData-gov/
├── app.py                          # Flask backend (24 API endpoints)
├── generate_datasets.py            # Synthetic dataset generator
├── README.md
│
├── data/
│   ├── rice-raw-2025.csv           # Foodstock dataset
│   ├── rainfall-india-2025.csv     # Rainfall dataset
│   └── air-quality-india-2025.csv  # Air quality dataset
│
├── templates/
│   ├── index.html                  # Landing page (dataset selector)
│   └── dashboard.html              # Dynamic dashboard template
│
└── hadoop/
    ├── run_hadoop_jobs.sh           # Master job runner (--dataset flag)
    │
    ├── mapper/
    │   ├── mapper_region_stock.py       # Foodstock: region → stock
    │   ├── mapper_daily_drawdown.py     # Foodstock: date → stock
    │   ├── mapper_rainfall_state.py     # Rainfall: state → rainfall_mm
    │   ├── mapper_rainfall_anomaly.py   # Rainfall: date → rainfall_mm
    │   ├── mapper_aqi_city.py           # Air Quality: city → AQI
    │   └── mapper_aqi_critical.py       # Air Quality: date → AQI (>200 only)
    │
    ├── reducer/
    │   ├── reducer_region_stock.py      # Sums/averages per region
    │   ├── reducer_daily_drawdown.py    # Day-over-day delta, DRAWDOWN flag
    │   ├── reducer_rainfall_state.py    # Sums/averages/max per state
    │   ├── reducer_rainfall_anomaly.py  # Daily totals, HEAVY_RAIN flag
    │   ├── reducer_aqi_city.py          # Avg/max/min AQI per city
    │   └── reducer_aqi_critical.py      # SEVERE/VERY_POOR/POOR flags
    │
    ├── sqoop/
    │   ├── sqoop_import_foodstock.sh    # MySQL → HDFS + HBase
    │   ├── sqoop_import_rainfall.sh     # MySQL → HDFS + HBase
    │   └── sqoop_import_airquality.sh   # MySQL → HDFS + HBase
    │
    └── hbase/
        ├── hbase_create_tables.sh       # Table creation with column families
        └── hbase_scan_examples.sh       # Query examples for all tables
```

---

## 🐘 Hadoop Ecosystem

### MapReduce Jobs

Each dataset has **2 MapReduce jobs** using Hadoop Streaming (Python mappers/reducers):

| # | Dataset | Job | Mapper Output | Reducer Output |
|---|---------|-----|---------------|----------------|
| 1 | Foodstock | Region Stock Aggregation | `region \t stock` | `region \t total \t avg \t count` |
| 2 | Foodstock | Daily Drawdown Detection | `date \t stock` | `date \t total \t delta \t pct \t DRAWDOWN/STABLE` |
| 3 | Rainfall | State Rainfall Aggregation | `state \t rainfall_mm` | `state \t total \t avg \t max \t count` |
| 4 | Rainfall | Anomaly Detection | `date \t rainfall_mm` | `date \t total \t avg \t delta \t pct \t HEAVY_RAIN/NORMAL` |
| 5 | Air Quality | City AQI Aggregation | `city \t AQI` | `city \t avg \t max \t min \t count` |
| 6 | Air Quality | Critical Event Detection | `date \t AQI \t city \t state` | `date \t AQI \t city \t state \t SEVERE/VERY_POOR/POOR` |

**Local testing (without Hadoop):**

```bash
# Test any mapper/reducer via Unix pipes
cat data/rainfall-india-2025.csv \
  | python3 hadoop/mapper/mapper_rainfall_state.py \
  | sort \
  | python3 hadoop/reducer/reducer_rainfall_state.py
```

### Sqoop Import

Each script handles a two-step pipeline:

1. **MySQL → HDFS** — Bulk import with `--target-dir` and `--fields-terminated-by`
2. **MySQL → HBase** — Direct import with `--hbase-table`, `--column-family`, `--hbase-row-key`

```bash
# Example: Import rainfall data
sqoop import \
  --connect jdbc:mysql://localhost:3306/opendata_gov \
  --table rainfall_india_2025 \
  --hbase-table rainfall_data \
  --column-family cf1 \
  --hbase-row-key id \
  --hbase-create-table \
  -m 4
```

### HBase Storage

Three tables with optimized schemas:

| Table | Row Key Pattern | Column Families |
|-------|----------------|-----------------|
| `foodstock_data` | `region_district_date` | `cf1` (raw data), `cf2` (aggregates, TTL=1yr) |
| `rainfall_data` | `state_district_date` | `cf1` (all columns, SNAPPY, bloom) |
| `airquality_data` | `city_station_date` | `cf1` (location/time), `cf2` (pollutants/AQI) |

**Example HBase queries:**

```ruby
# Scan all Kerala rainfall entries
scan 'rainfall_data', {ROWPREFIXFILTER => 'Kerala_', LIMIT => 20}

# Filter critical AQI readings (> 300)
scan 'airquality_data', {
  FILTER => "SingleColumnValueFilter('cf2', 'aqi', >, 'binary:300')",
  LIMIT => 20
}
```

### HDFS Commands

The master script `run_hadoop_jobs.sh` handles:

- Safe mode detection and wait
- Dataset upload to HDFS (`hdfs dfs -put`)
- Job submission via `hadoop jar` (streaming)
- Output preview after each job

```bash
# Run everything
./hadoop/run_hadoop_jobs.sh --dataset all

# Run only air quality jobs
./hadoop/run_hadoop_jobs.sh --dataset airquality
```

---

## 📡 API Reference

All endpoints return JSON. Base URL: `http://localhost:5000`

### Foodstock APIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/foodstock/summary` | KPIs: records, districts, regions, peak stock |
| GET | `/api/foodstock/daily` | Daily total stock time series |
| GET | `/api/foodstock/monthly` | Monthly average district-level stock |
| GET | `/api/foodstock/regions?n=10` | Top N regions by cumulative stock |
| GET | `/api/foodstock/districts?n=10&region=Punjab` | Top N districts, optionally filtered |
| GET | `/api/foodstock/region_trend` | Monthly stock by top 6 regions |
| GET | `/api/hadoop/foodstock/region_stock` | MapReduce Job 1 output |
| GET | `/api/hadoop/foodstock/daily_drawdown` | MapReduce Job 2 output |

### Rainfall APIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/rainfall/summary` | KPIs: records, states, avg rainfall, temp |
| GET | `/api/rainfall/daily` | Daily nationwide rainfall totals |
| GET | `/api/rainfall/monthly` | Monthly avg rain, temp, humidity |
| GET | `/api/rainfall/states?n=12` | Top N states by total rainfall |
| GET | `/api/rainfall/state_trend` | Monthly rainfall by top 6 states |
| GET | `/api/rainfall/heatmap` | State × Month rainfall matrix |
| GET | `/api/hadoop/rainfall/state_agg` | MapReduce Job 3 output |
| GET | `/api/hadoop/rainfall/anomaly` | MapReduce Job 4 output |

### Air Quality APIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/airquality/summary` | KPIs: cities, avg AQI, worst/best city |
| GET | `/api/airquality/daily` | Daily average AQI nationwide |
| GET | `/api/airquality/monthly` | Monthly avg AQI, PM2.5, PM10 |
| GET | `/api/airquality/cities?n=15&order=worst` | Top/bottom cities by AQI |
| GET | `/api/airquality/pollutants` | PM2.5, PM10, NO₂, SO₂, CO, O₃ by city |
| GET | `/api/airquality/categories` | AQI category distribution |
| GET | `/api/airquality/city_trend` | Monthly AQI by top 6 worst cities |
| GET | `/api/hadoop/airquality/city_agg` | MapReduce Job 5 output |
| GET | `/api/hadoop/airquality/critical` | MapReduce Job 6 output |

---

## 📸 Screenshots

### Landing Page
Premium dark-themed dataset selector with animated gradient background, pipeline visualization, and glassmorphism cards.

### Foodstock Dashboard
- **Stock Trends** — Daily national stock line chart, monthly bar, day-over-day delta
- **Regional** — Top 10 regions bar chart, stacked area by region
- **Districts** — Filterable by region, top N selector
- **Hadoop Jobs** — Region stock aggregation + drawdown detection tables

### Rainfall Dashboard
- **Trends** — Daily rainfall time series, monthly averages, temperature chart
- **By State** — Horizontal bar chart, stacked area by state
- **Heatmap** — State × Month matrix with intensity coloring
- **Hadoop Jobs** — State aggregation + anomaly detection (HEAVY_RAIN events)

### Air Quality Dashboard
- **AQI Trends** — Daily AQI line, monthly multi-line (AQI/PM2.5/PM10), category donut
- **By City** — Top 15 most polluted cities, monthly city trend
- **Pollutants** — Grouped bar chart (PM2.5, PM10, NO₂, O₃) by city
- **Hadoop Jobs** — City aggregation + critical events (SEVERE/VERY_POOR flags)

---

## 📄 License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <strong>Open Data Gov</strong> · Built with Flask + Hadoop Ecosystem
  <br />
  <sub>Data sourced from India's Open Government Data platform (data.gov.in)</sub>
</p>
