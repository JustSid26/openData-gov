"""
Open Data Gov — Multi-Dataset Analytics Platform
=================================================
Datasets: Foodstock (Rice-Raw), Rainfall, Air Quality
Backend: Flask + Pandas
Pipeline: Hadoop MapReduce · Sqoop · HBase · HDFS
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(__file__)

# ═══════════════════════════════════════════════════════════════
#  DATASET LOADING
# ═══════════════════════════════════════════════════════════════

def load_foodstock() -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "data", "rice-raw-2025.csv")
    df = pd.read_csv(path)
    df["Region"] = df["Code"].str.replace("Region Name: ", "", regex=False)
    df["Date"]   = pd.to_datetime(df["Date"])
    df["Month"]  = df["Date"].dt.month
    df["Week"]   = df["Date"].dt.isocalendar().week.astype(int)
    return df

def load_rainfall() -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "data", "rainfall-india-2025.csv")
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def load_airquality() -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "data", "air-quality-india-2025.csv")
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# Cache all three datasets
DF_FOOD = load_foodstock()
DF_RAIN = load_rainfall()
DF_AIR  = load_airquality()

DATASET_META = {
    "foodstock": {
        "name": "Foodstock (Rice-Raw)",
        "icon": "🌾",
        "records": len(DF_FOOD),
        "desc": "India rice-raw stock tracking across 151 districts, 26 states/regions · Jan–May 2025",
        "color": "#3B6D11",
    },
    "rainfall": {
        "name": "Rainfall",
        "icon": "🌧️",
        "records": len(DF_RAIN),
        "desc": "Daily rainfall, temperature & humidity across 25 states, 100+ districts · Jan–Jun 2025",
        "color": "#185FA5",
    },
    "airquality": {
        "name": "Air Quality",
        "icon": "🏭",
        "records": len(DF_AIR),
        "desc": "PM2.5, PM10, AQI & pollutant data from 30 cities, 50+ stations · Jan–Jun 2025",
        "color": "#A32D2D",
    },
}


# ═══════════════════════════════════════════════════════════════
#  ROUTES — PAGES
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", datasets=DATASET_META)

@app.route("/dashboard/<dataset>")
def dashboard(dataset):
    if dataset not in DATASET_META:
        return "Dataset not found", 404
    return render_template("dashboard.html", dataset=dataset, meta=DATASET_META[dataset], all_meta=DATASET_META)

@app.route("/api/datasets")
def api_datasets():
    return jsonify(DATASET_META)


# ═══════════════════════════════════════════════════════════════
#  FOODSTOCK API
# ═══════════════════════════════════════════════════════════════

@app.route("/api/foodstock/summary")
def food_summary():
    daily_total = DF_FOOD.groupby("Date")["Stock"].sum()
    peak_date   = daily_total.idxmax()
    return jsonify({
        "total_records": len(DF_FOOD),
        "districts":     int(DF_FOOD["District_name"].nunique()),
        "regions":       int(DF_FOOD["Region"].nunique()),
        "peak_stock_MT": round(float(daily_total.max()) / 1e6, 1),
        "min_stock_MT":  round(float(daily_total.min()) / 1e6, 1),
        "peak_date":     peak_date.strftime("%d %b %Y"),
        "drop_pct":      46.9,
    })

@app.route("/api/foodstock/daily")
def food_daily():
    daily = (
        DF_FOOD.groupby("Date")["Stock"]
        .sum().reset_index().sort_values("Date")
    )
    return jsonify({
        "dates":  daily["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "stocks": [round(v / 1e6, 2) for v in daily["Stock"].tolist()],
    })

@app.route("/api/foodstock/monthly")
def food_monthly():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May"}
    m = DF_FOOD.groupby("Month")["Stock"].mean().reset_index().sort_values("Month")
    return jsonify({
        "months": [month_map.get(x, str(x)) for x in m["Month"].tolist()],
        "avg_MT": [round(v / 1e6, 3) for v in m["Stock"].tolist()],
    })

@app.route("/api/foodstock/regions")
def food_regions():
    n = int(request.args.get("n", 10))
    r = (
        DF_FOOD.groupby("Region")["Stock"]
        .sum().sort_values(ascending=False).head(n).reset_index()
    )
    return jsonify({
        "regions": r["Region"].tolist(),
        "stocks":  [round(v / 1e9, 3) for v in r["Stock"].tolist()],
        "unit":    "Billion MT",
    })

@app.route("/api/foodstock/districts")
def food_districts():
    n = int(request.args.get("n", 10))
    region = request.args.get("region", None)
    sub = DF_FOOD if region is None else DF_FOOD[DF_FOOD["Region"] == region]
    d = (
        sub.groupby("District_name")["Stock"]
        .sum().sort_values(ascending=False).head(n).reset_index()
    )
    return jsonify({
        "districts": d["District_name"].tolist(),
        "stocks":    [round(v / 1e9, 3) for v in d["Stock"].tolist()],
    })

@app.route("/api/foodstock/region_trend")
def food_region_trend():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May"}
    pivot = DF_FOOD.groupby(["Month", "Region"])["Stock"].sum().unstack(fill_value=0)
    top_regions = DF_FOOD.groupby("Region")["Stock"].sum().sort_values(ascending=False).head(6).index.tolist()
    result = {"months": [month_map.get(m, str(m)) for m in pivot.index.tolist()], "series": {}}
    for reg in top_regions:
        if reg in pivot.columns:
            result["series"][reg] = [round(v / 1e9, 4) for v in pivot[reg].tolist()]
    return jsonify(result)

@app.route("/api/hadoop/foodstock/region_stock")
def hadoop_food_region():
    r = (
        DF_FOOD.groupby("Region")["Stock"]
        .agg(["sum", "mean", "count"])
        .sort_values("sum", ascending=False).reset_index()
    )
    r.columns = ["region", "total_stock", "avg_stock", "record_count"]
    records = []
    for _, row in r.iterrows():
        records.append({
            "region":       row["region"],
            "total_stock":  round(row["total_stock"] / 1e6, 2),
            "avg_stock":    round(row["avg_stock"] / 1e6, 4),
            "record_count": int(row["record_count"]),
        })
    return jsonify({"job": "region_stock_aggregation", "records": records})

@app.route("/api/hadoop/foodstock/daily_drawdown")
def hadoop_food_drawdown():
    daily = DF_FOOD.groupby("Date")["Stock"].sum().sort_index().reset_index()
    daily["delta"]   = daily["Stock"].diff().fillna(0)
    daily["pct_chg"] = daily["Stock"].pct_change().fillna(0) * 100
    records = []
    for _, row in daily.iterrows():
        records.append({
            "date":     row["Date"].strftime("%Y-%m-%d"),
            "stock_M":  round(row["Stock"] / 1e6, 2),
            "delta_M":  round(row["delta"] / 1e6, 2),
            "pct_chg":  round(float(row["pct_chg"]), 2),
        })
    return jsonify({"job": "daily_drawdown_analysis", "records": records})


# ═══════════════════════════════════════════════════════════════
#  RAINFALL API
# ═══════════════════════════════════════════════════════════════

@app.route("/api/rainfall/summary")
def rain_summary():
    daily_total = DF_RAIN.groupby("Date")["Rainfall_mm"].sum()
    peak_date   = daily_total.idxmax()
    return jsonify({
        "total_records":   len(DF_RAIN),
        "states":          int(DF_RAIN["State"].nunique()),
        "districts":       int(DF_RAIN["District"].nunique()),
        "total_rainfall":  round(float(DF_RAIN["Rainfall_mm"].sum()), 1),
        "avg_rainfall":    round(float(DF_RAIN["Rainfall_mm"].mean()), 2),
        "max_daily_rain":  round(float(daily_total.max()), 1),
        "peak_rain_date":  peak_date.strftime("%d %b %Y"),
        "avg_temp":        round(float(DF_RAIN["Avg_Temp_C"].mean()), 1),
        "avg_humidity":    round(float(DF_RAIN["Humidity_pct"].mean()), 1),
    })

@app.route("/api/rainfall/daily")
def rain_daily():
    daily = (
        DF_RAIN.groupby("Date")["Rainfall_mm"]
        .sum().reset_index().sort_values("Date")
    )
    return jsonify({
        "dates":    daily["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "rainfall": [round(v, 1) for v in daily["Rainfall_mm"].tolist()],
    })

@app.route("/api/rainfall/monthly")
def rain_monthly():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun"}
    m = DF_RAIN.groupby("Month").agg(
        avg_rain=("Rainfall_mm", "mean"),
        avg_temp=("Avg_Temp_C", "mean"),
        avg_hum=("Humidity_pct", "mean"),
    ).reset_index().sort_values("Month")
    return jsonify({
        "months":   [month_map.get(x, str(x)) for x in m["Month"].tolist()],
        "avg_rain": [round(v, 2) for v in m["avg_rain"].tolist()],
        "avg_temp": [round(v, 1) for v in m["avg_temp"].tolist()],
        "avg_hum":  [round(v, 1) for v in m["avg_hum"].tolist()],
    })

@app.route("/api/rainfall/states")
def rain_states():
    n = int(request.args.get("n", 10))
    r = (
        DF_RAIN.groupby("State")["Rainfall_mm"]
        .sum().sort_values(ascending=False).head(n).reset_index()
    )
    return jsonify({
        "states":   r["State"].tolist(),
        "rainfall": [round(v, 1) for v in r["Rainfall_mm"].tolist()],
    })

@app.route("/api/rainfall/state_trend")
def rain_state_trend():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun"}
    pivot = DF_RAIN.groupby(["Month", "State"])["Rainfall_mm"].sum().unstack(fill_value=0)
    top_states = DF_RAIN.groupby("State")["Rainfall_mm"].sum().sort_values(ascending=False).head(6).index.tolist()
    result = {"months": [month_map.get(m, str(m)) for m in pivot.index.tolist()], "series": {}}
    for st in top_states:
        if st in pivot.columns:
            result["series"][st] = [round(v, 1) for v in pivot[st].tolist()]
    return jsonify(result)

@app.route("/api/rainfall/heatmap")
def rain_heatmap():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun"}
    pivot = DF_RAIN.groupby(["State", "Month"])["Rainfall_mm"].mean().unstack(fill_value=0)
    states = pivot.index.tolist()
    months = [month_map.get(m, str(m)) for m in pivot.columns.tolist()]
    data = []
    for si, state in enumerate(states):
        for mi, month in enumerate(pivot.columns):
            data.append([mi, si, round(float(pivot.loc[state, month]), 1)])
    return jsonify({"states": states, "months": months, "data": data})

@app.route("/api/hadoop/rainfall/state_agg")
def hadoop_rain_state():
    r = (
        DF_RAIN.groupby("State")["Rainfall_mm"]
        .agg(["sum", "mean", "max", "count"])
        .sort_values("sum", ascending=False).reset_index()
    )
    r.columns = ["state", "total_rain", "avg_rain", "max_rain", "record_count"]
    records = []
    for _, row in r.iterrows():
        records.append({
            "state":        row["state"],
            "total_rain":   round(row["total_rain"], 2),
            "avg_rain":     round(row["avg_rain"], 2),
            "max_rain":     round(row["max_rain"], 1),
            "record_count": int(row["record_count"]),
        })
    return jsonify({"job": "state_rainfall_aggregation", "records": records})

@app.route("/api/hadoop/rainfall/anomaly")
def hadoop_rain_anomaly():
    daily = DF_RAIN.groupby("Date")["Rainfall_mm"].sum().sort_index().reset_index()
    daily["delta"]   = daily["Rainfall_mm"].diff().fillna(0)
    daily["pct_chg"] = daily["Rainfall_mm"].pct_change().fillna(0) * 100
    threshold = daily["Rainfall_mm"].quantile(0.90)
    records = []
    for _, row in daily.iterrows():
        flag = "HEAVY_RAIN" if row["Rainfall_mm"] > threshold else "NORMAL"
        records.append({
            "date":     row["Date"].strftime("%Y-%m-%d"),
            "total_mm": round(row["Rainfall_mm"], 1),
            "delta":    round(row["delta"], 1),
            "pct_chg":  round(float(row["pct_chg"]), 2),
            "flag":     flag,
        })
    return jsonify({"job": "rainfall_anomaly_detection", "records": records})


# ═══════════════════════════════════════════════════════════════
#  AIR QUALITY API
# ═══════════════════════════════════════════════════════════════

@app.route("/api/airquality/summary")
def air_summary():
    city_avg = DF_AIR.groupby("City")["AQI"].mean()
    return jsonify({
        "total_records":  len(DF_AIR),
        "cities":         int(DF_AIR["City"].nunique()),
        "states":         int(DF_AIR["State"].nunique()),
        "stations":       int(DF_AIR["Station_ID"].nunique()),
        "avg_aqi":        round(float(DF_AIR["AQI"].mean()), 1),
        "max_aqi":        int(DF_AIR["AQI"].max()),
        "worst_city":     city_avg.idxmax(),
        "worst_city_aqi": round(float(city_avg.max()), 1),
        "best_city":      city_avg.idxmin(),
        "best_city_aqi":  round(float(city_avg.min()), 1),
        "avg_pm25":       round(float(DF_AIR["PM25"].mean()), 1),
        "avg_pm10":       round(float(DF_AIR["PM10"].mean()), 1),
    })

@app.route("/api/airquality/daily")
def air_daily():
    daily = (
        DF_AIR.groupby("Date")["AQI"]
        .mean().reset_index().sort_values("Date")
    )
    return jsonify({
        "dates": daily["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "aqi":   [round(v, 1) for v in daily["AQI"].tolist()],
    })

@app.route("/api/airquality/monthly")
def air_monthly():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun"}
    m = DF_AIR.copy()
    m["Month"] = m["Date"].dt.month
    monthly = m.groupby("Month").agg(
        avg_aqi=("AQI", "mean"),
        avg_pm25=("PM25", "mean"),
        avg_pm10=("PM10", "mean"),
    ).reset_index().sort_values("Month")
    return jsonify({
        "months":   [month_map.get(x, str(x)) for x in monthly["Month"].tolist()],
        "avg_aqi":  [round(v, 1) for v in monthly["avg_aqi"].tolist()],
        "avg_pm25": [round(v, 1) for v in monthly["avg_pm25"].tolist()],
        "avg_pm10": [round(v, 1) for v in monthly["avg_pm10"].tolist()],
    })

@app.route("/api/airquality/cities")
def air_cities():
    n = int(request.args.get("n", 15))
    order = request.args.get("order", "worst")  # worst or best
    asc = order == "best"
    c = (
        DF_AIR.groupby("City")["AQI"]
        .mean().sort_values(ascending=asc).head(n).reset_index()
    )
    return jsonify({
        "cities": c["City"].tolist(),
        "aqi":    [round(v, 1) for v in c["AQI"].tolist()],
    })

@app.route("/api/airquality/pollutants")
def air_pollutants():
    city_data = DF_AIR.groupby("City").agg(
        pm25=("PM25", "mean"), pm10=("PM10", "mean"),
        no2=("NO2", "mean"), so2=("SO2", "mean"),
        co=("CO", "mean"), o3=("O3", "mean"),
    ).sort_values("pm25", ascending=False).head(10).reset_index()

    return jsonify({
        "cities": city_data["City"].tolist(),
        "pm25":   [round(v, 1) for v in city_data["pm25"].tolist()],
        "pm10":   [round(v, 1) for v in city_data["pm10"].tolist()],
        "no2":    [round(v, 1) for v in city_data["no2"].tolist()],
        "so2":    [round(v, 1) for v in city_data["so2"].tolist()],
        "co":     [round(v, 2) for v in city_data["co"].tolist()],
        "o3":     [round(v, 1) for v in city_data["o3"].tolist()],
    })

@app.route("/api/airquality/categories")
def air_categories():
    cats = DF_AIR["AQI_Category"].value_counts().to_dict()
    order = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
    return jsonify({
        "categories": [c for c in order if c in cats],
        "counts":     [cats.get(c, 0) for c in order if c in cats],
    })

@app.route("/api/airquality/city_trend")
def air_city_trend():
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun"}
    df = DF_AIR.copy()
    df["Month"] = df["Date"].dt.month
    top_cities = df.groupby("City")["AQI"].mean().sort_values(ascending=False).head(6).index.tolist()
    pivot = df[df["City"].isin(top_cities)].groupby(["Month", "City"])["AQI"].mean().unstack(fill_value=0)
    result = {"months": [month_map.get(m, str(m)) for m in pivot.index.tolist()], "series": {}}
    for city in top_cities:
        if city in pivot.columns:
            result["series"][city] = [round(v, 1) for v in pivot[city].tolist()]
    return jsonify(result)

@app.route("/api/hadoop/airquality/city_agg")
def hadoop_air_city():
    r = (
        DF_AIR.groupby("City")["AQI"]
        .agg(["mean", "max", "min", "count"])
        .sort_values("mean", ascending=False).reset_index()
    )
    r.columns = ["city", "avg_aqi", "max_aqi", "min_aqi", "record_count"]
    records = []
    for _, row in r.iterrows():
        records.append({
            "city":         row["city"],
            "avg_aqi":      round(row["avg_aqi"], 1),
            "max_aqi":      int(row["max_aqi"]),
            "min_aqi":      int(row["min_aqi"]),
            "record_count": int(row["record_count"]),
        })
    return jsonify({"job": "city_aqi_aggregation", "records": records})

@app.route("/api/hadoop/airquality/critical")
def hadoop_air_critical():
    critical = DF_AIR[DF_AIR["AQI"] > 200].copy()
    critical["flag"] = critical["AQI"].apply(
        lambda x: "SEVERE" if x > 400 else ("VERY_POOR" if x > 300 else "POOR")
    )
    records = []
    for _, row in critical.sort_values("AQI", ascending=False).head(50).iterrows():
        records.append({
            "date":  row["Date"].strftime("%Y-%m-%d"),
            "city":  row["City"],
            "state": row["State"],
            "aqi":   int(row["AQI"]),
            "pm25":  round(row["PM25"], 1),
            "flag":  row["flag"],
        })
    return jsonify({"job": "critical_aqi_detection", "records": records})


# ═══════════════════════════════════════════════════════════════
#  BACKWARD COMPATIBILITY — old routes redirect
# ═══════════════════════════════════════════════════════════════

@app.route("/api/summary")
def old_summary():         return food_summary()
@app.route("/api/daily")
def old_daily():           return food_daily()
@app.route("/api/monthly")
def old_monthly():         return food_monthly()
@app.route("/api/regions")
def old_regions():         return food_regions()
@app.route("/api/districts")
def old_districts():       return food_districts()
@app.route("/api/region_trend")
def old_region_trend():    return food_region_trend()
@app.route("/api/hadoop/region_stock")
def old_hadoop_region():   return hadoop_food_region()
@app.route("/api/hadoop/daily_drawdown")
def old_hadoop_drawdown(): return hadoop_food_drawdown()


if __name__ == "__main__":
    app.run(debug=True, port=5000)