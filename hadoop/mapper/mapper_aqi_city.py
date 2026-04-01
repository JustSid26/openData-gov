#!/usr/bin/env python3
"""
mapper_aqi_city.py
==================
Hadoop Streaming Mapper — City AQI Aggregation Job

Input  : CSV lines from air-quality-india-2025.csv (via HDFS)
Output : city \t AQI

Usage in Hadoop Streaming:
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input  hdfs:///data/air-quality-india-2025.csv \
        -output hdfs:///output/aqi_city \
        -mapper  mapper_aqi_city.py \
        -reducer reducer_aqi_city.py
"""

import sys

CITY_COL = 0   # 'City' column
AQI_COL  = 10  # 'AQI' column

def main():
    header_skipped = False
    for line in sys.stdin:
        line = line.rstrip("\n")

        if not header_skipped:
            header_skipped = True
            continue

        fields = line.split(",")
        if len(fields) < 12:
            continue

        city    = fields[CITY_COL].strip()
        raw_aqi = fields[AQI_COL].strip()

        if not city or not raw_aqi:
            continue

        try:
            aqi = int(raw_aqi)
        except ValueError:
            continue

        # Emit: city \t AQI
        print(f"{city}\t{aqi}")

if __name__ == "__main__":
    main()
