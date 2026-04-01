#!/usr/bin/env python3
"""
mapper_rainfall_state.py
========================
Hadoop Streaming Mapper — State Rainfall Aggregation Job

Input  : CSV lines from rainfall-india-2025.csv (via HDFS)
Output : tab-separated key-value pairs → state \t rainfall_mm

Usage in Hadoop Streaming:
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input  hdfs:///data/rainfall-india-2025.csv \
        -output hdfs:///output/rainfall_state \
        -mapper  mapper_rainfall_state.py \
        -reducer reducer_rainfall_state.py \
        -file    mapper_rainfall_state.py \
        -file    reducer_rainfall_state.py
"""

import sys

STATE_COL    = 0   # 'State' column
RAINFALL_COL = 5   # 'Rainfall_mm' column

def main():
    header_skipped = False
    for line in sys.stdin:
        line = line.rstrip("\n")

        if not header_skipped:
            header_skipped = True
            continue

        fields = line.split(",")
        if len(fields) < 8:
            continue

        state        = fields[STATE_COL].strip()
        raw_rainfall = fields[RAINFALL_COL].strip()

        if not state or not raw_rainfall:
            continue

        try:
            rainfall = float(raw_rainfall)
        except ValueError:
            continue

        # Emit: state \t rainfall_mm
        print(f"{state}\t{rainfall}")

if __name__ == "__main__":
    main()
