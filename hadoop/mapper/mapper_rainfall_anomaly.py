#!/usr/bin/env python3
"""
mapper_rainfall_anomaly.py
==========================
Hadoop Streaming Mapper — Daily Rainfall Anomaly Detection Job

Input  : CSV lines from rainfall-india-2025.csv (via HDFS)
Output : date \t rainfall_mm

The reducer will sum all district rainfalls per date and flag
days with abnormally high rainfall (HEAVY_RAIN events).
"""

import sys

DATE_COL     = 2   # 'Date' column
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

        date         = fields[DATE_COL].strip()
        raw_rainfall = fields[RAINFALL_COL].strip()

        if not date or not raw_rainfall:
            continue

        try:
            rainfall = float(raw_rainfall)
        except ValueError:
            continue

        # Emit: date \t rainfall_mm
        print(f"{date}\t{rainfall}")

if __name__ == "__main__":
    main()
