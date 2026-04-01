#!/usr/bin/env python3
"""
mapper_aqi_critical.py
======================
Hadoop Streaming Mapper — Critical AQI Event Detection Job

Input  : CSV lines from air-quality-india-2025.csv (via HDFS)
Output : date \t AQI \t city \t state

The reducer will identify days with critical pollution events.
"""

import sys

CITY_COL  = 0   # 'City'
STATE_COL = 1   # 'State'
DATE_COL  = 3   # 'Date'
AQI_COL   = 10  # 'AQI'

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
        state   = fields[STATE_COL].strip()
        date    = fields[DATE_COL].strip()
        raw_aqi = fields[AQI_COL].strip()

        if not date or not raw_aqi:
            continue

        try:
            aqi = int(raw_aqi)
        except ValueError:
            continue

        # Only emit readings above "Poor" threshold (AQI > 200)
        if aqi > 200:
            # Emit: date \t AQI \t city \t state
            print(f"{date}\t{aqi}\t{city}\t{state}")

if __name__ == "__main__":
    main()
