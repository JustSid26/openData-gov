#!/usr/bin/env python3
"""
reducer_rainfall_anomaly.py
===========================
Hadoop Streaming Reducer — Daily Rainfall Anomaly Detection Job

Input  : sorted tab-separated lines (date \t rainfall_mm)
Output : date \t total_rain \t avg_rain \t delta \t pct_change \t flag

flag = HEAVY_RAIN  if total daily rainfall > 500 mm (nationwide sum)
flag = NORMAL      otherwise
"""

import sys

HEAVY_RAIN_THRESHOLD = 500.0   # mm total across all reporting districts

def main():
    current_date  = None
    current_total = 0.0
    current_count = 0
    results       = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        date, raw_rain = parts
        try:
            rain = float(raw_rain)
        except ValueError:
            continue

        if date != current_date:
            if current_date is not None:
                results.append((current_date, current_total, current_count))
            current_date  = date
            current_total = rain
            current_count = 1
        else:
            current_total += rain
            current_count += 1

    if current_date is not None:
        results.append((current_date, current_total, current_count))

    # Second pass: emit with delta & flag
    for i, (date, total, count) in enumerate(results):
        avg = total / count if count > 0 else 0
        if i == 0:
            delta   = 0.0
            pct_chg = 0.0
        else:
            prev    = results[i - 1][1]
            delta   = total - prev
            pct_chg = (delta / prev * 100) if prev != 0 else 0.0

        flag = "HEAVY_RAIN" if total > HEAVY_RAIN_THRESHOLD else "NORMAL"
        print(f"{date}\t{total:.2f}\t{avg:.2f}\t{delta:.2f}\t{pct_chg:.2f}\t{flag}")

if __name__ == "__main__":
    main()
