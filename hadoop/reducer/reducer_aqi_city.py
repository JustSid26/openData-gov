#!/usr/bin/env python3
"""
reducer_aqi_city.py
===================
Hadoop Streaming Reducer — City AQI Aggregation Job

Input  : sorted tab-separated lines from mapper (city \t AQI)
Output : city \t avg_aqi \t max_aqi \t min_aqi \t record_count

Hadoop sorts mapper output by key before sending to reducer,
so all rows for the same city arrive consecutively.
"""

import sys

def emit(city, values):
    if not values:
        return
    avg   = sum(values) / len(values)
    mx    = max(values)
    mn    = min(values)
    count = len(values)
    print(f"{city}\t{avg:.1f}\t{mx}\t{mn}\t{count}")

def main():
    current_city   = None
    current_values = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        city, raw_aqi = parts
        try:
            aqi = int(raw_aqi)
        except ValueError:
            continue

        if city != current_city:
            if current_city is not None:
                emit(current_city, current_values)
            current_city   = city
            current_values = [aqi]
        else:
            current_values.append(aqi)

    if current_city is not None:
        emit(current_city, current_values)

if __name__ == "__main__":
    main()
