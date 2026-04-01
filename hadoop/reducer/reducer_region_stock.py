#!/usr/bin/env python3
"""
reducer_region_stock.py
=======================
Hadoop Streaming Reducer — Region Stock Aggregation Job

Input  : sorted tab-separated lines from mapper (region \t stock)
Output : region \t total_stock \t avg_stock \t record_count

Hadoop sorts mapper output by key before sending to reducer,
so all rows for the same region arrive consecutively.
"""

import sys

def emit(region, values):
    if not values:
        return
    total   = sum(values)
    avg     = total / len(values)
    count   = len(values)
    print(f"{region}\t{total:.2f}\t{avg:.4f}\t{count}")

def main():
    current_region = None
    current_values = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        region, raw_stock = parts
        try:
            stock = float(raw_stock)
        except ValueError:
            continue

        if region != current_region:
            # New key — flush previous group
            if current_region is not None:
                emit(current_region, current_values)
            current_region = region
            current_values = [stock]
        else:
            current_values.append(stock)

    # Flush last group
    if current_region is not None:
        emit(current_region, current_values)

if __name__ == "__main__":
    main()