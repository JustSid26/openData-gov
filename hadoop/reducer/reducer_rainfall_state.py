#!/usr/bin/env python3
"""
reducer_rainfall_state.py
=========================
Hadoop Streaming Reducer — State Rainfall Aggregation Job

Input  : sorted tab-separated lines from mapper (state \t rainfall_mm)
Output : state \t total_rain \t avg_rain \t max_rain \t record_count

Hadoop sorts mapper output by key before sending to reducer,
so all rows for the same state arrive consecutively.
"""

import sys

def emit(state, values):
    if not values:
        return
    total   = sum(values)
    avg     = total / len(values)
    mx      = max(values)
    count   = len(values)
    print(f"{state}\t{total:.2f}\t{avg:.2f}\t{mx:.1f}\t{count}")

def main():
    current_state  = None
    current_values = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        state, raw_rain = parts
        try:
            rain = float(raw_rain)
        except ValueError:
            continue

        if state != current_state:
            if current_state is not None:
                emit(current_state, current_values)
            current_state  = state
            current_values = [rain]
        else:
            current_values.append(rain)

    if current_state is not None:
        emit(current_state, current_values)

if __name__ == "__main__":
    main()
