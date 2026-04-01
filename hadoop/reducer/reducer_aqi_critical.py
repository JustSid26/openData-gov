#!/usr/bin/env python3
"""
reducer_aqi_critical.py
=======================
Hadoop Streaming Reducer — Critical AQI Event Detection Job

Input  : sorted tab-separated lines (date \t AQI \t city \t state)
Output : date \t max_aqi \t city \t state \t flag

flag = SEVERE       if AQI > 400
flag = VERY_POOR    if AQI > 300
flag = POOR         if AQI > 200
"""

import sys

def classify(aqi):
    if aqi > 400:
        return "SEVERE"
    elif aqi > 300:
        return "VERY_POOR"
    else:
        return "POOR"

def main():
    current_date = None
    events       = []   # (aqi, city, state) for current date

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 4:
            continue

        date, raw_aqi, city, state = parts
        try:
            aqi = int(raw_aqi)
        except ValueError:
            continue

        if date != current_date:
            # Flush previous date — emit worst event
            if current_date is not None and events:
                events.sort(key=lambda x: -x[0])
                for aqi_val, c, s in events[:5]:  # top 5 worst per date
                    flag = classify(aqi_val)
                    print(f"{current_date}\t{aqi_val}\t{c}\t{s}\t{flag}")

            current_date = date
            events = [(aqi, city, state)]
        else:
            events.append((aqi, city, state))

    # Flush last date
    if current_date is not None and events:
        events.sort(key=lambda x: -x[0])
        for aqi_val, c, s in events[:5]:
            flag = classify(aqi_val)
            print(f"{current_date}\t{aqi_val}\t{c}\t{s}\t{flag}")

if __name__ == "__main__":
    main()
