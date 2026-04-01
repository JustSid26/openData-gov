#!/usr/bin/env python3
"""
reducer_daily_drawdown.py
=========================
Hadoop Streaming Reducer — Daily National Stock Drawdown Job

Input  : sorted tab-separated lines (date \t stock)
Output : date \t total_stock \t delta \t pct_change \t flag

flag = DRAWDOWN  if delta < -5_000_000  (≥5 million MT drop in one day)
flag = STABLE    otherwise
"""

import sys

DRAWDOWN_THRESHOLD = -5_000_000   # 5 million MT single-day drop

def main():
    current_date  = None
    current_total = 0.0
    prev_total    = None
    results       = []   # collect all (date, total) first — need prev to compute delta

    for line in sys.stdin:
        line = line.rstrip("\n")
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        date, raw_stock = parts
        try:
            stock = float(raw_stock)
        except ValueError:
            continue

        if date != current_date:
            if current_date is not None:
                results.append((current_date, current_total))
            current_date  = date
            current_total = stock
        else:
            current_total += stock

    if current_date is not None:
        results.append((current_date, current_total))

    # Second pass: emit with delta & flag
    for i, (date, total) in enumerate(results):
        if i == 0:
            delta   = 0.0
            pct_chg = 0.0
        else:
            prev    = results[i - 1][1]
            delta   = total - prev
            pct_chg = (delta / prev * 100) if prev != 0 else 0.0

        flag = "DRAWDOWN" if delta < DRAWDOWN_THRESHOLD else "STABLE"
        print(f"{date}\t{total:.2f}\t{delta:.2f}\t{pct_chg:.3f}\t{flag}")

if __name__ == "__main__":
    main()