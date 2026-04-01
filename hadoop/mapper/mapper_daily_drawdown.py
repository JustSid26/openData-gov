#!/usr/bin/env python3
"""
mapper_daily_drawdown.py
========================
Hadoop Streaming Mapper — Daily National Stock Drawdown Job

Input  : CSV lines from rice-raw-2025.csv (via HDFS)
Output : date \t stock_value

The reducer will sum all district stocks per date and compute
day-over-day delta to detect drawdown events.

Usage in Hadoop Streaming:
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input  hdfs:///data/rice-raw-2025.csv \
        -output hdfs:///output/daily_drawdown \
        -mapper  mapper_daily_drawdown.py \
        -reducer reducer_daily_drawdown.py \
        -file    mapper_daily_drawdown.py \
        -file    reducer_daily_drawdown.py \
        -jobconf mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -jobconf mapred.text.key.comparator.options=-k1,1
"""

import sys

DATE_COL  = 4   # 'Date'  column → "2025-03-13"
STOCK_COL = 11  # 'Stock' column

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

        date      = fields[DATE_COL].strip()
        raw_stock = fields[STOCK_COL].strip()

        if not date or not raw_stock:
            continue

        try:
            stock = float(raw_stock)
        except ValueError:
            continue

        # Emit: date \t stock  (Hadoop sorts by date string — ISO format sorts correctly)
        print(f"{date}\t{stock}")

if __name__ == "__main__":
    main()