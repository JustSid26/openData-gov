#!/usr/bin/env python3
"""
mapper_region_stock.py
======================
Hadoop Streaming Mapper — Region Stock Aggregation Job

Input  : CSV lines from rice-raw-2025.csv (via HDFS)
Output : tab-separated key-value pairs → region \t stock_value

Usage in Hadoop Streaming:
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input  hdfs:///data/rice-raw-2025.csv \
        -output hdfs:///output/region_stock \
        -mapper  mapper_region_stock.py \
        -reducer reducer_region_stock.py \
        -file    mapper_region_stock.py \
        -file    reducer_region_stock.py
"""

import sys

REGION_COL  = 3   # 'Code' column  →  "Region Name: Punjab"
STOCK_COL   = 11  # 'Stock' column

def main():
    header_skipped = False
    for line in sys.stdin:
        line = line.rstrip("\n")

        # Skip CSV header row
        if not header_skipped:
            header_skipped = True
            continue

        fields = line.split(",")
        if len(fields) < 12:
            continue  # malformed row — skip

        raw_region = fields[REGION_COL].strip().replace("Region Name: ", "")
        raw_stock  = fields[STOCK_COL].strip()

        if not raw_region or not raw_stock:
            continue

        try:
            stock = float(raw_stock)
        except ValueError:
            continue

        # Emit: region \t stock
        print(f"{raw_region}\t{stock}")

if __name__ == "__main__":
    main()