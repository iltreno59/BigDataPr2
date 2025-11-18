#!/usr/bin/env python3
import sys

def reduce():
    pair_counts = {}
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            pair, count = line.split("\t")
            count = int(count)
            if pair in pair_counts.keys():
                pair_counts[pair] += count
            else:
                pair_counts[pair] = count
        except ValueError:
            continue
    for item in pair_counts.items():
        print(f"{item[0]}\t{item[1]}")

if __name__ == "__main__":
    reduce()