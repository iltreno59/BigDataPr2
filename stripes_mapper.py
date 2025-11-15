#!/usr/bin/env python3
import sys

def map_line(line):
    items = line.strip().split()
    for item_i in items:
        for item_j in items:
            if item_i != item_j:
                print(f"{item_i}\t{item_j}:1")

if __name__ == "__main__":
    for line in sys.stdin:
        map_line(line)
