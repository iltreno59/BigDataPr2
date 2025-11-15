#!/usr/bin/env python3
import sys

def map_line(line):
    items = line.strip().split()
    for item_i in items:
        for item_j in items:
            if item_i != item_j:
                # Emit pair and count 1
                print(f"{item_i},{item_j}\t1")

if __name__ == "__main__":
    for line in sys.stdin:
        map_line(line)
