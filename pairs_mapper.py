#!/usr/bin/env python3
import sys

def map(order):
    if not order:
        return
    items = [item.strip() for item in order.split(' ') if item.strip()]
    if len(items) > 1:
        for i in range(0, len(items) - 1):
            for j in range(i + 1, len(items)):
                if items[i] and items[j]:
                    if items[i] < items[j]:
                        print(f"{items[i]},{items[j]}\t1")
                    else: 
                        print(f"{items[j]},{items[i]}\t1")


if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        map(line)
        
