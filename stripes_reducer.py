#!/usr/bin/env python3
import sys
import json

def reduce_input():
    current_item = None
    stripe = {}
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            item, pair = line.split("\t")
            neighbor, count = pair.split(":")
            count = int(count)
        except ValueError:
            continue

        if current_item is None:
            current_item = item
            stripe = {neighbor: count}
        elif item == current_item:
            stripe[neighbor] = stripe.get(neighbor, 0) + count
        else:
            print(f"{current_item}\t{json.dumps(stripe)}")
            current_item = item
            stripe = {neighbor: count}

    if current_item is not None:
        print(f"{current_item}\t{json.dumps(stripe)}")

if __name__ == "__main__":
    reduce_input()
