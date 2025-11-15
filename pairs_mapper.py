#!/usr/bin/env python3
import sys

def map(order):
    for item_i in order:
        for item_j in order:
            if item_i != item_j:
                print(f"{item_i},{item_j}\t1")


if __name__ == "__main__":
    orders = []
    order_items = []
    order_id = 0
    for line in sys.stdin:
        if line.startswith("order_item_id"):
            continue
        parts = line.strip().split(",")
        if parts[1] != order_id:
            if len(order_items) >= 1:
                orders.append(order_items)
            order_items = []
            order_id = parts[1]
        order_items.append(parts[2])
    for order in orders:
        map(order)
