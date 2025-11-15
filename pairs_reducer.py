#!/usr/bin/env python3
import sys

def reduce_input():
    current_pair = None
    current_count = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            pair, count = line.split("\t")
            count = int(count)
        except ValueError:
            continue

        if current_pair is None:
            current_pair = pair
            current_count = count
        elif pair == current_pair:
            current_count += count
        else:
            print(f"{current_pair}\t{current_count}")
            current_pair = pair
            current_count = count

    if current_pair is not None:
        print(f"{current_pair}\t{current_count}")

if __name__ == "__main__":
    reduce_input()
