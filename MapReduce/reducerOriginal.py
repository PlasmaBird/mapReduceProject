#!/usr/bin/env python3

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# Input comes from STDIN
for line in sys.stdin:
    # Parse the input we got from mapper.py
    line = line.strip()
    # Split the line into word and count
    word, count = line.split('\t', 1)

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue

    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Write result to STDOUT
            print(f'{current_word}\t{current_count}')
        current_count = count
        current_word = word

if current_word == word:
    print(f'{current_word}\t{current_count}')
