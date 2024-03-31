#!/usr/bin/env python3
import sys

# Input comes from STDIN (standard input)
for line in sys.stdin:
    # Remove leading and trailing whitespace
    line = line.strip()
    # Split the line into words
    words = line.split()
    # Increase counters
    for word in words:
        # Write the results to STDOUT (standard output);
        # What we output here will be the input for the Reduce step,
        # i.e., the input for reducer.py.
        # Format: <word> 1
        print(f'{word}\t1')
