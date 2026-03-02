#!/usr/bin/env python
import sys
from itertools import groupby
from operator import itemgetter


def parse_input(stdin):
    for line in stdin:
        line = line.strip()
        if line:
            parts = line.split('\t', 1)
            if len(parts) == 2:
                yield parts[0], int(parts[1])


def main():
    # groupby works correctly here because MapReduce guarantees
    # that input to each reducer is sorted by key
    for word, group in groupby(parse_input(sys.stdin), key=itemgetter(0)):
        total = sum(count for _, count in group)
        print("%s\t%s" % (word, total))


if __name__ == "__main__":
    main()
