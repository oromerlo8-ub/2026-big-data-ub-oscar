#!/usr/bin/env python
import sys

for line in sys.stdin:
    line = line.strip().lower()
    for word in line.split():
        word = word.strip('.,!?":;()-[]{}\'')
        if word:
            print("%s\t1" % word)
