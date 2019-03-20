#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A more advanced Mapper, using Python iterators and generators."""

import sys
import re

def read_input(file):
    for line in file:
        # split the line into words
        line = re.sub(r"([.!?,;])",r"", line)
        line = line.lower()
        yield line.split()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # tab-delimited; the trivial word count is 1
        for word in words:
            print(word, separator, '1')

if __name__ == "__main__":
    main()