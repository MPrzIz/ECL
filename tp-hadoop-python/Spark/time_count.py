# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 09:03:48 2019

@author: perez
"""

#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import sys

from functools import reduce



def split_fn(str_time):
    return int(str_time[:2])*3600, int(str_time[3:5])*60, int(str_time[6:])*1
        

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    seconds = map(split_fn,sys.argv[1])
    sum = reduce(lambda a,x : a+x, seconds)

    print ("Pi is roughly ", sum)

