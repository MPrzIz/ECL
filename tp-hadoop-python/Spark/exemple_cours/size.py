#!/usr/bin/env python3
#-*- coding: utf-8 -*-

def size_S(l):
    s = 0
    for x in l:
        s += 1
    return s

def size_FP(l):
    if not l:
        return 0
    return 1 + size_FP(l[1:])

L = [1, 3, 7, 10, 3, 8]
print(size_S(L), size_FP(L))