#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

vector = [2, 4.5, 6]

class MatVecProd(MRJob):

    def mapper(self, _, line):
        index, values = line[0], line[1:]
        yield (index, values)


    def reducer(self, index, values):       
        try:
            res = 0.
            col_idx = 0
            
            for vals in values:
                for val in vals.strip().split(' '):
                    res += float(val)*vector[col_idx]
                    col_idx += 1
                
            res = str(res)
        
        except:
            res = str(0.)
            
        yield (index, res)
            

if __name__ == '__main__':
    MatVecProd.run()
    
