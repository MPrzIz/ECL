#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mrjob.job import MRJob

import re

WORD_RE = re.compile(r"[\w']+")


class MatVecProd(MRJob):
    
    def configure_args(self):
    	super(MatVecProd, self).configure_args()
    	self.add_file_arg('--vector')


    def mapper(self, _, line):
        index, values = line[0], line[1:]
        print(len(values.strip().split(' ')))
        yield (index, values)


    def reducer(self, index, values):       
        try:
            vector = []
            vec_lines = open(self.options.vector).read().strip().split('\n')
            for line in vec_lines:
                for val in line.split(' ')[1:]:
                    vector.append(float(val))
            print('len : ', len(vector))
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
    
