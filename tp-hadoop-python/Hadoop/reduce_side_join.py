#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Determine the most used word in the input."""
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper_tag_table(self, _, line):
        values = WORD_RE.findall(line)
        nb_columns = len(values)
        
        if nb_columns == 5:
            yield ('cust', values)
        else:
            yield ('trans', values)
            

    def mapper_foreign_key(self, tag, values):
        if tag == 'cust':
            foreignKey = values.pop(0)
        else:
            foreignKey = values.pop(2)
            
        values = values.append(tag)
        yield (foreignKey, values)
        

    def reducer_join(self, foreignKey, value_lists):
        # send all (num_occurrences, word) pairs to the same reducer.
        for liste in value_lists:
            if liste[-1] == 'cust':
                for liste2 in value_lists:
                    if liste[-1] == 'trans':
                        values = [foreignKey] + liste
                        liste2.pop()
                        for val in liste2:
                            if val not in values:
                                values.append(val)
                        
                        yield '', values
    


    def steps(self):
        return [MRStep(mapper=self.mapper_tag_table),
                MRStep(mapper=self.mapper_foreign_key,
                       reducer=self.reducer_join)]
                

if __name__ == '__main__':
    MRMostUsedWord.run()
