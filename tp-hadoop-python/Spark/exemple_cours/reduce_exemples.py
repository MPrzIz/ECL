#!/usr/bin/env python3
#-*- coding: utf-8 -*-

################ REDUCE - Exemple 1
print('REDUCE - Exemple 1')

from functools import reduce
sum = reduce(lambda a, x: a + x, [0, 1, 2, 3, 4])
print(sum)



################ REDUCE - Exemple 2
print('REDUCE - Exemple 2')
sentences = ['Mary read a story to Sam and Isla.',
             'Isla cuddled Sam.',
             'Sam chortled.']

sam_count = reduce(lambda a, x: a + x.count('Sam'), sentences, 0)
print(sam_count)

