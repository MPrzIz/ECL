#!/usr/bin/env python3
#-*- coding: utf-8 -*-

################ MFR - Exemple 1
print('MFR - Exemple 1')

people = [{'name': 'Mary', 'height': 160},
          {'name': 'Isla', 'height': 80},
          {'name': 'Sam'}]

height_total = 0
height_count = 0
for person in people:
    if 'height' in person:
        height_total += person['height']
        height_count += 1

if height_count > 0:
    average_height = height_total / height_count

print(average_height)


################ MFR - Exemple 2
print('MFR - Exemple 2')

from functools import reduce

people = [{'name': 'Mary', 'height': 160},
          {'name': 'Isla', 'height': 80},
          {'name': 'Sam'}]

heights = list(map(lambda x: x['height'],
              filter(lambda x: 'height' in x, people)))

from operator import add
average_height = reduce(add, heights) / len(heights)
print(average_height)



