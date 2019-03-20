#!/usr/bin/env python3
#-*- coding: utf-8 -*-



################ FILTER - Exemple 1
print('FILTER - Exemple 1')
number_list = range(-5, 5)
less_than_zero = list(filter(lambda x: x < 0, number_list))
print(less_than_zero)
# => [-5, -4, -3, -2, -1]



################ FILTER - Exemple 2
print('FILTER - Exemple 2')
carre = map(lambda x: x * x,  filter(lambda x: x % 2, range(10)))
print(list(carre))
# => [1, 9, 25, 49, 81]

carre2 = [x * x for x in range(10) if x % 2]
print(list(carre2))
# => [1, 9, 25, 49, 81]

