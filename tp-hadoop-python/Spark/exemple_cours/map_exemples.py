#!/usr/bin/env python3
#-*- coding: utf-8 -*-

################ MAP - Exemple 1
print('MAP - Exemple 1')

squares = map(lambda x: x * x, [0, 1, 2, 3, 4])

print(list(squares))



################ MAP - Exemple 2
print('MAP - Exemple 2')
import random

names = ['Mary', 'Isla', 'Sam']
code_names = ['Mr. Pink', 'Mr. Orange', 'Mr. Blonde']

secret_names = map(lambda x: random.choice(code_names), names)
print(list(secret_names))



################ MAP - Exemple 3
print('MAP - Exemple 3')
def multiply(x):
    return (x*x)
def add(x):
    return (x+x)

funcs = [multiply, add]
for i in range(5):
    value = list(map(lambda x: x(i), funcs))
    print(value)



################ MAP - Exemple 4
print('MAP - Exemple 4')
a = [1,2,3,4]
b = [17,12,11,10]
c = [-1,-4,5,9]
print(list(map(lambda x,y:x+y, a,b)))
print(list(map(lambda x,y,z:x+y-z, a,b,c)))
