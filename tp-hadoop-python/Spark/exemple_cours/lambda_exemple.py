#!/usr/bin/env python3

def f(x):
	return x*x

if __name__ == "__main__":

        # fonction classique
        print(f(3))

        # Lambda fonction sans nom
        print((lambda x: x*x)(3))

        # Lambda fonction nommee
        g = lambda x: x*x
        print(g(3))
