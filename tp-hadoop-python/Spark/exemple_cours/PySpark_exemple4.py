#!/usr/bin/env python
#-*- coding: utf-8 -*-

from pyspark import SparkContext

if __name__ == "__main__":
	sc=SparkContext(appName="Union")

	one = sc.parallelize(range(1,10))
	two = sc.parallelize(range(5,15))
	one.persist()
	two.persist()
	
	U = one.intersection(two).collect()
	print(list(U))
	# => [5, 6, 7, 8, 9]

	D = one.union(two).distinct().collect()
	print(list(D))
	# => [8, 12, 4, 1, 13, 5, 9, 2, 14, 10, 6, 11, 3, 7]

	sc.stop()