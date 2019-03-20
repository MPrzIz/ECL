#!/usr/bin/env python
#-*- coding: utf-8 -*-

from pyspark import SparkContext

if __name__ == "__main__":
	sc=SparkContext(appName="join example")

	names1 = sc.parallelize(("abe", "abby", "apple")).map(lambda a: (a, 1))
	names2 = sc.parallelize(("apple", "beatty", "beatrice")).map(lambda a: (a, 1))
	fulljoin = names1.join(names2).collect()
	print fulljoin
	# => [('apple', (1, 1))]
	leftjoin = names1.leftOuterJoin(names2).collect()
	print leftjoin
	# => [('abe', (1, None)), ('apple', (1, 1)), ('abby', (1, None))]
	rightjoin = names1.rightOuterJoin(names2).collect()
	print rightjoin
	# => [('apple', (1, 1)), ('beatrice', (None, 1)), ('beatty', (None, 1))]
	
	sc.stop()