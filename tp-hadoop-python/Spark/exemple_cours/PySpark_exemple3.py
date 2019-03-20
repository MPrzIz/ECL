#!/usr/bin/env python
#-*- coding: utf-8 -*-

from pyspark import SparkContext

if __name__ == "__main__":

	# Creation d un contexte Spark
	sc=SparkContext(appName="Spark flatMap example")

	A = sc.parallelize([2, 3, 4]).flatMap(lambda x: [x,x,x]).collect()
	print(list(A))
	# => [2, 2, 2, 3, 3, 3, 4, 4, 4]
 
	B = sc.parallelize([1,2,3]).map(lambda x: [x,x,x]).collect()
	print(list(B))
	# => [[1, 1, 1], [2, 2, 2], [3, 3, 3]]

	# Arret du contexte Spark
	sc.stop()