#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
from pyspark import SparkContext

if __name__ == "__main__":
	sc=SparkContext(appName="groupByKey example")

	baby_names = sc.textFile("baby_names_2013.csv")
	rows = baby_names.map(lambda line: line.split(","))

	namesToCounties = rows.map(lambda n: (str(n[1]), str(n[2]))).groupByKey()
	namesToCounties.map(lambda x : {x[0]: list(x[1])}).collect()
	#print namesToCounties.take(20)
	# => [{'GRIFFIN': ['ERIE', 'ONONDAGA', 'NEW YORK', 'ERIE', ...}, {...}]

	namesNumber = rows.map(lambda n: (str(n[1]), int(n[4]) ) ).reduceByKey(lambda v1,v2: v1 + v2).collect()
	print namesNumber
	# => [('GRIFFIN', 20), ('KALEB', 24), ('JOHNNY', 25), ('NAYELI', 11), ('ERIN', 58) ...

	sc.stop()