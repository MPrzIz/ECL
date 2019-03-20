#!/usr/bin/env python
#-*- coding: utf-8 -*-

from pyspark import SparkContext

if __name__ == "__main__":

	# Creation d un contexte Spark
	sc=SparkContext(appName="Text line count")

	lines = sc.textFile("README.md")
	lineLengths = lines.map(lambda s: len(s))
	# lineLengths.persist()
	totalLength = lineLengths.reduce(lambda a, b: a + b)
	print(totalLength)

	# Arret du contexte Spark
	sc.stop()