#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	
	# Create new config
	conf = (SparkConf().set("spark.cores.max", "4"))

	# Create new context
	sc = SparkContext(conf=conf, appName="Spark Count")

	# Lecture du fichier et decomposition du fichier en mots
	tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

	# Comptage de l'occurence de chaque mot
	wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2 : v1 + v2)

	# Stockage du resultat sur HDFS 
	# ne pas oublier "hdfs dfs -rm -r -f sortie" entre 2 executions
	wordCounts.saveAsTextFile("///home/tp3_13/tp-hadoop-python/Spark/sortie")
	
	# Arret du contexte Spark
	sc.stop()
	
