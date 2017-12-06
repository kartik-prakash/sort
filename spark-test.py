import sys
import os
from pyspark import SparkContext
from pyspark.conf import SparkConf
from datetime import datetime


def main():	
	sc = SparkContext() 

	FORMAT = '%Y%m%d%H%M%S'
	path = '/home/ubuntu/Downloads/start-time.txt'
	data = 'Start time\n %s' % (datetime.now().strftime(FORMAT))
	open(path, 'w').write(data)
	
	irdd = sc.textFile('hdfs:////home/ubuntu/spark-input/test.txt').map(lambda x: (x[0:10],x[10:]))
	ordd = irdd.sortByKey(True).map(lambda x: (x[0] + x[1].strip('\n')) + '\r')
	ordd.saveAsTextFile('hdfs:////home/ubuntu/spark-output/')
    
	pathend = '/home/ubuntu/Downloads/end-time.txt'
	dataend = 'Start time\n %s' % (datetime.now().strftime(FORMAT))
	open(pathend, 'w').write(dataend)

if __name__ == '__main__':
	main()
