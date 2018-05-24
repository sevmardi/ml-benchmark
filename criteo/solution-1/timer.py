import sys, csv, math, time

from StringIO import StringIO
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint


APP_NAME = "My Spark Application"

	
def parsePoint(line):
	line = line.replace("[",'')
	line = line.replace("]",'')
	line = line.replace(" ",'')
	values = [int(x) for x in line.split(',')]
	return LabeledPoint(values[0], values[1:])
	
def main(sc):	
	train_data = sc.textFile("input/ctc_data.txt").map(parsePoint)	
	parsedTrainData = train_data.randomSplit(weights=[0.2, 0.8])	
	start = time.time()	
	model = LogisticRegressionWithSGD.train(parsedTrainData)
	end = time.time()	
	time_elapsed = end - start
	output = "\nusing SGD " + str(time_elapsed)		
	print output	
	
if __name__ == "__main__":
	# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)

	# Execute Main functionality
	main(sc)


