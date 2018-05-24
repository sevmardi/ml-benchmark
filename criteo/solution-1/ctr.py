import sys, csv, math, time
from StringIO import StringIO
from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics


APP_NAME = "My Spark Application"
	
def parsePoint(line):
	line = line.replace("[",'')
	line = line.replace("]",'')
	line = line.replace(" ",'')
	values = [int(x) for x in line.split(',')]
	return LabeledPoint(values[0], values[1:])
	
def main(sc):	
	train_data = sc.textFile("input/trainl_data.txt").map(parsePoint)		
	model = LogisticRegressionWithSGD.train(train_data, iterations = 1000, miniBatchFraction = 0.0001, step = .001, regType = "l2")	
	 
	valid_data = sc.textFile("input/valid_data.txt").map(parsePoint)		
	labelsAndPreds = valid_data.map(lambda p: (float(model.predict(p.features)), p.label))
	Accuracy = labelsAndPreds.filter(lambda (pred, lab): lab == pred).count() / float(valid_data.count())		
	FP = labelsAndPreds.filter(lambda (pred, lab): lab == 0 and pred == 1).count() 		
	N = float(labelsAndPreds.filter(lambda (pred, lab): lab == 0).count())	
	FPR = FP /N			
	output = "Accuracy valid = "+ str(Accuracy) + "\nFPR valid = " + str(FPR)
	print output
	metrics = BinaryClassificationMetrics(labelsAndPreds)
	output += "Area under ROC valid = " + str(metrics.areaUnderROC)
	
	print output

	test_data = sc.textFile("input/test_data.txt").map(parsePoint)		
	labelsAndPreds = test_data.map(lambda p: (float(model.predict(p.features)), p.label))
	Accuracy = labelsAndPreds.filter(lambda (pred, lab): lab == pred).count() / float(test_data.count())		
	FP = labelsAndPreds.filter(lambda (pred, lab): lab == 0 and pred == 1).count() 		
	N = float(labelsAndPreds.filter(lambda (pred, lab): lab == 0).count())	
	FPR = FP /N			
	output += "\nAccuracy test = "+ str(Accuracy) + "\nFPR test = " + str(FPR)
	print output
	metrics = BinaryClassificationMetrics(labelsAndPreds)
	output += "Area under ROC test = " + str(metrics.areaUnderROC)
	
	print output
		
	
	output = sc.parallelize([output])
	output.saveAsTextFile("str")			
							
	
if __name__ == "__main__":
	# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)

	# Execute Main functionality
	main(sc)



