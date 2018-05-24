import sys, csv, math, time

from StringIO import StringIO
from pyspark import SparkConf
from pyspark import SparkContext



APP_NAME = "My Spark Application"
featureinfo = []

def parse(row):	
	#remove csv header
	if row[1] == "click":	
		return	
	else:
		# remove id, hour
		column = [row[1],row[3:]]				
		return column	

def split(line):
	reader = csv.reader(StringIO(line))
	return reader.next()

# function to determine vector size for each column
def vectorize(column):
	unique = list(set(column))
	count = len(unique)	
	if count < 11:	
		vecSize = count		
	elif count < 100:
		vecSize = 12		
	elif count < 1000:
		vecSize = 16	
	else:
		vecSize = 0		
	featureinfo.append([unique, count, vecSize])

#determine which column would contain 1 	
def newVector(value, unique, count, vecSize):	
	zeroes = [0]*vecSize
	pos = unique.index(value)
	position = int(math.floor(pos/math.ceil(count*1.0/vecSize)))
	zeroes[position] = 1
	return zeroes
	
#change all features in a row into a big vector
def convert (row):
	click = [int(row[0])]
	allFeature = row[1:]
	newrow = click
	for i in range(len(allFeature)):
		feature = allFeature[i]
		info = featureinfo[i]	
		if info[2] == 0:
			continue
		vec = newVector(feature,info[0],info[1], info[2])
		newrow = newrow + vec
	return newrow
	
def main(sc):	
	data = sc.textFile("input/ctc.csv")	
	collectedData = data.map(split).map(parse).filter(lambda line: line!=None )			
	
	features = collectedData.map(lambda x: x[1:])		
	for i in range(21):
		feature = features.map(lambda x: x[i]).collect()
		vectorize(feature)
	
	mydata = collectedData.map(lambda x: convert(x))
	## divide data into train, test, validation
	train_data, test_data = mydata.randomSplit(weights = [0.8, 0.2])
	train_data, valid_data = train_data.randomSplit(weights = [0.75, 0.25])
	## store data
	train_data.saveAsTextFile("train")
	valid_data.saveAsTextFile("valid")
	test_data.saveAsTextFile("test")
	
		

	
if __name__ == "__main__":
	# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)

	# Execute Main functionality
	main(sc)
