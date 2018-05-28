import numpy as np
import os.path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from math import exp
from math import log
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


def oneHotEncoding(rawFeats, OHEDict, numOHEFeats):
    sparse =  SparseVector(numOHEFeats, [(OHEDict[(featID, value)], 1) for (featID, value) in rawFeats])
    print("--------------------------One hot was created! ------------- ")
    return sparse



def createOneHotDict(inputData):
    features = inputData.flatMap(lambda x: x).distinct()
    OHEDict = features.zipWithIndex().collectAsMap()
    print("--------------------------One hot dict was created! ------------- ")
    return OHEDict

def parsePoint(point):
	feats = point.split(',')[1:]
	return [(idx, value) for (idx, value) in enumerate(feats)]    #return 0

def parseOHEPoint(point, OHEDict, numOHEFeats):
	return LabeledPoint(point.split(',')[0],oneHotEncoding(parsePoint(point), OHEDict, numCtrOHEFeats))


def bucketFeatByCount(featCount):
	"""Bucket the counts by powers of two."""
	for i in range(11):
		size = 2 ** i
		if featCount <= size:
			return size
	return -1

def oneHotEncoding(rawFeats, OHEDict, numOHEFeats):

	validFeatureTuples = []
	for (featID, value) in rawFeats:
		try:
			validFeatureTuples.append((OHEDict[(featID, value)],1))
		except KeyError:
			pass
	return SparseVector(numOHEFeats, validFeatureTuples)

def preparePlot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0):
    plt.close()
    fig, ax = plt.subplots(figsize=figsize, facecolor='white', edgecolor='white')
    ax.axes.tick_params(labelcolor='#999999', labelsize='10')
    for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
        axis.set_ticks_position('none')
        axis.set_ticks(ticks)
        axis.label.set_color('#999999')
        if hideLabels: axis.set_ticklabels([])
    plt.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
    map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
    return fig, ax

def computeLogLoss(p, y):
	epsilon = 10e-12
	if p == 0:
		p += epsilon
	elif p == 1:
		p -= epsilon
	if y == 1:
		return -log(p)
	else:
		return -log(1-p)

def getP(x, w, intercept):

	rawPrediction = w.dot(x) + intercept
	rawPrediction = min(rawPrediction, 20)
	rawPrediction = max(rawPrediction, -20)
	return 1/(1 + exp(-rawPrediction))

def evaluateResults(model, data):
	return (data
            .map(lambda x: (x.label, getP(x.features, model.weights, model.intercept)))
            .map(lambda (x,y): computeLogLoss(y,x))
            .mean())
# input_file = "/data/scratch/vw/criteo-display-advertising-dataset/train.txt"


if __name__ == '__main__':
    conf = SparkConf().setAppName('Click Prediction')
    conf.set("spark.storage.memoryFraction", "0.40")
    sc = SparkContext(conf=conf)
    sampleOne = [(0, 'mouse'), (1, 'black')]
    sampleTwo = [(0, 'cat'), (1, 'tabby'), (2, 'mouse')]
    sampleThree = [(0, 'bear'), (1, 'black'), (2, 'salmon')]
    sampleDataRDD = sc.parallelize([sampleOne, sampleTwo, sampleThree])
    
    # Calculate the number of features in sampleOHEDictManual
    # sampleOHEDictManual = {}
    # numSampleOHEFeats = len(sampleOHEDictManual)
    # sampleOHEData = sampleDataRDD.map(lambda x: oneHotEncoding(x, sampleOHEDictManual, numSampleOHEFeats))

    # fileName = "/local/criteo/train.txt"
    fileName = "/data/scratch/vw/criteo-display-advertising-dataset/train.txt"
    # work with either ',' or '\t' separated data
    rawData = (sc.textFile(fileName, 2).map(lambda x: x.replace('\t', ',')))
    print("----------------------Data was loaded----------------------")
    weights = [.8, .1, .1]
    seed = 42
    # Use randomSplit with weights and seed
    rawTrainData, rawValidationData, rawTestData = rawData.randomSplit(
        weights, seed)
    # Cache the data
    rawTrainData.cache()
    rawValidationData.cache()
    rawTestData.cache()

    nTrain = rawTrainData.count()
    nVal = rawValidationData.count()
    nTest = rawTestData.count()

    parsedTrainFeat = rawTrainData.map(parsePoint)

    numCategories = (parsedTrainFeat
                 .flatMap(lambda x: x)
                 .distinct()
                 .map(lambda x: (x[0], 1))
                 .reduceByKey(lambda x, y: x + y)
                 .sortByKey()
                 .collect())

    ctrOHEDict = createOneHotDict(parsedTrainFeat)
    numCtrOHEFeats = len(ctrOHEDict.keys())

    OHETrainData = rawTrainData.map(lambda point: parseOHEPoint(point, ctrOHEDict, numCtrOHEFeats))
    print("----------------------OHETrainData loaded----------------------")
    OHETrainData.cache()
    print("----------------------OHETrainData cache was finished----------------------")
    
    # # Check that oneHotEncoding function was used in parseOHEPoint
    backupOneHot = oneHotEncoding
    oneHotEncoding = None
    withOneHot = False
    try: parseOHEPoint(rawTrainData.take(1)[0], ctrOHEDict, numCtrOHEFeats)
    except TypeError: withOneHot = True
    oneHotEncoding = backupOneHot


    numNZ = sum(parsedTrainFeat.map(lambda x: len(x)).take(5))
    numNZAlt = sum(OHETrainData.map(lambda lp: len(lp.features.indices)).take(5))

    featCounts = (OHETrainData
              .flatMap(lambda lp: lp.features.indices)
              .map(lambda x: (x, 1))
              .reduceByKey(lambda x, y: x + y))
    print("----------------------featCountswas finished----------------------")
    # featCountsBuckets = (featCounts
    #                  .map(lambda x: (bucketFeatByCount(x[1]), 1))
    #                  .filter(lambda k, v: k != -1)
    #                  .reduceByKey(lambda x, y: x + y)
    #                  .collect())

    # x, y = zip(*featCountsBuckets)
    # x, y = np.log(x), np.log(y)

    # print("-------------------------------Feat Count buckts was finished!")
    


    # fig, ax = preparePlot(np.arange(0, 10, 1), np.arange(4, 14, 2))
    # ax.set_xlabel(r'$\log_e(bucketSize)$'), ax.set_ylabel(r'$\log_e(countInBucket)$')
    # plt.scatter(x, y, s=14**2, c='#d6ebf2', edgecolors='#8cbfd0', alpha=0.75)
    # plt.savefig('preparePlot.png')


    # OHEValidationData = rawValidationData.map(lambda point: parseOHEPoint(point, ctrOHEDict, numCtrOHEFeats))
    # OHEValidationData.cache()

    # numIters = 50
    # stepSize = 10.
    # regParam = 1e-6
    # regType = 'l2'
    # includeIntercept = True

    # model0 =  LogisticRegressionWithSGD.train(OHETrainData,iterations=numIters,step=stepSize,regParam=regParam,
    #                               regType=regType,intercept=includeIntercept)
    # print("----------------------LogisticRegressionWithSGD was finished!----------------------")
    # sortedWeights = sorted(model0.weights)

    # # print computeLogLoss(.5, 1)

    # classOneFracTrain = OHETrainData.map(lambda lp: lp.label).mean()
    # logLossTrBase = OHETrainData.map(lambda lp: computeLogLoss(classOneFracTrain, lp.label)).mean()

    # print ('Baseline Train Logloss = {0:.3f}\n'.format(logLossTrBase))

    # trainingPredictions = OHETrainData.map(lambda x: getP(x.features, model0.weights, model0.intercept))

    # logLossTrLR0 = evaluateResults(model0, OHETrainData)

    # print ('OHE Features Train Logloss:\n\tBaseline = {0:.3f}\n\tLogReg = {1:.3f}'
    #    .format(logLossTrBase, logLossTrLR0))
    # 