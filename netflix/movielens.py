from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('movielnes').getOrCreate()
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

data = spark.read.csv('data/ml-1m/movielens_ratings.csv',inferSchema=True,header=True)

# print(data.head()
# print(data.describe().show())


# Smaller dataset so we will use 0.8 / 0.2
(training, test) = data.randomSplit([0.8, 0.2])

# Building the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(training)

# Evaluating the model by computing the RMSE on the test data
predictions = model.transform(test)

# print(predictions.show())

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

single_user = test.filter(test['userId']==11).select(['movieId','userId'])
# single_user.show()
reccomendations = model.transform(single_user)

print(reccomendations.orderBy('prediction',ascending=False).show())

