from pyspark.sql import SparkSession


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS



spark = SparkSession.builder.appName('rec').getOrCreate()

data = spark.read.csv('movielens_ratings.csv',inferSchema=True,header=True)

data.head()

data.describe().show()



# Smaller dataset so we will use 0.8 / 0.2
(training, test) = data.randomSplit([0.8, 0.2])


# Building the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(training)


# Evaluating the model by computing the RMSE on the test data
predictions = model.transform(test)



predictions.show()


evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

single_user = test.filter(test['userId']==11).select(['movieId','userId'])

single_user.show()

reccomendations = model.transform(single_user)


reccomendations.orderBy('prediction',ascending=False).show()





