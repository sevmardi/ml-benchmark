import org.apache.spark.ml.classification.LogisticRegression

val training = spark.read().format("libsvm").load("data")

val lr = new LogisticRegression()
	.setMaxIter(10)
	.setRegParam(0.3)
	.setElasticNetParam(0.8)

// fit the model 
val lrModel = lr.fit(training)
