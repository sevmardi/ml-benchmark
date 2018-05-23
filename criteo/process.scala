import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import scala.compat.Platform._

object process {

  def main(args: Array[String]) = {
    val nnodes = 1
    val epochs = 3

    val conf = new SparkConf().setAppName("Test Name")
    val sc = new SparkContext(conf)

    val t0 = currentTime

    // Load training data in LIBSVM format.
    //val train = MLUtils.loadLibSVMFile(sc, "s3n://bidmach/RCV1train6.libsvm", true, 276544, nnodes*4)
    //val test = MLUtils.loadLibSVMFile(sc, "s3n://bidmach/RCV1test6.libsvm", true, 276544, nnodes*4)

    val train = MLUtils.loadLibSVMFile(sc, "/ebs2/preprocess/xaa", 262165, 1)
    val test = MLUtils.loadLibSVMFile(sc, "/ebs2/preprocess/xab", 262165, 1)
    train.coalesce(1)

    //262146
    //val train = MLUtils.loadLibSVMFile(sc, "/ebs2/preprocess/num_train.svm",  14, 1)
    //val test = MLUtils.loadLibSVMFile(sc, "/ebs2/preprocess/num_test.svm",   14, 1)
    //val train = MLUtils.loadLibSVMFile(sc, "/ebs2/spark-toy-benchmark-master/bin/toy_libsvm.svm", 2, 1)
    //val test = MLUtils.loadLibSVMFile(sc, "/ebs2/spark-toy-benchmark-master/bin/toy_libsvm.svm",  2, 1)

    val t1 = currentTime
    println("START")
    val lrAlg = new LogisticRegressionWithSGD()
    lrAlg.optimizer.setMiniBatchFraction(10.0 / 40000000.0)
    lrAlg.optimizer.setNumIterations(12000000)
    lrAlg.optimizer.setStepSize(0.01)

    val model = lrAlg.run(train)

    model.clearThreshold()

    val scoreAndLabels = test.map { point => val score = mode.predict(point.features)(score, point.label) }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // On an 8-node (r3.2xlarge) cluster, 4 tasks/node
    // 3 iterations in 200 seconds, weighted AUC 0.85, cat6 0.67
    // 10 iterations in 767 seconds, weighted AUC 0.904, cat6 0.76
    // 30 iterations in 2000 seconds, weighted AUC 0.92, cat6 0.774a

  }

}

