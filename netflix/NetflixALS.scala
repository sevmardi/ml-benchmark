import java.util.Random
import java.io.{ FileInputStream, DataInputStream, BufferedReader, InputStreamReader }
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.collection.mutable.Seq
import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.recommendation.{ ALS, Rating, MatrixFactorizationModel }

// https://github.com/viirya/netflix_als/blob/master/NetflixALS.scala

object NetflixALS {
  def main(arg: Array[String]) = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length < 2) {
      println("Usage: NetflixALS \"datasetHomeDir\" \"path to movie_title.txt\" \"path to probe.txt\" \"<number of ratings>\"")
      exit(1)
    }

    //setup env

    val conf = new SparkConf().setAppName("NetflixALS")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("rdd_checkpoint")

    val datasetHomeDir = args(0)
    val movieTitleFile = args(1)
    val probeFile = args(2)

    val numRatings = if (args.length == 4) Some(args(3).toInt) else None

    val movies = readAndParseMovieTitles(movieTitleFile)
    val probeSet = sc.parallelize(readAndParseProbeSet(probeFile))
    val ratings = loadNetflixRatings(datasetHomeDir, movies, sc, numRatings)
    val (training, validation) = getAllTrainingRatings(ratings, probeSet)
    train(training, validation)

    sc.stop();
  }

  def train(training: RDD[Ratings], validation: RDD[Rating]) = {
    val numTraining = training.count
    val numValidation = validation.count

    println("Training: " + numTraining + ", validation: " + numValidation)

    // train models and evaluate them on the validation set

    val ranks = List(50, 80, 100)
    val lambdas = List(0.01, 0.02, 0.03, 0.05, 0.07, 1, 1.5, 2, 2.5)
    val numIters = List(20, 50, 100)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    def getAllTrainingRatings(ratings: RDD([Long, Rating]), probeSet: RDD[Rating], partOfRating:Boolean=true): (RDD[Rating], RDD[Rating])  = {
    		
    }

    def getTrainingRatings(ratings: RDD[(Long, Rating)]):(RDD[Rating], RDD[Rating]) = {
    	
    }

    def loadNetflixRatings(dir: String, moviesMap: Map[Int, String], sc: SparkContext, numRatings: Option[Int]) = {

    }

    def readFile(path: String): Seq[String] = {

    }

    def readAndParseProbeSet(path: String): Seq[Rating] = {

    }

    def readAndParseMovieTitles(path: String): Map[Int, String] = {

    }

     /** Compute RMSE (Root Mean Squared Error). */

     def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {


     }

     def elicitateRatings(movies: Seq[Int, String]) = {
     	
     }

     
  }
}