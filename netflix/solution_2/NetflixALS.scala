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
    // call on movies title
    val movies = readAndParseMovieTitles(movieTitleFile)
    // call on prob set
    val probeSet = sc.parallelize(readAndParseProbeSet(probeFile))
    // call on ratings
    val ratings = loadNetflixRatings(datasetHomeDir, movies, sc, numRatings)

    val (training, validation) = getAllTrainingRatings(ratings, probeSet)
    // train(training, validation)

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
  }

    def getAllTrainingRatings(ratings: RDD([Long, Rating]), probeSet: RDD[Rating], partOfRating:Boolean=true): (RDD[Rating], RDD[Rating])  = {
    		// fitler out the rating in probe set

        val ((training, validation)) = if (parallelize){
          val joinRDD = ratings.values.map(x => (x.user, (x.product, x.rating)).groupByKey().leftOuterJoin(probeSet.map(x => (x.user, (x.product, x.rating))).groupByKey())
        }
    }

    /**
     * Get the training ratings
     * @type {[type]}
     */
    def getTrainingRatings(ratings: RDD[(Long, Rating)]):(RDD[Rating], RDD[Rating]) = {
    	val datasets = ratings.randomSplit(Array[Double](0.8, 0.2))
      val training = datasets(0).values.persist(StorageLevel.DISK_ONLY)
      val validation = datasets(1).values.persist(StorageLevel.DISK_ONLY)
      training.checkpoint()
      validation.checkpoint()
      (training, validation)
    }

    def loadNetflixRatings(dir: String, moviesMap: Map[Int, String], sc: SparkContext, numRatings: Option[Int]) = {
      var i = 0;
      var ratingRDD : RDD[(Long, Rating)] = null
      moviesMap.foreach{ (kv) => val movieId = kv._1

      if(numRatings == None || i < numRatings.get) {
        val ratings = sc.textFile(f"$dir/mv_$movieId%07d.txt").coalesce(sc.defaultParallelism).flatMap([Long, Rating]) {line => 

          val fields = line.split(",")
          if (fields.size == 3) {
            // format: (date, Rating(userid, movieId, rating))
            val timestamp = DateTime.parse(fields(2)).getMillis() / 1000
            Seq((timestamp % 10, Rating(fields(0).toInt, movieId, fields(1).toDouble)))
          }
          else
          {
            Seq()
          }
        }

        if(ratingRDD == null) {
          ratingRDD = ratings
        }
        else{
          ratingRDD = ratingRDD.union(ratings)
          ratingRDD.persist(StorageLevel.DISK_ONLY)
          ratingRDD.checkpoint()
        }
        i += 1
      }
    }
    ratingRDD.persist(StorageLevel.DISK_ONLY)
    ratingRDD.checkpoint()
    ratingRDD
  }
    def readFile(path: String): Seq[String] = {
      var lines = Seq[String]()
      try { 
        val fstream = new FileInputStream(path)
        val in = new DataInputStream(fstream)
        val br = new BufferedReader(new InputStreamReader(in))
        var line : String = null
        line = br.readLine()
        while(line != null){
          lines = lines.+:(line)
          line = br.readLine()
        }
        in.close()
      } catch {
        case e: Exception => println("Error: " = e.getMessage()) 
      }

      lines
    }

    def readAndParseProbeSet(path: String): Seq[Rating] = {
      var movieId:Int = -1
      readFile(path).flatMap[Rating, Seq[Rating]] { (line) => 
        if(line.indexOf(":") >= 0) {
          val fields = line.split(":")
          movieId = fields(0).toInt
          Seq()
        }
        else
        {
          Seq(Rating(line.toInt, movieId, 0.0d))
        }
      }
    }

    def readAndParseMovieTitles(path: String): Map[Int, String] = {
      readFile(path).map { (line) => val fields = line.split(", ") (fields(0).toInt, fields(2))}.toMap
    }

   /** Compute RMSE (Root Mean Squared Error). */
   def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {

      val predictions:RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
      val predictionAndRatings = predictions.map(x => ((x.user, x.product), x.rating)).join(data.map(x => ((x.user, x.product), x.rating)))
      .values

      math.sqrt(predictionAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
   }

    /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[Int, String]) = {
     val prompt = "Please rate hte following movie (1-5) (bes), or 0 if not seet"
     println(prompt)
     val ratings = movies.flatMap{x => var rating: Option[Rating]= None var valid = false
      while(!valid) {
        print(x._2 + ": ")
        try { 
          val r = Console.readInt
          if (r < 0 || r > 5)
          {
            println(prompt)
          } else{
            valid = true
            if (r > 0){
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
     rating match { 
       case Some(r) => Iterator(r)
       case None => Iterator.empty
     }
   }
   if (ratings.isEmpty)
   {
    error("No rating provided")
   } else {
      ratings
   }
  }
}