import org.apache.spark.{ SparkConf, SparkContext }

object transformRawToLibSvm {
  // Properties of the Criteo Data Set
  val NUM_LABELS = 1
  val NUM_INTEGER_FEATURES = 13
  val NUM_CATEGORICAL_FEATURES = 26
  val NUM_FEATURES = NUM_LABELS + NUM_INTEGER_FEATURES + NUM_CATEGORICAL_FEATURES

  def main(args: Array[String]) = {
    val options = args.map { arg =>
      arg.dropwhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
  }.toMap

  val inputPath = options.get("inputPath").get
  val outputPath = options.get("outputPath").get
  val percDataPoints = options.get("percDataPoints").get.toDouble

  val conf = new SparkConf().setAppName("transform raw to libsvm")

   // extract numerical and caterorical features from text
  val data = sc.textFile(inputPath)

  val input = if (percDataPoints != 1.0) {
    data.sample(false, percDataPoints, 42)
  } else {
    data
  }

  val outRDD = input.map {
    val features = line.split("\t", -1)

    val label = features.take(NUM_LABELS).head.toInt
    val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
      .map(string => if (string.isEmpty) 0 else string.toInt)
    val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)

    for (i <- categorialFeatures.indices) {
      categorialFeatures(i) = (i + integerFeatures.size + 1) + ":" + categorialFeatures(i)
    }

    (label, integerFeatures, categorialFeatures)
  }

  val transformedFeatures = outRDD.map(x => {

    val label = x._1
    val intFeatures = x._2
    val catFeatures = x._3

    val intStrings = for ((col, value) <- 1 to intFeatures.size zip intFeatures) yield s"$col:$value"
    val catStrings = for (elem <- catFeatures) yield s"$elem"

    val outLine = label + " " + intStrings.mkString(" ") + " " + catStrings.mkString(" ")

    outLine
  })

  transformedFeatures.saveAsTextFile(outputPath)

  sc.stop()

}