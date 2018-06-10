
object transformLabelsToVW {
  // Properties of the Criteo Data Set
  val NUM_LABELS = 1
  val NUM_INTEGER_FEATURES = 13
  val NUM_CATEGORICAL_FEATURES = 26
  val NUM_FEATURES = NUM_LABELS + NUM_INTEGER_FEATURES + NUM_CATEGORICAL_FEATURES

  def main(args: Array[String]) = {
  	val options = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val inputPath = options.get("inputPath").get
    val outputPath = options.get("outputPath").get
    val percDataPoints = options.get("percDataPoints").get.toDouble

    val conf =  new SparkConf().setAppName("transform raw criteo to VW")

    val sc = new SparkContext(conf)

    // extract numerical and caterorical features from text
    val data = sc.textFile(inputPath)

    val input = if(percDataPoints != 1.0){
    	data.sample(false, percDataPoints, 42)
    } else
    {
    	
    }

  }

}