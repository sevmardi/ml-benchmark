
object criteoToLibsvm {
	def rowToLibsvm(row:  org.apache.spark.sql.Row): String = {
		0 unitl row.length flatMap {
			caes 0 => Some(row(0).toString)
			case i if row(i) == null => None
			case i => Some(i.toString + ':' + (if (i < 14) row(i) else java.lang.Long.parseLong(row(i).toString, 16)).toString)

		}  mkString " "
	}

	def readDataFrame(path: String): org.apache.spark.sql.DataFrame = {
		spark.read.option("header", "false").option("inferSchema", "true").option("delimiter", "\t").csv(path)
	}

	def writeDataFrame(df: org.apache.spark.sql.DataFrame, path: String): Unit = {
		df.rdd.map(rowToLibsvm).saveAsTextFile(path)
	}

	def processDay(day: Int): Unit = {
		println(s"Processing of the day $day started")
		// val inputPath = s"/local/1TB/day_$day"
		val inputPath = s"/data/vw/1TB/day_$day"
		println(s"Loading data from $inputPath")
		val df = readDataFrame(inputPath)

		val outputPath = s"data/vw/1TB/libsvm/day_$day.libsvm"
		// val outputPath = s"local/1TB/libsvm/day_$day.libsvm"

		println(s"Saving data to $outputPath")
		writeDataFrame(df, outputPath)
		println(s"Processing of the day $day finished")

	}
}





// println("Do '0 to 23 foreach processDay' to convert all data to LibSVM format.")