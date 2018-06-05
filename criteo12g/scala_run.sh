date
spark-submit --conf spark.ui.port=4041 --master local[1] target/scala-2.11/criteo_scala_spark_project_2.11-0.1.jar |& tee logger.txt
date
