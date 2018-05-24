date
spark-submit --conf spark.ui.port=4041 --master local[1] target/scala-2.11/simple-project_2.11-1.0.jar |& tee logger.txt
date
