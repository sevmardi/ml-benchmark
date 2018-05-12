# netflix

## Build

    sbt/sbt assembly

## Usage

    bin/spark-submit --class NetflixALS --master yarn-client --executor-memory 4G --num-executors 4 <built jar filepath> <datasetHomeDir> <path to movie_title.txt> <path to probe.txt> <number of ratings>
 
For example:

    bin/spark-submit --class NetflixALS --master yarn-client --executor-memory 4G --num-executors 4 ../netflix_als/target/scala-2.10/netflix-als-assembly-0.1.jar training_set ../netflix_als/dataset/movie_titles.txt ../netflix_als/dataset/probe.txt 10