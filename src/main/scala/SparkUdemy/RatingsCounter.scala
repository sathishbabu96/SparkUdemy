package SparkUdemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("RatingsCounter")
      .setMaster("local[*]")
      .set("spark.driver.bindAddress","127.0.0.1")

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext(sparkConf)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/Users/sathish-6764/Downloads/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => (x.split("\t")(2), 1))

    // Count up how many times each value (rating) occurs
    val results = ratings.reduceByKey(_+_)

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.sortBy(_._2)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}

