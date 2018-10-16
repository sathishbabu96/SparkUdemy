package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MostPopularMovieUsingDataset extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class MoviesByRating(ID: Int, rating: Int)
  case class MoviesByCount(ID: Int)

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("MostPopularMovie")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file1 = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.data")
  val file2 = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.item")

  val movieNames = file2.map(x => x.split('|'))
    .map(x => (x(0).toInt, x(1)))
    .collectAsMap()

  val data1 = file1.map(x => x.split("\t"))
    .map(x => MoviesByRating(x(1).toInt, x(2).toInt))

  val data2 = file1.map(x => x.split("\t"))
    .map(x => MoviesByCount(x(1).toInt))

  import sparkSession.implicits._
  val movies = data1.toDS()

  val top10ByRating = movies.groupBy("ID")
    .mean("rating")
    .orderBy($"avg(rating)".desc)
      .take(10)

  val top10ByCount = movies.groupBy("ID")
    .count()
    .orderBy($"count".desc)
      .take(10)

  println("\nTop 10 Movies By Avg Rating: \n")
  top10ByRating.foreach(x =>
    println(movieNames(x(0).asInstanceOf[Int]) + " has a rating of " + x(1)))

  println("\n Top 10 Movies By Count: \n")
  top10ByCount.foreach(x =>
    println(movieNames(x(0).asInstanceOf[Int]) + " has a count of " + x(1)))

  sparkSession.stop()
}
