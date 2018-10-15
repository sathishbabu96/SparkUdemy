package SparkUdemy

import scala.math.sqrt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.charset.CodingErrorAction

import scala.io.{Codec, Source}

object MovieSimilarities extends App {

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def filterDuplicates(userRatingPair: UserRatingPair): Boolean = {
    val movie1 = userRatingPair._2._1
    val movie2 = userRatingPair._2._2

    val movieRating1 = movie1._2
    val movieRating2 = movie2._2

    movieRating1 < movieRating2
  }

  def makePairs(userRatingPair: UserRatingPair) = {
    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {

    var numPairs = 0
    var sum_xx = 0.0
    var sum_yy = 0.0
    var sum_xy = 0.0
    var sum_x = 0.0
    var sum_y = 0.0

    for(pair <- ratingPairs) {

      val ratingX = pair._1
      val ratingY = pair._2

      sum_x += ratingX
      sum_y += ratingY
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val X = (numPairs * sum_xy) - (sum_x * sum_y)
    val Y = sqrt( ((numPairs * sum_xx) - (sum_x*sum_x)) - ((numPairs * sum_yy) - (sum_y*sum_y)))

    var score = 0.0
    if(Y != 0) {
      score = X / Y
    }

    (score, numPairs)
  }

  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("/Users/sathish-6764/Downloads/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Movie Similarities")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  // (movieID, movieName)
  val nameDict = loadMovieNames()

  val data = sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.data")

  // (userId, (movieId, rating))
  val ratings = data.map(x => x.split("\t"))
    .map(x => (x(0).toInt, (x(1).toInt, x(2).toDouble)))

  // self join (userId, ((movieId1, rating1), (movieId2, rating2)))
  val joinedRatings = ratings.join(ratings)

  // filter the movies in one direction only i.e., movieId1 < movieId2
  val uniqueJoinedRatings = joinedRatings.filter(x => x._2._1._1 < x._2._2._1)

  // (userId, ((movieId1, rating1), (movieId2, rating)2)) ===> ((movieId1, movieId2), (rating1, rating2))
  val moviePair = uniqueJoinedRatings.map(makePairs)

  // ((movieId1, movieId2), ((rating1, rating2), (rating1, rating2), ...)
  val moviePairRatings = moviePair.groupByKey()

  // We are calculating some metrics for a movie pair
  // based on all the ratings given by the users for that particular pair.
  val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

  println("Enter movie ID: ")
  val movieID = scala.io.StdIn.readInt()

  val scoreThreshold = 0.90
  val coOccurrenceThreshold = 50.0

  val filteredResults = moviePairSimilarities.filter(x => {
    val moviePair = x._1
    val sim = x._2
    (moviePair._1 == movieID || moviePair._2 == movieID) && sim._1 > scoreThreshold
  })

  val results = filteredResults.map( x => (x._2, x._1)).sortByKey(ascending = false).take(10)

  println("\nTop 10 similar movies for " + nameDict(movieID))
  for (result <- results) {
    val sim = result._1
    val pair = result._2
    // Display the similarity result that isn't the movie we're looking at
    var similarMovieID = pair._1
    if (similarMovieID == movieID) {
      similarMovieID = pair._2
    }

    println(similarMovieID + " " + nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
  }


}
