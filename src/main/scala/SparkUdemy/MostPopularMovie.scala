package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MostPopularMovie extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("MostPopularMovie")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file1 = sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.item")
  val file2 = sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.data")

//  file1.foreach(x => println(x))
//  file2.take(5).foreach(x => println(x))

  val summa2 = file1.map(x => x.split("\t"))
    .filter(x => x.length > 1)
    .map(x => (x(0).toInt, x(1)))
    .collect()

  val nameDict = sparkContext.broadcast(summa2)

  val mostPopularMovieID = file2.map(x => x.split("\t"))
    .map(x => (x(1).toInt, 1))
    .reduceByKey(_+_)
    .sortBy(_._2)


  val movieNames = mostPopularMovieID.map(x => (nameDict.value(x._1-1)._2, x._2)).collect()

  movieNames.foreach(x => println(x))

}
