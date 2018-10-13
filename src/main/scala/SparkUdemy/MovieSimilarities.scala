package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MovieSimilarities extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Movie Similarities")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val namesFile = sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.item")
  val nameDict = namesFile.map(x => x.split("|"))
    .map(x => (x(0).toInt, x(1)))
    .collect

  val data = sparkContext.textFile("/Users/sathish-6764/Downloads/ml-100k/u.data")
//  val ratings = data.map(x => x.split("\t"))
//    .map(x => (x(0).toInt, (x(1).toInt, x(2).toDouble)))
  data.take(20).foreach(x => println(x))

}
