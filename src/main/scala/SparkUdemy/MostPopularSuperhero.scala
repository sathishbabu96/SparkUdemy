package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MostPopularSuperhero extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("MostPopularSuperherp")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file1 = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/Marvel-graph.txt")
  val file2 = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/Marvel-names.txt")

  val names = file2.map(x => x.split(" \""))
    .map(x => (x(0).toInt, x(1).replace("\"","")))
    .collect()

  val namesBroadcast = sparkContext.broadcast(names)

  val popularity = file1.map(x => x.split(" "))
    .map(x => (x(0).toInt, x.length-1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false)
    .take(15)

  val popularHero = popularity.map(x => (namesBroadcast.value(x._1)._2, x._2))

  popularHero.foreach(x => println(x._1 + ": " + x._2))
//  println(popularHero + ": " + popularity._2)
}
