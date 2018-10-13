package SparkUdemy

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.max

object MaxTemperature extends App {

  val sparkConf = new SparkConf()
    .setAppName("FakeFriends")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/1800.csv")

  val result1 = file.map(x => x.split(","))
    .filter(x => x(2) == "TMAX")
    .map(x => (x(0), Integer.parseInt(x(3)) * 0.1f * (9.0f/5.0f) + 32))
    .reduceByKey((x,y) => max(x,y))
    .sortByKey()
    .collect()

  val result2 = file.map(x => x.split(","))
    .filter(x => x(2) == "PRCP")
      .map(x => (x(1), Integer.parseInt(x(3))))
      .reduce((x,y) => if(x._2 > y._2) x else y)

  println(result2)

}