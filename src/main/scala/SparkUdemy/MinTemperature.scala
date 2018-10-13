package SparkUdemy

import org.apache.spark.{SparkConf, SparkContext}
import math.min

object MinTemperature extends App {

  val sparkConf = new SparkConf()
    .setAppName("FakeFriends")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/1800.csv")

  val result1 = file.map(x => x.split(","))
    .filter(x => x(2) == "TMIN")
    .map(x => (x(0), Integer.parseInt(x(3)) * 0.1 * (9.0f/5.0f) + 32))
    .reduceByKey((x,y) => min(x,y))
    .sortByKey()
    .collect()

  result1.foreach(x => println(x))

}
