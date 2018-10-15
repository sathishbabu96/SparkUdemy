package SparkUdemy

import org.apache.spark.{SparkConf, SparkContext}

object AmountByCustomer extends App {

  val sparkConf = new SparkConf()
    .setAppName("FakeFriends")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/customer-orders.csv")

  val result = file.map(x => x.split(","))
    .map(x => (x(0), x(2).toFloat))
    .reduceByKey(_+_)
    .sortBy(x => x._2)
    .collect()

  println(result.minBy(x => x._2))
  println(result.maxBy(x => x._2))
}
