package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AmountByCustomer extends App {

  case class Customer(ID: Int, Amount: Double)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession
    .builder()
    .appName("FakeFriends")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/customer-orders.csv")

  val result = file.map(x => x.split(","))
    .map(x => (x(0).toInt, x(2).toFloat))
    .reduceByKey(_+_)
    .sortBy(x => x._2)
    .collect()

  println(result.minBy(x => x._2))
  println(result.maxBy(x => x._2))

  /*
  * Using DataSet
  */

  val data = file.map(x => x.split(","))
      .map(x => Customer(x(0).toInt, x(2).toDouble))

  import sparkSession.implicits._

  val friends = data.toDS()
  friends.printSchema()

  friends.groupBy($"ID")
    .sum("Amount")
    .orderBy($"sum(Amount)".desc)
      .show(1)

  friends.groupBy($"ID")
    .sum("Amount")
    .orderBy($"sum(Amount)".asc)
    .show(1)

  sparkSession.stop()

}