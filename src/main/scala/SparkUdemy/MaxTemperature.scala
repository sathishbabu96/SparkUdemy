package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max


object MaxTemperature extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession
    .builder()
    .appName("FakeFriends")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/1800.csv")

//  val result1 = file.map(x => x.split(","))
//    .filter(x => x(2) == "TMAX")
//    .map(x => (x(0), Integer.parseInt(x(3)) * 0.1f * (9.0f/5.0f) + 32))
//    .reduceByKey((x,y) => max(x,y))
//    .sortByKey()
//    .collect()
//
//  val result2 = file.map(x => x.split(","))
//    .filter(x => x(2) == "PRCP")
//      .map(x => (x(1), Integer.parseInt(x(3))))
//      .reduce((x,y) => if(x._2 > y._2) x else y)
//
//  println(result2)

  case class Temperature(place: String, value: Double)

  val data = file.map(x => x.split(","))
    .filter(x => x(2) == "TMAX")
    .map(x => Temperature(x(0), x(3).toInt * 0.1f * (9.0f/5.0f) + 32))

  import sparkSession.implicits._
  val temperature = data.toDS()

  temperature.groupBy("Place")
      .max("value")
      .orderBy("Place")
      .show()

  val data2 = file.map(x => x.split(","))
      .filter(x => x(2) == "PRCP")
      .map(x => Temperature(x(1), x(3).toInt))

  val temperature2 = data2.toDS()

  val maxTemp = temperature2.select(max("value")).as[Double].collect().head

  temperature2.select("*").filter($"value" === maxTemp).show()

  temperature2.createOrReplaceTempView("Temperature")

  sparkSession.sql("select place, MAX(value from Temperature").show()

  sparkSession.stop()

}