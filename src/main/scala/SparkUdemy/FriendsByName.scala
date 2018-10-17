package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FriendsByName extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Friends(Name: String, NoOfFriends: Int)

  val sparkSession = SparkSession
    .builder()
    .appName("FakeFriends")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/fakefriends.csv")

//  val result1 = file.map(x => x.split(","))
//    .map(x => (x(1), x(3).toInt))
//    .aggregateByKey((0,0))(
//      (acc, value) => (acc._1 + value, acc._2 + 1),
//      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//    .mapValues(x => x._1 / x._2)
//    .sortByKey()
//    .collect()
//
//  val result2 = file.map(x => x.split(","))
//    .map(x => (x(1), x(3).toInt))
//    .mapValues(x => (x,1))
//    .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
//    .mapValues(x => x._1 / x._2)
//    .sortByKey()
//    .collect()
//
//  result1.foreach(x => println(x))

  import sparkSession.implicits._

  val data = file.map(x => x.split(","))
      .map(x => Friends(x(1), x(3).toInt))

  val friends = data.toDS()

  friends.groupBy("Name")
      .avg("NoOfFriends")
      .orderBy("Name")
      .show()

  sparkSession.stop()
}
