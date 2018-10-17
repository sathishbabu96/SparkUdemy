package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FriendsByAge extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Friends(Age: Int, NumOfFriends: Int)

  val sparkSession = SparkSession
    .builder()
    .appName("FakeFriends")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/fakefriends.csv")

  val data = file.map(x => x.split(","))
    .map(x => Friends(x(2).toInt, x(3).toInt))

  val result1 = file.map(x => x.split(","))
    .map(x => (x(2).toInt, x(3).toInt))
    .aggregateByKey((0,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc2._2 + acc2._2))
    .mapValues(x => x._1 / x._2)
      .sortByKey()
      .collect()

//  val result2 = file.map(x => x.split(","))
//      .map(x => (Integer.parseInt(x(2)), Integer.parseInt(x(3))))
//      .mapValues(x => (x,1))
//      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
//      .mapValues(x => x._1/x._2)
//      .sortByKey()
//      .collect()
//
//  result1.foreach(x => println(x))

  /*
   Using DataSet
   */

  import sparkSession.implicits._

  val friends = data.toDS()

  friends.groupBy("Age")
    .avg("NumOfFriends")
    .select($"Age", $"avg(NumOfFriends)".as("Average Friends"))
      .orderBy("Age")
      .show(55)

  sparkSession.stop()

}
