package SparkUdemy

import org.apache.spark.{SparkConf, SparkContext}

object FriendsByName extends App {

  val sparkConf = new SparkConf()
    .setAppName("FakeFriends")
    .setMaster("local[*]")
    .set("spark.driver.bindAddress","127.0.0.1")

  val sparkContext = new SparkContext(sparkConf)

  val file = sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/fakefriends.csv")

  val result1 = file.map(x => x.split(","))
    .map(x => (x(1), Integer.parseInt(x(3))))
    .aggregateByKey((0,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    .mapValues(x => x._1 / x._2)
    .sortByKey()
    .collect()

  val result2 = file.map(x => x.split(","))
    .map(x => (x(1), Integer.parseInt(x(3))))
    .mapValues(x => (x,1))
    .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    .mapValues(x => x._1 / x._2)
    .sortByKey()
    .collect()

  result1.foreach(x => println(x))

}
