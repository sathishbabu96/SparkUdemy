package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(ID: Int, name: String, age: Int, numOfFriends: Int)

  def mapper(line: String): Person = {
    val words = line.split(",")
    val person = Person(words(0).toInt, words(1), words(2).toInt, words(3).toInt)
    person
  }

  val sparkSession = SparkSession
    .builder()
    .appName("SparkSQL")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/fakefriends.csv")
  val people = file.map(mapper)

  import sparkSession.implicits._

  val schemaPeople = people.toDS()
  schemaPeople.printSchema()

  println("People with age greater than 16")
  schemaPeople.filter($"age" > 16)

  println("People grouped by their age")
  schemaPeople.groupBy("age").count().show()


  sparkSession.stop()
}