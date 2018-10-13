package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {

  val startCharacterId = 5306
  val targetCharacterId = 14

  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)

  var hitCounter: Option[LongAccumulator] = None

  def convertToBFS(line: String): BFSNode = {
    val words = line.split("\\s+")
    val heroID = line(0).toInt

    var connections:ArrayBuffer[Int] = ArrayBuffer()
    for(connection <- 1 until words.length) {
      connections += words(connection).toInt
    }

    var distance = 9999
    var color = "WHITE"

    if(heroID == startCharacterId) {
      distance = 0
      color = "GRAY"
    }

    (heroID, (connections.toArray, distance, color))
  }

  def createStartingRdd(sc: SparkContext): RDD[BFSNode] = {
    val file = sc.textFile("/Users/sathish-6764/Downloads/SparkScala/marvel-graph.txt")
    file.map(convertToBFS)
  }

  def BFSMap(node: BFSNode): Array[BFSNode] = {

    val characterID:Int = node._1
    val data:BFSData = node._2

    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3

    var results:ArrayBuffer[BFSNode] = ArrayBuffer()

    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"

        if (targetCharacterId == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      color = "BLACK"
    }

    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    results.toArray
  }

  def BFSReduce(data1: BFSData, data2: BFSData): BFSData = {

    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3

    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()

    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }

    (edges.toArray, distance, color)
  }

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setAppName("DegreesOfSeparation")
      .setMaster("local[*]")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(sparkConf)

    hitCounter = Some(sc.longAccumulator("HitCounter"))

    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      val mapped = iterationRdd.flatMap(BFSMap)

      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }

      iterationRdd = mapped.reduceByKey(BFSReduce)
    }
  }

}
