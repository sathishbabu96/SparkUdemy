package SparkUdemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import scala.math.{sqrt, pow}

object MLlib extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession
    .builder()
    .appName("SparkMLlib")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  val file = sparkSession.sparkContext.textFile("/Users/sathish-6764/Downloads/SparkScala/regression.txt")

  val data = file.map(x => x.split(","))
    .map(x => (x(1).toDouble, Vectors.dense(x(0).toDouble)))

  val columns = Seq("label", "features")

  import sparkSession.implicits._

  val trainData = data.toDF(columns: _*).cache()
  val testData = data.toDF(columns: _*)

  val algorithm = new LinearRegression()
      .setMaxIter(200)
      .setRegParam(0.001)

  val model = algorithm.fit(trainData)

  val prediction = model.transform(testData)

  val predictionModel = prediction.select("prediction", "label")
    .rdd.map(x => (x.getDouble(0), x.getDouble(1)))

  val n = predictionModel.count()

  val rmse = predictionModel.map(x => sqrt(pow(x._1 - x._2,2)/n))

  rmse.foreach(x => println(x))

  sparkSession.stop()

}
