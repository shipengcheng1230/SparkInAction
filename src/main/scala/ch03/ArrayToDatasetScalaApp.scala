package ch03

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object ArrayToDatasetScalaApp extends App {

  val spark: SparkSession = SparkSession.builder.appName("Array to Dataset<String>")
    .master("local").getOrCreate

  spark.sparkContext.setLogLevel("error")

  val stringList: Array[String] = Array("Jean", "Liz", "Pierre", "Lauric")
  val data: List[String] = stringList.toList

  val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)

  ds.show()
  ds.printSchema()

}
