package ch03

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

object ArrayToDatasetToDataframeScalaApp extends App {

  val spark = SparkSession.builder.appName("Array to dataframe")
    .master("local").getOrCreate

  spark.sparkContext.setLogLevel("error")

  val stringList = Array[String]("Jean", "Liz", "Pierre", "Lauric")

  val data: List[String] = stringList.toList
  /**
   * data:    parameter list1, data to create a dataset
   * encoder: parameter list2, implicit encoder
   */
  // Array to Dataset
  val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)
  ds.show()
  ds.printSchema()

  // Dataset to Dataframe
  val df = ds.toDF
  df.show()
  df.printSchema()

}
