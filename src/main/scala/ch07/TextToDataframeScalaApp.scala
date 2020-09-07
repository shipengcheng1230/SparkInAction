package ch07

import org.apache.spark.sql.SparkSession

object TextToDataframeScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("Text to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
  val df = spark.read
    .format("text")
    .load("data/romeo-juliet-pg1777.txt")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema

  spark.stop
}
