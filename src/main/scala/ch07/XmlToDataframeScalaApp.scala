package ch07

import org.apache.spark.sql.SparkSession

object XmlToDataframeScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("XML to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "row")
    .load("data/nasa-patents.xml")

  // Shows at most 5 rows from the dataframe
  df.show(5)
  df.printSchema

  spark.stop
}
