package ch01

import org.apache.spark.sql.SparkSession

object CsvToDataframeScalaApp extends App {

  val spark = SparkSession
    .builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark
    .read
    .format("csv")
    .option("header", "true")
    .load("data/books.csv")

  df.show(20)

  spark.stop
}
