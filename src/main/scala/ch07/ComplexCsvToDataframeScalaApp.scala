package ch07

import org.apache.spark.sql.SparkSession

object ComplexCsvToDataframeScalaApp extends App {

  val spark = SparkSession.builder
    .appName("Complex CSV to Dataframe")
    .master("local[*]")
    .getOrCreate

  println("Using Apache Spark v" + spark.version)

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("csv")
    .option("header", "true")
    .option("multiline", value = true)
    .option("sep", ";")
    .option("quote", "*")
    .option("dateFormat", "MM/dd/yyyy")
    .option("inferSchema", value = true)
    .load("data/books.csv")

  println("Excerpt of the dataframe content:")

  // Shows at most 7 rows from the dataframe, with columns as wide as 90
  // characters
  df.show(7, 70)
  println("Dataframe's schema:")
  df.printSchema()

  spark.stop
}
