package ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object JsonLinesToDataframeScalaApp extends App {
  val spark = SparkSession.builder
    .appName("JSON Lines to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("json")
    .load("data/durham-nc-foreclosure-2006-2016.json")


  // Shows at most 5 rows from the dataframe
  df.show(5) // , 13)

  df.printSchema

  df.withColumn("year", col("fields.year"))
    .withColumn("coordinates", col("geometry.coordinates"))
    .show(5)

  spark.stop
}
