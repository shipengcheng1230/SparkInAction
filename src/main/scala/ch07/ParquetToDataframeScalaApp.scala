package ch07

import org.apache.spark.sql.SparkSession

object ParquetToDataframeScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("Parquet to Dataframe")
    .master("local[*]")
    .getOrCreate

  // Reads a Parquet file, stores it in a dataframe
  val df = spark.read
    .format("parquet")
    .load("data/alltypes_plain.parquet")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema()
  println(s"The dataframe has ${df.count} rows.")

  spark.stop
}
