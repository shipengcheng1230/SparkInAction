package ch07

import org.apache.spark.sql.SparkSession

object OrcToDataframeScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("ORC to Dataframe")
    .config("spark.sql.orc.impl", "native")
    .master("local[*]")
    .getOrCreate

  // Reads an ORC file, stores it in a dataframe
  val df = spark.read
    .format("orc")
    .load("data/demo-11-zlib.orc")

  // Shows at most 10 rows from the dataframe
  df.show(10)
  df.printSchema()

  println(s"The dataframe has ${df.count} rows.")

  spark.stop
}
