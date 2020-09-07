package ch09

import org.apache.spark.sql.SparkSession

object PhotoMetadataIngestionScalaApp extends App {
  val spark = SparkSession.builder
    .appName("EXIF to Dataset")
    .master("local")
    .getOrCreate

  // Import directory
  val importDirectory = "data"

  // read the data
  val df = spark.read
    // need additional utils:
    // https://github.com/jgperrin/net.jgp.books.spark.ch09/tree/master/src/main/scala/net/jgp/books/spark/ch09/x
    // lots of stuff to fix in that
    .format("exif")
    .option("recursive", "true")
    .option("limit", "100000")
    .option("extensions", "jpg,jpeg")
    .load(importDirectory)

  println("I have imported " + df.count + " photos.")
  df.printSchema()
  df.show(5)
}
