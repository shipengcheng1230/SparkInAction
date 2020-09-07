package ch07

import org.apache.spark.sql.SparkSession

object MultilineJsonToDataframeWithCorruptRecordScalaApp extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("Multiline JSON to Dataframe, without multiline option")
    .master("local[*]")
    .getOrCreate

  // Reads a JSON, called countrytravelinfo.json, stores it in a
  // dataframe,
  // without specifying the multiline option
  val df = spark.read
    .format("json")
    .option("multiline", value = true) // without it will have corrupted col
    .load("data/countrytravelinfo.json")
    .cache()

  // Shows at most 3 rows from the dataframe
  // not applicable since spark 2.3
  //df.filter(df.col("_corrupt_record").isNotNull).count()

  df.show(5)
  df.printSchema

  spark.stop

}
