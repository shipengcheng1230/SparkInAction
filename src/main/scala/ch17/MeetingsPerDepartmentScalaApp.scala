package ch17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MeetingsPerDepartmentScalaApp extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("Counting the number of meetings per department")
    // To use Databricks Delta Lake, we should add delta core packages to SparkSession
    //.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
    .format("delta")
    .load("/tmp/delta_grand_debat_events")

  val df2 = df
    .groupBy(col("authorDept"))
    .agg(count(lit(1)).alias("COUNT"))
    .orderBy(col("COUNT").desc_nulls_first)

  df2.show(5)
  df2.printSchema()
  spark.stop()

}
