package ch04

import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.SparkSession

object TransformationExplainScalaApp extends App {

  val spark = SparkSession.builder.appName("Showing execution plan")
    .master("local[*]").getOrCreate

  // Step 2 - Reads a CSV file with header, stores it in a dataframe
  var df = spark.read.format("csv")
    .option("header", "true")
    .load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv")

  val df0 = df

  // Step 3 - Build a bigger dataset
  df = df.union(df0)

  // Step 4 - Cleanup. preparation
  df = df.withColumnRenamed("Lower Confidence Limit", "lcl")
  df = df.withColumnRenamed("Upper Confidence Limit", "ucl")

  // Step 5 - Transformation
  df = df.withColumn("avg", expr("(lcl+ucl)/2"))
    .withColumn("lcl2", col("lcl"))
    .withColumn("ucl2", col("ucl"))

  // Step 6 - explain
  df.explain(true)
}
