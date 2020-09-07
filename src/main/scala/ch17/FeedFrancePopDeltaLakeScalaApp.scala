package ch17

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object FeedFrancePopDeltaLakeScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("Load France's population dataset and store it in Delta")
    // To use Databricks Delta Lake, we should add delta core packages to SparkSession
    //.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
    .master("local[*]")
    .getOrCreate

  // Reads a CSV file, called population_dept.csv, stores it in a
  // dataframe
  val df: Dataset[Row] = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .load("data/france_population_dept/population_dept.csv")

  val df2 = df
    .withColumn("Code département",
      when(col("Code département").equalTo("2A"), "20")
        .otherwise(col("Code département")))
    .withColumn("Code département",
      when(col("Code département").equalTo("2B"), "20")
        .otherwise(col("Code département")))
    .withColumn("Code département",
      col("Code département").cast(DataTypes.IntegerType))
    .withColumn("Population municipale",
      regexp_replace(col("Population municipale"), ",", ""))
    .withColumn("Population municipale",
      col("Population municipale").cast(DataTypes.IntegerType))
    .withColumn("Population totale",
      regexp_replace(col("Population totale"), ",", ""))
    .withColumn("Population totale",
      col("Population totale").cast(DataTypes.IntegerType))
    .drop("_c9")

  // problem with parquet write if containing " ,;{}()\n\t="
  val df3 = df2.columns.foldLeft(df2)((df, s) => df.withColumnRenamed(s, s.replace(" ", "")))

  df3.show(25)
  df3.printSchema()

  df3.write
    .format("delta")
    .mode("overwrite")
    .save("/tmp/delta_france_population")

  println(s"${df3.count} rows updated.")

  spark.stop
}
