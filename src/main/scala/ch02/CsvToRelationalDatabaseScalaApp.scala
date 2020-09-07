package ch02

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, col}

object CsvToRelationalDatabaseScalaApp extends App {

  val spark = SparkSession
    .builder
    .appName("CSV to DB")
    .master("local[*]")
    .getOrCreate

  var df = spark
    .read
    .format("csv")
    .option("header", "true")
    .load("data/authors.csv")

  df = df.withColumn(
    "name",
    concat(col("lname"), lit(", "), col("fname")))

  df.show()

//  val dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"
//
//  val prop = new Properties
//  prop.setProperty("driver", "org.postgresql.Driver")
//  prop.setProperty("user", "jgp")
//  prop.setProperty("password", "Spark<3Java")
//
//  df.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch02", prop)

  spark.stop

}
