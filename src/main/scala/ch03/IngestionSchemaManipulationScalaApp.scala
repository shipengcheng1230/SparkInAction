package ch03

import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.Partition
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object IngestionSchemaManipulationScalaApp extends App {

  val spark = SparkSession
    .builder
    .appName("Restaurants in Wake County, NC")
    .master("local[*]")
    .getOrCreate

  spark.sparkContext.setLogLevel("error")

  var df = spark
    .read
    .format("csv")
    .option("header", "true")
    .load("data/Restaurants_in_Wake_County_NC.csv")

  println("*** Right after ingestion")

  df.show(5)
  df.printSchema()
  println("We have " + df.count + " records.")

  df = df
    .withColumn("county", lit("Wake"))
    .withColumnRenamed("HSISID", "datasetId")
    .withColumnRenamed("NAME", "name")
    .withColumnRenamed("ADDRESS1", "address1")
    .withColumnRenamed("ADDRESS2", "address2")
    .withColumnRenamed("CITY", "city")
    .withColumnRenamed("STATE", "state")
    .withColumnRenamed("POSTALCODE", "zip")
    .withColumnRenamed("PHONENUMBER", "tel")
    .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
    .withColumnRenamed("FACILITYTYPE", "type")
    .withColumnRenamed("X", "geoX")
    .withColumnRenamed("Y", "geoY")
    .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

  df = df
    .withColumn("id",
      concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")))

  println("*** Dataframe transformed")
  df.show(5)

  val drop_cols =
    List(
      "address2", "zip", "tel", "dateStart",
      "geoX", "geoY", "address1", "datasetId"
    )
  val dfUsedForBook = df.drop(drop_cols: _*)
  dfUsedForBook.show(5, 15)

  df.printSchema()

  println("*** Looking at partitions")
  val partitions = df.rdd.partitions
  val partitionCount = partitions.length
  println("Partition count before repartition: " + partitionCount)

  df = df.repartition(4)
  println("Partition count after repartition: " + df.rdd.partitions.length)
}
