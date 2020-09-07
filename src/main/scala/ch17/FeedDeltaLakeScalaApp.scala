package ch17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object FeedDeltaLakeScalaApp extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("Ingestion the 'Grand Débat' files to Delta Lake")
    // To use Databricks Delta Lake, we should add delta core packages to SparkSession
    //.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
    .master("local[*]")
    .getOrCreate()

  // Create the schema
  val schema: StructType = StructType(Array[StructField](
    StructField("authorId", DataTypes.StringType, nullable = false),
    StructField("authorType", DataTypes.StringType, nullable = true),
    StructField("authorZipCode", DataTypes.StringType, nullable = true),
    StructField("body", DataTypes.StringType, nullable = true),
    StructField("createdAt", DataTypes.TimestampType, nullable = false),
    StructField("enabled", DataTypes.BooleanType, nullable = true),
    StructField("endAt", DataTypes.TimestampType, nullable = true),
    StructField("fullAddress", DataTypes.StringType, nullable = true),
    StructField("id", DataTypes.StringType, nullable = false),
    StructField("lat", DataTypes.DoubleType, nullable = true),
    StructField("link", DataTypes.StringType, nullable = true),
    StructField("lng", DataTypes.DoubleType, nullable = true),
    StructField("startAt", DataTypes.TimestampType, nullable = false),
    StructField("title", DataTypes.StringType, nullable = true),
    StructField("updatedAt", DataTypes.TimestampType, nullable = true),
    StructField("url", DataTypes.StringType, nullable = true))
  )

  // Reads a JSON file, called 20190302 EVENTS.json, stores it in a
  // dataframe
  var df = spark.read
    .format("json")
    .schema(schema)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .load("data/france_grand_debat/20190302 EVENTS.json")

  df = df
    .withColumn("authorZipCode", col("authorZipCode").cast(DataTypes.IntegerType))
    .withColumn("authorZipCode",
      when(col("authorZipCode").lt(1000), null)
        .otherwise(col("authorZipCode")))
    .withColumn("authorZipCode",
      when(col("authorZipCode").geq(99999), null)
        .otherwise(col("authorZipCode")))
    .withColumn("authorDept", expr("int(authorZipCode / 1000)"))

  df.show(25)
  df.printSchema()

  df.write
    .format("delta")
    .mode("overwrite")
    .save("/tmp/delta_grand_debat_events")

  println(df.count + " rows updated.")

  spark.stop
}
