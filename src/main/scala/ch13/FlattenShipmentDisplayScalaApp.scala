package ch13

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

object FlattenShipmentDisplayScalaApp extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("Flatenning JSON doc describing shipments")
    .master("local[*]")
    .getOrCreate

  // Reads a JSON, stores it in a dataframe
  // Dataset[Row] == DataFrame
  val df: DataFrame = spark.read
    .format("json")
    .option("multiline", value = true)
    .load("data/json/shipment.json")

  val df2 = df
    .withColumn("supplier_name", F.col("supplier.name"))
    .withColumn("supplier_city", F.col("supplier.city"))
    .withColumn("supplier_state", F.col("supplier.state"))
    .withColumn("supplier_country", F.col("supplier.country"))
    .drop("supplier")
    .withColumn("customer_name", F.col("customer.name"))
    .withColumn("customer_city", F.col("customer.city"))
    .withColumn("customer_state", F.col("customer.state"))
    .withColumn("customer_country", F.col("customer.country"))
    .drop("customer")
    .withColumn("items", F.explode(F.col("books")))

  val df3 = df2
    .withColumn("qty", F.col("items.qty"))
    .withColumn("title", F.col("items.title"))
    .drop("items")
    .drop("books")

  // Shows at most 5 rows from the dataframe (there's only one anyway)
  df3.show(5, false)
  df3.printSchema()

  df3.createOrReplaceTempView("shipment_detail")

  val sqlQuery = "SELECT COUNT(*) AS bookCount FROM shipment_detail"
  val bookCountDf = spark.sql(sqlQuery)

  bookCountDf.show

  spark.stop
}
