package ch11

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object SimpleSelectScalaApp extends App {

  val spark = SparkSession.builder
    .appName("Simple SELECT using SQL")
    .master("local")
    .getOrCreate

  val schema = DataTypes.createStructType(Array[StructField](
    DataTypes.createStructField("geo", DataTypes.StringType, true),
    DataTypes.createStructField("yr1980", DataTypes.DoubleType, false))
  )

  // Reads a CSV file with header, called books.csv, stores it in a
  // dataframe
  val df = spark.read
    .format("csv")
    .option("header", value = true)
    .schema(schema)
    .load("data/populationbycountry19802010millions.csv")

  df.createOrReplaceTempView("geodata")
  df.printSchema()

  val query =
    """
      |SELECT * FROM geodata
      |WHERE yr1980 < 1
      |ORDER BY yr1980
      |LIMIT 5
      """.stripMargin

  val smallCountries = spark.sql(query)

  // Shows at most 10 rows from the dataframe (which is limited to 5
  // anyway)
  smallCountries.show(10, false)

  spark.stop
}
