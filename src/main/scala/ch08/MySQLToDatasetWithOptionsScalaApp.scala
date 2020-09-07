package ch08

import org.apache.spark.sql.SparkSession

object MySQLToDatasetWithOptionsScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("MySQL to Dataframe using a JDBC Connection")
    .master("local[*]")
    .getOrCreate

  // In a "one-liner" with method chaining and options
  var df = spark.read
    .option("url", "jdbc:mysql://localhost:3306/sakila")
    .option("dbtable", "actor")
    .option("user", "root")
    .option("password", "Spark<3Java")
    .option("useSSL", "false")
    .option("serverTimezone", "EST")
    .format("jdbc")
    .load

  df = df.orderBy(df.col("last_name"))

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")

  spark.stop
}
