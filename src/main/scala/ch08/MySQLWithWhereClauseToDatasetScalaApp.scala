package ch08

import java.util.Properties

import org.apache.spark.sql.SparkSession

object MySQLWithWhereClauseToDatasetScalaApp extends App {
  val spark: SparkSession = SparkSession.builder
    .appName("MySQL with where clause to Dataframe using a JDBC Connection")
    .master("local[*]")
    .getOrCreate

  // Using properties
  val props = new Properties
  props.put("user", "root")
  props.put("password", "Spark<3Java")
  props.put("useSSL", "false")
  props.put("serverTimezone", "EST")

  val sqlQuery = "select * from film where " +
    "(title like \"%ALIEN%\" or title like \"%victory%\" " +
    "or title like \"%agent%\" or description like \"%action%\") " +
    "and rental_rate>1 " +
    "and (rating=\"G\" or rating=\"PG\")"

  val mySQLURL = "jdbc:mysql://localhost:3306/sakila"
  // Let's look for all movies matching the query
  val df = spark.read
    .jdbc(mySQLURL, "(" + sqlQuery + ") film_alias", props)

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println(s"The dataframe contains ${df.count} record(s).")

  spark.stop
}
