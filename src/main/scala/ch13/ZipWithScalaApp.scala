package ch13

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, RowFactory, SparkSession, functions => F}

object ZipWithScalaApp extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("zip_with function")
    .master("local[*]")
    .getOrCreate

  val schema = StructType(List(
    StructField("c1", ArrayType(DataTypes.IntegerType), false),
    StructField("c2", ArrayType(DataTypes.IntegerType), false),
  ))

  val rows = spark.sparkContext.parallelize(Seq(
    Row(Array(1010, 1012), Array(1021, 1023, 1025)),
    Row(Array(2020, 2030, 2040), Array(2021, 2023)),
    Row(Array(3010, 3012), Array(3021, 3023))
  ))

  val df = spark.createDataFrame(rows, schema)
  df.show(5)

  val df2 = df.withColumn("zip_with", F.zip_with(
    F.col("c1"),
    F.col("c2"),
    (x, y) => F.when(x.isNull.or(y.isNull), F.lit(-1)).otherwise(x + y)
  ))

  df2.show(5)
  spark.stop()

}
