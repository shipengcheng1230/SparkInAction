package ch15

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

object PointsPerOrderScalaApp {

  class PointAttributionScalaUdaf(name: String) extends Aggregator[Row, Int, Int] {
    val MAX_POINT = 3
    override def zero = 0
    override def reduce(b: Int, a: Row): Int = b + (a.getAs[Int](name) max MAX_POINT)
    override def merge(b1: Int, b2: Int): Int = b1 + b2
    override def finish(reduction: Int): Int = reduction
    override def bufferEncoder: Encoder[Int] = Encoders.scalaInt
    override def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

  val pointAttribution = new PointAttributionScalaUdaf("quantity")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Orders loyalty point")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/orders/orders.csv")

    // Calculating the points for each customer, not each order
    val pointDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), (pointAttribution).toColumn.as("point"))

    pointDf.show(20)

    // Alternate way: calculate order by order
    val max = 3
    val eachOrderDf = df
      .withColumn("point", when(col("quantity").$greater(max), max).otherwise(col("quantity")))
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("point").as("point"))

    eachOrderDf.show(20)

    spark.stop
  }
}
