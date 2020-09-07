package ch13

import java.util

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object RestaurantDocumentScalaApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Building a restaurant fact sheet")
      .master("local[*]")
      .getOrCreate

    val businessDf = spark.read
      .format("csv")
      .option("header", true)
      .load("data/orangecounty_restaurants/businesses.CSV")

    val inspectionDf = spark.read
      .format("csv")
      .option("header", true)
      .load("data/orangecounty_restaurants/inspections.CSV")

    businessDf.show(3)
    businessDf.printSchema()

    inspectionDf.show(3)
    inspectionDf.printSchema()

    val factSheetDf = nestedJoin(businessDf, inspectionDf, "business_id", "business_id", "inner", "inspections")
    factSheetDf.show(5)
    factSheetDf.printSchema()
    spark.stop()
  }

  def nestedJoin(leftDf: Dataset[Row], rightDf: Dataset[Row],
                 leftJoinCol: String, rightJoinCol: String,
                 joinType: String, nestedCol: String
                ): Dataset[Row] = {
    var resDf = leftDf.join(rightDf, rightDf.col(rightJoinCol).equalTo(leftDf.col(leftJoinCol)), joinType)
    val leftColumns = getColumns(leftDf)
    val allColumns = util.Arrays.copyOf(leftColumns, leftColumns.length + 1)
    allColumns(leftColumns.length) = struct(getColumns(rightDf): _*).alias("temp_column")
    resDf = resDf.select(allColumns: _*)
    resDf = resDf.groupBy(leftColumns: _*).agg(collect_list(col("temp_column")).as(nestedCol))
    resDf
  }

  def getColumns(df: Dataset[Row]): Array[Column] = {
    df.columns.map(df.col)
  }

}
