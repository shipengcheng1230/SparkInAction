package ch14

import org.apache.spark.sql.functions.{array, callUDF}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

object ColumnAdditionScalaApp {
  val COL_COUNT = 8

  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Column addition")
      .master("local[*]")
      .getOrCreate

    def columnAdditionScalaUdf(t1: Int*): Int = t1.sum

    spark.udf.register("add", columnAdditionScalaUdf _)

    var df = createDataframe(spark)
    df.show(false)

    val cols = (0 until COL_COUNT).map(x => df.col("c" + x))
    // notice `array` used here
    df = df.withColumn("sum", callUDF("add", array(cols: _*)))
    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      StructField("c0", DataTypes.IntegerType, nullable = false),
      StructField("c1", DataTypes.IntegerType, nullable = false),
      StructField("c2", DataTypes.IntegerType, nullable = false),
      StructField("c3", DataTypes.IntegerType, nullable = false),
      StructField("c4", DataTypes.IntegerType, nullable = false),
      StructField("c5", DataTypes.IntegerType, nullable = false),
      StructField("c6", DataTypes.IntegerType, nullable = false),
      StructField("c7", DataTypes.IntegerType, nullable = false))
    )

    val rows = new scala.collection.mutable.ArrayBuffer[Row]
    rows.append(Row(int2Integer(1), int2Integer(2), int2Integer(4), int2Integer(8),
      int2Integer(16), int2Integer(32), int2Integer(64), int2Integer(128)))
    rows.append(Row(int2Integer(0), int2Integer(0), int2Integer(0), int2Integer(0),
      int2Integer(0), int2Integer(0), int2Integer(0), int2Integer(0)))
    rows.append(Row(int2Integer(1), int2Integer(1), int2Integer(1), int2Integer(1),
      int2Integer(1), int2Integer(1), int2Integer(1), int2Integer(1)))

    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

}
