package ch12

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object EpochConversionApp extends App {

  val spark = SparkSession.builder()
    .appName("Record Transform")
    .master("local")
    .getOrCreate()

  val schema = StructType(
    StructField("event", IntegerType, nullable = false) ::
      StructField("time", StringType, nullable = false) :: Nil
  )

  def epochToDateString(epoch: Long): String = {
    val df = new SimpleDateFormat("yyyy-mm-dd HH:MM:SS")
    df.format(epoch)
  }

  val rows = (0 until 1000)
    .map(i => Row(i, epochToDateString(System.currentTimeMillis())))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  df.show(5)

  val df2 = df.withColumn("date", from_unixtime(df("time")))

  df2.collect().foreach(x => println(s"${x.getInt(0)} # ${x.getString(1)} # ${x.getString(2)}"))

}
