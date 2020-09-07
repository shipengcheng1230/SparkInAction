package ch05

import org.apache.spark.api.java.function.{MapFunction, ReduceFunction}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import java.util.ArrayList

object PiComputeScalaApp {

  private var counter = 0

  @SerialVersionUID(38446L)
  final private class DartMapper extends MapFunction[Row, Integer] {
    @throws[Exception]
    override def call(r: Row): Integer = {
      val x = Math.random * 2 - 1
      val y = Math.random * 2 - 1
      counter += 1
      if (counter % 100000 == 0)
        println("" + counter + " darts thrown so far")
      if (x * x + y * y <= 1) 1
      else 0
    }
  }

  private def dartMap(r: Row): Int = {
    val x = math.random * 2 - 1
    val y = math.random * 2 - 1
    if (x*x + y*y <= 1) 1 else 0
  }

  @SerialVersionUID(12859L)
  final private class DartReducer extends ReduceFunction[Integer] {
    override def call(x: Integer, y: Integer): Integer = x + y
  }

  def main(args: Array[String]): Unit = {
    val slices = 10
    val numberOfThrows = 100000 * slices
    println("About to throw " + numberOfThrows + " darts, ready? Stay away from the target!")

    val t0 = System.currentTimeMillis
    val spark = SparkSession.builder
      .appName("Spark Pi")
      //.master("local[*]") // comment if to run on cluster specified by --master
      .getOrCreate

    spark.sparkContext.setLogLevel("error")

    val t1 = System.currentTimeMillis
    println("Session initialized in " + (t1 - t0) + " ms")

    // val numList = new ArrayList[Integer](numberOfThrows)
    val numList = Array.fill(numberOfThrows)(0)

    // For  Spark Encoder implicits
    import spark.implicits._

//    for(i <- 1.to(numberOfThrows))
//      numList.append(i)

    val incrementalDf = spark.createDataset(numList).toDF

    val t2 = System.currentTimeMillis
    println("Initial dataframe built in " + (t2 - t1) + " ms")

    val dartsDs = incrementalDf.map(dartMap)

    val t3 = System.currentTimeMillis
    println("Throwing darts done in " + (t3 - t2) + " ms")

//    val dartsInCircle = dartsDs.reduce(new DartReducer)
    val dartsInCircle = dartsDs.reduce(_ + _)
    val t4 = System.currentTimeMillis
    println("Analyzing result in " + (t4 - t3) + " ms")

    println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows)

    spark.stop()
  }

}
