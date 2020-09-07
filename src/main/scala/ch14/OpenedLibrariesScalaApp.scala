package ch14

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object OpenedLibrariesScalaApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Custom UDF to check if in range")
      .master("local[*]")
      .getOrCreate

    import IsOpenScalaService.isOpen
    spark.udf.register("isOpen", isOpen _)

    val librariesDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("encoding", "cp1252")
      .load("data/south_dublin_libraries/sdlibraries.csv")
      .drop("Administrative_Authority")
      .drop("Address1")
      .drop("Address2")
      .drop("Town")
      .drop("Postcode")
      .drop("County")
      .drop("Phone")
      .drop("Email")
      .drop("Website")
      .drop("Image")
      .drop("WGS84_Latitude")
      .drop("WGS84_Longitude")

    librariesDf.show(false)
    librariesDf.printSchema()

    val dateTimeDf = createDataframe(spark)
    dateTimeDf.show(false)
    dateTimeDf.printSchema()

    val df = librariesDf.crossJoin(dateTimeDf)
    df.show(false)

    val finalDf = df.withColumn("open",
      F.callUDF("isOpen", F.col("Opening_Hours_Monday"),
        F.col("Opening_Hours_Tuesday"), F.col("Opening_Hours_Wednesday"),
        F.col("Opening_Hours_Thursday"), F.col("Opening_Hours_Friday"),
        F.col("Opening_Hours_Saturday"), F.lit("Closed"), F.col("date")))
      .drop("Opening_Hours_Monday")
      .drop("Opening_Hours_Tuesday")
      .drop("Opening_Hours_Wednesday")
      .drop("Opening_Hours_Thursday")
      .drop("Opening_Hours_Friday")
      .drop("Opening_Hours_Saturday")

    finalDf.show()

    val sqlQuery = "SELECT Council_ID, Name, date, " +
      "isOpen(Opening_Hours_Monday, Opening_Hours_Tuesday, " +
      "Opening_Hours_Wednesday, Opening_Hours_Thursday, " +
      "Opening_Hours_Friday, Opening_Hours_Saturday, 'closed', date) AS open" +
      " FROM libraries "

    df.createOrReplaceTempView("libraries")
    val finalDf2 = spark.sql(sqlQuery)
    finalDf2.show()

    spark.stop
  }

  trait IsOpenScalaService {
    def isOpen(hoursMon: String, hoursTue: String, hoursWed: String,
               hoursThu: String, hoursFri: String, hoursSat: String,
               hoursSun: String, dateTime: Timestamp): Boolean

  }

  object IsOpenScalaService extends IsOpenScalaService {

    def isOpen(hoursMon: String, hoursTue: String, hoursWed: String,
               hoursThu: String, hoursFri: String, hoursSat: String,
               hoursSun: String, dateTime: Timestamp): Boolean = {

      val cal = Calendar.getInstance
      cal.setTimeInMillis(dateTime.getTime)
      val day = cal.get(Calendar.DAY_OF_WEEK)
      println(s"Day of the week: ${day}")
      val hours = day match {
        case Calendar.MONDAY =>
          hoursMon

        case Calendar.TUESDAY =>
          hoursTue

        case Calendar.WEDNESDAY =>
          hoursWed

        case Calendar.THURSDAY =>
          hoursThu

        case Calendar.FRIDAY =>
          hoursFri

        case Calendar.SATURDAY =>
          hoursSat

        case _ => // Sunday
          hoursSun
      }

      // quick return
      if (hours.compareToIgnoreCase("closed") == 0)
        return false
      // check if in interval
      val event = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND)
      val ranges = hours.split(" and ")
      for (i <- 0 until ranges.length) {
        println(s"Processing range #${i}: ${ranges(i)}")
        val operningHours = ranges(i).split("-")

        val start = Integer.valueOf(operningHours(0).substring(0, 2)) * 3600
        +Integer.valueOf(operningHours(0).substring(3, 5)) * 60

        val end = Integer.valueOf(operningHours(1).substring(0, 2)) * 3600
        +Integer.valueOf(operningHours(1).substring(3, 5)) * 60

        println(s"Checking between ${start} and ${end}")
        if (event >= start && event <= end)
          return true
      }
      false
    }
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema = StructType(List(StructField("date_str", StringType, false)))

    val rows = Array(
      Row("2019-03-11 14:30:00"),
      Row("2019-04-27 16:00:00"),
      Row("2020-01-26 05:00:00"),
    )

    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .withColumn("date", F.to_timestamp(F.col("date_str")))
      .drop("date_str")
  }

}
