package ch13

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

object FlattenJsonScalaApp {

  val ARRAY_TYPE = "Array"
  val STRUCT_TYPE = "Struc"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Automatic flattening of a JSON document")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    val invoicesDf = spark.read
      .format("json")
      .option("multiline", value = true)
      .load("data/json/nested_array.json")

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3)
    invoicesDf.printSchema()

    val flatInvoicesDf = flattenNestedStructure(spark, invoicesDf)
    flatInvoicesDf.show(20, truncate = false)
    flatInvoicesDf.printSchema()

    spark.stop
  }

  def flattenNestedStructure(spark: SparkSession, df: DataFrame): DataFrame = {
    var recursion = false
    var processedDf = df
    val schema = df.schema
    val fields = schema.fields

    for (field <- fields) {
      field.dataType.toString.substring(0, 5) match {
        case ARRAY_TYPE =>
          // Explodes array
          processedDf = processedDf.withColumnRenamed(field.name, field.name + "_tmp")
          // explode for array type
          processedDf = processedDf.withColumn(field.name, F.explode(F.col(field.name + "_tmp")))
          processedDf = processedDf.drop(field.name + "_tmp")
          recursion = true

        case STRUCT_TYPE =>
          // Mapping
          /**
           * field.toDDL = `author` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `publisher` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `books` STRUCT<`salesByMonth`: ARRAY<BIGINT>, `title`: STRING>
           */
          println(s"field.toDDL = ${field.toDDL}")
          val ddl = field.toDDL.split("`") // fragile :(
          var i = 3
          while (i < ddl.length) {
            processedDf = processedDf.withColumn(field.name + "_" + ddl(i), F.col(field.name + "." + ddl(i)))
            i += 2
          }
          processedDf = processedDf.drop(field.name)
          recursion = true
        case _ =>
          processedDf = processedDf
      }
    }

    if (recursion)
      processedDf = flattenNestedStructure(spark, processedDf)

    processedDf
  }
}
