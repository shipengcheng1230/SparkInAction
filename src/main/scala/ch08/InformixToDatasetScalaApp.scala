package ch08

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, DataTypes, MetadataBuilder}

object InformixToDatasetScalaApp extends App {

  class InformixJdbcDialectScala extends JdbcDialect {
    private val serialVersionUID = -672901

    override def canHandle(url: String): Boolean =
      url.startsWith("jdbc:informix-sqli")

    /**
     * Processes specific JDBC types for Catalyst.
     */
    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder):Option[DataType] =
      if (typeName.toLowerCase.compareTo("serial") == 0)
        Option.apply(DataTypes.IntegerType)
      else if (typeName.toLowerCase.compareTo("calendar") == 0)
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.compareTo("calendarpattern") == 0)
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.compareTo("se_metadata") == 0)
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.compareTo("sysbldsqltext") == 0)
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.startsWith("timeseries"))
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.compareTo("st_point") == 0)
        Option.apply(DataTypes.BinaryType)
      else if (typeName.toLowerCase.compareTo("tspartitiondesc_t") == 0)
        Option.apply(DataTypes.BinaryType)
      else
        Option.empty // An object from the Scala library
  }

  val spark: SparkSession = SparkSession.builder
    .appName("Informix to Dataframe using a JDBC Connection")
    .master("local[*]")
    .getOrCreate

  // Specific Informix dialect
  val dialect = new InformixJdbcDialectScala
  JdbcDialects.registerDialect(dialect)

  val informixURL = "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y"
  // Using properties
  val df = spark.read
    .format("jdbc")
    .option("url", informixURL)
    .option("dbtable", "customer")
    .option("user", "informix")
    .option("password", "in4mix")
    .load

  // Displays the dataframe and some of its metadata
  df.show(5)
  df.printSchema()
  println("The dataframe contains " + df.count + " record(s).")
}


