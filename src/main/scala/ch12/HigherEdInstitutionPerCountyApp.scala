package ch12

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HigherEdInstitutionPerCountyApp extends App {

  val spark = SparkSession.builder()
    .appName("Join")
    .master("local")
    // see below on when to enable this
    // https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html
    .config("spark.sql.adaptive.enabled", value = true)
    .getOrCreate()

  var censusDf = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "inferSchema" -> "true",
      "encoding" -> "cp1252",
    ))
    .load("data/census/PEP_2017_PEPANNRES.csv")

  censusDf = censusDf
    .drop("GEO.id")
    .drop("rescen42010")
    .drop("resbase42010")
    .drop("respop72010")
    .drop("respop72011")
    .drop("respop72012")
    .drop("respop72013")
    .drop("respop72014")
    .drop("respop72015")
    .drop("respop72016")
    .withColumnRenamed("respop72017", "pop2017")
    .withColumnRenamed("GEO.id2", "countyId")
    .withColumnRenamed("GEO.display-label", "county")

  censusDf.sample(0.1).show(3, false)

  var higherEdDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/dapip/InstitutionCampus.csv");

  higherEdDf = higherEdDf
    .filter("LocationType = 'Institution'")
    .withColumn(
      "addressElements",
      split(higherEdDf.col("Address"), " "));

  higherEdDf = higherEdDf
    .withColumn(
      "addressElementCount",
      size(higherEdDf.col("addressElements")));

  higherEdDf = higherEdDf
    .withColumn(
      "zip9",
      element_at(
        higherEdDf.col("addressElements"),
        higherEdDf.col("addressElementCount")));

  higherEdDf = higherEdDf
    .withColumn(
      "splitZipCode",
      split(higherEdDf.col("zip9"), "-"));

  higherEdDf = higherEdDf
    .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
    .withColumnRenamed("LocationName", "location")
    .drop("DapipId")
    .drop("OpeId")
    .drop("ParentName")
    .drop("ParentDapipId")
    .drop("LocationType")
    .drop("Address")
    .drop("GeneralPhone")
    .drop("AdminName")
    .drop("AdminPhone")
    .drop("AdminEmail")
    .drop("Fax")
    .drop("UpdateDate")
    .drop("zip9")
    .drop("addressElements")
    .drop("addressElementCount")
    .drop("splitZipCode")

  higherEdDf.sample(0.1).show(3, false)

  var countyZipDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/hud/COUNTY_ZIP_092018.csv");
  countyZipDf = countyZipDf
    .drop("res_ratio")
    .drop("bus_ratio")
    .drop("oth_ratio")
    .drop("tot_ratio");
  System.out.println("Counties / ZIP Codes (HUD)");

  countyZipDf.sample(0.1).show(3, false);

  var institPerCountyDf = higherEdDf.join(
    countyZipDf,
    higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
    "inner");
  System.out.println(
    "Higher education institutions left-joined with HUD");
  institPerCountyDf
    .filter(higherEdDf.col("zip").equalTo(27517))
    .show(20, false);

  institPerCountyDf = institPerCountyDf.join(
    censusDf,
    institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
    "left")

  institPerCountyDf = institPerCountyDf
    .drop(higherEdDf.col("zip"))
    .drop(countyZipDf.col("county"))
    .drop("countyId")
    .distinct()

  institPerCountyDf
    .filter(higherEdDf.col("zip").equalTo(27517))
    .show(20, false)
}
