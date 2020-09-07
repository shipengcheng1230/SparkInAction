package ch16

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object BrazilStatisticsApp {

  object Mode extends Enumeration {
    type Mode = Value
    val NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER = Value
  }

  import Mode._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Example of cache and checkpoint")
      .master("local[*]")
      .config("spark.executor.memory", "10g")
      .config("spark.driver.memory", "5g")
      .config("spark.memory.offHeap.enabled", value = true)
      .config("spark.memory.offHeap.size", "16g")
      .getOrCreate();

    spark.sparkContext.setCheckpointDir("/tmp")

    val df = spark.read
      .format("csv")
      .options(Map(
        "header" -> "true",
        "sep" -> ";",
        "enforceSchema" -> "true",
        "inferSchema" -> "true",
      ))
      .load("data/brazil/BRAZIL_CITIES.csv")

    println("***** Raw dataset and schema")
    df.show(100)
    df.printSchema()

    val t0 = process(df, NO_CACHE_NO_CHECKPOINT)
    val t1 = process(df, CACHE)
    val t2 = process(df, CHECKPOINT)
    val t3 = process(df, CHECKPOINT_NON_EAGER)

    println("\n***** Processing times (excluding purification)")
    println("Without cache ............... " + t0 + " ms")
    println("With cache .................. " + t1 + " ms")
    println("With checkpoint ............. " + t2 + " ms")
    println("With non-eager checkpoint ... " + t3 + " ms")
  }

  def process(df: Dataset[Row], mode: Mode): Long = {
    val t0 = System.currentTimeMillis()
    var _df = df
    _df = _df
      .orderBy(col("CAPITAL").desc)
      .withColumn(
        "WAL-MART",
        when(col("WAL-MART").isNull, 0).otherwise(col("WAL-MART")))
      .withColumn(
        "MAC",
        when(col("MAC").isNull, 0).otherwise(col("MAC")))
      .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
      .withColumn("GDP", col("GDP").cast("float"))
      .withColumn("area", regexp_replace(col("area"), ",", ""))
      .withColumn("area", col("area").cast("float"))
      .groupBy("STATE")
      .agg(
        first("CITY").alias("capital"),
        sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
        sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
        sum("POP_GDP").alias("pop_2016"),
        sum("GDP").alias("gdp_2016"),
        sum("POST_OFFICES").alias("post_offices_ct"),
        sum("WAL-MART").alias("wal_mart_ct"),
        sum("MAC").alias("mc_donalds_ct"),
        sum("Cars").alias("cars_ct"),
        sum("Motorcycles").alias("moto_ct"),
        sum("AREA").alias("area"),
        sum("IBGE_PLANTED_AREA").alias("agr_area"),
        sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
        sum("HOTELS").alias("hotels_ct"),
        sum("BEDS").alias("beds_ct"))
      .withColumn("agr_area", expr("agr_area / 100"))
      .orderBy(col("STATE"))
      .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"));

    _df = mode match {
      case CACHE => _df.cache()
      case CHECKPOINT => _df.checkpoint(true)
      case CHECKPOINT_NON_EAGER => _df.checkpoint(false)
      case _ => _df
    }
    println("***** Pure data")
    _df.show(5)
    val t1 = System.currentTimeMillis()
    println("Aggregation (ms) .................. " + (t1 - t0))

    // Regions per population
    println("***** Population")
    val popDf = _df.drop("area", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("pop_2016").desc)
    popDf.show(30)
    val t2 = System.currentTimeMillis
    println("Population (ms) ................... " + (t2 - t1))

    // Regions per size in km2
    System.out.println("***** Area (squared kilometers)")
    val areaDf = _df.withColumn("area", round(col("area"), 2)).drop("pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("area").desc)
    areaDf.show(30)
    val t3 = System.currentTimeMillis
    println("Area (ms) ......................... " + (t3 - t2))

    // McDonald's per 1m inhabitants
    println("***** McDonald's restaurants per 1m inhabitants")
    val mcDonaldsPopDf = _df.withColumn("mcd_1m_inh", expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("mcd_1m_inh").desc)
    mcDonaldsPopDf.show(5)
    val t4 = System.currentTimeMillis
    println("Mc Donald's (ms) .................. " + (t4 - t3))

    // Walmart per 1m inhabitants
    println("***** Walmart supermarket per 1m inhabitants")
    val walmartPopDf = _df.withColumn("walmart_1m_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("walmart_1m_inh").desc)
    walmartPopDf.show(5)
    val t5 = System.currentTimeMillis
    println("Walmart (ms) ...................... " + (t5 - t4))

    // GDP per capita
    println("***** GDP per capita")
    val gdpPerCapitaDf = _df.drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area").withColumn("gdp_capita", expr("int(gdp_capita)")).orderBy(col("gdp_capita").desc)
    gdpPerCapitaDf.show(5)
    val t6 = System.currentTimeMillis
    println("GDP per capita (ms) ............... " + (t6 - t5))

    // Post offices
    println("***** Post offices")
    var postOfficeDf = _df.withColumn("post_office_1m_inh", expr("int(post_offices_ct / pop_2016 * 100000000) / 100")).withColumn("post_office_100k_km2", expr("int(post_offices_ct / area * 10000000) / 100")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil").orderBy(col("post_office_1m_inh").desc)
    postOfficeDf = mode match {
      case CACHE => postOfficeDf.cache
      case CHECKPOINT => postOfficeDf.checkpoint(true)
      case CHECKPOINT_NON_EAGER => postOfficeDf.checkpoint(false)
      case _ => postOfficeDf
    }
    println("****  Per 1 million inhabitants")
    val postOfficePopDf = postOfficeDf.drop("post_office_100k_km2", "area").orderBy(col("post_office_1m_inh").desc)
    postOfficePopDf.show(5)
    println("****  per 100000 km2")
    val postOfficeArea = postOfficeDf.drop("post_office_1m_inh", "pop_2016").orderBy(col("post_office_100k_km2").desc)
    postOfficeArea.show(5)
    val t7 = System.currentTimeMillis
    println("Post offices (ms) ................. " + (t7 - t6) + " / Mode: " + mode)

    // Cars and motorcycles per 1k habitants
    println("***** Vehicles")
    val vehiclesDf = _df.withColumn("veh_1k_inh", expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area", "pop_brazil").orderBy(col("veh_1k_inh").desc)
    vehiclesDf.show(5)
    val t8 = System.currentTimeMillis
    println("Vehicles (ms) ..................... " + (t8 - t7))


    println("***** Agriculture - usage of land for agriculture")
    val agricultureDf = _df.withColumn("agr_area_pct", expr("int(agr_area / area * 1000) / 10")).withColumn("area", expr("int(area)")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod", "pop_2016").orderBy(col("agr_area_pct").desc)
    agricultureDf.show(5)
    val t9 = System.currentTimeMillis
    println("Agriculture revenue (ms) .......... " + (t9 - t8))

    val t_ = System.currentTimeMillis()
    System.out.println("Total with purification (ms) ...... " + (t_ - t0))
    System.out.println("Total without purification (ms) ... " + (t_ - t0))
    t_ - t1
  }
}
