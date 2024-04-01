package processor

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * This class consumes the raw reward logs and converts to the star schema  with dimensions and fact table
 * Also the transformation of the data is made in such a way that the report generation and metric calculation
 * is simple
 */
object RewardLogProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.dynamicAllocation.enabled", "false")
      .master("spark://spark-master:7077")
      .appName("Reward Log Processor").getOrCreate()

    //    val eventLogs = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat")
    //      .load("../assignment/src/main/resources/data/accounts/event_logs.ndjson")

    // Read data from reward logs
    val eventLogs = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat")
      .load("/opt/spark-data/rawlogs/*.ndjson")


    val report_Data = eventLogs.
      select(col("brand_id")
        , col("brand_name"), col("reward_id"),
        col("reward_name"), substring(col("created_at"), 0, 10).as("report_date")
        , col("event"), col("user_id"), col("points_spent"), col("user_reward_id")
        , substring(col("expired_at"), 0, 10).as("expired_at"))

    /**
     * created 3 different event dates like purchase_at , redeem_at , expiry_at
     * and prepare the fact tables
     */
    val purchaseEvents = report_Data.filter(col("event") === "purchase")
      .withColumnRenamed("report_date", "purchase_at")
      .withColumn("redeem_at", lit(null))
      .select("user_reward_id", "user_id", "brand_id", "brand_name"
        , "reward_id", "reward_name", "purchase_at", "redeem_at", "expired_at")

    val redeemEvents = report_Data.filter(col("event") === "redeem")
      .withColumnRenamed("report_date", "redeem_at")
      .withColumn("purchase_at", lit(null))
      .withColumn("expired_at", lit(null))
      .select("user_reward_id", "user_id", "brand_id", "brand_name"
        , "reward_id", "reward_name", "purchase_at", "redeem_at", "expired_at")


    val entireData = purchaseEvents.union(redeemEvents).groupBy("user_reward_id", "user_id", "brand_id"
      , "brand_name", "reward_id", "reward_name")
      .agg(max("purchase_at").as("purchase_at")
        , max("redeem_at").as("redeem_at")
        , max("expired_at").as("expiry_at"))

    entireData.createTempView("entireData");

    // create dimension tables
    var dimensionRewards = spark.sql("select distinct reward_id,reward_name from entireData");
    var dimensionBrands = spark.sql("select distinct brand_id,brand_name from entireData");

    // store the fact tables data in hadoop system
    entireData.select("user_reward_id", "user_id", "brand_id"
      , "reward_id", "purchase_at", "redeem_at", "expiry_at").coalesce(1).write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .mode("overwrite").option("header", "true")
      .save("hdfs://namenode:9000/data/reward_data/fact/processed/");

    // store dimension table data for rewards
    dimensionRewards.coalesce(1).coalesce(1).write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .mode("overwrite").option("header", "true")
      .save("hdfs://namenode:9000/data/reward_data/dimension/rewards/");


    // store dimensions data for brand
    dimensionBrands
      .coalesce(1)
      .write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .mode("overwrite").option("header", "true")
      .save("hdfs://namenode:9000/data/reward_data/dimension/brands/");

  }

}
