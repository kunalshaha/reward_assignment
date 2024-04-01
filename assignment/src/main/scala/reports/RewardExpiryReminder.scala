package reports

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct}


/**
 * This class will help to process the RewardExpiryReminder report connecting with hive
 * please pass the date as an argument as "2024-02-04"
 */
object RewardExpiryReminder {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("spark://spark-master:7077")
      .appName("RewardExpiryAlert")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "/users/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val filterDate = args(0)

    var dfWithInterVal = spark.sql("select *,datediff(expiry_at,TO_DATE('" + filterDate + "')) as interval_val from rewards_logs.reward_log");
    //filter the data for the rewards which are getting expired in next 1 day
    var alertUserForExpiryReminder = dfWithInterVal.filter(col("interval_val") === 1 and col("redeem_at").isNull);

    val finalData = alertUserForExpiryReminder.groupBy("user_id").agg(countDistinct(col("user_reward_id")).as("user_reward_count"))

    // The o/p  is generated as the CSV report  with the respective date for passed to the Job
    finalData.coalesce(1).write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .mode("overwrite").option("header", "true")
      .save("/opt/spark-data/output/reward_expiry_reminder/" + filterDate);

    // .save("hdfs://namenode:9000/data/expiry_report_data/"+filterDate);
  }

}
