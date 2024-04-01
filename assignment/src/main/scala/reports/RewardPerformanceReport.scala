package reports

import org.apache.spark.sql.SparkSession

/**
 * This class helps generate the Reward performance report  connecting with hive data warehouse
 *
 */
object RewardPerformanceReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("spark://spark-master:7077")
      .appName("RewardPerformanceReport")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "/users/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    var filterDate = args(0)

    /**
     * In order to prepare the purchase_count  , redeem_count , expiry_count  prepared 3 different datasets according to
     * the event .
     */
    val purchaseData = spark.sql("Select purchase_at as report_date ,reward_id ,brand_id , " +
      "count(distinct user_reward_id) as purchase_count , 0 as redeem_count,0 as expiry_count from  rewards_logs.reward_log  " +
      " where purchase_at = '" + filterDate + "' group by" +
      " purchase_at,reward_id  ,brand_id ")


    val redeemedData = spark.sql("Select redeem_at as report_date ,reward_id  ,brand_id , " +
      " 0 as purchase_count,count(distinct user_reward_id) as redeem_count,0 as expiry_count from  rewards_logs.reward_log  " +
      " where redeem_at =  '" + filterDate + "' group by" +
      " redeem_at,reward_id ,brand_id ")


    val expiredData = spark.sql("Select expiry_at as report_date  ,reward_id ,brand_id , " +
      " 0 as purchase_count,0 as redeem_count,count(distinct user_reward_id) as expiry_count from  rewards_logs.reward_log  " +
      " where expiry_at =  '" + filterDate + "' and redeem_at is null group by" +
      " expiry_at,reward_id ,brand_id ")

    val allData = purchaseData.union(redeemedData).union(expiredData);

    allData.createOrReplaceTempView("all_grouped_data")
    val groupedAllData = spark.sql("select report_date,reward_id,brand_id,sum(purchase_count) as purchase_count" +
      ",sum(redeem_count) as redeem_count,sum(expiry_count) as expiry_count from all_grouped_data group by " +
      "report_date,reward_id,brand_id")

    groupedAllData.createOrReplaceTempView("aggregated_reports_data")
    val joinedAggregatedData = spark.sql("SELECT  report_date,a.reward_id as reward_id,reward_name,b.brand_id,brand_name" +
      ",purchase_count,redeem_count,expiry_count   " +
      "from aggregated_reports_data  a JOIN rewards_logs.brands b  on a.brand_id = b.brand_id " +
      " JOIN rewards_logs.rewards c on a.reward_id = c.reward_id ")

    /**
     * once data is prepared is written to the respective  folder in docker
     */
    joinedAggregatedData
      .coalesce(1)
      .write
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .mode("overwrite").option("header", "true")
      .save("/opt/spark-data/output/reward_report_data/" + filterDate);

    //  .save("hdfs://namenode:9000/data/reward_report_data/"+filterDate);

  }

}
