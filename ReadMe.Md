# Reward Log Processor
- Tech
- Approach
- Docker

## Tech
- JDK 1.8
- maven 3.8.1
- Scala 2.12.10
- Hadoop-hive-spark (Docker container)

## Approach
This spark application reads the data from the respective directory & processes the rewards log data and creates data warehouse  schema and categorises the data  in fact and dimension tables, ETL process for the same is implemented in which spark job pushes the data into Hadoop system and Hive external tables are used to read the data .
Once processing of Upstream consecutive data is completed , two types  of reports are generated using spark jobs which connects with Hive data warehouse and then builds the report for the specific date .

All these components are weaved together using  dockerisation.


STEP 1
- Reward Data :
  Read the raw rewards data , and convert the same in schema based data categorising the Fact & Dimensions tables  .

         Fact table : reward_logs (user_reward_id , user_id , brand_id ,brand_name ,reward_id ,reward_name ,purchase_at ,redeem_at , expired_at)

            |user_reward_id| user_id|brand_id|reward_id| purchase_at| redeem_at|expired_at|
            +--------------+--------+--------+----------+---------+-----------+-----------+
            |      9D05D344|8f7a25f1|      03| 4C0DB2D9| 2024-02-01|2024-02-02|2024-02-06|
            |      61B320C8|52c356ab|      04| D6EB9B81| 2024-02-01|2024-02-02|2024-02-08|
            |      7E6F41A8|647b00ae|      04| D6EB9B81| 2024-02-02|      null|2024-02-08|
            +--------------+--------+--------+----------+---------+-----------+-----------+
        
        Dimension
         brands (brand_id,brand_name)
            
              |brand_id|brand_name|
              |   03   |    btg   |
              +--------+-----------
            
         rewards(reward_id,reward_name)

              |reward_id |reward_name|
              -----------|-------------
              |4C0DB2D9 ,| btg_rw001  |
              |D6EB9B81, |gu_rw001    |
              +-----------------------+
 Push the same data to HDFS system , and then external tables can be created  map the data in Hive warehouse .


STEP 2
-  Two spark jobs have been implemented to generate the following reports .
    1. Reward Performance Report : RewardPerformanceReport
       In order to prepare  the reward performance report , using the above schema for the respective date prepare metrics like purchase_count , redeem_count , expired_count . 
            - Sample output is present in cd mindenai/data/output/reward_report_data/2024-02-05

    2. Reward Expiry Reminder : RewardExpiryReminder
       In order to prepare  the Reward Expiry report , using the above schema for the respective date prepared metrics like   expired_reward_count
           - Sample output is present in cd mindenai/data/output/reward_expiry_reminder/2024-02-05


Below are the steps to Build and execute the project .

## Build
  ```sh 
1. cd mindenai
```  
```sh 
2. cd assignment
```  
```sh
3.  mvn clean install -DskipTests
```
```sh
4. cd target/
``` 
- In step 4  this  {}/apps path will be mapped as volume -> /opt/spark-apps in docker

```sh
5.  cp reward_log_ingestor-1.0-SNAPSHOT-jar-with-dependencies.jar ../../apps 
``` 
- In step 5  this  {}/data path will be mapped as volume -> /opt/spark-data in docker , as this folder contains rawlogs , or you can add more data here .
 ```sh
6. cd ../../data/
``` 
-  Run Docker following below section (Docker)
  ```sh
   7. cd ..
      docker-compose up -d
``` 

- After docker is Up  , run the below command
 ```sh    
  8.  Raw Reward Log Processor
      
      docker  exec -it   spark-master /spark/bin/spark-submit --deploy-mode client  --master spark://spark-master:7077  --num-executors 1 --total-executor-cores 1 --executor-cores 1 --executor-memory 1g --driver-memory 1g --class processor.RewardLogProcessor  /opt/spark-apps/reward_log_ingestor-1.0-SNAPSHOT-jar-with-dependencies.jar
 ```  
- After running the above command , data will be populated in HDFS so that the same can be used by Hive warehouse , once the job is sucessfull you can view the data inside the docker.
```sh 
   9. Checking the data
   
    1. docker exec -it namenode bash
    2. hadoop fs -ls /data/reward_data
```
- Map the Hadoop data with external tables in Hive with below steps .
```sh 
10. Mapping Data with hive
    1. docker exec -it hive-server bash
    2. beeline -u jdbc:hive2://localhost:10000 -n root
    3. !connect jdbc:hive2://127.0.0.1:10000 scott tiger
    4. create database rewards_logs;
    5. CREATE EXTERNAL TABLE IF NOT EXISTS reward_log(
                           user_reward_id CHAR(200)
                          ,user_id CHAR(200)
                          ,brand_id CHAR(200)
                          ,reward_id CHAR(200)
                          ,purchase_at Date
                          ,redeem_at date
                          ,expiry_at date
                        )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    location '/data/reward_data/fact/processed/'
                    TBLPROPERTIES ("skip.header.line.count"="1");
    
    6. CREATE EXTERNAL TABLE IF NOT EXISTS brands(
                    brand_id CHAR(200)
                    ,brand_name CHAR(200)
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                location '/data/reward_data/dimension/brands/'
                TBLPROPERTIES ("skip.header.line.count"="1");
                ;
                
    7. CREATE EXTERNAL TABLE IF NOT EXISTS rewards(
            reward_id CHAR(200)
            ,reward_name CHAR(200)
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        location '/data/reward_data/dimension/rewards/'
        TBLPROPERTIES ("skip.header.line.count"="1");
```

11. Now run the reporting creator jobs with the date argument.
```shell
      1. Reward Performance Report
         
         docker  exec -it   spark-master /spark/bin/spark-submit --deploy-mode client  --master spark://spark-master:7077 --num-executors 1 --total-executor-cores 1 --executor-cores 1 --executor-memory 1g --driver-memory 1g --class reports.RewardPerformanceReport /opt/spark-apps/reward_log_ingestor-1.0-SNAPSHOT-jar-with-dependencies.jar 2024-02-05
         
      2. Reward Expiry Reminder
        
         docker  exec -it   spark-master /spark/bin/spark-submit --deploy-mode client  --master spark://spark-master:7077 --num-executors 1 --total-executor-cores 1 --executor-cores 1 --executor-memory 1g --driver-memory 1g --class reports.RewardExpiryReminder  /opt/spark-apps/reward_log_ingestor-1.0-SNAPSHOT-jar-with-dependencies.jar 2024-02-05
```

- output folder will be created in the {}/data/output which is the volume mapped in step 5 and will contain 2 csv reports under the same
 ```sh   
9. cd data/output
        1. cd reward_report_data 
        2. cd ../reward_expiry_reminder
 ```      
## Docker

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create the following containers:

container|Exposed ports
---|---
spark-master| 8080
spark-worker-1|8081

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites
* Docker installed
* Docker compose  installed

## Run the docker-compose
The final step to create your test cluster will be to run the compose file:
```sh
docker-compose up -d
```

## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master
http://localhost:8080/

### Spark Worker 1
http://localhost:8081/

### Namenode Hadoop UI
http://localhost:9870/explorer.html#/data



# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart ,
this will help to make changes to the jar from outside the container  as well as changes to transaction data .

local Mount|Container Mount|Purposse
---|---|---
{}/apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
{}/data|/opt/spark-data| Used to make available your app's data on all workers & master
{}/data/output|/opt/spark-data/output| Used to make available your app's data from container to local machine
