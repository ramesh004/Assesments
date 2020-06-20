#Created Date : 01-05-2020
Started time :01-05-2020 1:30PM
End  time : 01-05-2020   03:30 PM
#File name : Sapient_assessment.scala
#Author :Ramesh Thamizhselvan
#----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------
//********************************************************************************************************************



											//******QUESTION 1 **********//

//Written in python 

infilename="C:\\Users\\tamilselvan\\Music\\sapient_dataset.txt"
dict1={}
for line in open(infilename, "r"):
    column = line.split("\t")
    if (column[0]+column[1] not in dict1) :
        dict1[column[0]+column[1]]=column
        print(column)
        


//********************************************************************************************************************


//********************************************************************************************************************

											//******QUESTION 2 **********//

import java.io.File


import org.apache.spark.sql.{Row, SaveMode, SparkSession}



//Setting the ware house directory
val warehouseLocation = new File("spark-warehouse").getAbsolutePath

//creating the spark session 
val spark = SparkSession
  .builder()
  .appName("clickstream_activity")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

	
import spark.implicits._
import spark.sql


//Creating the Table
sql(" CREATE TABLE IF NOT EXISTS db_name.clickstream_activity  ( `clickTime` String, `user_id` String,) USING hive")


//Loading the data into the table	
sql("LOAD DATA LOCAL INPATH 'path/clickstream_activity_data.txt' INTO TABLE db_name.clickstream_activity")
	
val tmo1: Long = (60 * 60)/2    //30minutes
val tmo2: Long = 2 * 60 * 60     //2 hours


val win1 = Window.partitionBy("user_id").orderBy("click_time")

//Reading the data
val userActivity= sql("SELECT * FROM clickstream_activity").show()

val newDF=clickDF.withColumn("clickTimestamp",unix_timestamp($"clickTime", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType).as("timestamp")).drop($"clickTime")  


//Parsing as a unix timestamo and creating a new column 
val df1 = userActivity.
  withColumn("ts_diff", unix_timestamp($"clickTimestamp") - unix_timestamp(
    lag($"clickTimestamp", 1).over(win1))
  ).
  withColumn("ts_diff", when(row_number.over(win1) === 1 || $"ts_diff" >= tmo1, 0L).
    otherwise($"ts_diff")
  )


//Aggregating
val df2 = df1.
  groupBy("user_id").agg(
    collect_list($"clickTimestamp").as("click_list"), collect_list($"ts_diff").as("ts_list")
  ).
  withColumn("click_sess_id",
    explode(clickSessList(tmo2)($"user_id", $"click_list", $"ts_list"))
  ).
  select($"user_id", $"click_sess_id._1".as("clickTimestamp"), $"click_sess_id._2".as("sess_id"))



df2.show	
	

//Writing the files into the parquet format
df2.write.option('mode','append').parquet("output/clickstream_activity.parquet")
	
//********************************************************************************************************************
	
	
	
	
	

//********************************************************************************************************************
	
	
	
												//******QUESTION 3 **********//

import java.io.File


import org.apache.spark.sql.{Row, SaveMode, SparkSession}



//Setting the ware house directory
val warehouseLocation = new File("spark-warehouse").getAbsolutePath

//creating the spark session 
val spark = SparkSession
  .builder()
  .appName("clickstream_activity")
  .config("spark.sql.warehouse.dir", warehouseLocation)    
   .config("hive.exec.dynamic.partition.mode", "true")    
  .config("hive.exec.dynamic.partition.mode", "nonstrict")    
   .config("hive.exec.max.dynamic.partitions.pernode", "1000")   
  .config("hive.enforce.bucketing", "true")
  .enableHiveSupport()
  .getOrCreate()

	
import spark.implicits._
import spark.sql


import org.apache.spark.sql.expressions.Window
val window = Window.orderBy("clickTimestamp")


val newDF = spark.read.parquet("output/clickstream_activity.parquet")


val leadCol = lead(col("clickTimestamp"), 1).over(window)
val withLeadCol=newDF.withColumn("LeadCol", leadCol)
withLeadCol.select("LeadCol").show()
	
withLeadCol.createOrReplaceTempView("tbl")	

val aggregated_Data = spark.sql("select user,session_id, sum(unix_timestamp(LeadCol, "yyyy-MM-dd'T'HH:mm:ss'Z'") - unix_timestamp(clickTimestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'"))over(partition by user) as time_spent_in_a_day, 
sum(unix_timestamp(LeadCol, "yyyy-MM-dd'T'HH:mm:ss'Z'") - unix_timestamp(clickTimestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'"))over(partition by dayofmonth(clickTimestamp)) as timespent_over_a_month,count( user )over( partition by cast(clickTimestamp as date)) as num_ses_gen_by_day
from tbl")	
	
	
spark.sql("CREATE TABLE click_stream_user_logs( session_id string,aggregated_Data string,time_spent_in_a_day INT,  timespent_over_a_month INT, num_ses_gen_by_day  INT, clickTimestamp string ,user string  )
        COMMENT 'A bucketed sorted user table'
        PARTITIONED BY (user )
        CLUSTERED BY (clickTimestamp) INTO 10 BUCKETS
        STORED AS SEQUENCEFILE  USING hive"  )	
	
spark.sql("INSERT OVERWRITE TABLE click_stream_user_logs PARTITION (user)   select session_id,aggregated_Data,time_spent_in_a_day,timespent_over_a_month, num_ses_gen_by_day, clickTimestamp,user")


//********************************************************************************************************************
