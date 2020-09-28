package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object RMDWCWithoutAggregation extends App {

  val spark = SparkSession.builder()
    .appName("RMDWCWithoutAggregation")
    .master("local[*]")
    .getOrCreate()
  // Read the json files

  // Schema to read data
  val mobileDataSchema = new StructType()
    .add("id", StringType, false)
    .add("action", StringType, false)
    .add("ts", TimestampType, false)

  // Reading data
  val mobileSSDF =spark.readStream
      .schema(mobileDataSchema)
    .json("src/main/scala/com/tpb/spark/streaming/chapter6/mobile/input")

  // we have to specify schema for streaming of application. because sometimes, the folder do not contain data at that time spark stream has to
  // create Dataframe so that to create data frame we need schema.

  /*val mobileSSDF =spark.readStream.schema(mobileDataSchema)
    .load("src/main/scala/com/tpb/spark/streaming/chapter6/mobile/input")
  // If without format ,we load spark default take the files as parquet
  */
/*
  //mobileSSDF.show()  // we should not start this. If we start then we will get error:
//Queries with streaming sources must be executed with writeStream.start()
*/

  mobileSSDF.isStreaming
  val actionCountDF = mobileSSDF
    .withColumn("new_date",current_timestamp())
   // .groupBy("id")   Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
    //.count()
  /*
     1. Same data file -- No change in the result
     1.1. If we change the file name then again it is calculating.
     2. Deleted data file -- No Change in the result but after some time ,we will following warning
     20/09/28 15:20:55 WARN InMemoryFileIndex:
     The directory file:/home/nagaraju/Technology/Repository/theprogrammersbook/spark-apps/spark-rdd-dataframe-dataset-examples/src/main/scala/com/tpb/spark/streaming/chapter6/mobile/input/file02.json~ was not found. Was it deleted very recently?
     3. New file added then immediatly, it will be identified.

   */
/*
      Tumbling window: I understand tumbling window is set for an interval and the event's don't overlap and expires at the set time interval
 */

// Existing files are picking.
  val mobileConsoleSQ = actionCountDF
    .writeStream
    .format("console")
   // .trigger(Trigger.ProcessingTime("2 seconds"))// --Got idea , how triggering is happening
    // Without data in input path , trigger is not happening.
    // if new file is not added then trigger will not happen
    // will show the following message something like
    //20/09/28 15:27:11 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 10000 milliseconds, but spent 11057 milliseconds
    .option("truncate", "false")
    //.outputMode("complete")
    /*
    Batch: 0
-------------------------------------------
+------+-----+
|id    |count|
+------+-----+
|phone3|1    |
|phone1|2    |
|phone2|1    |
+------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
|id    |count|
+------+-----+
|phone3|2    |
|phone1|2    |
|phone4|1    |
|phone2|1    |
+------+-----+

     */
    .outputMode("append") // with aggregation it is not working
    /*
    -------------------------------------------
Batch: 0
-------------------------------------------
+------+------+-------------------+-----------------------+
|id    |action|ts                 |new_date               |
+------+------+-------------------+-----------------------+
|phone1|open  |2018-03-02 10:02:33|2020-09-28 16:44:00.659|
|phone2|open  |2018-03-02 10:03:35|2020-09-28 16:44:00.659|
|phone3|open  |2018-03-02 10:03:50|2020-09-28 16:44:00.659|
|phone1|close |2018-03-02 10:04:35|2020-09-28 16:44:00.659|
+------+------+-------------------+-----------------------+

-------------------------------------------
Batch: 1  --- New File Data
-------------------------------------------
+------+------+-------------------+-----------------------+
|id    |action|ts                 |new_date               |
+------+------+-------------------+-----------------------+
|phone3|close |2018-03-02 10:07:35|2020-09-28 16:45:17.407|
|phone4|open  |2018-03-02 10:07:50|2020-09-28 16:45:17.407|
+------+------+-------------------+-----------------------+


     */
    //.outputMode("update")
    /*
    -------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+
|id    |count|
+------+-----+
|phone3|2    |
|phone1|2    |
|phone5|1    |
|phone4|1    |
|phone2|2    |
+------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
|id    |count|
+------+-----+
|phone5|2    |
|phone2|3    |
+------+-----+

     */
    .start()

  mobileConsoleSQ.status
  mobileConsoleSQ.lastProgress

  mobileConsoleSQ.awaitTermination()

  mobileConsoleSQ.stop()


  println(spark.sparkContext.getConf.getAppId)//TODO : need to check, shall we able to get sparkContext


}
