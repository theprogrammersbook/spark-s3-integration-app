package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object ReadMobileWriteConsole extends App {

  val spark = SparkSession.builder()
    .appName("ReadMobileData")
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

  import spark.implicits._
  val actionCountDF = mobileSSDF
    .groupBy(window($"ts", "10 minutes").as("minutes"),$"action")
    .count
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
   // .outputMode("complete")
    //.outputMode("append") // with aggregation it is not working
    .outputMode("update")
    .start()

  mobileConsoleSQ.status
  mobileConsoleSQ.lastProgress

  mobileConsoleSQ.awaitTermination()

  mobileConsoleSQ.stop()

  println(spark.sparkContext.getConf.getAppId)//TODO : need to check, shall we able to get sparkContext


}
