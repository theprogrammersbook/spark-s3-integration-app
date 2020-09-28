package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object ReadMobileFileConsole extends App {

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
/*
  val actionCountDF = mobileSSDF
    //.withWatermark("ts", "1 minutes")
    //.groupBy(window($"ts", "10 minutes").as("minutes"),$"action")
    .count
*/

// Existing files are picking.
  val mobileFileSQ = mobileSSDF
    .writeStream
    .option("path","/tmp/streaming/output")
    //.trigger(Trigger.ProcessingTime("2 seconds"))// --Got idea , how triggering is happening
    .queryName("file-data")
    //.format("console")
    .outputMode("append")//
    //Data source json does not support Update output mode;
    // Data source csv does not support Update output mode;
  //Data source parquet does not support Update output mode;
    //Data source parquet does not support Complete output mode;
   // .format("csv")
    .option("checkpointLocation", "src/main/resources/chkpoint_dir")
    .start()

  mobileFileSQ.awaitTermination()

  mobileFileSQ.stop()


}
