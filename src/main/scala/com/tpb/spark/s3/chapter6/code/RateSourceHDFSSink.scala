package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession

object RateSourceHDFSSink extends App {

  val spark = SparkSession.builder()
    .appName("RateSourceHDFSSink")
    .master("local[*]")
    .getOrCreate()
  // Read the json files

    // Rate Source
    val inputData = spark.readStream.format("rate")
      .option("rowsPerSecond","10")
      .option("numPartitions","2")
      .load()
    // File Source
  val outputData = inputData.writeStream.format("json")
    .outputMode("append")
    .option("path","hdfs://localhost:9000/streaming/output")
    .option("checkpointLocation","/tmp/checkpoint")
    .start()
  // Starting streaming
  outputData.awaitTermination()
  outputData.stop()

}
