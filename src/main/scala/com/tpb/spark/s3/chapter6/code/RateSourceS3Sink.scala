package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession

object RateSourceS3Sink extends App {

  val spark = SparkSession.builder()
    .appName("RateSourceS3Sink")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "s3n://")
    .getOrCreate()
  // Read the json files

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "dddd")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "dffffff")

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")


  // Rate Source
    val inputData = spark.readStream.format("rate")
      .option("rowsPerSecond","10")
      .option("numPartitions","2")
      .load()
    // File Source
  val outputData = inputData.writeStream.format("json")
    .outputMode("append")
    .option("path","s3n:/")
    .option("checkpointLocation","s3n://nag")
    .start()
  // Starting streaming
  outputData.awaitTermination()
  outputData.stop()

}
