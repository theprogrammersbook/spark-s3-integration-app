package com.tpb.spark.s3.chapter6.code

import org.apache.spark.sql.SparkSession

object RateSourceS3Sink extends App {

  val spark = SparkSession.builder()
    .appName("RateSourceS3Sink")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "s3n://nagaraju-cheking-bucket-name")
    .getOrCreate()
  // Read the json files

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "AKIAJUXWBYIHEKL2KHRQ")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "e1AsnM7Do6N8pMCuHLEqGMxiUK2LHeyTUkvFlVuv")

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")


  // Rate Source
    val inputData = spark.readStream.format("rate")
      .option("rowsPerSecond","10")
      .option("numPartitions","2")
      .load()
    // File Source
  val outputData = inputData.writeStream.format("json")
    .outputMode("append")
    .option("path","s3n://nagaraju-cheking-bucket-name")
    .option("checkpointLocation","s3n://nagaraju-cheking-bucket-name")
    .start()
  // Starting streaming
  outputData.awaitTermination()
  outputData.stop()

}
