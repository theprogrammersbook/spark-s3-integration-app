package com.tpb.spark.s3

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameReadTextFiles  extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("DataFrame")
    .getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "paste access key")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "paste secreat key")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
  spark.sparkContext.setLogLevel("ERROR")

  //returns DataFrame
  val df:DataFrame = spark.read.text("s3n://nagaraju-databricks-test1/neighbourhoods.csv")
  df.printSchema()
  df.show(false)
  df.write.text("neighbourprocessed.csv")
/*
  //converting to columns by splitting
  import spark.implicits._
  val df2 = df.map(f=>{
    val elements = f.getString(0).split(",")
    (elements(0),elements(1))
  })

  df2.printSchema()
  df2.show(false)*/
/*
  // returns Dataset[String]
  val ds:Dataset[String] = spark.read.textFile("s3n://nagaraju-databricks-test1/neighbourhoods.csv")
  ds.printSchema()
  ds.show(false)

  //converting to columns by splitting
  import spark.implicits._
  val ds2 = ds.map(f=> {
    val elements = f.split(",")
    (elements(0),elements(1))
  })

  ds2.printSchema()
  ds2.show(false)*/
}
