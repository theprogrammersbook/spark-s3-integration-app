package com.tpb.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDReadTextFiles extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("ReadTextFiles")
    .getOrCreate()
  // Replace Key with your AWS account key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "key")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "secretkey")

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
  spark.sparkContext.setLogLevel("ERROR")

  println("##spark read text files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("s3n://nagaraju-databricks-test1/neighbourhoods.csv")
  println(rddFromFile.getClass)
  println("Number of Partions:"+rddFromFile.getNumPartitions)
  println("##Get data Using collect")
  rddFromFile.collect().foreach(f => {
   // println(f)
  })


  println("##spark read text files from a directory into RDD rddFromFileHDFS")
  val rddFromFileHDFS = spark.sparkContext.textFile("hdfs://localhost:9000/user/nagaraju/input/neighbourdata.csv")
  println(rddFromFileHDFS.getClass)
  println("Number of partitins rddFromFileHDFS:"+rddFromFileHDFS.getNumPartitions)
  println("##Get data Using collect  rddFromFileHDFS")
  rddFromFileHDFS.collect().foreach(f => {
    //println(f)
  })



  println("##read whole text files")
  val rddWhole:RDD[(String,String)] = spark.sparkContext.wholeTextFiles("s3n://nagaraju-databricks-test1/neighbourhoods.csv")
  println(rddWhole.getClass)
  rddWhole.foreach(f=>{
   // println(f._1+"=>"+f._2)
  })

}
