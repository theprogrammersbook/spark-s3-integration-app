package com.tpb.spark

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object FilesExistanceCheckInS3 extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("FilesExistanceCheckInS3")
    .getOrCreate()
  // Replace Key with your AWS account key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "sf")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "fsfes")

  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
  spark.sparkContext.setLogLevel("ERROR")

// get configuRATION

  //val uri = new URI("s3n:s3.amazonaws.com")
  println("Accessing with URL and then Path")
  val configuration = spark.sparkContext.hadoopConfiguration
  try{
    val fileSystem = FileSystem.get(new URI("s3n://go"),configuration)
   // fileSystem.listStatus(new Path("/NagarajuGajula/")).foreach(println)
  }catch{
    case ex: Exception => {
       println("File not found ::"+ex.printStackTrace())
    }
  }
/*
  if(fileSystem.exists(new Path("/Nagaraju"))){
    println("File exist")
  }else{
    println("Fie not found")
  }*/

  println("Directly accessing ....path is not working ...")
 // val fileSystem2 = FileSystem.get(configuration)
  //fileSystem2.listStatus(new Path("s3n://nagaraju-databricks-test1/Nagaraju/")).foreach(println)


}
