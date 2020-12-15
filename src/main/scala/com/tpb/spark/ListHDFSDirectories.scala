package com.tpb.spark

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object ListHDFSDirectories extends App {
      val spark = SparkSession.builder().
        appName("ListHDFSDirectories")
        .master("local")
        .getOrCreate()

     // get Hadoop configuration
    val configuration = spark.sparkContext.hadoopConfiguration
  val uri = new URI("hdfs://localhost:9000")
  val fileSystem =  FileSystem.get(uri,configuration)
  val hdfspath = "/user/nagaraju/"
  println("Printing all")
  fileSystem.listStatus(new Path(s"${hdfspath}")).map(_.getPath).foreach(println)
  println("Printing all the Directories")
  fileSystem.listStatus(new Path(s"${hdfspath}")).filter(_.isDirectory).map(_.getPath).foreach(println)
}
