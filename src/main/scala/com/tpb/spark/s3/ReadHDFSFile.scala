import org.apache.spark.sql.SparkSession

object ReadHDFSFile extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ReadTextFiles")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  println("##spark read text files from a directory into RDD rddFromFileHDFS")
  val rddFromFileHDFS = spark.sparkContext.textFile("hdfs://localhost:9000/user/nagaraju/input/neighbourdata.csv")
  println(rddFromFileHDFS.getClass)
  println("Number of partitins rddFromFileHDFS:"+rddFromFileHDFS.getNumPartitions)
  println("##Get data Using collect  rddFromFileHDFS")
  rddFromFileHDFS.collect().foreach(f => {
    //println(f)
  })

 val csvDF =  spark.read.csv("hdfs://localhost:9000/user/nagaraju/input/neighbourdata.csv")
  println("CSVDF Partions: "+csvDF.rdd.getNumPartitions)

}