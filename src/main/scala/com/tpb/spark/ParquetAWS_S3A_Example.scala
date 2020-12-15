package com.tpb.spark

import org.apache.spark.sql.SparkSession

object ParquetAWS_S3A_Example extends App{



    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("nagaraju-databricks-test1.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIAIZNGMNQNI5HGQEKA")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
   // spark.sparkContext
     // .hadoopConfiguration.set("fs.s3a.path.style.access", "true")

    val data = Seq(("James ","Rose","Smith","36636","M",3000),
      ("Michael","Rose","","40288","M",4000),
      ("Robert","Mary","Williams","42114","M",4000),
      ("Maria","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","1234","F",-1)
    )

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    df.show()
    df.printSchema()

    df.write
      .parquet("s3a://nagaraju-databricks-test1/parquets3a2/people.parquet")


    val parqDF = spark.read.parquet("s3a://nagaraju-databricks-test1/parquets3a2/people.parquet")
    parqDF.createOrReplaceTempView("ParquetTable")

    spark.sql("select * from ParquetTable where salary >= 4000").explain()
    val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

    parkSQL.show()
    parkSQL.printSchema()

    df.write
      .partitionBy("gender","salary")
      .parquet("s3a://nagaraju-databricks-test1/parquets3a2/people2.parquet")

//    val parqDF2 = spark.read.parquet("s3a://nagaraju-databricks-test1/parquet/people2.parquet")
//    parqDF2.createOrReplaceTempView("ParquetTable2")
//
//    val df3 = spark.sql("select * from ParquetTable2  where gender='M' and salary >= 4000")
//    df3.explain()
//    df3.printSchema()
//    df3.show()
//
//    val parqDF3 = spark.read
//      .parquet("s3a://nagaraju-databricks-test1/parquet/people.parquet/gender=M")
//    parqDF3.show()


}
