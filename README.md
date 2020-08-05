### In this project I have tested Spark with S3.

#### Prerequisite 
I am using the following for testing 
- Spark 2.4.6
- Hadoop 2.8.5

Need not to run Hadoop.

### Have created an S3 Instance in Amazon AWS  
  - created one bucket : nagaraju-databricks-test1
  - uploaded a file : neighbourhoods.csv(this file in our project/data folder)

### Application Test:

### Generation	Usage	Description
- First – s3	s3:\\	s3 which is also called classic (s3: filesystem for reading from or storing objects in Amazon S3 This has been deprecated and recommends using either the second or third generation library.
- Second – s3n	s3n:\\	s3n uses native s3 object and makes easy to use it with Hadoop and other files systems. This is also not the recommended option.
- Third – s3a	s3a:\\	s3a – This is a replacement of s3n which supports larger files and improves in performance.

##### Used : s3n  of Amazon

- RDD Testing:`RDDReadTextFiles.scala`
- DataFrame Testing: `DataFrameReadTextFiles.scala`
