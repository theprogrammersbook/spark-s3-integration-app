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

### In order to get your Access Key ID and Secret Access Key follow next steps:

- Open the IAM console.
- From the navigation menu, click Users.
- Select your IAM user name.
- Click User Actions, and then click Manage Access Keys.
- Click Create Access Key.
- Your keys will look something like this:
- Access key ID example: AKIAIOSFODNN7EXAMPLE
- Secret access key example: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
- Click Download Credentials, and store the keys in a secure location.

## How to run the Application.

- RDD Testing:`RDDReadTextFiles.scala`
- DataFrame Testing: `DataFrameReadTextFiles.scala`

### Analysis of the app
 - Removing the following.
   ```` // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId ", "AKIAUWX5UY7A4UWIKEW4")
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "AF+eh+ZAnXX876WjJG944D1HS3AHMFSjVMib4JFK")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
    spark.sparkContext.setLogLevel("ERROR") ````
  
we will get the following error:
 ```Exception in thread "main" java.lang.IllegalArgumentException: AWS Access Key ID and Secret Access Key must be specified as the username or password (respectively) of a s3n URL, or by setting the fs.s3n.awsAccessKeyId or fs.s3n.awsSecretAccessKey properties (respectively).```
 
 - Not mandatory 
   ````
   scala spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com") 
   ````
 - Text file path explanation:
    ````
   s3n://nagaraju-databricks-test1/neighbourhoods.csv
    ````
   - s3n -> Generator (schema(protocal))
   - nagaraju-databricks-test1 -> bucket name
   - neighbourhoods.csv -> file name