**When we run the Application with wrong username and password**

- The AWS Access Key Id you provided does not exist in our records. (Service: Amazon S3; Status Code: 403; Error Code: InvalidAccessKeyId; Request ID: 0781F5E103B3BA09; S3 Extended Request ID

**When we give same path name for saving data then**
- path s3a://nagaraju-databricks-test1/parquets3a2/people.parquet already exists.;

**When we have different versions of hadoop client , hadoop command , hadoop aws then we will get something related to the following .**

- Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/hadoop/fs/store/EtagChecksum
   ```
            <dependency>
                  <groupId>org.apache.hadoop</groupId>
                  <artifactId>hadoop-common</artifactId>
                  <version>3.0.0</version>
              </dependency>
      
              <dependency>
                  <groupId>org.apache.hadoop</groupId>
                  <artifactId>hadoop-client</artifactId>
                  <version>3.0.0</version>
              </dependency>
              <dependency>
                  <groupId>org.apache.hadoop</groupId>
                  <artifactId>hadoop-aws</artifactId>
                  <version>3.1.1</version>
              </dependency>
  ```
- In the above hadoop-aws has different version which is : 3.1.1 . That is the reason,we are getting the above
exception.

****Correct dependencies are****

``` 
       <dependency>
               <groupId>org.apache.hadoop</groupId>
               <artifactId>hadoop-common</artifactId>
               <version>3.0.0</version>
           </dependency>
           <dependency>
               <groupId>org.apache.hadoop</groupId>
               <artifactId>hadoop-client</artifactId>
               <version>3.0.0</version>
           </dependency>
           <dependency>
               <groupId>org.apache.hadoop</groupId>
               <artifactId>hadoop-aws</artifactId>
               <version>3.0.0</version>
           </dependency>
```
