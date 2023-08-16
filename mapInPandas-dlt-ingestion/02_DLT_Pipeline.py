# Databricks notebook source
# MAGIC %md 
# MAGIC #DLT Pipeline to unpack Tar files
# MAGIC
# MAGIC This entire notebook can be configured as the source code for a new Delta Live Tables pipeline. The code below assumes a value has been set for the key `tar_source`, which is the path to the S3 bucket for Tar files.  
# MAGIC
# MAGIC NOTE: this pipeline uses Auto Loader's [File Notification Mode](https://docs.databricks.com/ingestion/auto-loader/file-notification-mode.html#required-permissions-for-configuring-file-notification-for-aws-s3). If you do not already have an SQS provisioned, the DLT cluster must have an appropriate instance profile to provision this as specified in the docs. 

# COMMAND ----------

import boto3, tarfile, os, traceback, tempfile
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.sql.functions import split, substring
try:
  from collections import Iterator # Older Python versions
except:
  from collections.abc import Iterator # Tested on DBR 13+

def s3_untar(itr: Iterator[pd.DataFrame]) -> pd.DataFrame:
    # Cluster must have instance profile associated
    # Otherwise, use Secrets to get S3 access keys
    s3_client = boto3.client("s3")
    # For every DataFrame:
    for df in itr:
        # For every row in the DataFrame:
        for (index, row) in df.iterrows():
            bucket_name = row["bucket_name"]
            source_key = row["source_key"]
            row.drop("source_key").drop("bucket_name") # Drop columns to match return schema
            try:
                # Create temp file, then download tar for unpacking using boto3
                tmp_file_name = tempfile.NamedTemporaryFile().name+".tar.gz"
                s3_client.download_file(bucket_name, source_key, tmp_file_name)

                # Open the .tar.gz file
                with tarfile.open(tmp_file_name, "r:gz") as tar:
                    id_iter = 0 # Counter to track number of emails unpacked
                    # See Python Tarfile library docs for more details on options
                    for member in tar.getmembers():
                        # Read the file data
                        file_data = tar.extractfile(member).read().rstrip()

                        # Use Pandas syntax to set new column values
                        row["raw_email"] = file_data
                        row["email_id"] = source_key.replace('/','_').replace('.','_')+str(id_iter)
                        id_iter+=1

                        #For each email unpacked, emit a new row
                        yield row.to_frame().transpose()
                      
                # Clean up local file
                os.remove(tmp_file_name)
            except:
                # Match the expected return format with errors for ease of debugging
                row["raw_email"], row["email_id"] = traceback.format_exc(), traceback.format_exc()
                yield row.to_frame().transpose()

untar_schema = StructType(
    [
        StructField("path", StringType(), True),
        StructField("modificationTime", TimestampType(), True),
        StructField("length", LongType(), True),
        StructField("email_id", StringType(), True),
        StructField("raw_email", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md ## Overview of MapInPandas() code
# MAGIC
# MAGIC Passed to our function is a list of files to process in the form of an iterator of pandas DataFrames. Using the standard Boto3 library and a tar-specific Python processing library (Tarfile), we’ll unpack each file and yield one return row for every raw email.
# MAGIC
# MAGIC
# MAGIC Let’s look into what each step of this function does (import statements and except clause omitted, but can be found in the demo repo):
# MAGIC
# MAGIC 1. This function is instantiated multiple times across the cluster (one per Spark executor), so we first instantiate our boto3 client. 
# MAGIC   * In this example, the DLT cluster has an associated instance profile with S3 access, so no need to set boto3 authorization. You should use [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html) if setting credentials.
# MAGIC   * If you see errors with Boto3, check your workspaces’ [AWS Instance Metadata Service](https://docs.databricks.com/administration-guide/cloud-configurations/aws/imdsv2.html) settings, and after changing, restart clusters.
# MAGIC 2. Start iterating through rows: our input has one S3 key for a file we need to unpack in each row, so we extract this bucket_name and source_key. These input columns are dropped as we don’t need them in the returned schema.
# MAGIC 3. Now we actually retrieve the tar file from S3 as a temporary file. We use temporary files as this file gets downloaded to each Spark worker’s local disk. This function will execute on each worker core, so unless we can guarantee that file names are unique on source, temporary files help avoid race conditions between instances of this function on each worker.
# MAGIC 4. Now that we have the file, use Python tarfile library to unpack it and read each .eml file inside. Also keep track of which email in this file it was (1 → N)
# MAGIC 5. Yield new rows of data as they are produced. One tar file can emit many rows to our Spark dataframe.
# MAGIC 6. Finally, clean up the temp file 
# MAGIC

# COMMAND ----------

import dlt

@dlt.table(
    table_properties = {
        "pipelines.reset.allowed": "true"
    }
)
def bronze_emails():
  tar_source = spark.conf.get("tar_source")

  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "BINARYFILE")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.includeExistingFiles", "true")
      .load(tar_source)
      .select("path", "modificationTime", "length")
      .withColumn("bucket_name", split("path",'/').getItem(2))
      .withColumn("source_key", split("path",'/',limit=4).getItem(3))
      .mapInPandas(s3_untar, schema=untar_schema)
  )

# COMMAND ----------

# MAGIC %md ## Overview of DLT table definition code
# MAGIC The bulk of our processing logic is contained in the final line, but we use the cloudFiles format and its corresponding options to set up efficient processing of new data files as they arrive in cloud storage. Note that the DLT pipeline requires an instance profile with IAM permissions to setup fileNotification infrastructure on your behalf (e.g. SNS and SQS on AWS). 
# MAGIC
# MAGIC We use the BINARYFILE type as a sort of placeholder, as we’re not actually loading the file contents using this readStream operation (that will happen in the mapInPandas() function). With some simple pattern matching, we can extract the bucket name and file key we’ll need to retrieve each file.
# MAGIC
# MAGIC It is important to note that with this approach, we are simply getting a list of files to process and do not incur the cost of a GET request against each S3 object when we start our stream. Since FileNotification mode uses the ObjectCreated event to trigger our read, and we’re never reading the “content” column of our Spark binary file source (e.g. see line with .select(...) ), our readstream operation does not actually retrieve the files themselves. We’ll rely on the next step for that.
# MAGIC
# MAGIC This works for this particular application because Spark lacks a codec for reading Tar files. However, there is no restriction on using the "content" column returned when reading BinaryFile sources for subsequent processing steps. Just ensure that you appropriately handle the binary content, such as with Python’s io.BytesIO methods.
# MAGIC

# COMMAND ----------

# Example aggregation SQL code to paste in a different notebook, then add to the pipeline.
# %sql
# CREATE OR REFRESH LIVE TABLE bronze_email_count AS
# SELECT COUNT(*) 
# FROM LIVE.bronze_emails

# COMMAND ----------

# MAGIC %md JSON Spec for DLT pipeline used in testing:
# MAGIC ```
# MAGIC {
# MAGIC     "pipeline_type": "WORKSPACE",
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "spark_conf": {
# MAGIC                 "spark.databricks.delta.optimizeWrite.enabled": "false",
# MAGIC                 "spark.databricks.delta.autoCompact.enabled": "false"
# MAGIC             },
# MAGIC             "aws_attributes": {
# MAGIC                 "instance_profile_arn": "arn:aws:iam::...:instance-profile/..."
# MAGIC             },
# MAGIC             "num_workers": 2
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "channel": "CURRENT",
# MAGIC     "photon": true,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/.../02_DLT_Pipeline"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "uncommon_file_formats",
# MAGIC     "edition": "CORE",
# MAGIC     "storage": "dbfs:/pipelines/...",
# MAGIC     "configuration": {
# MAGIC         "tar_source": "s3://.../tar_files/"
# MAGIC     },
# MAGIC     "target": "uncommon_file_formats",
# MAGIC     "data_sampling": false
# MAGIC }
# MAGIC ```

# COMMAND ----------


