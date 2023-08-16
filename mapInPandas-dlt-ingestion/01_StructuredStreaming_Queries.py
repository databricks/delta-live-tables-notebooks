# Databricks notebook source
# MAGIC %md 
# MAGIC # 01 Spark Structured Streaming + MapInPandas()
# MAGIC
# MAGIC `This notebook is runnable in any Databricks workspace and was tested on DBR 12.2 LTS cluster runtime.`
# MAGIC
# MAGIC ## Understanding mapInPandas()
# MAGIC
# MAGIC In addition to MapInPandas usage to unpack Tar files, the example code below illustrates these important Databricks concepts:
# MAGIC * Autoloader using `cloudFiles`, which treats a cloud storage location (S3, ADLS, GCS) as a streaming input source with exactly-once ingestion guarantees
# MAGIC * Autoloader using `useNotifications` for [file notification mode](https://docs.databricks.com/ingestion/auto-loader/file-detection-modes.html#file-notification-mode) for performant and scalable handling of large input directories
# MAGIC * Using arbitrary file handling libraries inside a pandas function
# MAGIC
# MAGIC The sample files below are GZipped Tar files, which contain a number of email (.eml) files. The contents of the emails are random content, and were originally created with a Python script. For replication in your environment, see the accompanying script `resources/create_mock_tar_files`.
# MAGIC  

# COMMAND ----------

# MAGIC %fs ls s3://shard-demo-dbfs-root/shard-demo/0/tmp/tar_files/

# COMMAND ----------

import boto3, tarfile, os, traceback, tempfile
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.sql.functions import split, substring
try:
  from collections import Iterator # Older Python versions
except:
  from collections.abc import Iterator # Tested on DBR 13+

# COMMAND ----------

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
            row.drop("source_key").drop("bucket_name")
            try:
                tmp_file_name = tempfile.NamedTemporaryFile().name+".tar.gz"
                s3_client.download_file(bucket_name, source_key, tmp_file_name)

                # Open the .tar.gz file
                with tarfile.open(tmp_file_name, "r:gz") as tar:
                    id_iter = 0
                    for member in tar.getmembers():
                        # Read the file data
                        file_data = tar.extractfile(member).read().rstrip()

                        row["raw_email"] = file_data
                        row["email_id"] = source_key.replace('/','_').replace('.','_')+str(id_iter)
                        id_iter+=1
                        yield row.to_frame().transpose()
                      
                # Clean up local file
                os.remove(tmp_file_name)
            except:
                # Match the expected output format described above:
                row["raw_email"] = traceback.format_exc()
                row["email_id"] = traceback.format_exc()
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

tar_source = "s3://shard-demo-dbfs-root/shard-demo/0/tmp/tar_files/"

data = (
  spark.readStream
      .format('cloudFiles')
      .option('cloudFiles.format', "BINARYFILE")
      .option("cloudFiles.useNotifications", "true")
      .option("cloudFiles.includeExistingFiles", "true")
      .load(tar_source)
      .select("path", "modificationTime", "length")
      .withColumn("bucket_name", split("path",'/').getItem(2))
      .withColumn("source_key", split("path",'/',limit=4).getItem(3))
      .mapInPandas(s3_untar, schema=untar_schema)
      )

display( 
  data.select("path", "modificationTime", "length", "email_id", substring(data.raw_email, 1, 100)) 
  )


# COMMAND ----------


