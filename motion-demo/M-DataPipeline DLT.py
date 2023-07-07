# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data-pipeline2.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Streaming IoT Data
# MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/b7144aa9-b6c6-4e56-9300-e86ea7a542cd/updates/7bef333d-4d49-4ba8-89d6-da83c9b63cfd)
# MAGIC * [Realtime Data Analytics with SSS](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/2284724766303631/command/2748961082439221)
# MAGIC * [Self link to repo](https://data-ai-lakehouse.cloud.databricks.com/browse/folders/2234705129664752?o=2847375137997282)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC add picture

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_stream: Ingest a raw event stream from AWS Kinesis 
# MAGIC Talking points: 
# MAGIC * Spark Structured Streaming: similarity between reading stream or batch data
# MAGIC * SSS [project Lightspeed](https://www.databricks.com/blog/2022/06/28/project-lightspeed-faster-and-simpler-stream-processing-with-apache-spark.html)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

STREAM = "dais2023"
awsKey = spark.conf.get("AWS_KEY")
awsSecretKey = spark.conf.get("AWS_SECRET_KEY")


# read AWS Kinesis STREAM

myStream = (spark.readStream
    .format("kinesis")
    .option("streamName", STREAM)
    .option("region", "us-east-1")
    .option("awsAccessKey", awsKey)
    .option("awsSecretKey", awsSecretKey)
    .load()
    )


# no special keyword for streaming tables in Python DLT, just return stream
# table_properties={"pipelines.reset.allowed":"false"} prevents table refresh, table prop must be string
# enable asyncStateStore


reset = "True"
pasync = "True"

@dlt.create_view(comment="data ingested from kinesis stream")
def raw_stream():
  return myStream
    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Enhanced FanOut(EFO) with Kinesis? read this blog...
# MAGIC [Check out this blog on Databricks](https://www.databricks.com/blog/2023/01/31/announcing-support-enhanced-fan-out-kinesis-databricks.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Schema Handling in Spark with Kinesis
# MAGIC
# MAGIC we have to define the schema to get the payload from the AWS Kinesis stream
# MAGIC

# COMMAND ----------


# example event {"magnitude":7,"time":"2023-05-24T18:56:56.800Z","device":"client-61751e743724"}

motion_schema = StructType([ \
    StructField("magnitude", FloatType(),True),             \
    StructField("time", TimestampType(),True),      \
    StructField("device", StringType(),True)        \
  ])
  


# COMMAND ----------

# MAGIC %md 
# MAGIC ### events: with applied schema
# MAGIC use the schema from above to decode JSON from the Kinesis stream

# COMMAND ----------

# temporary table = alias (visible in pipeline but not in data browser, cannot be queried interactively)
@dlt.create_view(comment="data from kinesis payload")
def events():
  return (
    # no automatic schema inference
    # kinesis payload is (partitionKey,data)
    dlt.read_stream("raw_stream")
    .select(col("partitionKey"), 
     from_json(col("data").cast("string"), motion_schema).alias("motion"))
    .select("partitionKey", "motion.*")     
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## sensor: Streaming Table with proper data

# COMMAND ----------

@dlt.table(table_properties={"pipelines.reset.allowed": reset})
@dlt.expect_or_drop("device is set", "device IS NOT NULL")

def sensor():
  return (
    dlt.read_stream("events")
    .select("device", "time", "magnitude").withColumnRenamed("magnitude", "magn") 
    
  )


# COMMAND ----------

# MAGIC %md
# MAGIC ## global_stat: Stats table

# COMMAND ----------

# aggregation of global statistics
# redefined table name


@dlt.table(name="global_stat")
def xyz_global(temporary=False):
  return (
    dlt.read_stream("sensor").groupBy()
    .agg(count('magn').alias('events'),
         max('magn').alias('max'),
         min('magn').alias('min'),
         avg('magn').alias('avg'),
         stddev('magn').alias('stddev')
         )
  )

