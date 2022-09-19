# Databricks notebook source
# MAGIC %md 
# MAGIC ## Data Donation Project
# MAGIC * [Create Events](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/3706245661190314)
# MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/4de02bba-a61d-4f9a-bfc7-35f26d8a29a7)
# MAGIC * [Realtime Data Analytics](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/4392583634910141)
# MAGIC * [The Data Donation Project](https://corona-datenspende.de/science/en/reports/longcovidlaunch/)

# COMMAND ----------

# MAGIC %md
# MAGIC ![KafkaStream](https://raw.githubusercontent.com/fmunz/kafka-dlt-streaminganalytics/main/images/kafka.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest a raw event stream from Apache Kafka with Confluent Cloud options
# MAGIC * Spark Structured Streaming: similarity between reading stream or batch data
# MAGIC * Article: Creating [streaming data pipelines with Apache Kafka](https://www.databricks.com/blog/2022/08/09/low-latency-streaming-data-pipelines-with-delta-live-tables-and-apache-kafka.html)
# MAGIC * SSS [project Lightspeed](https://www.databricks.com/blog/2022/06/28/project-lightspeed-faster-and-simpler-stream-processing-with-apache-spark.html)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC = "fitness-tracker"
KAFKA_BROKER = spark.conf.get("KAFKA_SERVER")
confluentApiKey = spark.conf.get("confluentApiKey")
confluentSecret = spark.conf.get("confluentSecret")


# subscribe to TOPIC at KAFKA_BROKER
# ConfluentCloud is using Simple Authentication and Security Layer (SASL)

raw_kafka_events = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
    )

# no special keyword for streaming tables in Python DLT, just return stream

# table_properties={"pipelines.reset.allowed":"false"} prevents table refresh

reset = "true"

@dlt.table(comment="The data ingested from kafka topic",
           table_properties={"pipelines.reset.allowed": reset}
          )
def kafka_events():
  return raw_kafka_events
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##How to consume Kafka messages from Confluent?
# MAGIC [see this blog on the Confluent web site](https://www.confluent.io/blog/consume-avro-data-from-kafka-topics-and-secured-schema-registry-with-databricks-confluent-cloud-on-azure/)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Schema Handling in Spark with Kafka
# MAGIC 
# MAGIC the spark Kafka data source does not allow schema inference, we have to define it

# COMMAND ----------

# define the schema 
# example event: {'ip': '54.65.54.189', 'time': '2022-04-11 12:46:37.476076', 'version': 6, 'model': 'fitbit', 'color': 'navy', 'heart_bpm': 69, 'kcal': 2534} 
event_schema = StructType([ \
    StructField("ip", StringType(),True), \
    StructField("time", TimestampType(),True)      , \
    StructField("version", StringType(),True), \
    StructField("model", StringType(),True)     , \
    StructField("color", StringType(),True)     , \
    StructField("heart_bpm", IntegerType(),True), \
    StructField("kcal", IntegerType(),True)       \
  ])



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Using DLT with Python
# MAGIC use the schema from above to decode JSON from the kafka event

# COMMAND ----------

# temporary table = alias, visible in pipeline but not in data browser, cannot be queried interactively
@dlt.table(comment="real schema for Kakfa payload",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=True)

def bpm_raw():
  return (
    # kafka streams are (timestamp,value) with value containing the kafka payload, i.e. event
    # no automatic schema inference!
    dlt.read_stream("kafka_events")
    .select(col("timestamp"), 
     from_json(col("value").cast("string"), event_schema).alias("event"))
    .select("timestamp", "event.*")     
  )


# COMMAND ----------

# MAGIC %md 
# MAGIC ### cleansed table with expectations, cols renamed

# COMMAND ----------

# @dlt.table(table_properties={"pipelines.reset.allowed":"false"})
@dlt.table()
@dlt.expect_or_drop("heart rate is set", "bpm IS NOT NULL")
@dlt.expect_or_drop("event time is set", "time IS NOT NULL")
@dlt.expect_or_drop("human has pulse >0", "bpm > 0")
@dlt.expect_or_drop("tracker is valid", "model <> 'undef' ")


def bpm_cleansed(table_properties={"pipelines.reset.allowed": reset}):
  return (
    dlt.read_stream("bpm_raw")
    .withColumnRenamed("heart_bpm", "bpm")
    .select("time", "bpm", "model") 
    
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Analytics with DLT

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Global Aggregations (not based on time)

# COMMAND ----------

# aggregation of global statistics
# redefined table name
@dlt.table(name="global_stat")
def bpm_global():
  return (
    dlt.read_stream("bpm_cleansed").groupBy()
    .agg(count('bpm').alias('eventsTotal'),
         max('bpm').alias('max'),
         min('bpm').alias('min'),
         avg('bpm').alias('average'),
         stddev('bpm').alias('stddev')
        )
  )
  
