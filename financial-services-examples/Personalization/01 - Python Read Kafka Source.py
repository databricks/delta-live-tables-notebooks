# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Delta Live Tables Reading Kafka
# MAGIC 
# MAGIC The code below reads from Kafka topics created in our data producer and writes to DLT-managed tables called `customer_patterns_bronze` and `checking_account_bronze`. 

# COMMAND ----------

import dlt 
from pyspark.sql.functions import *

behavioral_input_path = "/home/fs/banking_personalization/"
behavioral_input_schema = spark.read.format("json").option("multiline", "true").load(behavioral_input_path).schema

kafka_bootstrap_servers_tls = dbutils.secrets.get(scope="oetrta", key="rp_kafka_brokers")

@dlt.table
def customer_patterns_bronze():
  return (
    (spark.readStream
    .format("kafka")
    .option("subscribe", 'purchase_trends')
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
    .option("kafka.security.protocol", "SSL")
    .option("startingOffsets", "earliest")
    .load()).select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), behavioral_input_schema).alias("json"))
  )

# COMMAND ----------

transaction_input_schema = spark.table("banking.checking_account").schema

@dlt.table
def checking_account_bronze():
  return (
    (spark.readStream
    .format("kafka")
    .option("subscribe", 'checking_acct')
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
    .option("kafka.security.protocol", "SSL")
    .option("startingOffsets", "earliest")
    .load()).select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), transaction_input_schema).alias("json"))
  )