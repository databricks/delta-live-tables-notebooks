# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Running Light DLT Transformations
# MAGIC 
# MAGIC Delta Live Tables allows users to bring your own libraries and apply within functions applied to tables. We have a brief example below; the heavy lifting of applying CDC changes, which is highly automatable has already been implemented in previous scripts. The dashboard view of the customer is provided as the resolution of this suite of DLT scripts.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.create_table(
  comment="Customer Transaction Tiers",  
  table_properties={
    "quality": "gold"
  }    
)
def customer_engagement():
  return dlt.read("customer_patterns_silver").withColumn("engagement", when(col("number_of_online_txns") <= 2, lit('LOW')).otherwise(when(col("number_of_online_txns").between(2, 10), lit('MEDIUM')).otherwise(lit('HIGH'))))

# COMMAND ----------

@dlt.create_table(
  table_properties={
    "quality":"gold"}
)
def customer_lifetime():
  return dlt.read("customer_patterns_silver_copy").groupBy("customer_id").agg( max(col("datetime_updated")).alias("max_time"), min(col("datetime_created")).alias("min_time")).withColumn("customer_lifetime", round((col("max_time") - col("min_time"))/(1000*60*60*24)))