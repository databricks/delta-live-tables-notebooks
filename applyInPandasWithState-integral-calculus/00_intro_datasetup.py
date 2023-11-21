# Databricks notebook source
# MAGIC %md # 00 Introduction and Data Setup
# MAGIC
# MAGIC `This notebook is runnable in any Databricks workspace and was tested on DBR 14.2 ML cluster runtime.`
# MAGIC
# MAGIC Default settings for this notebook can be found in the `/Resources/config` notebook. You can change these to modify where sample data is written. 

# COMMAND ----------

# MAGIC %run ./resources/config

# COMMAND ----------

# MAGIC %md ## Data Setup
# MAGIC We will use the example code below to create a few sample records. 

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{raw_table};")
spark.sql(f""" 
  CREATE TABLE {schema_name}.{raw_table} (
    location_id STRING, 
    sensor STRING, 
    timestamp TIMESTAMP, 
    value FLOAT
  )
  USING DELTA
  LOCATION '{demo_path}'
""")
print("CREATED TABLE:", schema_name,".",raw_table)

# COMMAND ----------

# Time window 1
input_batch = spark.sql(f"""
    SELECT
      col1 AS location_id,
      col2 AS sensor,
      cast(col3 AS TIMESTAMP) AS timestamp,
      cast(col4 AS FLOAT) as value
    FROM (
      VALUES
      ("L1", "Wind_Speed", "2024-01-01 12:11:00.000000", 10.0),
      ("L1", "Wind_Speed", "2024-01-01 12:12:00.000000", 20.0)
    );  
  """)
display(input_batch)

input_batch.write.format("delta").mode("append").save(demo_path)

# COMMAND ----------


