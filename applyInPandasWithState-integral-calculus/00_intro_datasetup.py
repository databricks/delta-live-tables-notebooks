# Databricks notebook source
# MAGIC %md # 00 Introduction and Data Setup
# MAGIC
# MAGIC `This notebook is runnable in any Databricks workspace and was tested on DBR 14.2 ML cluster runtime.`
# MAGIC
# MAGIC Default settings for this notebook can be found in the `/Resources/config` notebook. You can change these to modify where sample data is written. 

# COMMAND ----------

# MAGIC %run ./resources/config

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Understanding applyInPandasWithState()
# MAGIC
# MAGIC Introduced in Apache Spark 3.4.0, [applyInPandasWithState()](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html), allows you to efficiently apply a function written in Pandas to grouped data in Spark while maintaining state. It is conceptually similar to [applyInPandas()](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html), with the added benefit of being able to use it on a Stateful Streaming pipeline with watermarking in place. See this blog for a basic overview of this concept: [Python Arbitrary Stateful Processing in Structured Streaming](https://www.databricks.com/blog/2022/10/18/python-arbitrary-stateful-processing-structured-streaming.html)
# MAGIC
# MAGIC Let’s review the signature of using this function in Spark Structured Streaming :
# MAGIC
# MAGIC ```
# MAGIC def applyInPandasWithState(
# MAGIC     func:             # Takes in Pandas DF, perform any Python actions needed 
# MAGIC     outputStructType: # Schema for the output DataFrame.
# MAGIC     stateStructType:  # Schema for the state variable  
# MAGIC     outputMode:       # Output mode such as "Update" or "Append"
# MAGIC     timeoutConf:      # Timeout setting for when to trigger group state timeout
# MAGIC ) -> DataFrame        # Returns DataFrame
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Use Case Overview
# MAGIC
# MAGIC In this set of examples notebooks, we will build a performant data pipeline to calculate **time-weighted averages** on wind turbine sensor data. See the accompanying blog post for more context on why this is useful, but here we'll focus on the technical details. 
# MAGIC
# MAGIC The input data is an append-only stream of sensor readings, similar to this example:
# MAGIC | location_id | sensor     | timestamp        | value |
# MAGIC |-------------|------------|------------------|-------|
# MAGIC | L1          | Wind_Speed | 2024-01-01T12:11 | 10    |
# MAGIC | L1          | Wind_Speed | 2024-01-01T12:12 | 20    |
# MAGIC | L1          | Wind_Speed | 2024-01-01T12:14 | 40    |
# MAGIC | L1          | Wind_Speed | 2024-01-01T12:19 | 30    |
# MAGIC | L2          | Wind_Speed | 2024-01-01T12:10 | 15    |
# MAGIC | L2          | Oil_Temp   | 2024-01-01T12:12 | 200   |
# MAGIC | ...         | ...        | ...              | ...   |
# MAGIC
# MAGIC Notice that we can have many locations (hundreds or thousands), many sensors (dozens or hundreds), across many incremental time periods that may arrive to our table out of order. 
# MAGIC
# MAGIC Our task then is to write a Python fun that calculates an accurate time-weighted average for each sensor, uniquely per location and time interval. In this computation, we want to use the Riemann sum method to calculate the integral of the measurements, then divide by the total duration of each time interval (10 minutes in this example). 
# MAGIC
# MAGIC Taking the first 4 rows, we define a set of "keys" for this group: 
# MAGIC * `location_id` = `L1`
# MAGIC * `sensor` = `Wind_Speed`
# MAGIC * `time_interval` = `12:10 -> 12:20`
# MAGIC
# MAGIC **Our time-weighted average result should be: 29**
# MAGIC
# MAGIC ![time weighted average ex](./resources/twa_ex.png)
# MAGIC
# MAGIC Detailed calculation steps: 
# MAGIC 1. `10 x 1 min` --> We use the first reading in our interval as a "synthetic" data point, as we do not know the value the last interval ended as each set of state keys are independent
# MAGIC
# MAGIC 1. `10 x 1 min` --> The first value 10 lasts for 1 min, from 12:11 to 12:12
# MAGIC
# MAGIC 1. `20 x 2 min` --> The second value 20 lasts for 2 mins, from 12:12 to 12:14
# MAGIC
# MAGIC 1. `40 x 5 min` --> The third value 40 lasts for 5 mins, from 12:14 to 12:19
# MAGIC
# MAGIC 1. `30 x 1 min` -> The fourth value 30 lasts for 1 min until the end of our interval
# MAGIC
# MAGIC 1. `(10+10+40+200+30)/10 = 29`
# MAGIC
# MAGIC The `applyInPandasWithState()` Spark action will run one instance of our function (`func` in the signature above) on each of the groupings of data for which unique keys exist in that Structured Streamingmicrobatch. 
# MAGIC
# MAGIC In the accompanying notebook, you will see an implementation of this logic. Before we get started, lets setup our example dataset similar to the above!

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


