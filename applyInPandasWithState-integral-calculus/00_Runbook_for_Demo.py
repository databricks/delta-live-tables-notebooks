# Databricks notebook source
# MAGIC %md # 00 Runbook for DLT Integrals Demo
# MAGIC
# MAGIC Use this notebook as your guide to running this multi-part demo from end-to-end. We recommend having it open in one tab, then navigating to the other parts of the Databricks workspace in another tab so you can quickly find your way. 
# MAGIC
# MAGIC `This notebook is runnable in any Databricks workspace and was tested on DBR 14.2 ML cluster runtime.`
# MAGIC
# MAGIC Default settings for this notebook can be found in the `/Resources/config` notebook. You can change these to modify where sample data is written. 

# COMMAND ----------

# MAGIC %run ./resources/config

# COMMAND ----------

# MAGIC %md ## 1 Data Setup
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
      -- Key Group 1
      ("L1", "Wind_Speed", "2024-01-01 12:11:00.000000", 10.0),
      ("L1", "Wind_Speed", "2024-01-01 12:12:00.000000", 20.0)
    );  
  """)
display(input_batch)

input_batch.write.format("delta").mode("append").save(demo_path)

# COMMAND ----------

# MAGIC %md ## 2 DLT Transformations Review
# MAGIC
# MAGIC The logic for the pipeline is contained within noteobok `01_DLT_StatefulTimeWeightedAverage` in the same folder as this current notebook. Go check it out now to see the logic involved in this stateful time-weighted average calculation, then return to the cell below.

# COMMAND ----------

# MAGIC %md ## 3 Pipeline Setup
# MAGIC Now that we have some initial data ready and have reviewd the DLT logic that will run, lets setup our Delta Live Tables pipeline. Note: you will need permissions to create a DLT cluster to follow along. If you do not have unlimited cluster creation entitlements, you can ask your admin to create a [minimal DLT cluster policy](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policy-definition#--define-limits-on-delta-live-tables-pipeline-compute)
# MAGIC
# MAGIC On the left-nav, click the `Delta Live Tables` icon under Data Engineering. Then, complete these steps:
# MAGIC 1. At the top of the Delta Live Tables page, click `Create pipeline`
# MAGIC 1. In `Pipeline Name`, provide a name such as `Demo_StatefulTimeWeightedAverage`
# MAGIC 1. Select `Product Edition` as "Core" and `Pipeline Mode` as "Triggered"
# MAGIC 1. Under the `Source Code Paths` selector, navigate and select the notebook `01_DLT_StatefulTimeWeightedAverage`. (You can select multiple notebooks to run as part of the DLT pipeline)
# MAGIC 1. Leave `Destination` selected as "Hive Metastore" (in 2024 this demo will work against "Unity Catalog" as well). 
# MAGIC 1. Leave `Storage Location` blank
# MAGIC 1. Set `Target Schema` as the same schema as the raw data. If you did not change the `resources/config` file, the value should be "demo_dlt_integrals"
# MAGIC 1. Under Compute, select `Cluster mode` as "Fixed Size" and `Workers` as "0" (zero). 
# MAGIC    * This will allow us to observe logs on the driver directly, and is only for demo purposes. In a production setting, you should use "Enhanced Autoscaling".
# MAGIC    * If you need to use a `Cluster policy` provided by your admin, select that here.
# MAGIC 1. Under `Advanced`, add the following key-value pairs to specify the schema and raw table name, these should match what is in the `resources/config` file:
# MAGIC    * `schema_name` = `demo_dlt_integrals`
# MAGIC    * `raw_table` = `raw`
# MAGIC
# MAGIC Once done, click `Create` - you now have a DLT pipeline to run this logic! You can double-check your config matches the screenshot below:
# MAGIC
# MAGIC ![dlt config](https://github.com/tj-cycyota/delta-live-tables-notebooks/blob/main/applyInPandasWithState-integral-calculus/resources/dlt_config.png?raw=true)

# COMMAND ----------

# MAGIC %md ## 4 Initial Pipeline Update
# MAGIC
# MAGIC Now we can process our first records through our DLT pipeline. At the top-right of your newly-create pipeline, click `Start`. This will kick off a DLT update, which is the incremental unit of processing for Triggered pipelines. This first update will process the 2 records that we created earlier in this notebook. You may need to wait a few minutes for the cluster to be provisioned.
# MAGIC
# MAGIC ![dlt update 1](https://github.com/tj-cycyota/delta-live-tables-notebooks/blob/main/applyInPandasWithState-integral-calculus/resources/dlt_update1.png?raw=true)
# MAGIC
# MAGIC Note in this first batch, the second table `dlt_integrals` does not have any records written to it, as our **time interval window for these records has not "closed"**. The pipeline will continue to buffer observations for a given set of keys until the watermark we've specified with `.withWatermark('timestamp_10min_interval','10 minutes')` passes. 

# COMMAND ----------

# MAGIC %md ## 5 Second Pipeline Run
# MAGIC
# MAGIC Let's now insert a few more records to simulate time values arriving. We'll include 2 more values for the initial group, but also several later records to make sure our first time interval closes so we can see the integral calculated for our first set:

# COMMAND ----------

# Time window 2
input_batch = spark.sql(f"""
    SELECT
      col1 AS location_id,
      col2 AS sensor,
      cast(col3 AS TIMESTAMP) AS timestamp,
      cast(col4 AS FLOAT) as value
    FROM (
      VALUES
      -- Key Group 1
      ("L1", "Wind_Speed", "2024-01-01 12:14:00.000000", 40.0),
      ("L1", "Wind_Speed", "2024-01-01 12:19:00.000000", 30.0),
      -- Key Group 2, will close the watermark for Group 1 as its +10 mins after the interval
      ("L1", "Wind_Speed", "2024-01-01 12:40:00.000000", 1.0)  
    );  
  """)
display(input_batch)

input_batch.write.format("delta").mode("append").save(demo_path)

# COMMAND ----------

# MAGIC %md After running the cell above, click `Start` again on your DLT pipeline to start a new triggered update. This simulates a batch process that incrementally processes new data as its available. If your development cluster is still up, this update should process very quickly.  
# MAGIC
# MAGIC Your results should look like the below: 3 new rows were appended to the input table, and we emitted one integral value to the `dlt_integrals` table. 
# MAGIC
# MAGIC ![dlt update 2](https://github.com/tj-cycyota/delta-live-tables-notebooks/blob/main/applyInPandasWithState-integral-calculus/resources/dlt_update2.png?raw=true)
# MAGIC
# MAGIC Taking a closer look, we can click on the Target table hyperlink in the table details, and see a data preview. This is the value **29** we expect, which is the time weighted average of the 4 records in Key Group 1!
# MAGIC
# MAGIC ![dlt update 2 results](https://github.com/tj-cycyota/delta-live-tables-notebooks/blob/main/applyInPandasWithState-integral-calculus/resources/dlt_update2results.png?raw=true)

# COMMAND ----------

# MAGIC %md ## 6 Ramping up data volumes
# MAGIC Now lets add a few more key groups to see how this stateful pipeline handles:
# MAGIC * Records that fall before the current watermark, and should therefore be dropped. These simulate "late-arriving" data
# MAGIC * Multiple state groups for various locations and sensorts
# MAGIC * Out of sequence data
# MAGIC
# MAGIC Notice as well that if we go to the cluster from the DLT UI, we can observe how many records each microbatch is dropping due to the watermark:

# COMMAND ----------

# Time window 3
input_batch = spark.sql(f"""
    SELECT
      col1 AS location_id,
      col2 AS sensor,
      cast(col3 AS TIMESTAMP) AS timestamp,
      cast(col4 AS FLOAT) as value
    FROM (
      VALUES
      -- Key Group 1, this records get dropped by the watermark as its output has already been emitted
      ("L1", "Wind_Speed", "2024-01-01 12:18:00.000000", 35.0),

      -- Key Group 2, the group is still open to new records.
      ("L1", "Wind_Speed", "2024-01-01 12:41:00.000000", 6.0),  

      -- Key Group 3, data is not necessarily in-order in source table
      ("L2", "Oil_Temp", "2024-01-01 12:48:00.000000", 110.0),
      ("L2", "Oil_Temp", "2024-01-01 12:41:00.000000", 100.0),
      ("L2", "Oil_Temp", "2024-01-01 12:45:00.000000", 95.0 ),

      -- Key Group 4, only 1 records, so this values becomes time-weighted over the interval
      ("L3", "Humidity", "2024-01-01 12:50:00.000000", 32.5),

      -- Key Group 5, these records cause the earlier time intervals to expire
      ("L3", "Wind_Speed", "2024-01-01 13:30:00.000000", 50.0)
    );  
  """)
display(input_batch)

input_batch.write.format("delta").mode("append").save(demo_path)

# COMMAND ----------

# MAGIC %md
# MAGIC As before, click `Start` on your DLT pipeline to trigger a new update. Results should look like this:
# MAGIC
# MAGIC ![dlt update 3 ]()
# MAGIC ![dlt update 3 results]()
# MAGIC
# MAGIC We had 5 groups of sample data, but only 3 are finally written out because:
# MAGIC * Key groups 2,3,4 are appended to the `dlt_integrals` table
# MAGIC * Key group 5 is still open, and will continue buffering results until its watermark passes. 
# MAGIC * Key group 1 is dropped. If we go to the cluster from the DLT UI, we can see that one record was dropped due to the watermark. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 7 Production Considerations:
# MAGIC
# MAGIC Even within Delta Live Tables, we need to consider
# MAGIC * Think twice before Full Refresh!
# MAGIC * Switch to Enhanced Autoscaling
# MAGIC * Review Stateful Streaming Best Practices
# MAGIC * Uses RocksDB for State Store
# MAGIC * Think about how to do historical loads
# MAGIC
