-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Fitness Tracker: Streaming Data Analytics
-- MAGIC * [Create Events](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/3706245661190314)
-- MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/4de02bba-a61d-4f9a-bfc7-35f26d8a29a7)
-- MAGIC * [Data Donation Project](https://corona-datenspende.de/science/en/reports/longcovidlaunch/)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Global Statistics

-- COMMAND ----------

select *  from heart_stream.global_stat

-- COMMAND ----------

select count(*)  from heart_stream.bpm_cleansed

-- COMMAND ----------

select model, count (*) as c  from heart_stream.bpm_cleansed group by model order by c

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Streaming Data Analytics 
-- MAGIC streaming data: BPM aggregated over x minute [tumbling window](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(spark.readStream.format("delta").table("heart_stream.bpm_cleansed").
-- MAGIC         groupBy("bpm",window("time", "1 minute")).avg("bpm").orderBy("window",ascending=True))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC The data donation project: https://corona-datenspende.de/science/en/reports/longcovidlaunch/ 
