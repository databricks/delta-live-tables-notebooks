-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables Twitter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ![Twitter Architecture Diagram](https://github.com/fmunz/twitter-dlt-huggingface/blob/main/markup/twitterstream.jpeg?raw=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC * [jump to Twitter-Stream-S3 notebook]($./Twitter-Stream-S3)
-- MAGIC * [jump to Twitter-SentimentAnalysis notebook]($./Twitter-SentimentAnalysis)
-- MAGIC * [Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/37ff1cdf-0400-4d6d-b22c-b31bb6209a28)

-- COMMAND ----------

-- streaming ingest + schema inference with Auto Loader
CREATE OR REFRESH STREAMING LIVE TABLE bronze
AS SELECT * FROM cloud_files(
  "dbfs:/data/twitter_dais2022", "json"
)

-- COMMAND ----------

-- constraints policies: track #badrecords/ drop record/ abort processing record 
CREATE OR REFRESH STREAMING LIVE TABLE silver 
(CONSTRAINT valid_language EXPECT (lang == "en") ON VIOLATION DROP ROW,
CONSTRAINT valid_id EXPECT (id != "") ON VIOLATION DROP ROW)
COMMENT 'data is cleansed - other languages than EN are dropped'
AS
  SELECT id, geo, lang, text FROM STREAM (LIVE.bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE languages
COMMENT 'table for statistics of different languages
that showed up in the pipeline' 
AS
  SELECT lang, count(*)  AS count FROM STREAM (LIVE.bronze) GROUP BY lang ORDER BY count
