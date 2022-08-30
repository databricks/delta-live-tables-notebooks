# Databricks notebook source
# MAGIC %md # DMS MYSQL CDC Analysis

# COMMAND ----------

# MAGIC %md ### Setting Database

# COMMAND ----------

from pyspark.sql import Catalog
catalog = Catalog(spark)
for db in catalog.listDatabases():
    if 'dms_mysql_cdc_demo' in db.name:
        spark.sql(f"use {db.name}")

# COMMAND ----------

# MAGIC %md ### Showing Tables
# MAGIC * Only silver tables are visible, bronze tables are set as temporary in DLT so they don't get registered to the metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md ### Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM stores

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM transactions

# COMMAND ----------

# MAGIC %md ### Looking at SCD Type 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from stores
# MAGIC where store_id in (2, 10, 18)

# COMMAND ----------

# MAGIC %md ### Querying Latest Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW stores_latest AS
# MAGIC SELECT *
# MAGIC FROM stores
# MAGIC WHERE __END_AT IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM stores_latest
