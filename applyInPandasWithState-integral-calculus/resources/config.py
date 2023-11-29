# Databricks notebook source
#Specify demo path and table variables
demo_path = "dbfs:/tmp/dlt_integrals/"
# Create demo location
dbutils.fs.mkdirs(demo_path)

# Database/schema as the TARGET for DLT pipeline
catalog = "hive_metastore"
schema_name = "demo_dlt_integrals"
raw_table = "raw"

print(f"--> Demo data will be configured in schema {schema_name} and materialized at location {demo_path}")

# COMMAND ----------


