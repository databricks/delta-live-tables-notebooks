# Databricks notebook source
import dlt
from pyspark.sql.functions import *

DATA_LOCATION = "dbfs:/customer_json"

@dlt.table
def customer_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(DATA_LOCATION)
        .withColumn("date", col("created_ts").cast("date"))
    )
