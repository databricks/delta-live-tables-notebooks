# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# Change this to your prefer UC volume path
DATA_LOCATION = "/Volumes/main/data/datagen/customers"

@dlt.table
def customer_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(DATA_LOCATION)
        .withColumn("date", col("created_ts").cast("date"))
    )
