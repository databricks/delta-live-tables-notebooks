# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr
from datetime import datetime, timedelta

database_name = spark.conf.get("snapshot_source_database")
table = "orders_snapshot"
table_name = f"{database_name}.{table}"
snapshot_source_table_name = f"{database_name}.orders_snapshot"

def get_current_timestamp_millis():
  import datetime
  current_datetime = datetime.datetime.now()
  millis = int(current_datetime.timestamp())
  return(millis)

dlt.create_streaming_table(
  name = "orders"
  )
dlt.apply_changes_from_snapshot(
  target = "orders",
  snapshot_and_version = (spark.read.table(snapshot_source_table_name), get_current_timestamp_millis()),
  keys = ["order_id"],
  stored_as_scd_type = 1
)
