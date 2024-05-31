# Databricks notebook source

import dlt
from pyspark.sql.functions import col, expr
from datetime import datetime, timedelta

database_name = spark.conf.get("snapshot_source_database")
table = "orders_snapshot"
table_name = f"{database_name}.{table}"
snapshot_source_table_name = f"{database_name}.orders_snapshot"

@dlt.view(name="source")
def source():
  return spark.read.table(snapshot_source_table_name)


dlt.create_streaming_table(
  name = "orders"
  )
dlt.apply_changes_from_snapshot(
  target = "orders",
  source="source",
  keys = ["order_id"],
  stored_as_scd_type = 1
)
