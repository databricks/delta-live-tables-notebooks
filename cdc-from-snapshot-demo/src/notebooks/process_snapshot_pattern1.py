# Databricks notebook source
import dlt
from datetime import datetime
import datetime

catalog_name = spark.conf.get("snapshot_source_catalog")
database_name = spark.conf.get("snapshot_source_database")
table = "orders_snapshot"
snapshot_source_table_name = f"{catalog_name}.{database_name}.orders_snapshot"


def get_current_timestamp_millis():
    current_datetime = datetime.datetime.now()
    millis = int(current_datetime.timestamp())
    return (millis)


dlt.create_streaming_table(
    name="orders"
)

dlt.apply_changes_from_snapshot(
    target="orders",
    snapshot_and_version=(spark.read.table(snapshot_source_table_name), get_current_timestamp_millis()),
    keys=["order_id"],
    stored_as_scd_type=1
)
