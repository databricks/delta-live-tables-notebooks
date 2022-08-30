# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

data = spark.conf.get('data')
tables = {
  'stores': {'id':'store_id'},
  'customers': {'id':'customer_id'},
  'products': {'id':'product_id'},
  'transactions': {'id':'transaction_id'}
}

def generate_tables(table, info):
  @dlt.table(
    name=f"{table}_cdc_raw",
    table_properties={ "quality": "bronze"},
    comment=f"Raw MySQL Data from DMS for the table: {table}",
    temporary=True
  )
  def create_call_table():
    stream = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "csv")\
              .option("cloudFiles.inferSchema", "true")\
              .option("cloudFiles.inferColumnTypes", "true")\
              .load(f"{data}/{table}")
    
    if 'Op' not in stream.columns:
      stream = stream.withColumn("Op", lit(None).cast(StringType()))
    
    return stream.withColumn("_ingest_file_name", input_file_name())
  
  dlt.create_streaming_live_table(
    name=f"{table}",
    comment="Silver(Merged) MySQL Data from DMS for the table: {table}"
    )

  dlt.apply_changes(
    target = f"{table}",
    source = f"{table}_cdc_raw",
    keys = [info['id']],
    sequence_by = col("dmsTimestamp"),
    apply_as_deletes = expr("Op = 'D'"),
    except_column_list = ["Op", "dmsTimestamp", "_rescued_data"],
    stored_as_scd_type = 2
  )
    
for table,info in tables.items():
  generate_tables(table,info)
