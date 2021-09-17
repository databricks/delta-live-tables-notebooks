# Databricks notebook source
# 
# Synthetic Retail Dataset
# Data Set Information
# ====================
# * Sales Orders: **sales_orders/sales_orders.json** records the customers' originating purchase order.
# * Customers: **customers/customers.csv** contains those customers who are located in the US and are buying the finished products.
#

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

@dlt.create_view(
  comment="The customers buying finished products, ingested from /databricks-datasets."
)
def customers():
  return spark.read.csv('/databricks-datasets/retail-org/customers/customers.csv', header=True)
  
@dlt.create_table(
  comment="The raw sales orders, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_orders_raw():
  return (
    spark.read.option("inferSchema", "true").json('/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json')
  )

  
@dlt.create_table(
  comment="The cleaned sales orders with valid order_number(s) and partitioned by order_datetime",
  table_properties={
    "quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid order_number", "order_number IS NOT NULL")
def sales_orders_cleaned():
    return dlt.read_stream("sales_orders_raw").join(dlt.read("customers"), ["customer_id", "customer_name"], "left")


  
@dlt.create_table(
  comment="Sales orders in LA",
  table_properties={
    "quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_order_in_la():
  return dlt.read_stream("sales_orders_cleaned").where("city == 'Los Angeles'")  

  
@dlt.create_table(
  comment="Sales orders in Chicago",
  table_properties={
    "quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_order_in_chicago():
  return dlt.read_stream("sales_orders_cleaned").where("city == 'Chicago'")