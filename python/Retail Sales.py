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


# COMMAND ----------

@dlt.create_table(
  comment="The raw sales orders, ingested from /databricks-datasets.",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_orders_raw():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.schemaLocation", "/tmp/john.odwyer/pythonsalestest") \
      .option("cloudFiles.format", "json") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("/databricks-datasets/retail-org/sales_orders/")
  )

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned sales orders with valid order_number(s) and partitioned by order_date",
  partition_cols=["order_date"],
  table_properties={
    "myCompanyPipeline.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid order_number", "order_number IS NOT NULL")
def sales_orders_cleaned():
  df = dlt.read_stream("sales_orders_raw").join(dlt.read("customers"), ["customer_id", "customer_name"], "left")
  df = df.withColumn("order_datetime", from_unixtime(df.order_datetime).cast("TIMESTAMP")) 
  df = df.withColumn("order_date", df.order_datetime.cast("DATE")) 
  df = df.select("customer_id", "customer_name", "number_of_line_items", "order_datetime", "order_date",
    "order_number", "ordered_products", "state", "city", "lon", "lat", "units_purchased", "loyalty_segment")
  return df


# COMMAND ----------

@dlt.create_table(
  comment="Aggregated sales orders in LA",
  table_properties={
    "myCompanyPipeline.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_order_in_la():
  df = dlt.read_stream("sales_orders_cleaned").where("city == 'Los Angeles'") 
  df = df.select(df.city, df.order_date, df.customer_id, df.customer_name, explode(df.ordered_products).alias("ordered_products_explode"))

  dfAgg = df.groupBy(df.order_date, df.city, df.customer_id, df.customer_name, df.ordered_products_explode.curr.alias("currency"))\
    .agg(sum(df.ordered_products_explode.price).alias("sales"), sum(df.ordered_products_explode.qty).alias("quantity"))

  return dfAgg


# COMMAND ----------

@dlt.create_table(
  comment="Sales orders in Chicago",
  table_properties={
    "myCompanyPipeline.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def sales_order_in_chicago():
  df = dlt.read_stream("sales_orders_cleaned").where("city == 'Chicago'") 
  df = df.select(df.city, df.order_date, df.customer_id, df.customer_name, explode(df.ordered_products).alias("ordered_products_explode"))

  dfAgg = df.groupBy(df.order_date, df.city, df.customer_id, df.customer_name, df.ordered_products_explode.curr.alias("currency"))\
    .agg(sum(df.ordered_products_explode.price).alias("sales"), sum(df.ordered_products_explode.qty).alias("quantity"))

  return dfAgg
