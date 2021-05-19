# Databricks notebook source
# 
# Synthetic Retail Dataset
# Data Set Information
# ====================
# * Sales Orders: **sales_orders/sales_orders.json** records the customers' originating purchase order.
# * Products: **products/products.csv** contains products that the company sells.
# * Customers: **customers/customers.csv** contains those customers who are located in the US and are buying the finished products.

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

#
# Import 
#
from pyspark.sql.functions import *
from pyspark.sql.types import *

@create_view(name="products_master")
def products_master():
    return spark.read.csv('/databricks-datasets/retail-org/products/products.csv', header=True, sep=";", multiLine=True)

@create_view(name="customers_master")
def customers_master():
    return spark.read.csv('/databricks-datasets/retail-org/customers/customers.csv', header=True)
  
@create_view(name="sales_orders_raw")
def sales_orders_raw():
    return spark.read.json('/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json')

@create_table(
    name="sales_orders_bronze",
    partition_cols=["order_datetime"]
)
@expect_or_drop("valid order_number", "order_number IS NOT NULL")
def sales_orders_blonze():
    return read_stream("sales_orders_raw")
  
@create_table(name="expensive_products")
def expensive_products():
     # The silver table is updated incrementally.
    return read_stream("products_master").where("sales_price > 300")  

@create_table(name="sales_orders_silver")
def sales_orders_silver():
    return read_stream("sales_orders_bronze").join(read("customers_master"), ["customer_id", "customer_name"], "left")

@create_table(name="sales_order_in_la")
def sales_order_in_la():
    return read("sales_orders_silver").where("city == 'Los Angeles'")  
  
@create_table(name="sales_order_in_chicago")
@expect_or_fail("order_number not null", "order_number IS NOT NULL")
def sales_order_in_chicago():
    return read("sales_orders_silver").where("city == 'Chicago'")    

