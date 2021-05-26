-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv")

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS
--SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_stream/sales_stream.json/", "json", map("cloudFiles.inferColumnTypes", "true"))
SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (order_datetime)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, f.order_datetime, f.order_number, f.ordered_products,
       c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM LIVE.sales_orders_raw f
    LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.sales_orders_cleaned WHERE city = 'Los Angeles'

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.sales_orders_cleaned WHERE city = 'Chicago'
