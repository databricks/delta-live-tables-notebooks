-- Databricks notebook source
CREATE LIVE VIEW customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/customers.csv", "csv")

-- COMMAND ----------

CREATE LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM json.`/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json`

-- COMMAND ----------

CREATE LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (order_datetime)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT clicked_items, customer_id, customer_name, number_of_line_items, order_datetime, order_number, ordered_products, promo_info, c.*
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
