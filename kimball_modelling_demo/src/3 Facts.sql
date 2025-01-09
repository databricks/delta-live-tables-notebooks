-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Fact Tables
-- MAGIC Fact tables are the tables in a data warehouse that store quantitative data for analysis. Fact tables contain foreign keys that reference dimension tables, linking the quantitative data to descriptive attributes.
-- MAGIC
-- MAGIC ##### Characteristics
-- MAGIC - **Quantitative Data**: Fact tables store quantitative data for analysis, such as sales amounts, quantities, or transaction counts.
-- MAGIC - **Foreign Keys**: Fact tables contain foreign keys that reference dimension tables, linking the quantitative data to descriptive attributes.
-- MAGIC - **Granularity**: The level of detail in a fact table is defined by its **grain**, which determines the lowest level of information captured (e.g., 1 row per individual transactions, 1 row per customer per day).
-- MAGIC - **Additive Measures**: Measures in fact tables are often additive, meaning they can be summed across dimensions (e.g., total sales by region).
-- MAGIC - **Aggregations**: Fact tables can include pre-aggregated data to improve query performance, such as monthly or yearly totals.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: sales_facts
-- MAGIC ##### Grain: 1 row per order line item
-- MAGIC
-- MAGIC This table contains sales transaction data, capturing detailed information about each sale. It includes keys to related dimension tables, sales metrics, and status flags.
-- MAGIC
-- MAGIC | Column           | Type          | Description                                      |
-- MAGIC |------------------|---------------|--------------------------------------------------|
-- MAGIC | order_num        | BIGINT        | Unique ID of the order                           |
-- MAGIC | line_item_num    | BIGINT        | Line item number within the order                |
-- MAGIC | customer_key     | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key         | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts        | DECIMAL(18,2) | Number of parts sold                             |
-- MAGIC | gross_sales      | DECIMAL(18,2) | Gross sales amount                               |
-- MAGIC | discount         | DECIMAL(18,2) | Discount applied                                 |
-- MAGIC | tax              | DECIMAL(18,2) | Tax applied                                      |
-- MAGIC | net_sales        | DECIMAL(18,2) | Net sales amount after discount and tax          |
-- MAGIC | is_fulfilled     | INT           | Flag indicating if the order is fulfilled (1/0)  |
-- MAGIC | is_returned      | INT           | Flag indicating if the order is returned (1/0)   |
-- MAGIC | order_date_key   | BIGINT        | Foreign key to the order date dimension          |
-- MAGIC | commit_date_key  | BIGINT        | Foreign key to the commit date dimension         |
-- MAGIC | receipt_date_key | BIGINT        | Foreign key to the receipt date dimension        |
-- MAGIC | ship_date_key    | BIGINT        | Foreign key to the ship date dimension           |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sales_facts (
  order_num BIGINT NOT NULL,
  line_item_num INT NOT NULL,
  customer_key BIGINT FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(18,2),
  gross_sales DECIMAL(18,2),
  discount DECIMAL(18,2),
  tax DECIMAL(18,2),
  net_sales DECIMAL(18,2),
  is_fulfilled INT,
  is_returned INT,
  order_date_key BIGINT CONSTRAINT order_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  commit_date_key BIGINT CONSTRAINT commit_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  receipt_date_key BIGINT CONSTRAINT receipt_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  ship_date_key BIGINT CONSTRAINT ship_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  CONSTRAINT sales_facts_pk PRIMARY KEY(order_num, line_item_num)
) AS WITH sales AS (
  SELECT
    o.o_orderkey order_num,
    l.l_linenumber line_item_num,
    IFNULL(o.o_custkey, -1) customer_key,
    IFNULL(l.l_partkey, -1)  part_key,
    l.l_quantity num_parts,
    l.l_extendedprice gross_sales,
    l.l_discount discount,
    l.l_tax tax,
    CASE WHEN l.l_linestatus='F' THEN 1 ELSE 0 END is_fulfilled,
    CASE WHEN l.l_returnflag='R' THEN 1 ELSE 0 END is_returned,
    IFNULL(dbdemos.tpch_kimball.date_key(o.o_orderdate), -1) order_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_commitdate), -1) commit_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_receiptdate), -1) receipt_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_shipdate), -1) ship_date_key
  FROM samples.tpch.lineitem l
  JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey
  WHERE 
    o.o_orderkey IS NOT NULL AND l.l_linenumber IS NOT NULL
)
SELECT
  *,
  dbdemos.tpch_kimball.net_sales(gross_sales, discount, tax, is_returned) net_sales
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: daily_sales_snapshot
-- MAGIC ##### Grain: 1 row per customer, part, and receipt date
-- MAGIC
-- MAGIC This table captures daily aggregated sales data, summarizing the sales activities for each customer and part on a given receipt date. It includes keys to related dimension tables and aggregated sales metrics.
-- MAGIC
-- MAGIC | Column             | Type          | Description                                      |
-- MAGIC |--------------------|---------------|--------------------------------------------------|
-- MAGIC | customer_key       | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key           | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts          | DECIMAL(28,2) | Total number of parts sold                       |
-- MAGIC | num_parts_returned | DECIMAL(38,2) | Total number of parts returned                   |
-- MAGIC | gross_sales        | DECIMAL(28,2) | Total gross sales amount                         |
-- MAGIC | sales_returned     | DECIMAL(38,2) | Total sales amount returned                      |
-- MAGIC | net_sales          | DECIMAL(28,2) | Total net sales amount after returns             |
-- MAGIC | receipt_date_key   | BIGINT        | Foreign key to the receipt date dimension        |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW daily_sales_snapshot (
  customer_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(28,2) NOT NULL,
  num_parts_returned DECIMAL(38,2) NOT NULL,
  gross_sales DECIMAL(28,2) NOT NULL,
  sales_returned DECIMAL(38,2) NOT NULL,
  net_sales DECIMAL(28,2) NOT NULL,
  receipt_date_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  CONSTRAINT daily_sales_snapshot_pk PRIMARY KEY(receipt_date_key, customer_key, part_key)
)
AS SELECT
  d.key AS receipt_date_key,
  IFNULL(s.customer_key, -1) customer_key,
  IFNULL(s.part_key, -1) part_key,
  IFNULL(SUM(s.num_parts), 0) num_parts,
  IFNULL(SUM(s.num_parts * s.is_returned), 0) num_parts_returned,
  IFNULL(sum(s.gross_sales), 0) gross_sales,
  IFNULL(SUM(s.gross_sales * s.is_returned), 0) sales_returned,
  IFNULL(SUM(s.net_sales), 0) net_sales
FROM LIVE.sales_facts s
RIGHT OUTER JOIN dbdemos.tpch_kimball.date_dim d ON s.receipt_date_key = d.key
GROUP BY d.key, customer_key, part_key
ORDER BY receipt_date_key, customer_key, part_key ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: monthly_sales_snapshot
-- MAGIC ##### Grain: 1 row per customer, part, and receipt month
-- MAGIC
-- MAGIC This table captures monthly aggregated sales data, summarizing the sales activities for each customer and part within a given receipt month. It includes keys to related dimension tables and aggregated sales metrics.
-- MAGIC
-- MAGIC | Column             | Type          | Description                                      |
-- MAGIC |--------------------|---------------|--------------------------------------------------|
-- MAGIC | customer_key       | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key           | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts          | DECIMAL(38,2) | Total number of parts sold                       |
-- MAGIC | num_parts_returned | DECIMAL(38,2) | Total number of parts returned                   |
-- MAGIC | gross_sales        | DECIMAL(38,2) | Total gross sales amount                         |
-- MAGIC | sales_returned     | DECIMAL(38,2) | Total sales amount returned                      |
-- MAGIC | net_sales          | DECIMAL(38,2) | Total net sales amount after returns             |
-- MAGIC | reciept_month_key  | BIGINT        | Foreign key to the receipt month dimension       |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW monthly_sales_snapshot (
  customer_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(38,2) NOT NULL,
  num_parts_returned DECIMAL(38,2) NOT NULL,
  gross_sales DECIMAL(38,2) NOT NULL,
  sales_returned DECIMAL(38,2) NOT NULL,
  net_sales DECIMAL(38,2) NOT NULL,
  reciept_month_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.month_dim,
  CONSTRAINT monthly_sales_snapshot_pk PRIMARY KEY(reciept_month_key, customer_key, part_key)
)
AS SELECT
  if(receipt_date_key != -1, receipt_date_key div 100, receipt_date_key) AS reciept_month_key,
  IFNULL(customer_key, -1) customer_key,
  IFNULL(part_key, -1) part_key,
  IFNULL(SUM(num_parts), 0) num_parts,
  IFNULL(SUM(num_parts_returned), 0) num_parts_returned,
  IFNULL(SUM(gross_sales), 0) gross_sales,
  IFNULL(SUM(sales_returned), 0) sales_returned,
  IFNULL(SUM(net_sales), 0) net_sales
FROM LIVE.daily_sales_snapshot
GROUP BY reciept_month_key, customer_key, part_key
ORDER BY reciept_month_key, customer_key, part_key ASC