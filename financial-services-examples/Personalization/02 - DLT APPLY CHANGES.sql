-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 
-- MAGIC # Running DLT APPLY CHANGES for Change Data Capture
-- MAGIC 
-- MAGIC Delta Live Tables offers a high-level API for handling Change Data Capture data. In particular, since events may arrive out of order or in cases of late arriving data, the DLT API handles both of these scenarios by specifying a sequence column. The `APPLY CHANGES` API provides by SCD Type 1/2 views; here, we are simply updating out target analytics store with the most recent updates from customer transactions and purchasing trends.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customer_patterns_silver_copy
(
  CONSTRAINT customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT json.payload.after.* , json.payload.op
FROM stream(live.customer_patterns_bronze)
;

-- COMMAND ----------

CREATE STREAMING LIVE TABLE  customer_patterns_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customer behavior";

-- COMMAND ----------

APPLY CHANGES INTO live.customer_patterns_silver
FROM stream(live.customer_patterns_silver_copy)
  KEYS (customer_id)
  APPLY AS DELETE WHEN op = "d"
  SEQUENCE BY datetime_updated --primary key, auto-incrementing ID of any kind that can be used to identity order of events, or timestamp  COLUMNS * EXCEPT (op, datetime_updated);

-- COMMAND ----------

 CREATE STREAMING LIVE TABLE checking_account_silver_copy
 (
   CONSTRAINT customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
 )
 TBLPROPERTIES ("quality" = "silver")
 COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
 AS SELECT json.*
 FROM stream(live.checking_account_bronze)
 ;

-- COMMAND ----------

 CREATE STREAMING LIVE TABLE  checking_accounts_silver
 TBLPROPERTIES ("quality" = "silver")
 COMMENT "Clean, merged customer behavior" 
 as select * from stream(live.checking_account_silver_copy)