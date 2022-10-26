-- Databricks notebook source
-- DBTITLE 1,Table 1: contact_data from Salesforce + Enforce Quality Expectations
CREATE LIVE TABLE contact_data (
 CONSTRAINT `id should not be null` EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT `mailing_country should be US` EXPECT (mailing_country = 'United States') ON VIOLATION DROP ROW,
 CONSTRAINT `mailing_geocode_accuracy should be Address` EXPECT (mailing_geocode_accuracy = 'Address') ON VIOLATION DROP ROW
) COMMENT "bronze table properly takes contact data Ingested from salesforce through Fivetran on each sync" TBLPROPERTIES ("quality" = "bronze") AS
SELECT
 *
from
 retail_demo_salesforce.contact;

-- COMMAND ----------

-- DBTITLE 1,Table 2: transactions_data from MySql + Enforce Quality Expectations
CREATE LIVE TABLE transactions_data (
 CONSTRAINT `item_count should be positive value` EXPECT (item_count > 0) ON VIOLATION DROP ROW
 ) COMMENT "bronze table properly takes transaction data Ingested from mysql through Fivetran on each sync" TBLPROPERTIES ("quality" = "bronze") AS
SELECT
 *
from
 mysql_azure_banking_db.transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create history for the dimenstion table Contacts

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_contacts;
APPLY CHANGES INTO
  LIVE.silver_contacts
FROM
  stream(LIVE.contact_data)
KEYS
  (id)
APPLY AS DELETE WHEN
  is_deleted = "true"
SEQUENCE BY
  _fivetran_synced
COLUMNS * EXCEPT
  (is_deleted, _fivetran_synced)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

-- DBTITLE 1,Analytics-ready gold tables
CREATE LIVE TABLE customer_360
COMMENT "Join contact data with transaction data and materialize a live table"
TBLPROPERTIES ("quality" = "gold")
AS SELECT contact.*,
 transactions._fivetran_id,
 transactions.operation,
 transactions.customer_id,
 transactions.transaction_date,
 transactions.id as transaction_id,
 transactions.operation_date,
 transactions.amount,
 transactions.category,
 transactions.item_count,
 transactions._fivetran_index,
 transactions._fivetran_deleted
FROM LIVE.transactions_data as transactions
LEFT JOIN live.silver_contacts as contact ON contact.id = transactions.customer_id;

-- COMMAND ----------

CREATE LIVE TABLE categorized_transactions
COMMENT "Join contact data with transaction data and materialize a gold live table with aggregations"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
 account_id,
 first_name,
 last_name,
 sum(amount) as total_expense,
 transaction_date,
 category
FROM LIVE.customer_360
GROUP BY
 account_id,
 first_name,
 last_name,
 transaction_date,
 category
