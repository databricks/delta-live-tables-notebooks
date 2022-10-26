-- Databricks notebook source
SELECT 
first_name, 
last_name, 
category, 
sum(total_expense) 
FROM Customer360_Database.categorized_transactions 
WHERE category IS NOT null 
GROUP BY first_name, last_name, category;

-- COMMAND ----------

SELECT 
title,
sum(amount) AS total_amt_by_title 
FROM Customer360_Database.customer_360
GROUP BY title 
LIMIT 1000;

-- COMMAND ----------

SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM Customer360_Database.pipeline_logs
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

SELECT 
mailing_city, 
lead_source, 
Avg(item_count) 
FROM Customer360_Database.customer_360
WHERE mailing_city IS NOT null and lead_source IS NOT null
GROUP BY mailing_city, lead_source;
