-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT pipeline log analysis
-- MAGIC 
-- MAGIC Please Make sure you specify your own Database and Storage location. You'll find this information in the configuration menu of your Delta Live Table Pipeline.
-- MAGIC 
-- MAGIC **NOTE:** Please use Databricks Runtime 9.1 or above when running this notebook

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text('storage_location', '/home/PipelineStorage')
-- MAGIC dbutils.widgets.text('latest_update_id', 'Update_ID_fromUpdateDetails')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(dbutils.widgets.get('storage_location')))

-- COMMAND ----------

CREATE OR REPLACE Table Customer360_Database.pipeline_logs
AS SELECT * FROM delta.`${storage_location}/system/events`;

SELECT * FROM Customer360_Database.pipeline_logs
ORDER BY timestamp;

-- COMMAND ----------

-- DBTITLE 1,Data Quality Metrics
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
             , schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records': 42, 'failed_records': 42}]"))) expectations
  FROM Customer360_Database.pipeline_logs   -- retail-demo-db
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

-- DBTITLE 1,Runtime information of the Latest Pipeline Update
SELECT details:create_update:runtime_version:dbr_version FROM Customer360_Database.pipeline_logs WHERE event_type = 'create_update' LIMIT 1;

-- COMMAND ----------

-- DBTITLE 1,Cluster performance metrics
SELECT
  timestamp,
  Double(details :cluster_utilization.num_executors) as current_num_executors,
  Double(details :cluster_utilization.avg_num_task_slots) as avg_num_task_slots,
  Double(
    details :cluster_utilization.avg_task_slot_utilization
  ) as avg_task_slot_utilization,
  Double(
    details :cluster_utilization.avg_num_queued_tasks
  ) as queue_size,
  Double(details :flow_progress.metrics.backlog_bytes) as backlog
FROM
  Customer360_Database.pipeline_logs
WHERE
  event_type IN ('cluster_utilization', 'flow_progress')
  AND origin.update_id = '${latest_update_id}'
  ORDER BY timestamp ASC; 
