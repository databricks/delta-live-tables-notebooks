# Databricks notebook source
# MAGIC %md # Delta Live Tables - Monitoring  
# MAGIC   
# MAGIC Each DLT Pipeline stands up its own events table in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/dlt_end_to_end_flow.png"/>

# COMMAND ----------

# MAGIC %md ## 01 - CONFIG 

# COMMAND ----------

dbutils.widgets.removeAll()
# -- REMOVE WIDGET old

# COMMAND ----------

dbutils.widgets.text('storage_path','/tmp/dlt_cdc')

# COMMAND ----------

# MAGIC %md ## 02 - SETUP 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS demo_cdc_dlt_system_event_log_raw using delta LOCATION '$storage_path/system/events';
# MAGIC select * from demo_cdc_dlt_system_event_log_raw;

# COMMAND ----------

# MAGIC %md #Delta Live Table expectation analysis
# MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information
# MAGIC 
# MAGIC **Make sure you set your DLT storage path in the widget!**
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_quality_expectations&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Notebook extracting DLT expectations as delta tables used to build DBSQL data quality Dashboard.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["DLT Data Quality Stats"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Event Logs Analysis
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC 
# MAGIC | Type of event | behavior |
# MAGIC | --- | --- |
# MAGIC | `user_action` | Events occur when taking actions like creating the pipeline |
# MAGIC | `flow_definition`| FEvents occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information |
# MAGIC | `output_dataset` and `input_datasets` | output table/view and its upstream table(s)/view(s) |
# MAGIC | `flow_type` | whether this is a complete or append flow |
# MAGIC | `explain_text` | the Spark explain plan |
# MAGIC | `flow_progress`| Events occur when a data flow starts running or finishes processing a batch of data |
# MAGIC | `metrics` | currently contains `num_output_rows` |
# MAGIC | `data_quality` (`dropped_records`), (`expectations`: `name`, `dataset`, `passed_records`, `failed_records`)| contains an array of the results of the data quality rules for this particular dataset   * `expectations`|

# COMMAND ----------

# DBTITLE 1,Event Log - Raw Sequence of Events by Timestamp
# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        details
# MAGIC   FROM demo_cdc_dlt_system_event_log_raw
# MAGIC  ORDER BY timestamp ASC
# MAGIC ;  

# COMMAND ----------

# MAGIC %md ## 2 - DLT Lineage 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List of output datasets by type and the most recent change
# MAGIC create or replace temp view cdc_dlt_expectations as (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status as status_update,
# MAGIC     explode(from_json(details:flow_progress.data_quality.expectations
# MAGIC              ,'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) expectations
# MAGIC   FROM demo_cdc_dlt_system_event_log_raw
# MAGIC   where details:flow_progress.status='COMPLETED' and details:flow_progress.data_quality.expectations is not null
# MAGIC   ORDER BY timestamp);
# MAGIC select * from cdc_dlt_expectations

# COMMAND ----------

# MAGIC %sql
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC -- Lineage
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC SELECT max_timestamp,
# MAGIC        details:flow_definition.output_dataset,
# MAGIC        details:flow_definition.input_datasets,
# MAGIC        details:flow_definition.flow_type,
# MAGIC        details:flow_definition.schema,
# MAGIC        details:flow_definition.explain_text,
# MAGIC        details:flow_definition
# MAGIC   FROM demo_cdc_dlt_system_event_log_raw e
# MAGIC  INNER JOIN (
# MAGIC               SELECT details:flow_definition.output_dataset output_dataset,
# MAGIC                      MAX(timestamp) max_timestamp
# MAGIC                 FROM demo_cdc_dlt_system_event_log_raw
# MAGIC                WHERE details:flow_definition.output_dataset IS NOT NULL
# MAGIC                GROUP BY details:flow_definition.output_dataset
# MAGIC             ) m
# MAGIC   WHERE e.timestamp = m.max_timestamp
# MAGIC     AND e.details:flow_definition.output_dataset = m.output_dataset
# MAGIC --    AND e.details:flow_definition IS NOT NULL
# MAGIC  ORDER BY e.details:flow_definition.output_dataset
# MAGIC ;

# COMMAND ----------

# MAGIC %md ## 3 - Quality Metrics 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select sum(expectations.failed_records) as failed_records, 
# MAGIC sum(expectations.passed_records) as passed_records, 
# MAGIC expectations.name 
# MAGIC from cdc_dlt_expectations 
# MAGIC group by expectations.name

# COMMAND ----------

# MAGIC %md ## 4. Business Aggregate Checks

# COMMAND ----------

# MAGIC %python 
# MAGIC import plotly.express as px
# MAGIC expectations_metrics = spark.sql("""select sum(expectations.failed_records) as failed_records, 
# MAGIC                                  sum(expectations.passed_records) as passed_records, 
# MAGIC                                  expectations.name 
# MAGIC                                  from cdc_dlt_expectations
# MAGIC                                  group by expectations.name""").toPandas()
# MAGIC px.bar(expectations_metrics, x="name", y=["passed_records", "failed_records"], title="DLT expectations metrics")
