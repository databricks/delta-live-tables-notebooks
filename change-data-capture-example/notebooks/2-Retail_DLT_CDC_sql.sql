-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC 
-- MAGIC -----------------
-- MAGIC ###### By Morgan Mazouchi
-- MAGIC -----------------
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/dlt_end_to_end_flow.png">

-- COMMAND ----------

-- MAGIC %python
-- MAGIC slide_id = '10Dmx43aZXzfK9LJvJjH1Bjgwa3uvS2Pk7gVzxhr3H2Q'
-- MAGIC slide_number = 'id.p9'
-- MAGIC  
-- MAGIC displayHTML(f'''<iframe
-- MAGIC  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
-- MAGIC   frameborder="0"
-- MAGIC   width="75%"
-- MAGIC   height="600"
-- MAGIC ></iframe>
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Importance of Change Data Capture (CDC)
-- MAGIC 
-- MAGIC Change Data Capture (CDC) is the process that captures the changes in records made to a data storage like Database, Data Warehouse, etc. These changes usually refer to operations like data deletion, addition and updating.
-- MAGIC 
-- MAGIC A straightforward way of Data Replication is to take a Database Dump that will export a Database and import it to a LakeHouse/DataWarehouse/Lake, but this is not a scalable approach. 
-- MAGIC 
-- MAGIC Change Data Capture, only capture the changes made to the Database and apply those changes to the target Database. CDC reduces the overhead, and supports real-time analytics. It enables incremental loading while eliminates the need for bulk load updating.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### CDC Approaches 
-- MAGIC 
-- MAGIC **1- Develop in-house CDC process:** 
-- MAGIC 
-- MAGIC ***Complex Task:*** CDC Data Replication is not a one-time easy solution. Due to the differences between Database Providers, Varying Record Formats, and the inconvenience of accessing Log Records, CDC is challenging.
-- MAGIC 
-- MAGIC ***Regular Maintainance:*** Writing a CDC process script is only the first step. You need to maintain a customized solution that can map to aformentioned changes regularly. This needs a lot of time and resources.
-- MAGIC 
-- MAGIC ***Overburdening:*** Developers in companies already face the burden of public queries. Additional work for building customizes CDC solution will affect existing revenue-generating projects.
-- MAGIC 
-- MAGIC **2- Using CDC tools** such as Debezium, Hevo Data, IBM Infosphere, Qlik Replicate, Talend, Oracle GoldenGate, StreamSets.
-- MAGIC 
-- MAGIC In this demo repo we are using CDC data coming from a CDC tool. 
-- MAGIC Since a CDC tool is reading database logs:
-- MAGIC We are no longer dependant on developers updating a certain column 
-- MAGIC 
-- MAGIC — A CDC tool like Debezium takes care of capturing every changed row. It records the history of data changes in Kafka logs, from where your application consumes them. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Setup/Requirements:
-- MAGIC 
-- MAGIC Prior to running this notebook as a pipeline, make sure to include a path to 1-CDC_DataGenerator notebook in your DLT pipeline, to let this notebook runs on top of the generated CDC data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How to synchronize your SQL Database with your Lakehouse? 
-- MAGIC CDC flow with a CDC tool, autoloader and DLT pipeline:
-- MAGIC 
-- MAGIC - A CDC tool reads database logs, produces json messages that includes the changes, and streams the records with changes description to Kafka
-- MAGIC - Kafka streams the messages  which holds INSERT, UPDATE and DELETE operations, and stores them in cloud object storage (S3 folder, ADLS, etc).
-- MAGIC - Using Autoloader we incrementally load the messages from cloud object storage, and stores them in Bronze table as it stores the raw messages 
-- MAGIC - Next we can perform APPLY CHANGES INTO on the cleaned Bronze layer table to propagate the most updated data downstream to the Silver Table
-- MAGIC 
-- MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_flow_new.png" alt='Make all your data ready for BI and ML'/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###How does CDC tools like Debezium output looks like?
-- MAGIC 
-- MAGIC A json message describing the changed data has interesting fields similar to the list below: 
-- MAGIC 
-- MAGIC - operation: an operation code (DELETE, APPEND, UPDATE, CREATE)
-- MAGIC - operation_date: the date and timestamp for the record came for each operation action
-- MAGIC 
-- MAGIC Some other fields that you may see in Debezium output (not included in this demo):
-- MAGIC - before: the row before the change
-- MAGIC - after: the row after the change
-- MAGIC 
-- MAGIC To learn more about the expected fields check out [this reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Incremental data loading using Auto Loader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="700px" src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/DLT_CDC.png"/>
-- MAGIC </div>
-- MAGIC Working with external system can be challenging due to schema update. The external database can have schema update, adding or modifying columns, and our system must be robust against these changes.
-- MAGIC Databricks Autoloader (`cloudFiles`) handles schema inference and evolution out of the box.
-- MAGIC 
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale. In this notebook we leverage Autoloader to handle streaming (and batch) data.
-- MAGIC 
-- MAGIC Let's use it to create our pipeline and ingest the raw JSON data being delivered by an external provider. 

-- COMMAND ----------

-- DBTITLE 1,Let's explore our incoming data - Bronze Table - Autoloader & DLT
SET
  spark.source;
CREATE
  OR REFRESH STREAMING LIVE TABLE customer_bronze (
    address string,
    email string,
    id string,
    firstname string,
    lastname string,
    operation string,
    operation_date string,
    _rescued_data string
  ) TBLPROPERTIES ("quality" = "bronze") COMMENT "New customer data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  cloud_files(
    "${source}/customers",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
  );

-- COMMAND ----------

-- DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE customer_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_address EXPECT (address IS NOT NULL),
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.customer_bronze);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Materializing the silver table
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_silver_layer.png" alt='Make all your data ready for BI and ML' style='float: right' width='1000'/>
-- MAGIC 
-- MAGIC The silver `customer_silver` table will contains the most up to date view. It'll be a replicate of the original MYSQL table.
-- MAGIC 
-- MAGIC To propagate the `Apply Changes Into` operations downstream to the `Silver` layer, we must explicitly enable the feature in pipeline by adding and enabling the applyChanges configuration to the DLT pipeline settings.

-- COMMAND ----------

-- DBTITLE 1,Delete unwanted clients records - Silver Table - DLT SQL 
CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.customer_silver
FROM stream(LIVE.customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (operation, operation_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Next step, create DLT pipeline, add a path to this notebook and **add configuration with enabling applychanges to true**. For more detail see notebook "PipelineSettingConfiguration.json". 
-- MAGIC 
-- MAGIC After running the pipeline, check "3. Retail_DLT_CDC_Monitoring" to monitor log events and lineage data.
