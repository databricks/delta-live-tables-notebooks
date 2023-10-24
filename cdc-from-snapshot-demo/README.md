# DLT APPLY CHANGES FROM SNAPSHOT

When you run `databricks bundle deploy`, it will deploy two jobs:
* DLT Snapshot Ingestion - Pattern1 [<USER_ID>]
* DLT Snapshot Ingestion - Pattern2 [<USER_ID>]
Both jobs have two tasks:
* generate_orders_snapshot: This is a Python notebook task that generates synthetic orders snapshot data. For Pattern 1, the snapshot data will be written to a delta table and for Pattern 2, each snapshot will be written to a unique path
* cdc_from_snapshot: This is a DLT pipeline. 


## How to run this demo
First, you'll need to setup the environment variables:
```bash
# Mandatory: First, set databricks host and token
export DATABRICKS_HOST=<MYWORKSPACE>.cloud.databricks.com
export DATABRICKS_TOKEN=<MY_DATABRICKS_TOKEN>
```
Next, to run the demo with Hive Metastore, run the following commands from the root of the repo:
```bash
cd cdc-from-snapshot
databricks bundle validate --target development
databricks bundle deploy --target development
databricks bundle run dlt_snapshot_ingestion_pattern1_job
databricks bundle run dlt_snapshot_ingestion_pattern2_job
```

Next, to run the demo with Unity Catalog, run the following commands from the root of the repo:
```bash
cd cdc-from-snapshot
databricks bundle validate --target development-uc
databricks bundle deploy --target development-uc
databricks bundle run dlt_snapshot_ingestion_pattern1_job
databricks bundle run dlt_snapshot_ingestion_pattern2_job
```