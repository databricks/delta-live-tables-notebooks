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

# Optional: Next, for pattern 2, if you want to write snapshots to an S3 bucket, then, you'll need to set the following additional environment variables. By default, it will write to a DBFS path in which case, you don't need to set these environment variables.

# set environment variables for IAM role and S3 bucket. If you don't set environment variables, 
# you'll need to pass the variable name and value as part of the bundle deploy command.
# e.g. databricks bundle validate --var="my_instance_profile_arn=<MY_IAM_ROLE>"
export BUNDLE_VAR_my_iam_role=<MY_IAM_ROLE> && export BUNDLE_VAR_my_s3_bucket=<MY_S3_BUCKET>

```
Next, run the following commands from the root of the repo:
```bash
cd cdc-from-snapshot
databricks bundle validate
databricks bundle deploy
databricks bundle run dlt_snapshot_ingestion_pattern1_job
databricks bundle run dlt_snapshot_ingestion_pattern2_job
```