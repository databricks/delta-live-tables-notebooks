# DLT APPLY CHANGES FROM SNAPSHOT
## How to run this demo
First, you'll need to setup the environment variables:
```bash
# First, set databricks host and token
export DATABRICKS_HOST=<MYWORKSPACE>.cloud.databricks.com
export DATABRICKS_TOKEN=<MY_DATABRICKS_TOKEN>

# Next, for pattern 2, you'll need to set the following environment variables
# set environment variables for IAM role and S3 bucket. If you don't set environment variables, 
# you'll need to pass the variable name and value as part of the bundle command.
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