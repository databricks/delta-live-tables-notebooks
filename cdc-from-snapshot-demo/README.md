# DLT APPLY CHANGES FROM SNAPSHOT
```bash
cd cdc-from-snapshot

# set environment variables for IAM role and S3 bucket. If you don't set environment variables, 
# you'll need to provide the variable key=value when running bundle command
# e.g. databricks bundle validate --var="my_instance_profile_arn=<MY_IAM_ROLE>"

export =<MY_IAM_ROLE> && export BUNDLE_VAR_my_s3_bucket=<MY_S3_BUCKET>
databricks bundle validate
databricks bundle deploy
databricks bundle run dlt_snapshot_ingestion_pattern1_job
databricks bundle run dlt_snapshot_ingestion_pattern2_job
```