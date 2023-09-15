# DLT APPLY CHANGES FROM SNAPSHOT
```bash
cd cdc-from-snapshot
databricks bundle validate
databricks bundle deploy
databricks bundle run dlt_snapshot_ingestion_pattern1_job
databricks bundle run dlt_snapshot_ingestion_pattern2_job
```