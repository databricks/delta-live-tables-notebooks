# DLT APPLY CHANGES FROM SNAPSHOT

When you run `databricks bundle deploy`, it will deploy two jobs:
* DLT Snapshot Ingestion - Pattern1 [<USER_ID>]
* DLT Snapshot Ingestion - Pattern2 [<USER_ID>]

Each job has two tasks:
* generate_orders_snapshot: This is a Python notebook task that generates synthetic orders snapshot data. 
  * For Pattern 1, the snapshot data will be written to a delta table
  * For Pattern 2, each snapshot will be written to a unique DBFS or UC Volume path. 
* cdc_from_snapshot: This is a DLT pipeline that processes the snapshot data and applies the changes to the target table. 
 


## How to run this demo
First, you'll need to setup the environment variables:
```bash
# Mandatory: First, set databricks host and token
export DATABRICKS_HOST=<MYWORKSPACE>.cloud.databricks.com
export DATABRICKS_TOKEN=<MY_DATABRICKS_TOKEN>
```
Next, you'll use Databricks CLI to deploy and run the workflow. If you haven't already installed Databricks CLI, 
please follow the instructions from [here](https://docs.databricks.com/en/dev-tools/cli/install.html)

*Note*: The minimum version of Databricks CLI required is `Databricks CLI v0.208.2`. You can run `databricks -v` 
to check the CLI version.

Note: `databricks bundle run` command runs the job and waits for it to finish. If you want to run all of them, you'll need to run each `databricks bundle run` command in the new terminal.

You can always go to the Databricks `Workflows` page and click `Jobs`and find the job . You can either trigger the run manually or scheduled them to run hourly.


```bash
to run the demo with **Hive Metastore**, run the following commands from the root of the repo:
```bash
cd cdc-from-snapshot
databricks bundle validate --target development
databricks bundle deploy --target development
databricks bundle run dlt_snapshot_ingestion_pattern1_job --target development
databricks bundle run dlt_snapshot_ingestion_pattern2_job --target development
```

Next, to run the demo with **Unity Catalog**, run the following commands from the root of the repo:
```bash
cd cdc-from-snapshot
databricks bundle validate --target development-uc
databricks bundle deploy --target development-uc
databricks bundle run dlt_snapshot_ingestion_pattern1_job --target development-uc
databricks bundle run dlt_snapshot_ingestion_pattern2_job --target development-uc
```

To view the Job runs, you can go to your Databricks workspace and go to `Workflow` and `Job runs`. You can further filter the job runs by clicking on `Only my job runs` and you should see the job your runs as shown below.

![Job Runs](dlt_snapshot_processing_job_runs.png)