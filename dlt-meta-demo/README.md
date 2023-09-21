 # [DLT-META](https://github.com/databrickslabs/dlt-meta) DEMO's
 1. [DAIS 2023 DEMO](#dais-2023-demo): Showcases DLT-META's capabilities of creating Bronze and Silver DLT pipelines with initial and incremental mode automatically.
 2. [Databricks Techsummit Demo](#databricks-tech-summit-fy2024-demo): 100s of data sources ingestion in bronze and silver DLT pipelines automatically.

# DAIS 2023 DEMO
This Demo launches Bronze and Silver DLT pipleines with following activities:
- Customer and Transactions feeds for initial load
- Adds new feeds Product and Stores to existing Bronze and Silver DLT pipelines with metadata changes.
- Runs Bronze and Silver DLT for incremental load for CDC events

### Steps:
1. Launch Terminal/Command promt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. ```git clone https://github.com/databricks/delta-live-tables-notebooks.git ```

4. ```cd delta-live-tables-notebooks/dlt-meta-demo```

5. Get DATABRICKS_HOST:
    - Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. To get your workspace URL, see Workspace instance names, URLs, and IDs.

6. Generate DATABRICKS_TOKEN:
    - In your Databricks workspace, click your Databricks username in the top bar, and then select User Settings from the drop down.

    - On the Access tokens tab, click Generate new token.

    - (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).

    - Click Generate.

    - Copy the displayed token

7. Set environment variable into terminal
```
export DATABRICKS_HOST=<DATABRICKS HOST> # Paste from Step#5
export DATABRICKS_TOKEN=<DATABRICKS TOKEN> # Paste Token here from Step#6, Account needs permission to create clusters/dlt pipelines.
```

8. Run the command ```python launch_demo.py --cloud_provider_name=aws --dbr_version=12.2.x-scala2.12 --dbfs_path=dbfs:/dais-dlt-meta-demo-automated/```
    - cloud_provider_name : aws or azure or gcp
    - db_version : Databricks Runtime Version
    - dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines

# Databricks Tech Summit FY2024 DEMO:
This Demo will launch dlt-meta with 100s tables for bronze and silver DLT. Demo will create test tables dynamically and will launch for bronze and silver DLT pipelines.

### Steps:
1. Launch Terminal/Command promt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. ```git clone https://github.com/databricks/delta-live-tables-notebooks.git ```

4. ```cd delta-live-tables-notebooks/dlt-meta-demo```

5. Get DATABRICKS_HOST:
    - Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. To get your workspace URL, see Workspace instance names, URLs, and IDs.

6. Generate DATABRICKS_TOKEN:
    - In your Databricks workspace, click your Databricks username in the top bar, and then select User Settings from the drop down.

    - On the Access tokens tab, click Generate new token.

    - (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).

    - Click Generate.

    - Copy the displayed token

7. Set environment variable into terminal
```
export DATABRICKS_HOST=<DATABRICKS HOST> # Paste from Step#5
export DATABRICKS_TOKEN=<DATABRICKS TOKEN> # Paste Token here from Step#6, Account needs permission to create clusters/dlt pipelines.
```

8. Run the command ```python launch_techsummit_demo.py --cloud_provider_name=aws --dbr_version=12.2.x-scala2.12 --dbfs_path=dbfs:/tech-summit-dlt-meta-demo-automated/```
    - cloud_provider_name : aws or azure or gcp
    - db_version : Databricks Runtime Version
    - dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines
    - worker_nodes : (Optional) Provide number of worker nodes for data generation e.g 4
    - table_count : (Optional) Provide table count e.g 100
    - table_column_count : (Optional) Provide column count e.g 5
    - table_data_rows_count : (Optional) Provide data rows count e.g 10