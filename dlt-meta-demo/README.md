 This is easist way to launch DAIS 2023 [DLT-META](https://github.com/databrickslabs/dlt-meta) demo to your databricks workspace with following steps.

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

6. Run the command ```python launch_demo.py --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --dbfs_path=dbfs:/tmp/DLT-META_DAIS```
    - cloud_provider_name : aws or azure or gcp
    - db_version : Databricks Runtime Version
    - dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines