# Delta-Live-Tables (DLT)

Welcome to the repository for the Databricks Delta Live Tables Change Data Capture example.

You can use [Databricks Projects](https://docs.databricks.com/repos.html) to clone this repo and get started with this demo, or download the .dbc archive and import the notebooks manually.

## Reading Resources

* [Delta Live Table Quickstart on AWS](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html)
* [Delta Live Table Quickstart on Azure](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-quickstart)
* [Delta Live Table Blog](https://databricks.com/discover/pages/getting-started-with-delta-live-tables)

## Setup/Requirements

- Please use settings specified in notebook PipelineSettingConfig.json, and make sure you define the storage prior to creating you DLT pipeline.
- Add the configuration to enable Change Data Capture according to the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-cdc.html#requirements)

### DBR Version
The features used in the notebooks require DBR 8.3+