# Delta-Live-Tables (DLT)

Welcome to the repository for the Databricks Delta Live Tables Demo!

This repository contains the sample notebooks that demonstrate the use of Delta Live Tables in Sql and Python that aims to enable data engineers to streamline and democratize their production ETL pipelines.

To accessing the notebooks please use [Databricks Projects](https://docs.databricks.com/repos.html) to clone this repo and get started with some Databricks DLT demo:

## Demos being worked on:

### - Retail & Sale Data
* Build DLT pipeline with quality checks (SQL)
* Build DLT pipeline with CDC to track changes over delta tables (SQL)

## Demos being WIP:

### - Retail & Sale Data
* Build DLT pipeline with quality checks (Python)
* Build DLT pipeline with CDC to track changes over delta tables (Python)


## Reading Resources

* [Delta Live Table Quickstart on AWS](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html)
* [Delta Live Table Quickstart on Azure](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-quickstart)
* [Delta Live Table Blog](https://databricks.com/discover/pages/getting-started-with-delta-live-tables)

## Setup/Requirements
- Notebooks requires a running Databricks workspace. There is generator scripts located in Data Generators. Run 00-Retail_Data_CDC_Generator.py in Databricks to generate retail data for 2-Retail_DLT_CDC_sql and 01-Retail_Data_Generator.py to generate data for 1-Retail_DLT_sql. 
- Please use settings specified in notebook PipelineSettingConfig.json, and make sure you define the storage prior to creating you DLT pipeline. You need this storage path to be able to run Cmd 10 in 3-Retail_DLT_CDC_Monitoring notebook. 


### DBR Version
The features used in the notebooks require DBR 8.3+

### Repos
If you have repos enabled on your Databricks workspace. You can directly import this repo and run the notebooks as is and avoid the DBC archive step.

### DBC Archive
Download the DBC archive from releases and import the archive into your Databricks workspace.

