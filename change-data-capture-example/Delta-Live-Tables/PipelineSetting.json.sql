-- Databricks notebook source
{
     "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "development": true,
    "continuous": false,
    "edition": "advanced",
    "photon": false,
    "libraries": [
        {
            "notebook": {
"path":"/Repos/mojgan.mazouchi@databricks.com/Delta-Live-Tables/notebooks/1-CDC_DataGenerator"
            }
        },
        {
            "notebook": {
"path":"/Repos/mojgan.mazouchi@databricks.com/Delta-Live-Tables/notebooks/2-Retail_DLT_CDC_sql"
            }
        }
    ],
    "name": "CDC_blog",
    "storage": "dbfs:/home/mydir/myDB/dlt_storage",
    "configuration": {
        "source": "/tmp/demo/cdc_raw",
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "target": "my_database"
}

