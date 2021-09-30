# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

import dlt
import mlflow
from pyspark.sql.functions import struct

run_id= "mlflow_run_id"
model_name = "the_model_name_in_run"
model_uri = "runs:/{run_id}/{model_name}".format(run_id=run_id, model_name=model_name)
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
features = categoricals + numerics


# COMMAND ----------

@dlt.create_table(
  comment="GBT ML scored training dataset based on Loan Risk",  
  table_properties={
    "quality": "gold"
  }    
)
def gtb_scoring_train_data():
  return dlt.read("train_data").withColumn('predictions', loaded_model(struct(features)))


# COMMAND ----------

@dlt.create_table(
  comment="GBT ML scored valid dataset based on Loan Risk",  
  table_properties={
    "quality": "gold"
  }    
)
def gtb_scoring_valid_data():
  return dlt.read("valid_data").withColumn('predictions', loaded_model(struct(features)))

