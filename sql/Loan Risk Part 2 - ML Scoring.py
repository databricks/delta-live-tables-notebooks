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
spark.udf.register("loanRiskScoring", loaded_model)
