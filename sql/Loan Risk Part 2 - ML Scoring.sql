-- Databricks notebook source
-- MAGIC %pip install mlflow

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # import dlt
-- MAGIC import mlflow
-- MAGIC from pyspark.sql.functions import struct
-- MAGIC 
-- MAGIC # run_id= "mlflow_run_id"
-- MAGIC # model_name = "the_model_name_in_run"
-- MAGIC run_id= "585dc1096437488e99e37488ca132e52"
-- MAGIC model_name = "model"
-- MAGIC 
-- MAGIC model_uri = "runs:/{run_id}/{model_name}".format(run_id=run_id, model_name=model_name)
-- MAGIC loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)
-- MAGIC spark.udf.register("loanRiskScoring", loaded_model)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
-- MAGIC numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
-- MAGIC features = categoricals + numerics
-- MAGIC 
-- MAGIC # sql = "SELECT * FROM jodLoanRiskSQL.train_data LIMIT 2"
-- MAGIC sql = "SELECT * FROM jodLoanRiskSQL.train_data where loan_amnt is not null and emp_length is not null and annual_inc is not null and dti is not null and delinq_2yrs is not null and revol_util is not null and total_acc is not null and credit_length_in_years is not null LIMIT 2"
-- MAGIC   
-- MAGIC df = spark.sql(sql)
-- MAGIC # df = df.withColumn('predictions', loaded_model(struct(numerics)))
-- MAGIC display(df)
-- MAGIC # display(df.withColumn('predictions', loaded_model(struct(features))))

-- COMMAND ----------

SELECT --*, 
  loanRiskScoring("loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years") 
--   loanRiskScoring(term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length, annual_inc, dti, delinq_2yrs, revol_util, total_acc, credit_length_in_years) 
FROM jodLoanRiskSQL.train_data
where loan_amnt is not null and emp_length is not null and annual_inc is not null and dti is not null and delinq_2yrs is not null and revol_util is not null and total_acc is not null and credit_length_in_years is not null
LIMIT 2

-- term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length, annual_inc, dti, delinq_2yrs, revol_util, total_acc, credit_length_in_years




-- COMMAND ----------

SELECT *, 
  loanRiskScoring(features.term, features.home_ownership, features.purpose, features.addr_state, features.verification_status, features.application_type, features.loan_amnt, features.emp_length, features.annual_inc, features.dti, features.delinq_2yrs, features.revol_util, features.total_acc, features.credit_length_in_years) 
FROM 
(SELECT term as 0, home_ownership as 1, purpose as 2, addr_state as 3, verification_status as 4, application_type as 5, loan_amnt as 6, emp_length as 7, annual_inc as 8, dti as 9, delinq_2yrs as 10, revol_util as 11, total_acc as 12, credit_length_in_years as 13 
from jodLoanRiskSQL.train_data) as features
LIMIT 2


-- COMMAND ----------


