-- Databricks notebook source
CREATE LIVE TABLE gtb_scoring_train_data
COMMENT "GBT ML scored training dataset based on Loan Risk"
TBLPROPERTIES ("quality" = "gold")
AS
-- named_struct and explicitly naming the column is needed for SparkML models
-- for other model inferenece, scikit-learn for example, you do not need named_struct and the udf call would look like this...
-- loanRiskScoring(term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length, annual_inc, dti, delinq_2yrs, revol_util, total_acc, credit_length_in_years)
SELECT *, 
  loanRiskScoring(named_struct(
    "term", term, "home_ownership", home_ownership, "purpose", purpose, "addr_state", addr_state, "verification_status", verification_status, 
    "application_type", application_type, "loan_amnt", loan_amnt, "emp_length", emp_length, "annual_inc", annual_inc, "dti", dti, 
    "delinq_2yrs", delinq_2yrs, "revol_util", revol_util,"total_acc", total_acc, "credit_length_in_years", credit_length_in_years
  )) AS prediction 
FROM live.train_data

-- COMMAND ----------

CREATE LIVE TABLE gtb_scoring_valid_data
COMMENT "GBT ML scored valid dataset based on Loan Risk"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *, 
  loanRiskScoring(named_struct(
    "term", term, "home_ownership", home_ownership, "purpose", purpose, "addr_state", addr_state, "verification_status", verification_status, 
    "application_type", application_type, "loan_amnt", loan_amnt, "emp_length", emp_length, "annual_inc", annual_inc, "dti", dti, 
    "delinq_2yrs", delinq_2yrs, "revol_util", revol_util,"total_acc", total_acc, "credit_length_in_years", credit_length_in_years
  )) AS prediction 
FROM live.valid_data
