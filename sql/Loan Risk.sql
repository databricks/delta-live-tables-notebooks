-- Databricks notebook source
CREATE LIVE TABLE lendingclub_raw
COMMENT "The raw loan risk dataset, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM parquet.`/databricks-datasets/samples/lending_club/parquet/`

-- COMMAND ----------

CREATE LIVE TABLE lendingclub_clean(
  CONSTRAINT avg_cur_bal EXPECT (avg_cur_bal >= 0) ON VIOLATION DROP ROW
)
COMMENT "Loan risk dataset with cleaned-up datatypes / column names and quality expectations."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT CASE 
         WHEN loan_status = "Fully Paid" THEN "false"
         Else "true"
       END AS bad_loan,
       CAST(REPLACE(int_rate, '%', '') AS float) AS int_rate,
       CAST(REPLACE(revol_util, '%', '') AS float) AS revol_util,
       CAST(SUBSTR(issue_d, 5, 4) AS double) AS issue_year,
       CAST(SUBSTR(earliest_cr_line, 5, 4) AS double) AS earliest_year,
       (CAST(SUBSTR(issue_d, 5, 4) AS double) - CAST(SUBSTR(earliest_cr_line, 5, 4) AS double)) AS credit_length_in_years,
       CAST(REPLACE(REPLACE(REGEXP_REPLACE(emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", ""), "< 1", ""), "+", "") AS float) AS emp_length,
       TRIM(REPLACE(verification_status, "Source Verified", "Verified")) AS verification_status,
       ROUND(total_pymnt - loan_amnt, 2) AS net,
       grade, 
       loan_amnt, 
       annual_inc, 
       dti, 
       addr_state,
       term,
       home_ownership,
       purpose,
       application_type,
       delinq_2yrs,
       total_acc,
       avg_cur_bal
  FROM live.lendingclub_raw
 WHERE loan_status IN ("Default", "Charged Off", "Fully Paid")  

-- COMMAND ----------

CREATE LIVE TABLE summary_data
COMMENT "Loan risk summary dataset for analytics."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT grade, loan_amnt, annual_inc, dti, credit_length_in_years, addr_state, bad_loan, net
  FROM live.lendingclub_clean

-- COMMAND ----------

CREATE LIVE TABLE features
COMMENT "Loan risk features dataset for training and validation datasets."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length,
       annual_inc, dti, delinq_2yrs, revol_util, total_acc, credit_length_in_years, bad_loan, int_rate, net, issue_year
  FROM live.lendingclub_clean

-- COMMAND ----------

CREATE LIVE TABLE train_data
COMMENT "ML training dataset based on Loan Risk data features."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT bad_loan, term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length, annual_inc, dti, delinq_2yrs, revol_util, 
       total_acc, credit_length_in_years, int_rate, net, issue_year
  FROM live.features
 WHERE issue_year <= 2015

-- COMMAND ----------

CREATE LIVE TABLE valid_data
COMMENT "ML validation dataset based on Loan Risk data features."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT bad_loan, term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, emp_length, annual_inc, dti, delinq_2yrs, revol_util, 
       total_acc, credit_length_in_years, int_rate, net, issue_year
  FROM live.features
 WHERE issue_year > 2015

-- COMMAND ----------


