# Databricks notebook source
# 
# Loan Risk Analysis
#

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

# Important: Modify the database name so it goes to your chosen database
database_name = "loan_risk"

#
# Import 
#
from pyspark.sql.functions import *
from pyspark.sql.types import *


#
# Bronze Table
#

# Loan Risk data (from Kaggle)
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
table_name = database_name + ".bz_lending_club"
@create_table(
  name="bz_lending_club",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }
)
@expect_or_drop("avg_cur_bal", "avg_cur_bal >= 0")
def bz_lending_club():
  return (
    spark.read.parquet(lspq_path)
  )


#
# Silver Tables
#
table_name = database_name + ".ag_loan_stats"
@create_table(
  name="ag_loan_stats",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }
)
def ag_loan_stats():
  loan_stats = read("bz_lending_club")
  
  # Create bad loan label, this will include charged off, defaulted, and late repayments on loans...
  loan_stats = (loan_stats
                  .filter(loan_stats.loan_status.isin(["Default", "Charged Off", "Fully Paid"]))
                  .withColumn("bad_loan", (~(loan_stats.loan_status == "Fully Paid")).cast("string")))

  # Turning string interest rate and revoling util columns into numeric columns...
  loan_stats = (loan_stats
                .withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float')) 
                .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float')) 
                .withColumn('issue_year',  substring(loan_stats.issue_d, 5, 4).cast('double') ) 
                .withColumn('earliest_year', substring(loan_stats.earliest_cr_line, 5, 4).cast('double')))

  # Include credit_length_in_years
  loan_stats = loan_stats.withColumn('credit_length_in_years', (loan_stats.issue_year - loan_stats.earliest_year))

  # Converting emp_length column into numeric...  
  loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", "") ))
  loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "< 1", "0") ))
  loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "10\\+", "10") ).cast('float'))

  # Map multiple levels into one factor level for verification_status...
  loan_stats = loan_stats.withColumn('verification_status', trim(regexp_replace(loan_stats.verification_status, 'Source Verified', 'Verified')))
  
  return (
    # Calculate the total amount of money earned or lost per loan...    
    loan_stats.withColumn('net', round( loan_stats.total_pymnt - loan_stats.loan_amnt, 2))
  )


#
# Gold Tables
#

# Summary Statistics table
table_name = database_name + ".au_summary_data"
@create_table(
  name="au_summary_data",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }
)
@expect_or_drop("valid addr_state", "addr_state IS NOT NULL")
def au_summary_data():
  return (
    read("ag_loan_stats").select("grade", "loan_amnt", "annual_inc", "dti", "credit_length_in_years", "addr_state", "bad_loan", "net")
  )


# Features table
table_name = database_name + ".au_features"
@create_table(
  name="au_features",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }    
)
def au_features():
  return (
    read("ag_loan_stats").select("term","home_ownership","purpose","addr_state","verification_status","application_type","loan_amnt","emp_length", "annual_inc","dti","delinq_2yrs","revol_util","total_acc", "credit_length_in_years","bad_loan","int_rate","net","issue_year")    
  )


#
# ML Training Data Table
#
table_name = database_name + ".train_data"
@create_table(
  name="train_data",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }  
)
def train_data():
  # Setting variables to predict bad loans
  myY = "bad_loan"
  categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
  numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
  myX = categoricals + numerics

  # Setup dataset
  loan_stats2 = read("au_features").select(myX + [myY, "int_rate", "net", "issue_year"])
  train_data = loan_stats2.filter(loan_stats2.issue_year <= 2015)
  
  return (
    train_data
  )


#
# ML Validation Data Table
#
table_name = database_name + ".valid_data"
@create_table(
  name="valid_data",
  table_properties={
      "pipelines.metastore.tableName": table_name
  }  
)
def valid_data():
  # Setting variables to predict bad loans
  myY = "bad_loan"
  categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
  numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
  myX = categoricals + numerics

  # Setup dataset
  loan_stats2 = read("au_features").select(myX + [myY, "int_rate", "net", "issue_year"])
  valid_data = loan_stats2.filter(loan_stats2.issue_year > 2015)
  
  return (
    valid_data
  )
