# Databricks notebook source
# 
# Loan Risk Analysis
# An example Delta Live Tables pipeline that loan risk data and builds some aggregate and feature engineering tables.
#
#   More information can be found in the Lending Club dictionary at: https://resources.lendingclub.com/LCDataDictionary.xlsx
#

from pyspark.sql.functions import *
from pyspark.sql.types import *

import dlt


lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
@dlt.create_table(
  comment="The raw loan risk dataset, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze"
  }
)
def lendingclub_raw():
  return (
    spark.read.parquet(lspq_path)
  )


@dlt.create_table(
  comment="Loan risk dataset with cleaned-up datatypes / column names and quality expectations.",  
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect_or_drop("avg_cur_bal", "avg_cur_bal >= 0")
def lendingclub_clean():
  loan_stats = dlt.read("lendingclub_raw")
  
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



@dlt.create_table(
  comment="Loan risk summary dataset for analytics.",  
  table_properties={
    "quality": "gold"
  }
)
@dlt.expect_or_drop("valid addr_state", "addr_state IS NOT NULL")
def summary_data():
  return (
    dlt.read("lendingclub_clean").select("grade", "loan_amnt", "annual_inc", "dti", "credit_length_in_years", "addr_state", "bad_loan", "net")
  )



@dlt.create_table(
  comment="Loan risk features dataset for training and validation datasets.",  
  partition_cols=["issue_year"],
  table_properties={
    "quality": "gold"
  }  
)
def features():
  return (
    dlt.read("lendingclub_clean")
      .select("term","home_ownership","purpose","addr_state","verification_status","application_type","loan_amnt","emp_length",
              "annual_inc","dti","delinq_2yrs","revol_util","total_acc", "credit_length_in_years","bad_loan","int_rate","net","issue_year")    
  )


@dlt.create_table(
  comment="ML training dataset based on Loan Risk data features.",  
  table_properties={
    "quality": "gold"
  }    
)
def train_data():
  # Setting variables to predict bad loans
  myY = "bad_loan"
  categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
  numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
  myX = categoricals + numerics

  # Setup dataset
  features = dlt.read("features").select(myX + [myY, "int_rate", "net", "issue_year"])
  train_data = features.filter(features.issue_year <= 2015)
  
  return (
    train_data
  )


@dlt.create_table(
  comment="ML validation dataset based on Loan Risk data features.",  
  table_properties={
    "quality": "gold"
  }      
)
def valid_data():
  # Setting variables to predict bad loans
  myY = "bad_loan"
  categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
  numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
  myX = categoricals + numerics

  # Setup dataset
  features = dlt.read("features").select(myX + [myY, "int_rate", "net", "issue_year"])
  valid_data = features.filter(features.issue_year > 2015)
  
  return (
    valid_data
  )
