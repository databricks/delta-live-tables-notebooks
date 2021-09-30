# Databricks notebook source
# target database from Loan Risk Pipeline
targetName = "yourTargetDatabase"

# get the training set from the Loan Risk DLT pipeline
trainSql = "SELECT * FROM " + targetName + ".train_data"
train = spark.sql(trainSql)

display(train)

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer

# Setting variables to predict bad loans
categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]

# Establish stages for our GBT model
indexers = map(lambda c: StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid = 'keep'), categoricals)
imputers = Imputer(inputCols = numerics, outputCols = numerics)
featureCols = list(map(lambda c: c+"_idx", categoricals)) + numerics

# Define vector assemblers
model_matrix_stages = list(indexers) + [imputers] + \
                     [VectorAssembler(inputCols=featureCols, outputCol="features"), StringIndexer(inputCol="bad_loan", outputCol="label")]

# Define a GBT model.
gbt = GBTClassifier(featuresCol="features",
                    labelCol="label",
                    lossType = "logistic",
                    maxBins = 52,
                    maxIter=20,
                    maxDepth=5)

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=model_matrix_stages+[gbt])

# Train model.  This also runs the indexer.
gbt_model = pipeline.fit(train)

# COMMAND ----------


