# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Evaluating Risk for Loan Approvals
# MAGIC 
# MAGIC This is the analysis and ML prediction notebook for the *Loan Risk Pipeline* Delta Live Table notebook; it is based on the [Loan Risk Analysis with XGBoost and Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/08/09/loan-risk-analysis-with-xgboost-and-databricks-runtime-for-machine-learning.html)
# MAGIC 
# MAGIC ## Business Value
# MAGIC 
# MAGIC Being able to accurately assess the risk of a loan application can save a lender the cost of holding too many risky assets. Rather than a credit score or credit history which tracks how reliable borrowers are, we will generate a score of how profitable a loan will be compared to other loans in the past. The combination of credit scores, credit history, and profitability score will help increase the bottom line for financial institution.
# MAGIC 
# MAGIC Having a interporable model that an loan officer can use before performing a full underwriting can provide immediate estimate and response for the borrower and a informative view for the lender.
# MAGIC 
# MAGIC ![](https://github.com/databricks/tech-talks/raw/master/images/loan-risk-analysis-flow.png)
# MAGIC 
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)

# COMMAND ----------

# configure database naem
database_name = "loan_risk"

sql("USE " + database_name)
display(sql("SHOW TABLES"))

# COMMAND ----------

# MAGIC %md ## Analysis
# MAGIC The following are Databricks charts of the Loan Risk Analysis dataset including:
# MAGIC * Asset Allocation
# MAGIC * Feature Distribution and Correlation
# MAGIC * Loans by State

# COMMAND ----------

# DBTITLE 1,Asset Allocation (by Grade)
# MAGIC %sql
# MAGIC SELECT grade, loan_amnt FROM au_summary_data;

# COMMAND ----------

# DBTITLE 1,Feature Distribution and Correlation
# MAGIC %sql
# MAGIC SELECT loan_amnt, annual_inc, dti, credit_length_in_years FROM au_summary_data;

# COMMAND ----------

# DBTITLE 1,Loans by State
# MAGIC %sql
# MAGIC SELECT addr_state, COUNT(annual_inc) AS ratio FROM au_summary_data GROUP BY addr_state

# COMMAND ----------

# DBTITLE 1,Asset Allocation (by bad loan and grade)
# MAGIC %sql
# MAGIC SELECT grade, bad_loan, SUM(net) AS sum_net FROM au_summary_data group by grade, bad_loan ORDER by bad_loan, grade

# COMMAND ----------

# MAGIC %md ## Predictions
# MAGIC The following section runs a ML Pipeline using GLM with standardization and cross validation.
# MAGIC * First, we will run the ML pipeline
# MAGIC * We will also calculate the confusion matrix based on the validation data
# MAGIC * which will allow us to also calculate the business value

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.feature import StandardScaler, Imputer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Setting variables to predict bad loans
myY = "bad_loan"
categoricals = ["term", "home_ownership", "purpose", "addr_state","verification_status","application_type"]
numerics = ["loan_amnt", "emp_length", "annual_inc", "dti", "delinq_2yrs", "revol_util", "total_acc", "credit_length_in_years"]
myX = categoricals + numerics

# Setup dataset
train_data = sql("SELECT * FROM " + database_name + ".train_data")
valid_data = sql("SELECT * FROM " + database_name + ".valid_data")

## Current possible ways to handle categoricals in string indexer is 'error', 'keep', and 'skip'
indexers = map(lambda c: StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid = 'keep'), categoricals)
ohes = map(lambda c: OneHotEncoder(inputCol=c + "_idx", outputCol=c+"_class"),categoricals)
imputers = Imputer(inputCols = numerics, outputCols = numerics)

# Establish features columns
featureCols = list(map(lambda c: c+"_class", categoricals)) + numerics

# Build the stage for the ML pipeline
model_matrix_stages = list(indexers) + list(ohes) + [imputers] + \
                     [VectorAssembler(inputCols=featureCols, outputCol="features"), StringIndexer(inputCol="bad_loan", outputCol="label")]

# Apply StandardScaler to create scaledFeatures
scaler = StandardScaler(inputCol="features",
                        outputCol="scaledFeatures",
                        withStd=True,
                        withMean=True)

# Use logistic regression 
lr = LogisticRegression(maxIter=10, elasticNetParam=0.5, featuresCol = "scaledFeatures")

# Build our ML pipeline
pipeline = Pipeline(stages=model_matrix_stages+[scaler]+[lr])

# Build the parameter grid for model tuning
paramGrid = ParamGridBuilder() \
              .addGrid(lr.regParam, [0.1, 0.01]) \
              .build()

# Execute CrossValidator for model tuning
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5)

# Train the tuned model and establish our best model
cvModel = crossval.fit(train_data)
glm_model = cvModel.bestModel

# Create predictions table
predictions = glm_model.transform(valid_data)

# COMMAND ----------

# MAGIC %md ### Confusion Matrix

# COMMAND ----------

cmdf = predictions.groupBy("label", "prediction").count()
cmdf.createOrReplaceTempView("cmdf")
display(cmdf)

# COMMAND ----------

# MAGIC %python
# MAGIC # Source code for plotting confusion matrix is based on `plot_confusion_matrix` 
# MAGIC # via https://runawayhorse001.github.io/LearningApacheSpark/classification.html#decision-tree-classification
# MAGIC import matplotlib.pyplot as plt
# MAGIC import numpy as np
# MAGIC import itertools
# MAGIC 
# MAGIC def plot_confusion_matrix(cm, title):
# MAGIC   # Clear Plot
# MAGIC   plt.gcf().clear()
# MAGIC 
# MAGIC   # Configure figure
# MAGIC   fig = plt.figure(1)
# MAGIC   
# MAGIC   # Configure plot
# MAGIC   classes = ['Bad Loan', 'Good Loan']
# MAGIC   plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
# MAGIC   plt.title(title)
# MAGIC   plt.colorbar()
# MAGIC   tick_marks = np.arange(len(classes))
# MAGIC   plt.xticks(tick_marks, classes, rotation=45)
# MAGIC   plt.yticks(tick_marks, classes)
# MAGIC 
# MAGIC   # Normalize and establish threshold
# MAGIC   normalize=False
# MAGIC   fmt = 'd'
# MAGIC   thresh = cm.max() / 2.
# MAGIC 
# MAGIC   # Iterate through the confusion matrix cells
# MAGIC   for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
# MAGIC       plt.text(j, i, format(cm[i, j], fmt),
# MAGIC                horizontalalignment="center",
# MAGIC                color="white" if cm[i, j] > thresh else "black")
# MAGIC 
# MAGIC   # Final plot configurations
# MAGIC   plt.tight_layout()
# MAGIC   plt.ylabel('True label')
# MAGIC   plt.xlabel('Predicted label') 
# MAGIC   
# MAGIC   # Display images
# MAGIC   image = fig
# MAGIC   
# MAGIC   # Show plot
# MAGIC   #fig = plt.show()
# MAGIC   
# MAGIC   # Save plot
# MAGIC   fig.savefig("confusion-matrix.png")
# MAGIC 
# MAGIC   # Display Plot
# MAGIC   display(image)
# MAGIC   
# MAGIC   # Close Plot
# MAGIC   plt.close(fig)

# COMMAND ----------

# MAGIC %python
# MAGIC # Query confusion matrix DataFrame (cmdf)
# MAGIC cmdf = spark.sql("select * from cmdf order by label desc, prediction desc")
# MAGIC 
# MAGIC # Convert to pandas
# MAGIC cm_pdf = cmdf.toPandas()
# MAGIC 
# MAGIC # Create 1d numpy array of confusion matrix values
# MAGIC cm_1d = cm_pdf.iloc[:, 2]
# MAGIC 
# MAGIC # Create 2d numpy array of confusion matrix values
# MAGIC cm = np.reshape([cm_1d], (-1, 2))
# MAGIC 
# MAGIC # Plot confusion matrix  
# MAGIC plot_confusion_matrix(cm, "Confusion Matrix")

# COMMAND ----------

# MAGIC %md ## Quantify the Business Value
# MAGIC A great way to quickly understand the business value of this model is to create a confusion matrix.  The definition of our matrix is as follows:
# MAGIC 
# MAGIC * Prediction=1, Label=1 : Correctly found bad loans. sum_net = loss avoided.
# MAGIC * Prediction=1, Label=0 : Incorrectly labeled bad loans. sum_net = profit forfeited.
# MAGIC * Prediction=0, Label=1 : Incorrectly labeled good loans. sum_net = loss still incurred.
# MAGIC * Prediction=0, Label=0 : Correctly found good loans. sum_net = profit retained.
# MAGIC 
# MAGIC The following code snippet calculates the following confusion matrix.

# COMMAND ----------

from pyspark.sql.functions import *
display(predictions.groupBy("label", "prediction").agg((sum(col("net"))).alias("sum_net")))

# COMMAND ----------

# MAGIC %md To calculate business value:
# MAGIC 
# MAGIC ```value = -(loss avoided [1,1] - profit forfeited [1,0]) = -(-6.24 - 0.86) = 7.1```
