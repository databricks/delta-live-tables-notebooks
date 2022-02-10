# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, from_utc_timestamp, to_date
import dlt

# COMMAND ----------

sourceBucket = "s3a://databricks-field-eng-audit-logs/field-eng/workspaceId=468793438776649/"

# COMMAND ----------

@dlt.table(
  comment="The raw databricks logs",
  spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"},
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("actionName", "actionName is not Null")
def audit_logs_bronze():  
  return spark \
    .readStream \
    .format("cloudFiles") \
    .option("cloudFiles.schemaLocation", "/tmp/john.odwyer/testAutoloaderPython") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaHints", "requestParams string") \
    .load(sourceBucket)

# COMMAND ----------

@dlt.table(
  comment="The refined databricks logs",
  spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"},
  table_properties={
    "myCompanyPipeline.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  },
  partition_cols=["date"]
)
@dlt.expect_or_drop("date", "date > '2021-01-31'")
def audit_logs_silver():
  df = dlt.read_stream("audit_logs_bronze")\
    .withColumn("email", col("userIdentity.email"))\
    .withColumn("utc_timestamp", from_unixtime(col("timestamp")/1000))
  
  df = df.withColumn("date_time", from_utc_timestamp(col("utc_timestamp"), "UTC"))\
    .withColumn("date", to_date(col("utc_timestamp")))

  df = df.drop("userIdentity")\
    .drop("utc_timestamp")\
    .drop("_rescued_data")

  df = df.withWatermark("date_time", "1 day")\
    .dropDuplicates()

  return df

# COMMAND ----------

def goldTableByService(serviceName):
  @dlt.table(
    comment="The gold table for databricks logs service name {}".format(serviceName),
    name="audit_logs_gold_{}".format(serviceName),
    spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"},
    table_properties={
      "myCompanyPipeline.quality": "gold",
      "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["date"]
  )
  @dlt.expect_or_drop("serviceName", "serviceName = '{}'".format(serviceName))
  def audit_logs_gold():
    return dlt.read_stream("audit_logs_silver").filter(col("serviceName") == serviceName)

# COMMAND ----------

serviceNameList = ["accounts",
                   "clusters",
                   "dbfs",
                   "deltaPipelines",
                   "genie",
                   "globalInitScripts",
                   "groups",
                   "iamRole",
                   "instancePools",
                   "jobs",
                   "mlflowExperiment",
                   "notebook",
                   "repos",
                   "secrets",
                   "sqlAnalytics",
                   "sqlPermissions",
                   "ssh",
                   "workspace"] 

# COMMAND ----------

for serviceName in serviceNameList:
  goldTableByService(serviceName)
