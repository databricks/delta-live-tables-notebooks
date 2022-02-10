# Databricks notebook source
# 
# Wikipedia Clickstream
# An example Delta Live Tables pipeline that ingests wikipedia click stream data and builds some simple summary tables.
#
#   Source: February 2015 English Wikipedia Clickstream in JSON
#   More information of the columns can be found at: https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
#

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

@dlt.create_table(
  comment="The raw wikipedia click stream dataset, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze"
  }
)
def clickstream_raw():          
  return (
    spark.read.option("inferSchema", "true").json(json_path)
  )


@dlt.create_table(
  comment="Wikipedia clickstream dataset with cleaned-up datatypes / column names and quality expectations.",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
def clickstream_clean():
  return (
    dlt.read("clickstream_raw")
      .withColumn("current_page_id", expr("CAST(curr_id AS INT)"))
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumn("previous_page_id", expr("CAST(prev_id AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_id", "current_page_title", "click_count", "previous_page_id", "previous_page_title")
  )


@dlt.create_table(
  comment="A table of the most common pages that link to the Apache Spark page.",
  table_properties={
    "quality": "gold"  
  }  
)
def top_spark_referrers():
  return (
    dlt.read("clickstream_clean")
      .filter(expr("current_page_title == 'Apache_Spark'"))
      .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("click_count"))
      .select("referrer", "click_count")
      .limit(10)
  )


@dlt.create_table(
  comment="A list of the top 50 pages by number of clicks.",
  table_properties={
    "quality": "gold"  
  }  
)
def top_pages():
  return (
    dlt.read("clickstream_clean")
      .groupBy("current_page_title")
      .agg(sum("click_count").alias("total_clicks"))
      .sort(desc("total_clicks"))
      .limit(50)
  )
