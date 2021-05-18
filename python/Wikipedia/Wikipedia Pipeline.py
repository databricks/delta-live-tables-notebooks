# Databricks notebook source
# 
# Wikipedia Clickstream
# February 2015 English Wikipedia Clickstream in JSON
# * `prev_id`: if the referer does not correspond to an article in the main namespace of English Wikipedia, this value will be empty. Otherwise, it contains the unique 
#              MediaWiki page ID of the article corresponding to the referer i.e. the previous article the client was on
# * `curr_id`: the MediaWiki unique page ID of the article the client requested
# * `prev_title`: the result of mapping the referer URL to the fixed set of values described above
# * `curr_title`: the title of the article the client requested
# * `n`: the number of occurrences of the (referer, resource) pair
# * `type`
#   * "`link`" if the referer and request are both articles and the referer links to the request
#  * "`redlink`" if the referer is an article and links to the request, but the request is not in the production enwiki.page table
#  * "`other`" if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

#
# Import 
#
from pyspark.sql.functions import *
from pyspark.sql.types import *

# json clickstream data
@create_view()
def json_clickstream():
  return (
    spark.read.json("/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json")
  )

# wiki Spark occurences
@create_table()
def wiki_spark():
  return (
    read("json_clickstream").withColumn("n", expr("CAST(n AS integer) AS n")).filter(expr("curr_title == 'Apache_Spark'")).sort(desc("n"))
  )

# wiki top 50 occurences
@create_table()
def wiki_top50():
  return (
    read("json_clickstream").groupBy("curr_title").agg(sum("n").alias("sum_n")).sort(desc("sum_n")).limit(50)
  )
