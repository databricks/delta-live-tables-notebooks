# Databricks notebook source
# 
# 2015 NYC Taxi Pipeline setup
#

# Use the following command to get the full filepath
#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() 

#
# Import 
#
from pyspark.sql.functions import *
from pyspark.sql.types import *

#
# Specify schemas
#
schema_taxi_rate_code = StructType([
    StructField("RateCodeID", IntegerType()),
    StructField("RateCodeDesc", StringType())
])

schema_taxi_payment_type = StructType([
    StructField("payment_type", IntegerType()),
    StructField("payment_desc", StringType())
])


#
# Lookup Tables
#

# Taxi Rate Code
@create_view(name="map_rateCode")
def map_rateCode():
  return (
    spark.read.format("csv")
      .schema(schema_taxi_rate_code)
      .option("delimiter", ",")
      .option("header", "true")
      .load("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")    
  )

@create_view(name="map_paymentType")
def map_paymentType():
  return (
      spark.read.format("csv")
        .schema(schema_taxi_payment_type)
        .option("delimiter", ",")
        .option("header", "true")
        .load("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")  
  )
  
@create_view(name="map_point2Location")
def map_point2Location():
  return (
    spark.read.format("delta").load("/user/denny.lee/nyctaxi/map_point2Location") 
  )

#
# Source View
#

# Green Cab Source View
@create_view(name="source_GreenCab")
def source_GreenCab():
  return (
    spark.read.format("delta").load("/user/denny.lee/nyctaxi/nyctaxi_greencab_source")
  )
  #.expect("valid pickup_datetime", "lpep_pickup_datetime IS NOT NULL")
  #.expect("valid dropoff_datetime", "lpep_dropoff_datetime IS NOT NULL")


# 
# Bronze Tables
#
  
@create_table(
    name="bronze_GreenCab",
    partition_cols=["do_date"]
)
def bronze_GreenCab():
  return (
    read("source_GreenCab")
        .withColumnRenamed("lpep_dropoff_datetime", "do_datetime")
        .withColumnRenamed("lpep_pickup_datetime", "pu_datetime")
        .withColumnRenamed("dropoff_latitude", "do_lat")
        .withColumnRenamed("dropoff_longitude", "do_long")
        .withColumnRenamed("RatecodeID", "RateCodeID")
        .withColumn("do_date", expr("DATE(do_datetime)"))
        .withColumn("hour", expr("HOUR(do_datetime) AS hour"))
  )
#   .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")
#   .tableProperty("pipelines.metastore.tableName", "DAIS21.bronze_GreenCab")
#   .expectOrDrop("valid do_date", "do_date IS NOT NULL")
  
  
# 
# Silver Tables
# 

@create_table(
    name="silver_GreenCab",
    partition_cols=["do_date"]
)
def silver_GreenCab():
  ft = read("bronze_GreenCab")
  rc = read("map_rateCode")
  pt = read("map_paymentType")
  pl = read("map_point2Location")
  return (
      ft.join(rc, rc.RateCodeID == ft.RateCodeID)
        .join(pt, pt.payment_type == ft.payment_type)
        .join(pl, (pl.dropoff_latitude == ft.do_lat) & (pl.dropoff_longitude == ft.do_long) & (pl.lpep_dropoff_datetime == ft.do_datetime))
        .select("do_datetime", "pu_datetime", "do_date", "hour", "passenger_count", "do_lat", "do_long", 
                "RateCodeDesc", "payment_desc", "borough", "zone", 
                "fare_amount", "extra", "tip_amount", "tolls_amount", "total_amount")  
  )
#     .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")    
#     //.expect("non zero passenger count", "passenger_count > 0")
#     //.expectOrFail("non zero passenger count", "passenger_count > 0")
#     .expectOrDrop("non zero passenger count", "passenger_count > 0")
  
  
# 
# Gold Tables
# 
  
# Summary Stats  
@create_table(
  name="gold_summaryStats"
)  
def gold_summaryStats():
  return(
      read("bronze_GreenCab")
        .groupBy("do_date").agg(
            expr("COUNT(DISTINCT pu_datetime) AS pickups"), 
            expr("COUNT(DISTINCT do_datetime) AS dropoffs"),
            expr("COUNT(1) AS trips")
          )
  )
#  .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_summaryStats")


# Payment Type By Hour (across time)
@create_table(
    name="gold_paymentByHour"
)
def gold_paymentByHour():
    return (
        read("silver_GreenCab")
            .groupBy("hour", "payment_desc").agg(expr("SUM(total_amount) AS total_amount"))


    )
#     .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_paymentByHour")

  
# Four (of the 5) main boroughs
@create_table(
    name="gold_boroughs"
)
def gold_boroughs():
    return (
        read("silver_GreenCab")
            .where(expr("borough IN ('Bronx', 'Brooklyn', 'Queens', 'Manhattan')"))
            .select("borough", "do_datetime", "do_lat", "do_long", "RateCodeDesc", "payment_desc", "zone", "total_amount")
    )
#     .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_boroughs")  
#     .expectOrDrop("total amount > $3.00", "total_amount > 3.00")
