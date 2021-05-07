// Databricks notebook source
package com.databricks.pipelines.examples.nyctaxi_dashboard

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{lit, schema_of_json, from_json}
import collection.JavaConverters._

import com.databricks.pipelines._


class NYCTaxiPipeline extends Pipeline with Implicits {
  // 
  // Specify schemas
  //
  val schema_taxi_rate_code = new StructType()
    .add("RateCodeID", IntegerType)
    .add("RateCodeDesc", StringType)

  val schema_taxi_payment_type = new StructType()
    .add("payment_type", IntegerType)
    .add("payment_desc", StringType)

  //
  // Lookup Tables
  //

  // Taxi Rate Code
  createView("map_rateCode")
    .query {
        spark.read.format("csv")
          .schema(schema_taxi_rate_code)
          .option("delimiter", ",")
          .option("header", "true")
          .load("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")
    }
  
  // Taxi Payment Type
  createView("map_paymentType")
    .query {
        spark.read.format("csv")
          .schema(schema_taxi_payment_type)
          .option("delimiter", ",")
          .option("header", "true")
          .load("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")  
    }
  
  // map_point2Location
  createView("map_point2Location")
    .query {
        spark.read.format("delta").load("/user/denny.lee/nyctaxi/map_point2Location")    
    }
  

  //
  // Source View
  //
  
  // Green Cab Source View
  createView("source_GreenCab")
    .query {
        spark.read.format("delta").load("/user/denny.lee/nyctaxi/nyctaxi_greencab_source") 
    }
    .expect("valid pickup_datetime", "lpep_pickup_datetime IS NOT NULL")   
    .expect("valid dropoff_datetime", "lpep_dropoff_datetime IS NOT NULL")

  
  // 
  // Bronze Tables
  //
  
  // bronze_GreenCab
  createTable("bronze_GreenCab")
    .query {
        read("source_GreenCab")
            .withColumnRenamed("lpep_dropoff_datetime", "do_datetime")
            .withColumnRenamed("lpep_pickup_datetime", "pu_datetime")
            .withColumnRenamed("dropoff_latitude", "do_lat")
            .withColumnRenamed("dropoff_longitude", "do_long")
            .withColumn("do_date", expr("DATE(do_datetime)"))
            .withColumn("hour", expr("HOUR(do_datetime) AS hour"))
    }
   .partitionBy("do_date")
   .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")
   .tableProperty("pipelines.metastore.tableName", "DAIS21.bronze_GreenCab")
   .expectOrDrop("valid do_date", "do_date IS NOT NULL")
  
  
  // 
  // Silver Tables
  // 
  
  // silver_GreenCab
  createTable("silver_GreenCab")
    .query {
        val map_paymentType = read("map_paymentType")
        val map_rateCode = read("map_rateCode")
        val map_point2Location = read("map_point2Location")
        val bronze_GreenCab = read("bronze_GreenCab")
        bronze_GreenCab
            .join(map_rateCode, (bronze_GreenCab("RateCodeID") === map_rateCode("RateCodeID")))
            .join(map_paymentType, (bronze_GreenCab("payment_type") === map_paymentType("payment_type")))
            .join(map_point2Location, 
                    (bronze_GreenCab("do_lat") === map_point2Location("dropoff_latitude")) && 
                    (bronze_GreenCab("do_long") === map_point2Location("dropoff_longitude")) &&
                    (bronze_GreenCab("do_datetime") === map_point2Location("lpep_dropoff_datetime"))
                )
            .select("do_datetime", "pu_datetime", "do_date", "hour", "passenger_count", "do_lat", "do_long",
                    "RateCodeDesc", "payment_desc", "borough", "zone", 
                    "fare_amount", "extra", "tip_amount", "tolls_amount", "total_amount")
    }
    .partitionBy("do_date")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")    
    //.expect("non zero passenger count", "passenger_count > 0")
    //.expectOrFail("non zero passenger count", "passenger_count > 0")
    .expectOrDrop("non zero passenger count", "passenger_count > 0")
  
  
  //
  // Gold Tables
  //
  
  // Summary Stats  
  createTable("gold_summaryStats")  
    .query {
        read("bronze_GreenCab")
          .groupBy("do_date").agg(
              expr("COUNT(DISTINCT pu_datetime) AS pickups"), 
              expr("COUNT(DISTINCT do_datetime) AS dropoffs"),
              expr("COUNT(1) AS trips")
            )
    }
    .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_summaryStats")
  
  // Payment Type By Hour (across time)
  createTable("gold_paymentByHour")
    .query {
        read("silver_GreenCab")
          .groupBy("hour", "payment_desc").agg(expr("SUM(total_amount) AS total_amount"))
    }
    .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_paymentByHour")

  
  // Four (of the 5) main boroughs
  createTable("gold_boroughs")
    .query {
        read("silver_GreenCab")
          .where(expr("borough IN ('Bronx', 'Brooklyn', 'Queens', 'Manhattan')"))
          .select("borough", "do_datetime", "do_lat", "do_long", "RateCodeDesc", "payment_desc", "zone", "total_amount")
    }
    .tableProperty("pipelines.metastore.tableName", "DAIS21.gold_boroughs")  
    .expectOrDrop("total amount > $3.00", "total_amount > 3.00")
  
  
  //
  // Event Log Tables
  //
  // Uncomment after running 2nd run
  createTable("expectations_log")
    .query {
        val pipelinesId = spark.conf.get("pipelines.id")
        //val pipelinesId = "59cf8076-61aa-488b-9139-edba476b0c91"
        val sqlQuery = """SELECT id, origin, timestamp, details 
                            FROM delta.`dbfs:/pipelines/""" + pipelinesId + """/system/events/` 
                           WHERE details LIKE '%flow_progress%data_quality%expectations%'"""
        val df = spark.sql(sqlQuery)
        val schema = schema_of_json("""{"flow_progress":{
                                      "status":"COMPLETED",
                                      "metrics":{"num_output_rows":91939},
                                      "data_quality":{"dropped_records":32,
                                      "expectations":[{"name":"non zero passenger count","dataset":"silver_GreenCab","passed_records":91939,"failed_records":32}]}}
                                     }""")      
        //val schema = schema_of_json(lit(df.select($"details").as[String].first))
        val df_expectations = df.withColumn("details_json", from_json($"details", schema, Map[String, String]().asJava))
        df_expectations.select("id", "timestamp", "origin.pipeline_id", "origin.pipeline_name", "origin.cluster_id", "origin.flow_id", "origin.flow_name", "details_json") 
    }
   .tableProperty("pipelines.autoOptimize.zOrderCols", "do_datetime")
   .tableProperty("pipelines.metastore.tableName", "DAIS21.expectations_log")
  
}
