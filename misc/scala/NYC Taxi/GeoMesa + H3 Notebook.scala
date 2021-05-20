// Databricks notebook source
// MAGIC %md # [GeoMesa](https://www.geomesa.org/) + [H3](https://uber.github.io/h3/#/) Notebook
// MAGIC 
// MAGIC <hr>
// MAGIC NOTE: This is a **modified** version of the %md # [GeoMesa](https://www.geomesa.org/) + [H3](https://uber.github.io/h3/#/) Notebook from the blog [Processing Geospatial Data at Scale With Databricks](https://databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html).
// MAGIC * This builds the Delta table `map_points2Location` in the `/user/denny.lee/nyctaxi` directory
// MAGIC * This notebook supports the **2015 NYC Taxi Dashboard Pipeline**
// MAGIC <hr>
// MAGIC 
// MAGIC 1. Read NYC Taxi Data from Delta Table augmented with pickup / dropoff geometry columns
// MAGIC 2. Read WKT NYC Zone data from CSV augmented with geometry column
// MAGIC 3. Spatial Indexing of pickup / dropoff and zone DataFrames with H3
// MAGIC 4. Join indexed Zone DataFrame on pickup / dropoff indexed DataFrame.
// MAGIC 
// MAGIC __[Install Cluster Libraries](https://docs.databricks.com/libraries.html#install-a-library-on-a-cluster):__
// MAGIC 
// MAGIC * GeoMesa Maven Coordinates: `org.locationtech.geomesa:geomesa-spark-jts_2.11:2.3.2`
// MAGIC * H3 Maven Coordinates: `com.uber:h3:3.6.0`

// COMMAND ----------

import org.locationtech.jts.geom._
import org.locationtech.geomesa.spark.jts._

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

spark.withJTS

// COMMAND ----------

// MAGIC %md
// MAGIC ## NYC Taxi Data 
// MAGIC The  taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. We will be using data from 01-2009 to 06-2019 stored in https://registry.opendata.aws/nyc-tlc-trip-records-pds/ and originally from TLC website https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
// MAGIC 
// MAGIC For simplicity, we have copied a version of the records and have it accessible as in Delta format under 
// MAGIC /ml/blogs/

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Coordinates

// COMMAND ----------

//val dfRaw = spark.read.format("delta").load("/ml/blogs/geospatial/delta/nyc-green")
// Load GreenCab 2015 data
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema_green = new StructType()
    .add("VendorID", IntegerType)
    .add("lpep_pickup_datetime", TimestampType)
    .add("lpep_dropoff_datetime", TimestampType)
    .add("store_and_fwd_flag", StringType)
    .add("RatecodeID", IntegerType)
    .add("Pickup_longitude", StringType)
    .add("Pickup_latitude", StringType)
    .add("Dropoff_longitude", StringType)
    .add("Dropoff_latitude", StringType)
    .add("passenger_count", IntegerType)
    .add("trip_distance", DoubleType)
    .add("fare_amount", DoubleType)
    .add("extra", DoubleType)
    .add("mta_tax", DoubleType)
    .add("tip_amount", DoubleType)
    .add("tolls_amount", DoubleType)
    .add("ehail_fee", StringType)
    .add("improvement_surcharge", DoubleType)
    .add("total_amount", DoubleType)
    .add("payment_type", IntegerType)
    .add("trip_type", IntegerType)

val dfRaw = spark.read.option("header", true).schema(schema_green).csv("/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2015*.csv.gz")

// COMMAND ----------

val df = dfRaw.withColumn("pickup_point", st_makePoint(col("pickup_longitude"), col("pickup_latitude"))).withColumn("dropoff_point", st_makePoint(col("dropoff_longitude"),col("dropoff_latitude")))

// COMMAND ----------

display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

display(df.select("dropoff_point","lpep_dropoff_datetime"))

// COMMAND ----------

// MAGIC %md ## Load WKT of NYC Zone Data
// MAGIC 
// MAGIC * Data from https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc, exported as CSV which has the WKT.
// MAGIC * Data was placed in DBFS at `dbfs:/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv`

// COMMAND ----------

// MAGIC %sh mkdir -p /dbfs/tmp/nyctaxi/WKT/ && wget -O /dbfs/tmp/nyctaxi/WKT/taxi_zone.csv https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD && ls -al /dbfs/tmp/nyctaxi/WKT/

// COMMAND ----------

val wktDFText = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/tmp/nyctaxi/WKT/taxi_zone.csv")

val wktDF = wktDFText.withColumn("the_geom", st_geomFromWKT(col("the_geom"))).cache

display(wktDF)

// COMMAND ----------

// MAGIC %md ## Add H3 Indexes

// COMMAND ----------

import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object H3 extends Serializable {
  val instance = H3Core.newInstance()
}

val geoToH3 = udf{ (latitude: Double, longitude: Double, resolution: Int) => 
  H3.instance.geoToH3(latitude, longitude, resolution) 
}
                  
val polygonToH3 = udf{ (geometry: Geometry, resolution: Int) => 
  var points: List[GeoCoord] = List()
  var holes: List[java.util.List[GeoCoord]] = List()
  if (geometry.getGeometryType == "Polygon") {
    points = List(
      geometry
        .getCoordinates()
        .toList
        .map(coord => new GeoCoord(coord.y, coord.x)): _*)
  }
  H3.instance.polyfill(points, holes.asJava, resolution).toList 
}

val multiPolygonToH3 = udf{ (geometry: Geometry, resolution: Int) => 
  var points: List[GeoCoord] = List()
  var holes: List[java.util.List[GeoCoord]] = List()
  if (geometry.getGeometryType == "MultiPolygon") {
    val numGeometries = geometry.getNumGeometries()
    if (numGeometries > 0) {
      points = List(
        geometry
          .getGeometryN(0)
          .getCoordinates()
          .toList
          .map(coord => new GeoCoord(coord.y, coord.x)): _* )
    }
    if (numGeometries > 1) {
      holes = (1 to (numGeometries - 1)).toList.map(n => {
        List(
          geometry
            .getGeometryN(n)
            .getCoordinates()
            .toList
            .map(coord => new GeoCoord(coord.y, coord.x)): _*).asJava 
      })
    }
  }
  H3.instance.polyfill(points, holes.asJava, resolution).toList 
}

// COMMAND ----------

val res = 7
val dfH3 = df.withColumn("h3index", geoToH3(col("dropoff_latitude"), col("dropoff_longitude"), lit(res)))
val wktDFH3 = wktDF.withColumn("h3index", multiPolygonToH3(col("the_geom"),lit(res))).withColumn("h3index", explode($"h3index"))

// COMMAND ----------

// MAGIC %md ## Join Borough Polygons on Pickup Points (using H3 Indexes)

// COMMAND ----------

val dfWithBoroughH3 = dfH3.join(wktDFH3,"h3index") 

display(dfWithBoroughH3.select("zone","borough","dropoff_point","dropoff_longitude", "dropoff_latitude", "lpep_dropoff_datetime","h3index"))

// COMMAND ----------

// MAGIC %md ## Save generated table 

// COMMAND ----------

val map_pointToLocation = dfWithBoroughH3
  .select("zone","borough","dropoff_longitude", "dropoff_latitude", "lpep_dropoff_datetime", "h3index")

// COMMAND ----------

// dbutils.fs.rm("/user/denny.lee/nyctaxi/map_point2Location", recurse=true)

// COMMAND ----------

// Save table
map_pointToLocation.write.format("delta").save("/user/denny.lee/nyctaxi/map_point2Location")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md _While it is not shown here, in the blog we adapted some logic from [h3 example notebooks](https://github.com/uber/h3-py-notebooks/blob/master/H3%20API%20examples%20on%20Urban%20Analytics.ipynb) to visualization of taxi dropoff locations, with latitude and longitude binned at a resolution of 7 (1.22km edge length) and colored by aggregated counts within each bin._
