# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data-histo.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_frank.motion;
# MAGIC DESCRIBE sensor;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col, expr, max
# MAGIC
# MAGIC
# MAGIC # Read the sensor stream as a DataFrame
# MAGIC sensor_df = spark.readStream.format("delta").table("demo_frank.motion.sensor")
# MAGIC
# MAGIC # Apply the filter and aggregation
# MAGIC df = sensor_df \
# MAGIC     .groupBy("device") \
# MAGIC     .agg(max("magn").alias("max_magn"))
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(df)
