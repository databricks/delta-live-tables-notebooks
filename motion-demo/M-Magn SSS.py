# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/magn.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_frank.motion;
# MAGIC DESCRIBE table sensor;

# COMMAND ----------

from pyspark.sql.functions import window

# set watermark for 1 minute for time column
# spark.sql.shuffle.partitions (default 200, match number of cores)
spark.conf.set("spark.sql.shuffle.partitions", 30)


display(spark.readStream.format("delta").
        table("sensor").withWatermark("time", "3 seconds"). \
        groupBy(window("time", "1 second")).avg("magn").orderBy("window",ascending=False).limit(30))
