# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data_analytics.png)

# COMMAND ----------

!pip install plotly plotly_express
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # read the UC sensor table as a pandas df

# COMMAND ----------

import pandas as pd


#read UC table
df = spark.read.format("delta").table("demo_frank.motion.sensor")
pt = df.toPandas()

# make device ids smaller 
pt['device'] = pt['device'].str[-3:]

#pt.columns
pt

# COMMAND ----------

# MAGIC %md
# MAGIC ## stats
# MAGIC

# COMMAND ----------


#print(f"Size rebinned: {pt.shape} vs size before: {p.shape}")
print(f"first timestamp found:    {pt['time'].min()}") 
print(f"last timestamp found:     {pt['time'].max()}")
print(f"no devices:               {pt['device'].unique().size}")

#print(f"info:                     {pt.info}")

# COMMAND ----------

# MAGIC %md
# MAGIC # resample the data to 10s windows

# COMMAND ----------

# reduce the devices 
#pt = pt[pt['device'].str[-1] > '9']

# COMMAND ----------

# Resample the DataFrame for each 1-second interval, 
# taking the maximum 'magn' value per each device

pt.set_index('time', inplace=True)  # Set 'time' as the index
resampled_pt = pt.groupby('device').resample('10S')['magn'].max().fillna(0)

print(resampled_pt)

# COMMAND ----------

pt= resampled_pt
pt.info
#pt = pt[pt['time'] < '2023-06-28 23:02:00']

# COMMAND ----------

import plotly.express as px

fig = px.scatter_3d(pt.reset_index(), x='device', y='time', z='magn' )
fig.update_layout(width=800,height=600)
fig.show()

