# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data_analytics.png)

# COMMAND ----------


!pip install plotly plotly_express
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### read UC streaming table 

# COMMAND ----------

import pandas as pd


#read UC table
df = spark.read.format("delta").table("demo_frank.motion.sensor")
pt = df.toPandas()

# make device ids shorter  
pt['device'] = pt['device'].str[-3:]

pt.columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### sizes and stats

# COMMAND ----------


#print(f"Size rebinned: {pt.shape} vs size before: {p.shape}")
print(f"first timestamp found:    {pt['time'].min()}") 
print(f"last timestamp found:     {pt['time'].max()}")
print(f"info():                   {pt.info}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### downsample the time axis to 10 seconds for all data

# COMMAND ----------

pt.set_index('time', inplace=True)  # Set 'time' as the index

# Resample the DataFrame for each 1-second interval, taking the maximum 'magn' value for each device
resampled_pt = pt.groupby('device').resample('2S')['magn'].max().fillna(0)

print(resampled_pt)

# COMMAND ----------

pt= resampled_pt

# COMMAND ----------

import plotly.graph_objects as go

# Reset the index of your DataFrame
pt_reset = pt.reset_index()

# Pivot the DataFrame
pt_pivot = pt_reset.pivot(index='device', columns='time', values='magn')

# Create the heatmap
# x=pt_pivot.columns[-5:], 

fig = go.Figure(data=go.Heatmap( 
    x=pt_pivot.columns[-10:] ,
    y=pt_pivot.index,
    z=pt_pivot.values,
    colorscale='Magma'  # fire-like colors
    
))

# warning layout limits the number of devices shown
#fig.update_layout(autosize=True)
fig.update_layout(width=1000, height=800) 
fig.show()

