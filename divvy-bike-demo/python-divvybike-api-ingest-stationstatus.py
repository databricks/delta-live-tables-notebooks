# Databricks notebook source
import os, time
import requests
import json
from datetime import datetime

# COMMAND ----------

# Config section:
# Define the API response JSON storage path:
api_resp_path = '/FileStore/DivvyBikes/api_response/station_status'
# If dir doesnt exist then mkdir, else ignore:
dbutils.fs.mkdirs(api_resp_path)

# COMMAND ----------

# Delete dir if exists, else ignore:
#dbutils.fs.rm(api_resp_path, True)

# COMMAND ----------

# Define Realtime DivvyBike API URLs:
system_information = "https://gbfs.divvybikes.com/gbfs/en/system_information.json"
station_information = "https://gbfs.divvybikes.com/gbfs/en/station_information.json"
station_status = "https://gbfs.divvybikes.com/gbfs/en/station_status.json"
free_bike_status = "https://gbfs.divvybikes.com/gbfs/en/free_bike_status.json"
system_hours = "https://gbfs.divvybikes.com/gbfs/en/system_hours.json"
system_calendar = "https://gbfs.divvybikes.com/gbfs/en/system_calendar.json"
system_regions = "https://gbfs.divvybikes.com/gbfs/en/system_regions.json"
system_alerts = "https://gbfs.divvybikes.com/gbfs/en/system_alerts.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create API Response Path - Dir & File(s)
# MAGIC ##### Call RealTime DivvyBike APIs and GET JSONs:

# COMMAND ----------

now = datetime.now() # current date and time
fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
#print("date and time:",fmt_now)	
# Create the JSON file:
try:
  print('---------------------------------------------------')
  print('Creating empty JSON response file.')
  dbutils.fs.put(f"{api_resp_path}/station_status_{fmt_now}.json", "")
except:
  print('File already exists')
  
# Call API & get JSON response:
#----------------------------------------------------------
resp = requests.get(station_status)
if resp.status_code != 200:
    # This means something went wrong
    raise ApiError(f'GET /tasks/ {resp.status_code}')

print("Response Status Code : ", resp.status_code)
resp_json_str = resp.content.decode("utf-8")
print("Byte size of JSON Response: ", len(resp_json_str))
#----------------------------------------------------------
  
with open(f"/dbfs/{api_resp_path}/station_status_{fmt_now}.json","w") as f:
  f.write(resp_json_str)