# Databricks notebook source
# Import libraries
import os, time
import requests
import json
from datetime import datetime

# COMMAND ----------

# Define the weather API response JSON storage path:
api_resp_path_weather = '/FileStore/DivvyBikes/api_response/weather_info'

# If dir doesnt exist then mkdir, else ignore:
dbutils.fs.mkdirs(api_resp_path_weather)

# COMMAND ----------

# Delete dir if exists, else ignore:
#dbutils.fs.rm(api_resp_path_weather, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get the list of station details to pass to the API

# COMMAND ----------

# Get a list of stations and locations and put into a DataFrame
stationsDF = spark.sql("""
  SELECT 
    stations.station_id AS station_id,
    stations.lat AS lat,
    stations.lon AS lon
  FROM (
    SELECT 
      EXPLODE(data.stations) AS stations
    FROM json.`/FileStore/DivvyBikes/api_response/station_information`)
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create API Response Path - Dir & File(s)
# MAGIC ##### Call RealTime OpenWeather API and GET JSONs:

# COMMAND ----------

# Replace XXXXX below with your own API key by signing up for free at https://home.openweathermap.org/users/sign_up
api_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

# Some more stuff to fill out the URL:
url_part1 = "https://api.openweathermap.org/data/2.5/weather?lat="
url_part2 = "&appid="

# OpenWeather API only allows 60 calls per min so need to pause in between calls:
sleep_time = 1

# Record counter
rec_cnt = 0

# Loop through each row in the DataFrame
dataCollect = stationsDF.collect()
for row in dataCollect:
  now = datetime.now() # current date and time
  fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
  rec_cnt = rec_cnt + 1 # Increment counter
  
  print('---------------------------------------------------')
  print('Processing Record Number: ', rec_cnt)
  
  # Define the full API call for current record in the DataFrame
  full_url = url_part1 + str(row['lat']) + "&lon=" + str(row['lon']) + url_part2 + api_key
  
  # Call API & get JSON response:
  resp = requests.get(full_url)
  if resp.status_code != 200:
      # This means something went wrong
      raise ApiError(f'GET /tasks/ {resp.status_code}')

  print("Response Status Code : ", resp.status_code)
  resp_json_str = resp.content.decode("utf-8")
  print("Byte size of JSON Response: ", len(resp_json_str))

  #Define full path + file name string
  full_string = api_resp_path_weather + "/weather_info_" + fmt_now + "_station_" + row['station_id'] + ".json"

  # Write to DBFS dir
  dbutils.fs.put(full_string, resp_json_str, True)
    
  # Wait so we limit to 1 API call per second
  print(f"Sleeping for {sleep_time} seconds")
  time.sleep(sleep_time)