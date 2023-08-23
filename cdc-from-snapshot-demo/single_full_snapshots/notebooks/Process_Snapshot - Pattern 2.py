# Databricks notebook source
# DBTITLE 1,DLT Snapshot Processing Logic
import dlt
from datetime import timedelta
from datetime import datetime

def exist(path):
  try:
    if dbutils.fs.ls(path) is None:
      return False
    else:
      return True
  except:
    return False  
  

snapshot_root_path = spark.conf.get("snapshot_root_path")  #"s3a://one-env/snapshots/"

# List all objects in the bucket using dbutils.fs
object_paths = dbutils.fs.ls(snapshot_root_path)

datetimes = []
for path in object_paths:
    # Extract the datetime string from the path using string manipulation
    datetime_str = path.name.split('=')[1].strip('/"')

    # Parse the datetime string to a datetime object
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H')
    datetimes.append(datetime_obj)

# Find the earliest datetime
earliest_datetime = min(datetimes)

# Convert the earliest datetime back to a string if needed
earliest_datetime_str = earliest_datetime.strftime('"%Y-%m-%d %H"')

print(f"The earliest datetime in the bucket is: {earliest_datetime_str}")

def next_snapshot_and_version(latest_snapshot_datetime):
  latest_datetime_str = latest_snapshot_datetime or earliest_datetime_str 

  # Convert the earliest datetime back to a string if needed
  # latest_datetime_str = earliest_datetime_str
  
  latest_datetime = datetime.strptime(latest_datetime_str, '"%Y-%m-%d %H"')
  print(latest_datetime)

  # Calculate the next datetime
  increment = timedelta(hours=1)  # Increment by 1 hour
  next_datetime = latest_datetime + increment 
  print(f"The next datetime in the bucket is: {next_datetime}")

  # Convert the next_datetime to a string with the desired format
  next_snapshot_datetime= next_datetime.strftime('"%Y-%m-%d %H"')
  snapshot_path = snapshot_root_path + "datetime={}".format(next_snapshot_datetime)
  print("reading from snapshot " + snapshot_path)

  if (exist(snapshot_path)):
    return(spark.read.format("delta").load(snapshot_path), next_snapshot_datetime)
  else:
    # No snapshot available
    return None 
  
  """
##Create the target table 
"""

dlt.create_streaming_table(name="orders_pattern2",
  comment="Clean, merged final table from the full snapshots",
  table_properties={
    "quality": "gold"
  }
)

dlt.apply_changes_from_snapshot(
target = "orders_pattern2",
snapshot_and_version = next_snapshot_and_version,
keys = ["order_id"],
stored_as_scd_type = 2,
track_history_column_list = ["order_status"]
)

# COMMAND ----------

# DBTITLE 1,Test Logic in The Notebook (Optional)
from datetime import datetime
from datetime import timedelta

def exist(path):
  try:
    if len(dbutils.fs.ls(path)) == 0:
      return False
    else:
      return True
  except:
    return False  
snapshot_root_path = "s3a://one-env/snapshots/" 

# List all objects in the bucket using dbutils.fs
object_paths = dbutils.fs.ls(snapshot_root_path)

datetimes = []
for path in object_paths:
    # Extract the datetime string from the path using string manipulation
    datetime_str = path.name.split('=')[1].strip('/"')

    # Parse the datetime string to a datetime object
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H')
    datetimes.append(datetime_obj)

# Find the earliest datetime
earliest_datetime = min(datetimes)

# Convert the earliest datetime back to a string if needed
latest_datetime_str = earliest_datetime.strftime('"%Y-%m-%d %H"')

print(f"The earliest datetime in the bucket is: {latest_datetime_str}")

# Convert the input date string to a datetime object
latest_datetime = datetime.strptime(latest_datetime_str, '"%Y-%m-%d %H"')
print(latest_datetime)

# Calculate the next datetime
increment = timedelta(hours=1)  # Increment by 1 minute
next_datetime = latest_datetime + increment #datetime.timedelta(days=1)
print(f"The next datetime in the bucket is: {next_datetime}")

# Convert the next_datetime to a string with the desired format
next_snapshot_datetime= next_datetime.strftime('"%Y-%m-%d %H"')

# # Convert the next date to a string
# next_snapshot_datetime = next_date.strftime('"%Y-%m-%d"')
print(next_snapshot_datetime)
snapshot_path = snapshot_root_path + "datetime={}".format(next_snapshot_datetime)
print("reading from snapshot " + snapshot_path)

if (exist(snapshot_path)):
  print(spark.read.format("delta").load(snapshot_path), latest_datetime_str)
  display(spark.read.format("delta").load(snapshot_path))

