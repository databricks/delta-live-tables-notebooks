# Databricks notebook source
# MAGIC %md # 01 DLT Logic for Stateful Time Weighted Average
# MAGIC
# MAGIC This notebook contains the Delta Live Tables (DLT) logic to compute stateful integrals. Before attaching to a pipeline, review the logic below.
# MAGIC
# MAGIC Default settings for this notebook can be found in the `/Resources/config` notebook. You can change these to modify where sample data is written. 

# COMMAND ----------

import dlt
import pandas as pd
from datetime import datetime, timedelta
from typing import Iterator
from pyspark.sql.functions import *
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, ArrayType, TimestampType, FloatType, IntegerType, StringType

# COMMAND ----------

# Get parameters from Pipeline Conf
try: schema_name = spark.conf.get("schema_name"); raw_table = spark.conf.get("raw_table")
except: schema_name = "demo_dlt_integrals"; raw_table = "raw"

# Determine if UC is being used, if not use HMS
try: catalog = spark.conf.get("catalog")
except: catalog = "hive_metastore"

print(f"Sourcing data from {catalog}.{schema_name}.{raw_table}")

# COMMAND ----------

# MAGIC %md # Source table for raw events
# MAGIC
# MAGIC First, we will create a DLT table to read from our source of events as a stream. We do this here for demo purposes so the table appears in our DLT UI and to help understand how records flow through the state logic. 
# MAGIC
# MAGIC For more information on DLT decorators (e.g. `@dlt.table()`) and other syntax, please see [Delta Live Tables intro](https://www.databricks.com/discover/pages/getting-started-with-delta-live-tables).

# COMMAND ----------

@dlt.table()
def input_table():
  df = (
      spark
        .readStream.format("delta")
        .table(f"{catalog}.{schema_name}.{raw_table}")
   )
  return df

# COMMAND ----------

# MAGIC %md # ApplyInPandasWithState() Logic
# MAGIC
# MAGIC The code below builds our logic to use ApplyInPandasWithState(). 

# COMMAND ----------

# MAGIC %md ## Pandas Time-Weighted Average Logic
# MAGIC We will start by defining our Pandas (e.g. single-node) logic to calculate time-weighted averages given arrays of timestamps and measurements. This function takes in the arrays of observed sensor vales that have been "buffered" in the state variables, namely `values_arr` and `timestamps_arr`. We cannot guarantee that values arrive in order (e.g. a value from timestamp 2 may arrive or be processed before the value from teimstamp 1), so this funciton also handles data ordering. In addition, since each time interval is "blind" (does not have information) about the preceeding or following time interval, this handles calculating an initial starting value for a window using the first observation. 
# MAGIC
# MAGIC Note that this is just a regular Python function (not a Spark or DLT function), as its serialized across the cluster when called by `ApplyInPandasWtihState()` on a group of data.

# COMMAND ----------

def pd_time_weighted_average(location_id, sensor
, timestamp_10min_interval, values_arr, timestamps_arr, interval_mins=10):
    """Calculated time-weighted average by constructing a Pandas DF from a buffered list of values and timestamps. This function is called by the Stateful Streaming aggregator.
    Parameters:
    ----------
    location_id
    sensor
    timestamp_10min_interval:
      The grouping keys for ApplyInPandasWithState, e.g. location_id, sensor, timestamp_10min_interval
      E.g. Location_id = L1, sensor = Windspeed, timestamp_..._interval = "12-12:10"
    values_arr: List
      List of values buffered for this state
    timestamps_arr: List
      List of timestamps buffered for this sate

    Yields:
    --------
    time_weighted_row: Pandas Row 
      Row object with schema: location_id string, sensor string, timestamp timestamp, value float, count_rows int
    """

    # Reconstruct Pandas DataFrame using inputs
    df_pd = pd.DataFrame({
      'location_id':location_id,
      'sensor':sensor,
      'value': values_arr,
      'timestamp': timestamps_arr
      })

    # Sort dataframe by timestamp column ascending
    df_pd = df_pd.sort_values(by='timestamp', ascending=True)

    # Get first row and use that as the starting value; Set timestamp to start of interval
    opening_row = df_pd.iloc[[0],:].copy()
    opening_row['timestamp'] = timestamp_10min_interval - timedelta(minutes=10)

    # Get latest row for group, make a copy, then reset timestamp to the END of the interval, then append back
    closing_row = df_pd.iloc[[-1],:].copy()
    closing_row['timestamp'] = timestamp_10min_interval 

    # Concat rows back together
    df_agg = pd.concat([opening_row, df_pd, closing_row], ignore_index=True)

    # Get count of rows used in calculating this aggregates
    count_rows = df_pd.shape[0]

    # Calculate difference between current timestamp and start of next one, fill nulls
    df_agg['time_diff'] = df_agg['timestamp'].diff(periods=-1).dt.total_seconds()/60*-1
    df_agg['time_diff'] = df_agg['time_diff'].fillna(0).astype(int)
      
    # Calculate weighted value as: value x time_diff
    # This is the area within each "Rieman sum"
    df_agg['weighted_value'] = df_agg['value'] * df_agg['time_diff'] 
      
    # Divide the area under the value curve by the number of mins in the interval
    time_interval_weighted_avg = df_agg['weighted_value'].sum() / interval_mins

    # Return single Pandas Dataframe row with agg
    return pd.DataFrame({'location_id':[location_id], 
                         'sensor':[sensor],
                         'timestamp': [timestamp_10min_interval], # Rename timestamp column
                         'value': [time_interval_weighted_avg],
                         'count_rows': [count_rows] })

# COMMAND ----------

# MAGIC %md ### Optional: Pandas test function
# MAGIC Uncomment and run the cell below to test this function. Before applying our function, our Spark stream will add logic to compute what 10-min time interval each group of record belongs to; we'll add that directly to the test examples. Note that this cell needs to be commented out to run in DLT. 

# COMMAND ----------

# from pandas.testing import assert_frame_equal

# # Call pandas function. Note that values are provided out of order to demo function can still calculate result.
# function_results = pd_time_weighted_average(
#   location_id="L1", 
#   sensor="Wind_Speed", 
#   timestamp_10min_interval=pd.to_datetime('2024-01-01 12:20:00'), 
#   values_arr= [10.0, 20.0, 30.0, 40.0], 
#   timestamps_arr= [
#     pd.to_datetime("2024-01-01 12:11:00"),  
#     pd.to_datetime("2024-01-01 12:12:00"),
#     pd.to_datetime("2024-01-01 12:19:00"),  
#     pd.to_datetime("2024-01-01 12:14:00")
#     ], 
#   interval_mins=10)

# expected_results = pd.DataFrame({
#         'location_id': ['L1'],
#         'sensor': ['Wind_Speed'],
#         'timestamp': [pd.to_datetime('2024-01-01 12:20:00')],
#         'value': [29.0],
#         'count_rows': [4]
#     })

# # Assert equals using Pandas testing function
# assert_frame_equal(function_results, expected_results, check_like=True)

# display(function_results)
# display(expected_results)

# COMMAND ----------

# MAGIC %md ## Spark ApplyInPandasWithState Function
# MAGIC
# MAGIC The function below is our Spark [ApplyInPandasWithState() function](https://spark.apache.org/docs/latest/api//python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandasWithState.html), which contains the logic of 
# MAGIC * How to initiate and maintain state for each key group
# MAGIC * How long to wait for new records for each group
# MAGIC * How to buffer new observations as they arrive for each group
# MAGIC * When to emit the row for each state group (which calls our time-weighted average Pandas function)
# MAGIC
# MAGIC The important concept here is this function is called for each key group that arrives in the input stream; in other words, for each tuple of `(location_id, sensor, timestamp_10min_interval)`, this function runs one time.

# COMMAND ----------

# ApplyInPandasWithState Function
def stateful_time_weighted_average(key, df_pds: Iterator[pd.DataFrame], state: GroupState) -> pd.DataFrame:
  """Calculate stateful time weighted average for a time interval + location + sensor combination
  Parameters:
  ----------
  key: tuple of Numpy data types
    The grouping keys for ApplyInPandasWithState, e.g. location_id, sensor, timestamp_10min_interval
    E.g. Location_id = L1, sensor = Windspeed, timestamp_..._interval = "12-12:10"
  df_pdfs: Iterator[pd.DataFrame]
    Iterator of Pandas DataFrames. 
  state: GroupState
    State of the current key group

  Yields:
  --------
  time_weighted_row: Pandas Row 
    Row object with schema: location_id string, sensor string, timestamp timestamp, value float, count_rows int
  """

  # Time interval for group, in minutes
  interval_mins = 10

  # Read Grouping Keys
  (location_id, sensor, timestamp_10min_interval) = key

  # If state has timed out, emit this row
  if state.hasTimedOut:
    # Instantiate arrays with "buffered" state (all observations so far)
    (values_arr, timestamps_arr) = state.get
    
    # Clean up state
    state.remove()
      
    # Call function to compute time weighted average
    time_weighted_row = pd_time_weighted_average(location_id, sensor, timestamp_10min_interval, values_arr, timestamps_arr)

    # NOTE ON PRINT STATEMENTS: run the DLT pipeline with a single node (zero workers) to see print statements in Driver Logs UI
    print("IN TIMEOUT LOOP")
    print(f". EMITTING ROW FOR KEY: {location_id}, {sensor}, {timestamp_10min_interval}")
    print(f". CURRENT WATERMARK OF STREAM: {state.getCurrentWatermarkMs()}")
    
    # Return resulting row to calling stream
    yield time_weighted_row

  else:
    # Variables from state, if exists (other records for these keys have been added to state)
    if state.exists:
      # Instantiate arrays with "buffered" state (all observations so far)
      # value_arr = [10, 20, ...]
      # timestamps_arr = [1:11, 1:12, ...]
      (values_arr, timestamps_arr) = state.get
        
    # No state exists for this set of keys
    else:
      # Initialize empty arrays to buffer values/timestamps
      (values_arr, timestamps_arr) = ([], [])

    # Iterate through input pandas dataframes
    for df_pd in df_pds:
      # Extend buffers with list of values and timestamps
      values_arr.extend(df_pd['value'].tolist())
      timestamps_arr.extend(df_pd['timestamp'].tolist())

    # Update state with new values
    state.update([values_arr, timestamps_arr])
    
    # Determine upper bound of timestamps in this interval using grouping TS values
    interval_upper_timestamp = int(timestamp_10min_interval.timestamp() * 1000)
    print("IN UPDATE LOOP")
    print(f". FOR KEY: {location_id}, {sensor}, {timestamp_10min_interval}")
    print(f". INTERVAL UPPER TIMESTAMP: {interval_upper_timestamp}")

    # Reset timeout timestamp to upper limit of interval. When the watermark has passed this interval, it will emit a record.
    # E.g. if this is the 12:10 interval including all records between 12:00->12:10, timeout will be once the watermark passes 12:10
    timeout = int(interval_upper_timestamp)

    # To rather include a buffer before new rows get emitted, in the case you are modifying the watermark, uncomment below
    # timeout = int(interval_upper_timestamp+(60000 * interval_mins))

    # Catch edge cases for watermark, e.g. where equal to epoch or boundary
    if timeout <= state.getCurrentWatermarkMs():
      timeout = int(state.getCurrentWatermarkMs()+(60000 * interval_mins))

    print(f". SETTING TIMEOUT TIMESTAMP: {timeout}")

    state.setTimeoutTimestamp(timeout)

    print(f". CURRENT WATERMARK OF STREAM: {state.getCurrentWatermarkMs()}")

# COMMAND ----------

# MAGIC %md ## DLT Table Definition
# MAGIC
# MAGIC Finally, we can write our DLT function (with decorator) to create a Streaming Table with these results. When we first run this, no rows will be added to the target table until its watermark interval is closed; in other words, we won't see any data added to our DLT table called `dlt_integrals` until there are records in the stream +10 mins after each group's timestamps. This is what we want: the stateful stream will keep track of each state as records arrive over time, then append that row only once to the output table when its watermark is "closed" 

# COMMAND ----------

@dlt.table()
def dlt_integrals():
  # output_schema defines the Spark dataframe type that is returned from stateful_time_weighted_average
  output_schema = StructType([
    StructField("location_id", StringType(), True),
    StructField("sensor", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", FloatType(), True),
    StructField("count_rows", IntegerType(), True)
  ])

  # state_schema persists between microbatches, and is used to "buffer" our observations
  state_schema = StructType([
    StructField('values_arr', ArrayType(FloatType()), True),
    StructField('timestamps_arr', ArrayType(TimestampType()), True)
  ])
  
  # Read source data from the first DLT table we created in this notebook
  df = dlt.read_stream("input_table")

  # Use window function to determine which timestamp interval each record belongs to
  df = (df.withColumn("timestamp_10min_interval", window('timestamp', '10 minutes')['end'])
  )

  # Apply watermark to our stream based on time interval, then groupBy.Apply() logic
  grp = (df
        .withWatermark('timestamp_10min_interval','10 minutes')
        .groupBy(['location_id', 'sensor', 'timestamp_10min_interval'])
        .applyInPandasWithState(
            func = stateful_time_weighted_average, 
            outputStructType = output_schema,
            stateStructType  = state_schema,
            outputMode = "append",
            timeoutConf = GroupStateTimeout.EventTimeTimeout
          )
  )

  return grp

# COMMAND ----------

# MAGIC %md Now that you've reviewed the DLT logic, we're ready to add this to a DLT pipeline! Return to the `00_Runbook_for_Demo` notebook to put this logic into a DLT pipeline.
