# Databricks notebook source
# MAGIC %md 
# MAGIC # 00 Introduction to MapInPandas
# MAGIC
# MAGIC `This notebook is runnable in any Databricks workspace and was tested on DBR 12.2 LTS cluster runtime.`
# MAGIC
# MAGIC ## Understanding mapInPandas()
# MAGIC Enter the hero for this use case: Pyspark mapInPandas()
# MAGIC
# MAGIC Introduced in Spark 3.0.0., [mapInPandas()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html) allows you to efficiently complete arbitrary actions on each row of a Spark DataFrame with a Python-native function and yield more than one return row. Let’s go through an example of a basic function signature:
# MAGIC
# MAGIC ```
# MAGIC # MapInPandas Signature
# MAGIC def my_mapInPandas(dfs: Iterator[pd.DataFrame]) -> pd.DataFrame:
# MAGIC     # For every DataFrame:
# MAGIC     for df in dfs:
# MAGIC         # For every row in the DataFrame:
# MAGIC         for (index, row) in df.iterrows():
# MAGIC           # Do something with Pandas syntax...
# MAGIC           row[...] = ...
# MAGIC           # Yield resulting row
# MAGIC           yield row.to_frame().transpose()
# MAGIC
# MAGIC mapped_df = df.mapInPandas(my_mapInPandas, StructType(...))
# MAGIC ```
# MAGIC
# MAGIC A mapInPandas() function takes in an iterator of Pandas DataFrames, and returns a Pandas DataFrame. Most commonly, we will iterate to perform our task:
# MAGIC
# MAGIC 1. For every DataFrame in our iterator: in this statement, we can perform native Pandas dataframe operations. Most of the time though, if we wanted to operate at this level, we would either do it natively in Spark or convert to Pyspark Pandas.
# MAGIC 2. For every row in the DataFrame: now we’re operating row-wise. The most common use case for mapInPandas is to perform some kind of transform for each row of our input. 
# MAGIC 3. Within this inner loop, perform whatever Pandas or Python operations are needed. You can do anything here: unpack a data structure, make an API call, iterate again through some list of transformations, etc. 
# MAGIC 4. Finally, emit (e.g. `yield`) this row back to the calling Spark command. We convert each row back into a DataFrame using `row.to_frame().transpose()` to preserve columns and indices. 
# MAGIC 5. On your main Spark Dataframe, call the `.mapInPandas` method and pass in your function name and the Spark schema `StructType(...)` of the returned Spark DataFrame. This return schema is a crucial part of this approach, as it provides the “handshake” between Pandas and Spark as data is transferred in the cluster. 
# MAGIC
# MAGIC Seasoned Spark developers may look at this and think “For-loops should be avoided at all costs!”, but in this implementation, mapInPandas() uses Apache Arrow to exchange data directly between JVM and Python driver/executors with near-zero (de)serialization cost, and allows for efficient vectorized execution in Pandas via NumPy. While best practice is still to use native Spark functions if possible, we’re focusing on challenges that native Spark functions do not solve, but that are still more performant than a normal Spark UDF.
# MAGIC
# MAGIC To better understand this concept, let's simulate a basic example of unpacking JSON structures. 

# COMMAND ----------

# MAGIC %md ## Basic mapInPandas() Example
# MAGIC
# MAGIC Setup: Let’s say we have incoming records where each row needs to be “unpacked” into more than one row, and for some reason a Pyspark native function will not suffice (e.g. explode() command will not work). Put another way, we're just picking the minimum  example to illustrate this concept.
# MAGIC
# MAGIC The script below generates a few rows of random data to illustrate this concept:

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import random

# Define the schema for the key-value pair
pair_schema = StructType([
    StructField("fruit", StringType(), nullable=False),
    StructField("qty", StringType(), nullable=False)
])

# Function to generate a random key-value pair
def generate_random_key_value():
    keys = ["apples", "oranges"]  # Modify with desired keys
    pairs = []
    for _ in range(2):  # Generate 2 key-value pairs
        key = random.choice(keys)
        value = str(random.randint(1, 9))
        pairs.append((key, value))
    return pairs

# UDF to apply the random key-value pair generation function
generate_key_value_udf = udf(generate_random_key_value, ArrayType(pair_schema))

# Generate the DataFrame
df = spark.createDataFrame([(i+1,) for i in range(3)], ["id"]) \
          .withColumn("json_data", generate_key_value_udf())

display(df)

# COMMAND ----------

# MAGIC %md Observe that we have 3 rows, with multiple JSON objects in each row. We want to "unpack" these objects using some arbitrary logic. In the end, we should have 6 rows, as each input row has 2 items in its `json_data` list.
# MAGIC
# MAGIC The cell below illustrates how to complete this with mapInPandas():

# COMMAND ----------

# DBTITLE 1,With try/except logic
try:
  from collections import Iterator # Older Python versions
except:
  from collections.abc import Iterator # Tested on DBR 13+
from pyspark.sql.types import *
import pandas as pd
import traceback

# Return Schema from MapInPandas() function
return_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("row_id", LongType(), True),
        StructField("item", StringType(), True)
    ]
)

# MapInPandas Function
def unpack_json(itr: Iterator[pd.DataFrame]) -> pd.DataFrame:
    # For every DataFrame:
    for df in itr:
        # For every row in the DataFrame:
        for (index, row) in df.iterrows():
            # Use Try/Except block for debugging
            try:
                # For every item in each list in json_data
                item_counter = 0
                for json_item in row['json_data']:
                    row['row_id'] = item_counter
                    row['item'] = str(json_item)
                  
                    yield row.to_frame().transpose()
                    item_counter+=1  
            except:
                # Match the expected output format in case of errors, and log errors as return value:
                row['item']  = traceback.format_exc()
                row['row_id'] = traceback.format_exc()
                yield row.to_frame().transpose()

# Pass function and scheme to mapInPandas() method
mapped_df = df.mapInPandas(unpack_json, return_schema)

display(mapped_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Observations
# MAGIC * Return schema is likely different than input schema. You need to handle added+dropped columns in returned Pandas row object.
# MAGIC * Input is iterator of Pandas DataFrames, output is DataFrame.
# MAGIC * For-loops can get confusing (3 in this example!). Best to build incrementally. 
# MAGIC * Function must use all Pandas-like syntax (not Spark) (e.g. loc/iloc, value setting with square brackets). But you can do anything at all in this logic! 
# MAGIC * Yield allows you to return more than one output row per input row. Super useful!
# MAGIC * Development and debugging with mapInPandas() can get tricky, so including the `traceback.format_exc()` in case of errors is a little trick to more gracefully log error messages as part of any inner-loop logic. 
# MAGIC
# MAGIC ## Next Steps
# MAGIC Continue on to the next notebook, `01_StructuredStreaming_Queries`
