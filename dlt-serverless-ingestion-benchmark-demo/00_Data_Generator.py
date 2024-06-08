# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dbldatagen

# COMMAND ----------

from math import ceil
import dbldatagen as dg
from pyspark.sql import DataFrame

NUM_INSERT_ROWS = 10_000_000_000

NUM_ROWS_PER_FILE = 100_000

# Change this to your prefer UC volume path
OUTPUT_LOCATION = "/Volumes/main/data/datagen/customers"

partitions = ceil(NUM_INSERT_ROWS / NUM_ROWS_PER_FILE)
print("PARTITIONS: ", partitions)

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

unique_customers = ceil(NUM_INSERT_ROWS / 10)
dataspec = (
    dg.DataGenerator(
        spark,
        rows=NUM_INSERT_ROWS,
        partitions=partitions
    )
    .withIdOutput()
    .withColumn("customer_id", "long", uniqueValues=unique_customers, random=True)
    .withColumn("name", percentNulls=0.01, template=r"\\w \\w|\\w a. \\w")
    .withColumn(
        "payment_instrument_type",
        values=[
            "paypal",
            "Visa",
            "Mastercard",
            "American Express",
            "discover",
            "branded visa",
            "branded mastercard",
        ],
        random=True,
        distribution="normal",
    )
    .withColumn("amount", "float", minValue=1, maxValue=9999, random=True)
    .withColumn("email", template=r"\\w.\\w@\\w.com|\\w-\\w@\\w")
    .withColumn("ip_address", template=r"\\n.\\n.\\n.\\n")
    .withColumn(
        "created_ts",
        "timestamp",
        begin="2023-01-01 00:00:00",
        end="2024-01-01 00:00:00",
        random=True,
    )
)
df: DataFrame = dataspec.build()

df.write.json(
    path=OUTPUT_LOCATION,
    mode="overwrite",
)
