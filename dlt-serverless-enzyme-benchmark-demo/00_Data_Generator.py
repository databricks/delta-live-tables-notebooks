# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dbldatagen

# COMMAND ----------

from math import ceil
import dbldatagen as dg
from pyspark.sql import DataFrame


NUM_INSERT_ROWS = 200_000_000_000

NUM_APPEND_ROWS = 1000

NUM_ROWS_PER_FILE = 100_000

TABLE_NAME = "main.data.customers"


spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

unique_customers = ceil(NUM_INSERT_ROWS / 100)


def build_data_spec(year: str, num_rows: int) -> dg.DataGenerator:
    partitions = ceil(num_rows / NUM_ROWS_PER_FILE)
    print("PARTITIONS: ", partitions)
    return (
        dg.DataGenerator(spark, rows=num_rows, partitions=partitions)
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
            begin=f"{year}-01-01 00:00:00",
            end=f"{year}-12-31 23:59:59",
            random=True,
        )
        .withColumn("date", "date", expr="created_ts", baseColumn=["created_ts"])
    )


df: DataFrame = build_data_spec("2023", NUM_INSERT_ROWS).build()
df.write.format("delta").saveAsTable(TABLE_NAME, mode="overwrite")
for i in range(1, 4):
    df: DataFrame = build_data_spec("2024", NUM_APPEND_ROWS).build()
    df.write.format("delta").saveAsTable(f"{TABLE_NAME}_{i}", mode="overwrite")
