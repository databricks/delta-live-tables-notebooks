# Databricks notebook source
import dlt
from pyspark.sql.functions import *

TABLE_NAME = "main.data.customers"


@dlt.table
def customer_agg():
    return spark.sql(
        f"""
        SELECT
            customer_id,
            min(amount) AS min_amount,
            max(amount) AS max_amount,
            avg(amount) AS avg_amount,
            sum(amount) AS total_amount
        FROM
            {TABLE_NAME}
        GROUP BY
            1
        """
    )
