# Databricks notebook source
pip install Faker

# COMMAND ----------

from faker import Faker
from datetime import datetime, timedelta
from random import randint, choice, uniform
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------



def generate_order_data(num_orders, start_order_id):
  # Initialize Faker and seed it for reproducibility
  fake = Faker()
  Faker.seed(42)

  # Define date range for order_date
  end_date = datetime.now() - timedelta(days=1)  # Maximum order date is yesterday
  start_date = end_date - timedelta(days=30)
  orders = []

  for order_id in range(start_order_id, num_orders + start_order_id):
        # Generate random timedelta for the current order_id
        days_offset = uniform(0, num_orders)  # Uniform random float between 0 and num_orders
        seconds_offset = uniform(0, (end_date - start_date).total_seconds())
        random_timedelta = timedelta(seconds=seconds_offset)

        order_date = start_date + random_timedelta
        order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        customer_id = str(randint(10001, 20000))
        price = fake.random_int(min=10, max=1000)
        product_id = randint(2000, 2100)
        # Randomly decide whether the order status will be "re-ordered"/"pending"
        # or any other status with 70% probability
        if uniform(0, 1) <= 0.3:
            order_status = choice(["re-ordered", "pending"])
        else:
            order_status = choice(["shipping", "delivered", "canceled", "returned"])

        order = {
            "order_id": order_id,
            "price": price,
            "order_status": order_status,
            "order_date": order_date_str,
            "customer_id": customer_id,            
            "product_id": product_id
            
        }
        orders.append(order)
  return orders

# COMMAND ----------

def create_new_orders(num_orders, current_max_order_id):
  return generate_order_data(num_orders,current_max_order_id + 1)

# COMMAND ----------

# DBTITLE 1,Randomly Update Records from Existing Orders
def update_existing_orders(existing_orders, percentage=0.3):
  # Filter the DataFrame to get the oldest pending orders
  num_existing_orders = existing_orders.count()
  num_orders_to_update = int(num_existing_orders * percentage)
  oldest_pending_orders_df = (
    existing_orders
      .filter(existing_orders.order_status == "pending")
      .orderBy("order_date")
      .limit(num_orders_to_update)
  )

  # Update the status of the oldest pending orders to "shipping", "delivered", or "cancelled"
  # set order_date to current_timestamp
  new_statuses = ["shipping", "delivered", "cancelled"]
  updated_pending_orders_df = (
    oldest_pending_orders_df
      .withColumn("order_status", expr("""
                                       CASE WHEN order_id % 3 = 0 THEN 'shipping' 
                                            WHEN order_id % 3 = 1 THEN 'delivered' 
                                            ELSE 'cancelled' END"""))
      .withColumn("order_date", date_format("current_timestamp", "yyyy-MM-dd HH:mm:ss"))
  )
  return updated_pending_orders_df

# COMMAND ----------

# DBTITLE 1,Randomly Delete from Existing Orders
def delete_existing_orders(existing_orders, percentage=0.1):
  # Filter the DataFrame to get the oldest returned orders
  num_existing_orders = existing_orders.count()
  num_orders_to_delete = int(num_existing_orders * percentage)
  oldest_returned_orders_df = (
    existing_orders
      .filter(existing_orders.order_status == "returned")
      .orderBy("order_date")
      .limit(num_orders_to_delete)
  )

  # Delete orders randomly from old returned orders associated with even ids
  # set order_date to current_timestamp
  deleted_returned_orders_df = (
    oldest_returned_orders_df[~((oldest_returned_orders_df['order_id'] % 2 == 0) & (oldest_returned_orders_df['order_status'] == 'returned'))]
  )
  return deleted_returned_orders_df

# COMMAND ----------

#Drop the database
# spark.sql(f"DROP DATABASE IF EXISTS {database_name}")

# COMMAND ----------

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),    
    StructField("product_id", IntegerType(), True)    
])
# userName = dbutils.entry_point.getDbutils().notebook().getContext().userName().getOrElse(None).split("@")[0].replace(".", "_") # changed it to underline to avoid conflict with catalog creation! 

# catalog_name = "orders" ## with UC <> DLT
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
# print(f"Catalog {catalog_name} created successfully.")
# spark.catalog.setCurrentCatalog(f"{catalog_name}")

# database_name = f"{userName}_ordersdb"

database_name = dbutils.widgets.get("snapshot_source_database")
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}` LOCATION '/user/hive/warehouse/{database_name}.db'")

# Check if the database was created and exists
print(f"{database_name} database already exists: {database_name in spark.catalog.listDatabases()}")

table_name = f"{database_name}.orders_snapshot"
# table_name = "orders_snapshots"
table_exists = spark.catalog.tableExists(table_name)

# print(f"{catalog_name}")
print(f"{database_name}")
print(f"{table_name}")     
print(table_exists)

# COMMAND ----------

"""
Set up configuration 
"""
# Define the dropdown options
dropdown_options = ["Pattern 1", "Pattern 2"]
# Create the dropdown widget
dbutils.widgets.dropdown("param_value", "Pattern 1", dropdown_options, "Parameter Value:")
# Define S3 bucket path
dbutils.widgets.text("source_s3_path", "s3a://one-env/snapshots/", "Source S3 Path:")
source_s3_path = dbutils.widgets.get("source_s3_path") #"s3a://one-env/snapshots/"
# Get the parameter value
param_value = dbutils.widgets.get("param_value") #set pattern as a widget in notebook, as a parameter in task
print(param_value)

# COMMAND ----------

from datetime import datetime

database_name = dbutils.widgets.get("snapshot_source_database")
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
# Set default database for current session
spark.catalog.setCurrentDatabase(database_name)

# Check if the database is set as default
print(spark.catalog.currentDatabase())
# spark.sql(f"USE {database_name}")
# spark.sql(f"USE CATALOG {catalog_name}")

if table_exists:
    print(f"{table_name} already exists. Creating new snapshots with updates and inserts, and deletes")
    existing_orders = spark.table(table_name)
    order_updates_df = update_existing_orders(existing_orders)
    print(f"number of updates: {order_updates_df.count()}")
    order_deletes_df = delete_existing_orders(existing_orders)
    print(f"number of deletes: {order_deletes_df .count()}")
    current_max_order_id = int(existing_orders.selectExpr("max(order_id)").collect()[0][0])
    print(f"Current Max order Id: {current_max_order_id}")
    new_orders = create_new_orders(10, current_max_order_id)
    new_orders_df = spark.createDataFrame(new_orders, schema=order_schema)
    print(f"number of new orders: {len(new_orders)}")

    remaining_existing_orders_df1 = existing_orders.join(order_updates_df, on="order_id", how="left_anti")
    remaining_existing_orders_df = remaining_existing_orders_df1 .join(order_deletes_df, on="order_id", how="left_anti")
    print(f"Existing rows retained: {remaining_existing_orders_df.count()}")
 
    if param_value == 'Pattern 1':
      (
        remaining_existing_orders_df
        .union(order_updates_df)
        .union(new_orders_df)
        .write
        .format("delta")
        .mode("overwrite") # for pattern 1
        .saveAsTable(table_name)
      )
      df = remaining_existing_orders_df.union(order_updates_df).union(new_orders_df)
      print("df in pattern 1 is:", display(df))
    else:
      df = remaining_existing_orders_df.union(order_updates_df).union(new_orders_df)
      print("df in pattern 2 is:", display(df))
      current_datetime = datetime.now()
      datetime_str = current_datetime.strftime('"%Y-%m-%d %H"')
      # Construct the output path with the date
      output_path = source_s3_path + "datetime=" +datetime_str
      print("output_path", output_path)
      (
          df
          .write
          .format("delta")
          .mode("overwrite") 
          .save(output_path)
      )

else:
    print(f"{table_name} doesn't exist. Creating orders table with initial snapshots data.")
    # Generate initial orders
    initial_orders = generate_order_data(100, 1)
    initial_orders_snapshot = spark.createDataFrame(initial_orders, schema=order_schema)

    if param_value == 'Pattern 1':
      initial_orders_snapshot.write.format("delta").mode("overwrite").saveAsTable(table_name)

    else:
      print("pattern 2")
      initial_orders_snapshot.write.format("delta").mode("overwrite").saveAsTable(table_name)
      current_datetime = datetime.now()
      datetime_str = current_datetime.strftime('"%Y-%m-%d %H"')
      # Construct the output path with the date
      output_path = source_s3_path + "datetime=" +datetime_str
      print("output_path", output_path)
      (
          initial_orders_snapshot
          .write
          .format("delta")
          .mode("overwrite") 
          .save(output_path)
      )
