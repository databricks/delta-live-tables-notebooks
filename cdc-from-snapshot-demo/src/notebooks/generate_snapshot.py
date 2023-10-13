# Databricks notebook source
# DBTITLE 1, Import modules and functions
from faker import Faker
from datetime import datetime, timedelta
from random import randint, choice, uniform
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------
# DBTITLE 1,Notebook Parameters
dbutils.widgets.removeAll()

snapshot_pattern_options = ["Pattern 1", "Pattern 2"]
dbutils.widgets.dropdown(name="snapshot_pattern", defaultValue="Pattern 1", choices=snapshot_pattern_options)
dbutils.widgets.text(name="num_orders", defaultValue="10") # number of orders to generate
snapshot_pattern = dbutils.widgets.get("snapshot_pattern")
num_orders = int(dbutils.widgets.get("num_orders"))
if snapshot_pattern == 'Pattern 1':
    dbutils.widgets.text(name="snapshot_source_database", defaultValue="")  # required for pattern 1
    database_name = dbutils.widgets.get("snapshot_source_database")
    table_name = f"{database_name}.orders_snapshot"

    print(f"""Number of Orders: {num_orders}
Snapshot Pattern: {snapshot_pattern}
Snapshot Database Name: {database_name}
Snapshot Table Name: {table_name}""")
    
if snapshot_pattern == 'Pattern 2':
    dbutils.widgets.text(name="snapshot_source_path", defaultValue="")  # required for pattern 2
    snapshot_source_path = dbutils.widgets.get("snapshot_source_path")
    print(f"""Number of Orders: {num_orders}
Snapshot Pattern: {snapshot_pattern}
Snapshot root path: {snapshot_source_path}""")

# COMMAND ----------
# DBTITLE 1, Generate Random Orders

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
            order_status = choice(["shipping", "delivered", "cancelled", "returned"])

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

def create_new_orders(num_orders, current_max_order_id):
    return generate_order_data(num_orders, current_max_order_id + 1)

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
        oldest_returned_orders_df[~((oldest_returned_orders_df['order_id'] % 2 == 0) & (
                oldest_returned_orders_df['order_status'] == 'returned'))]
    )
    return deleted_returned_orders_df


# COMMAND ----------

# schema of the order table
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", IntegerType(), True)
])


def get_initial_order_snapshot():
    initial_orders = generate_order_data(num_orders, 1)
    initial_orders_snapshot = spark.createDataFrame(initial_orders, schema=order_schema)
    print(f"Initial snpashot data:  {display(initial_orders_snapshot)}")
    return initial_orders_snapshot


def get_incremental_order_snapshot(pattern):
    existing_orders = None
    if pattern == 'Pattern 1':
        existing_orders = spark.table(table_name)
    elif pattern == 'Pattern 2':
        existing_orders = spark.read.format("parquet").load(path=snapshot_source_path)
    else:
        raise ValueError(f"Unknown Pattern - {pattern}")

    order_updates_df = update_existing_orders(existing_orders)
    print(f"number of updates: {order_updates_df.count()}")
    order_deletes_df = delete_existing_orders(existing_orders)
    print(f"number of deletes: {order_deletes_df.count()}")
    current_max_order_id = int(existing_orders.selectExpr("max(order_id)").collect()[0][0])
    print(f"Current Max order Id: {current_max_order_id}")

    # ensure all dataframes have the same schema
    common_columns = ['order_id', 'price', 'order_status', 'order_date', 'customer_id', 'product_id']
    existing_orders = existing_orders.select(*common_columns)
    order_updates_df = order_updates_df.select(*common_columns)

    order_deletes_df = order_deletes_df.select(*common_columns)
    num_new_orders = int(num_orders * 0.2)
    new_orders = create_new_orders(num_new_orders, current_max_order_id)
    new_orders_df = spark.createDataFrame(new_orders, schema=order_schema).select(*common_columns)
    print(f"number of new orders: {len(new_orders)}")

    existing_orders_with_new_udpates = existing_orders.join(order_updates_df, on="order_id", how="left_anti")
    existing_orders_with_updates_and_deletes = existing_orders_with_new_udpates.join(order_deletes_df, on="order_id",
                                                                                     how="left_anti")
    print(f"existing_orders_with_updates_and_deletes: {existing_orders_with_updates_and_deletes.count()}")

    new_snapshot = (
        existing_orders_with_updates_and_deletes
        .union(order_updates_df)
        .union(new_orders_df)
    )
    # for debugging
    print(f"updated_orders:\n")
    order_updates_df.show(20, False)
    print("deleted_orders:\n")
    order_deletes_df.show(20, False)
    print(f"new_orders:\n")
    new_orders_df.show(20, False)
    print(f"existing_orders with updates and deletes:\n")
    existing_orders_with_updates_and_deletes.show(20, False)
    print(f"new_snapshot:\n")
    new_snapshot.show(20, False)
    return new_snapshot


# COMMAND ----------


# write snapshots
if snapshot_pattern == 'Pattern 1':
    print(f"""Generating Order Snapshot Data for Pattern {snapshot_pattern}. 
The order snapshot data will be written to the delta table {table_name}.
Every new snapshot data will overwrite the existing delta table.""")
    table_exists = spark.catalog.tableExists(table_name)
    # check if table is empty
    table_empty = spark.table(table_name).count() == 0 if table_exists else True   
    if table_exists and not table_empty:
        print(
            f"Previous snapshot found in table {table_name}. New snapshots are created with updates and inserts, and deletes")
        new_snapshot = get_incremental_order_snapshot(snapshot_pattern)

        # Deduplicate the DataFrame based on specific columns and retain the first appearance
        dedup_new_snapshot = new_snapshot.dropDuplicates(
            subset=["order_id", "price", "order_status", "customer_id", "product_id"])
        # overwrite the snapshot delta table with new snapshot
        (
            dedup_new_snapshot
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
    else:
        print(f"{table_name} doesn't exist or table is empty. Generating initial snapshots data.")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
        initial_snapshot = get_initial_order_snapshot()
        dedup_initial_snapshot = initial_snapshot.dropDuplicates(
            subset=["order_id", "price", "order_status", "customer_id", "product_id"])
        (
            dedup_initial_snapshot
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

elif snapshot_pattern == 'Pattern 2':
    print(f"""Generating Order Snapshot Data for Pattern {snapshot_pattern}. 
The order snapshot data will be written to the given snapshot path: {snapshot_source_path}.
Every new snapshot data will be written to a new path in parquet formats. 
The new path is constructed as: /<base_path>/datetime=yyyy-mm-dd hh""")

    path_exists = False
    try:
        path_exists = len(dbutils.fs.ls(snapshot_source_path)) >= 1 if len(snapshot_source_path) > 0 else False
    except:
        path_exists = False

    # construct a path for this snapshot
    current_datetime = datetime.now()
    datetime_str = current_datetime.strftime('"%Y-%m-%d %H"')
    snapshot_path = snapshot_source_path + "/datetime=" + datetime_str
    if path_exists:
        print(
            f"Previous snapshots Found at path  {snapshot_source_path}. New snapshots are created with updates and inserts, and deletes")
        new_snapshot = get_incremental_order_snapshot(snapshot_pattern)
        dedup_new_snapshot = new_snapshot.dropDuplicates(
            subset=["order_id", "price", "order_status", "customer_id", "product_id"])
        # overwrite the snapshot delta table with new new snapshot
        (
            dedup_new_snapshot
            .write
            .format("parquet")
            .mode("overwrite")
            .save(snapshot_path)
        )
    else:
        print(f"Initial orders snapshot are created and written to path {snapshot_path} in Parquet format")
        initial_snapshot = get_initial_order_snapshot()
        dedup_initial_snapshot = initial_snapshot.dropDuplicates(
            subset=["order_id", "price", "order_status", "customer_id", "product_id"])
        (
            dedup_initial_snapshot
            .write
            .format("parquet")
            .mode("overwrite")
            .save(snapshot_path)
        )
else:
    raise ValueError(f"Unknown Pattern - {pattern}")