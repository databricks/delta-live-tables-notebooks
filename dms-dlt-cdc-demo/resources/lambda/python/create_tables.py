# Databricks notebook source
import os
from pip import main
from sqlalchemy import create_engine, MetaData, \
    Column, Integer, Numeric, String, Date, Table, ForeignKey 

# Get ENV Variables
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "databricks1")
DB_HOST = os.getenv("DB_HOSTNAME", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_DATABASE = os.getenv("DB_DATABASE", "databricks")

# Set up connections between sqlalchemy and postgres dbapi
# Instantiate metadata object
engine = engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')

# Set up connections between sqlalchemy and db system dbapi
# Instantiate metadate object
metadata = MetaData()

# DDL for customers, products, stores, and transactions 
customers_table = Table(
    "customers",
    metadata,
    Column("customer_id", Integer, primary_key=True),
    Column("first_name", String(35), nullable=False),
    Column("last_name", String(35), nullable=False),
    Column("email", String(35), nullable=False),
    Column("address", String(135), nullable=False),
    Column("dob", Date, nullable=False)
)

products_table = Table(
    "products",
    metadata,
    Column("product_id", Integer, primary_key=True),
    Column("name", String(35), nullable=False),
    Column("price", Numeric(10,2), nullable=False)
)

stores_table = Table(
    "stores",
    metadata,
    Column("store_id", Integer, primary_key=True),
    Column("address", String(135), nullable=True)
)

transactions_table = Table(
    "transactions",
    metadata,
    Column("transaction_id", Integer, primary_key=True),
    Column("transaction_date", Date, nullable=False),
    Column("customer_id", ForeignKey("customers.customer_id"), nullable=False),
    Column("product_id", ForeignKey("products.product_id"), nullable=False),
    Column("store_id", ForeignKey("stores.store_id"), nullable=False)
)

def main(event, context):
    # Start transaction to commit DDL to MySQL database
    with engine.begin() as conn:
        metadata.create_all(conn)

    return {
        'statusCode' : 200,
        'body': "All Tables created successfully"
    }

if __name__ == "__main__":
    main({},{})