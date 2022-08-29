# Databricks notebook source
from sqlalchemy import create_engine, MetaData, select, func, String, Column
from faker import Faker
import os 
import sys 
import random 
import time
from migrate.versioning.schema import Table, Column
import sqlalchemy

# Get ENV Variables
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "databricks1")
DB_HOST = os.getenv("DB_HOSTNAME", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_DATABASE = os.getenv("DB_DATABASE", "databricks")

# Set up connections between sqlalchemy and postgres dbapi
# Instantiate metadata object
engine = engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')
metadata = MetaData()

# Instantiate faker object
faker = Faker()

# Reflect metadata/schema from existing database to bring in existing tables 
with engine.connect() as conn: 
    metadata.reflect(conn)

# product list
product_list = ["hat", "cap", "shirt", "sweater", "sweatshirt", "shorts", 
    "jeans", "sneakers", "boots", "coat", "accessories"]

class ModifyData:
    def __init__(self, runtime_seconds):
        """
        initialize command line arguments
        """
        self.runtime = int(runtime_seconds)
        self.t_end = time.time() + self.runtime

        self.customers = metadata.tables["customers"]
        self.products = metadata.tables["products"]
        self.stores = metadata.tables["stores"]
        self.transactions = metadata.tables["transactions"]


    def modify_data(self):
        cnter = 0
        while time.time() < self.t_end:
            with engine.begin() as conn:
                record_cnt = conn.execute(select([func.count()]).select_from(self.customers)).fetchall()[0][0]
                record_id = random.randint(1, record_cnt)
                update_stmt = self.customers.update().where(self.customers.c.customer_id == record_id) \
                    .values(
                        address = faker.address().replace("\n", " "),
                        email = faker.email()
                        )
                conn.execute(update_stmt)

                record_cnt = conn.execute(select([func.count()]).select_from(self.stores)).fetchall()[0][0]
                record_id = random.randint(1, record_cnt)
                update_stmt = self.stores.update().where(self.stores.c.store_id == record_id) \
                    .values(
                        address = faker.address().replace("\n", " ")
                        )
                conn.execute(update_stmt)
            
                if cnter == 3:
                    try:
                        conn.execute(f"""
                            ALTER TABLE {DB_DATABASE}.stores ADD COLUMN country VARCHAR(3) DEFAULT 'US'
                        """)
                    except sqlalchemy.exc.OperationalError as err:
                        if err.orig.args[0]==1060:
                            print("Column Already Added.")
                        else:
                            raise

            cnter += 1                    

            time.sleep(2)

def main(event, context):
    try:
        runtime_seconds = event['runtime']
    except:
        runtime_seconds = 60

    generate_data = ModifyData(runtime_seconds)
    generate_data.modify_data()

    return {
        'statusCode' : 200,
        'body': "Modified Tables"
    }

if __name__ == "__main__":

    main({'runtime':200},{})