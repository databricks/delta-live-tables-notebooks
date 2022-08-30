# Databricks notebook source
from sqlalchemy import create_engine, MetaData, select
from faker import Faker
import os 
import random 
import datetime 

# Get ENV Variables
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "databricks1")
DB_HOST = os.getenv("DB_HOSTNAME", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_DATABASE = os.getenv("DB_DATABASE", "databricks")

# Set up connections between sqlalchemy and postgres dbapi
# Instantiate metadata object
engine = engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}',
    connect_args={'client_flag':0}
    )
metadata = MetaData()

# Instantiate faker object
faker = Faker()

# Reflect metadata/schema from existing database to bring in existing tables 
with engine.connect() as conn: 
    metadata.reflect(conn)

# product list
product_list = ["hat", "cap", "shirt", "sweater", "sweatshirt", "shorts", 
    "jeans", "sneakers", "boots", "coat", "accessories"]


class GenerateData:
    """
    generate a specific number of records to a target table in the 
    database
    """

    def __init__(self, table, num_records):
        """
        initialize command line arguments
        """
        self.table = table
        self.num_records = int(num_records)

        self.customers = metadata.tables["customers"]
        self.products = metadata.tables["products"]
        self.stores = metadata.tables["stores"]
        self.transactions = metadata.tables["transactions"]

    
    def create_data(self):
        """
        using faker library, generate data and execute DML 
        """
        
        if self.table not in metadata.tables.keys():
            return print(f"{self.table} does not exist")

        if self.table == "customers":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = self.customers.insert().values(
                        first_name = faker.first_name(),
                        last_name = faker.last_name(),
                        email = faker.email(),
                        address = faker.address().replace("\n", " "),
                        dob = faker.date_of_birth(minimum_age=16, maximum_age=60)
                    )
                    conn.execute(insert_stmt)

        if self.table == "products":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = self.products.insert().values(
                        name = random.choice(product_list),
                        price = faker.random_int(1,100000) / 100.0
                    )
                    conn.execute(insert_stmt)

        if self.table == "stores":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = self.stores.insert().values(
                        address = faker.address().replace("\n", " ")
                    )
                    conn.execute(insert_stmt)

        if self.table == "transactions":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    date_obj = datetime.datetime.now() - datetime.timedelta(days=random.randint(0,30))

                    insert_stmt = self.transactions.insert().values(
                        transaction_date=date_obj.strftime("%Y/%m/%d"),
                        customer_id=random.choice(conn.execute(select([self.customers.c.customer_id])).fetchall())[0],
                        product_id=random.choice(conn.execute(select([self.products.c.product_id])).fetchall())[0],
                        store_id=random.choice(conn.execute(select([self.stores.c.store_id])).fetchall())[0]
                    )
                    conn.execute(insert_stmt)

def main(event, handler):
    # customers = metadata.tables["customers"]
    # products = metadata.tables["products"]
    # stores = metadata.tables["stores"]
    # transactions = metadata.tables["transactions"]

    tables_info = {
        'customers' : 100,
        'products' : 25,
        'stores' : 20,
        'transactions' : 1000
    }

    for table, num_records in tables_info.items():
        generate_data = GenerateData(table, num_records)
        generate_data.create_data()

    return {
        'statusCode' : 200,
        'body': "Inserted Records"
    }

if __name__ == "__main__":

    main({},{})