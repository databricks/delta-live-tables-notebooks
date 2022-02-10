# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Read me:
# MAGIC 1. Run Cmd 2 to show widgets
# MAGIC 2. Specify Storage path in widget
# MAGIC 3. "Run All" to generate the data and final data path
# MAGIC 4. Cmd 5 output should show data being generated into storage path
# MAGIC 5. Path to the generated data is printed in Cmd 5
# MAGIC 6. When finished generating data, "Stop Execution"
# MAGIC 7. To refresh landing zone, run Cmd 7

# COMMAND ----------

# DBTITLE 1,Run First for Widgets
dbutils.widgets.text('path', '/home/demos/ChangeDataCapture', 'Storage Path')
dbutils.widgets.combobox('batch_wait', '1', ['10', '20', '30', '50'], 'Speed (secs between writes)')
dbutils.widgets.combobox('num_recs', '50', ['100', '1000', '2000'], 'Volume (# records per writes)')

# COMMAND ----------

# MAGIC %pip install iso3166 Faker

# COMMAND ----------

import argparse
import json
import iso3166
import logging
import random
import timeit
from datetime import datetime
from faker import Faker


output_path = dbutils.widgets.get('path')
dbutils.fs.rm(output_path + "/retail_schema.json", True)

dbutils.fs.put(output_path + "/retail_schema.json",
"""{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Retail Schema",
  "description": "An retail data for CDC represents customers data that needs to get modified - updated/deleted",
  "type": "object",
  "properties": {
    "id": {
      "description": "The unique identifier for customers.",
      "type": "string"
    },
    "operation_date": {
      "description": "The date we recieved the information for taking an action listed in operation column. Formatted as YYYY-MM-DDTHH:MM:SSZ in accordance with ISO 8601.",
      "type": "string",
      "format": "date-time"
    },
    "name":{
      "description": "customer name",
      "type": "string"
    },
    "operation": {
    "description": "Operation holds the action needs to be taken on corresponding customer record in the data",
      "type": "string",
      "enum": ["DELETE", "APPEND"]
    },
    "email": {
      "description": "Customer Email address.",
      "type":  "string"
  },
    "address": {
      "description": "Customer physical address.",
      "type":  "string"
    }
  },
  "required": ["id"],
  "additionalProperties": true
}
""")


fake = Faker()
fake.random.seed(42)  

output_path = dbutils.widgets.get('path')

def random_name_email():  
    first_name = fake.first_name()
    last_name = fake.last_name()
    name = first_name+ " "+ last_name
    email = first_name+ last_name+"@"+fake.domain_name()
    return [name, email] 
  
def random_address():    
    return fake.address() 
  
def random_country():
    country_codes = [c[1] for c in iso3166.countries]
    return random.choice(country_codes)

def random_integer(min, max):
    return random.randrange(min, max)

def random_enum(enum_list):
    return random.choice(enum_list)

def random_word(n):
    return " ".join(fake.words(n))

def random_text(n):
    return fake.text(n)

def random_date():
    d = fake.date_time_between(start_date="-10y", end_date="+0y")
    return d.strftime('%Y-%m-%dT%H:%M:%SZ')

def insert(product, attr, attr_value):
    return product["data"][0].update({attr: attr_value})

def generate_users(user_schema, n=100):
    """
    Given a list of user schemas, generate random data 
    """
    start_time = timeit.default_timer()

    f = open('/dbfs/' + output_path + "/retail_schema.json", "r") 
    schema = json.load(f)
    data_type = user_schema.split("/")[-1].split(".json")[0]
    data = generate_product_fire(schema, data_type, n)

    end_time = timeit.default_timer() - start_time
    logging.warning(
        "Generating batches and writing to files"
        " took {} seconds".format(end_time)
    )
    return data

def write_batches_to_file(data, output):
    f = open(output, "w")
    for r in data:
        f.write(json.dumps(r) + "\n")
    f.close()

def include_embedded_schema_properties(schema):
    try:
        for i in range(len(schema["allOf"])):
            inherited_schema = schema["allOf"][i]["$ref"].split("/")[-1]
            f = open('/dbfs/' + output_path + "/retail_schema.json", "r") 
            inherited_schema = json.load(f)
            schema["properties"] = dict(
                schema["properties"].items() +
                inherited_schema["properties"].items()
            )

    except KeyError:
        pass

    return schema

def generate_product_fire(schema, data_type, n):
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    batch = []

    schema = include_embedded_schema_properties(schema)
    schema_attrs = schema["properties"].keys()

    for i in range(n):
        p = {}
        batch.append(p)
        for attr in schema_attrs:

            attr_obj = schema["properties"][attr]

            if attr == "id":
                attr_value = str(i)
                batch[i].update({attr: attr_value})
                continue

            elif attr == "operation_date":
                #attr_value = now
                attr_value = random_date()
                batch[i].update({attr: attr_value})
                continue
                
            elif attr == "name":
                fake.random.seed(i)  
                attr_value = random_name_email()[0]
                batch[i].update({attr: attr_value})
                continue
                
            elif attr == "email":
                fake.random.seed(i)  
                attr_value = random_name_email()[1]
                batch[i].update({attr: attr_value})
                continue
                
            elif attr == "address":
                attr_value = random_address()
                batch[i].update({attr: attr_value})
                continue
            else:
                try:
                    attr_type = schema["properties"][attr]["type"]
                except KeyError:
                    continue

                if attr_type == "number":
                    attr_value = random_integer(0, 500) / 100.0
                    batch[i].update({attr: attr_value})
                    continue

                elif attr_type == "integer":
                    try:
                        attr_min = attr_obj["minimum"]
                    except KeyError:
                        attr_min = -10000
                    try:
                        attr_max = attr_obj["maximum"]
                    except KeyError:
                        attr_max = 100000

                    attr_value = random_integer(attr_min, attr_max)
                    batch[i].update({attr: attr_value})
                    continue

                elif attr_type == "string":
                    try:
                        attr_enums = attr_obj["enum"]
                        attr_value = random_enum(attr_enums)

                    except KeyError:
                        try:
                            attr_format = attr_obj["format"]
                            if attr_format == "date-time":
                                attr_value = random_date()
                            else:
                                pass

                        except KeyError:
                            attr_value = random_word(1)

                    batch[i].update({attr: attr_value})
                    continue

                elif attr_type == "boolean":
                    attr_value = random.choice([True, False])
                    batch[i].update({attr: attr_value})
                    continue

                else:
                    continue

    return batch

# COMMAND ----------

# DBTITLE 1,Data Storage Path (Landing Zone)
import time

dbutils.fs.mkdirs(f'{output_path}/landing')

for i in range(0, 1):
  time.sleep(int(dbutils.widgets.get('batch_wait')))
  write_batches_to_file(generate_users("user", int(dbutils.widgets.get('num_recs'))), f'/dbfs{output_path}/landing/users{i}.json')
  print(f'Finished writing batch: {i}')
  
print ("Storage Path is:", f'{output_path}/landing')

# COMMAND ----------

df = spark.read.json(f'{output_path}/landing')
# df.createOrReplaceTempView("retail_data_cdc");
df.display(5)

columns = ['address', 'email', 'id', 'name', 'operation', 'operation_date']
vals = [('160 Spear St 13th floor, San Francisco, CA 94105', 'mojgan@yahoo.com', 'NULL', 'mojgan', 'DELETE', '2018-07-05T19:54:46Z'
),      ('160 Spear St 13th floor, San Francisco, CA 94105', 'Alex@yahoo.com', 'NULL', 'Alex', 'APPEND', '2020-01-30T00:40:41Z'
),      ('160 Spear St 13th floor, San Francisco, CA 94105', 'George@gmail.com', 'NULL', 'George', 'APPEND', '2015-09-25T03:17:46Z'
),      ('160 Spear St 13th floor, San Francisco, CA 94105', 'Maria@yahoo.com', 'NULL', 'Maria', 'APPEND', '2020-01-30T00:40:40Z'
),      ('160 Spear St 13th floor, San Francisco, CA 94105', 'Allison@yahoo.com', 'NULL', 'Allison', 'APPEND', '2020-01-30T00:40:40Z'
),      ('160 Spear St 13th floor, San Francisco, CA 94105', 'm@yahoo.com', 'NULL', 'stephanie', 'DELETE', '2020-01-30T00:40:40Z'
)]
newRows = spark.createDataFrame(vals, columns)
appended = df.union(newRows)
appended.write.mode('overwrite').format('json').save(f'{output_path}/landingWithProblematicRecords')
appended.show()
print ("Storage Path is:", f'{output_path}/landingWithProblematicRecords')

# COMMAND ----------

# DBTITLE 1,Reset Landing Zone
# dbutils.fs.rm(f'{output_path}', True)
