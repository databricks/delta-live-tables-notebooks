# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Producer 
# MAGIC 
# MAGIC The code below produces data using Python and saves files in JSON format to a cloud Kafka instance. In particular, there are 2 topics written to: i) `purchase_trends` and ii) `checking_account`. T
# MAGIC 
# MAGIC This producer mocks up 2 common data sources which an online bank or insurance provider might capture to understand customer's preferences. In reality, the data will most likely come from an OLTP database such as a MySQL or PostgreSQL database. There are common methods to extract such data including connectors such as Debezium, which pipe the data to Kafka topics. This data and schema represents this scenario. 
# MAGIC 
# MAGIC <img src='https://miro.medium.com/max/1089/1*a204V3tGkz696a1fjZTw_w.png'>
# MAGIC 
# MAGIC Docs: https://docs.databricks.com/spark/latest/structured-streaming/kafka.html#apache-kafka

# COMMAND ----------

# DBTITLE 1,Create Database for Data Sources
# MAGIC %sql create database if not exists banking

# COMMAND ----------

# MAGIC %sql use banking

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The code below creates a shell Debezium record which is parametrized so we can create new records
# MAGIC 
# MAGIC <p></p>
# MAGIC 
# MAGIC * Create template record with customer attributes paramterized
# MAGIC * Create 200 JSON files, one per time event with new buying patterns changing over time
# MAGIC * Create ending point - purchase of a mortgage
# MAGIC * Write to Kafka in JSON format

# COMMAND ----------

# DBTITLE 1,Clean up Data Paths
# MAGIC %fs rm -r /home/fs/banking_personalization/

# COMMAND ----------

# MAGIC %fs mkdirs /home/fs/banking_personalization/

# COMMAND ----------

# DBTITLE 1,Create Template Debezium Record
debezium_str = '''{{
  "schema": {{
    "type": "struct",
    "fields": [
      {{
        "type": "struct",
        "fields": [
          {{
            "type": "date",
            "optional": false,
            "field": "date"
          }},
          {{
            "type": "int32",
            "optional": true,
            "field": "number_of_dr_visits"
          }},
          {{
            "type": "int32",
            "optional": true,
            "field": "customer_id"
          }},
          {{
            "type": "int64",
            "optional": true,
            "name": "io.debezium.time.MicroTimestamp",
            "version": 1,
            "field": "datetime_created"
          }},
          {{
            "type": "int64",
            "optional": true,
            "name": "io.debezium.time.MicroTimestamp",
            "version": 1,
            "field": "datetime_updated"
          }}
        ],
        "optional": true,
        "name": "claim.Value",
        "field": "before"
      }},
      {{
        "type": "struct",
        "fields": [
          {{
            "type": "date",
            "optional": false,
            "field": "date"
          }},
          {{
            "type": "int32",
            "optional": true,
            "field": "number_of_dr_visits"
          }},
          {{
            "type": "int32",
            "optional": true,
            "field": "customer_id"
          }},
          {{
            "type": "int64",
            "optional": true,
            "name": "io.debezium.time.MicroTimestamp",
            "version": 1,
            "field": "datetime_created"
          }},
          {{
            "type": "int64",
            "optional": true,
            "name": "io.debezium.time.MicroTimestamp",
            "version": 1,
            "field": "datetime_updated"
          }}
        ],
        "optional": true,
        "name": "claim.Value",
        "field": "after"
      }},
      {{
        "type": "struct",
        "fields": [
          {{
            "type": "string",
            "optional": false,
            "field": "version"
          }},
          {{
            "type": "string",
            "optional": false,
            "field": "connector"
          }},
          {{
            "type": "string",
            "optional": false,
            "field": "name"
          }},
          {{
            "type": "int64",
            "optional": false,
            "field": "ts_ms"
          }},
          {{
            "type": "string",
            "optional": true,
            "name": "io.debezium.data.Enum",
            "version": 1,
            "parameters": {{
              "allowed": "true,last,false"
            }},
            "default": "false",
            "field": "snapshot"
          }},
          {{
            "type": "string",
            "optional": false,
            "field": "db"
          }},
          {{
            "type": "string",
            "optional": false,
            "field": "schema"
          }},
          {{
            "type": "string",
            "optional": false,
            "field": "table"
          }},
          {{
            "type": "int64",
            "optional": true,
            "field": "txId"
          }},
          {{
            "type": "int64",
            "optional": true,
            "field": "lsn"
          }},
          {{
            "type": "int64",
            "optional": true,
            "field": "xmin"
          }}
        ],
        "optional": false,
        "name": "io.debezium.connector.postgresql.Source",
        "field": "source"
      }},
      {{
        "type": "string",
        "optional": false,
        "field": "op"
      }},
      {{
        "type": "int64",
        "optional": true,
        "field": "ts_ms"
      }},
      {{
        "type": "struct",
        "fields": [
          {{
            "type": "string",
            "optional": false,
            "field": "id"
          }},
          {{
            "type": "int64",
            "optional": false,
            "field": "total_order"
          }},
          {{
            "type": "int64",
            "optional": false,
            "field": "data_collection_order"
          }}
        ],
        "optional": true,
        "field": "transaction"
      }}
    ],
    "optional": false,
    "name": "bankserver1.bank.holding.Envelope"
  }},
  "payload": {{
    "before": {{
      "date": "{0}", 
      "number_of_concert_tickets" : {1},
      "number_of_comedy_shows" : {2},
      "number_of_sport_events" : {3},
      "number_of_online_txns" : {4},
      "home_mortgage_purchased_ts" : {5},
      "customer_id" : "{6}",
      "datetime_created": {7},
      "datetime_updated": {8}
    }},
    "after": {{
      "date": "{9}", 
      "number_of_concert_tickets" : {10},
      "number_of_comedy_shows" : {11},
      "number_of_sport_events" : {12},
      "number_of_online_txns" : {13},
      "home_mortgage_purchased_ts" : {14},
      "customer_id" : "{15}",
      "datetime_created": {16},
      "datetime_updated": {17}
    }},
    "source": {{
      "version": "1.1.1.Final",
      "connector": "postgresql",
      "name": "bankserver1",
      "ts_ms": 1589121006548,
      "snapshot": "false",
      "db": "start_data_engineer",
      "schema": "bank",
      "table": "holding",
      "txId": 492,
      "lsn": 24563232,
      "xmin": null
    }},
    "op": "{18}",
    "ts_ms": 1589121006813,
    "transaction": null
  }}
}}'''

import numpy as np 
from datetime import datetime, timedelta
import math
import calendar
import time

## Create 200 records among customer IDs 0 - 9. This code will resolve a claim (1 per customer) based on a binomial distribution.

resolved_list = {}
for i in range(1, 200):
  time.sleep(1)
  print(resolved_list.keys())
  if str(i%10) in resolved_list.keys():
    continue
  resolved = "N"
  curr_entry = debezium_str.format('2022-01-01', math.floor(math.log(i)+5), math.floor(math.log(i)), math.floor(math.log(i+10)), i, 'null', str(i%10), calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, '2022-01-01', math.floor(math.log(i)+5), math.floor(math.log(i)), math.floor(math.log(i+10)), i, 'null', str(i%10), calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, "i")
  if i >= 10:
    resolved_choices = ["N", "Y"] 
    resolved = np.random.choice(resolved_choices, 1 , p=[0.8, 0.2])[0]
    rating = np.random.uniform(1, 5)
    if resolved == "Y":
      resolved_list[str(i%10)] = "Y"
      curr_entry = debezium_str.format('2022-01-01', math.floor(math.log(i)+5), math.floor(math.log(i)), math.floor(math.log(i+10)), i, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, str(i%10), calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, '2022-01-01', math.floor(math.log(i)+5), math.floor(math.log(i)), math.floor(math.log(i+10)), i, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, str(i%10), calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, calendar.timegm((datetime.now()+timedelta(days=i)).timetuple())*1000, "u")
  dbutils.fs.put("/home/fs/banking_personalization/customer_patterns_{}.json".format(str(i)), curr_entry, True)

# COMMAND ----------

# You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# Just choose the set of corresponding endpoints to use.
# If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
kafka_bootstrap_servers_tls       = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-tls"       )
kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "oetrta", "kafka-bootstrap-servers-plaintext" )

# COMMAND ----------

# DBTITLE 1,Create your a Kafka topic unique to your name
# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = "/home/fs_cdc/"

checkpoint_location = f"{project_dir}/patterns"

topic = "purchase_trends"

# COMMAND ----------

# DBTITLE 1,Create UDF for UUID
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid

uuidUdf = udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# DBTITLE 1,Load streaming dataset
input_path = "/home/fs/banking_personalization/"
input_schema = spark.read.format("json").option("multiline", "true").load(input_path).schema


input_stream = (spark
  .readStream
  .schema(input_schema)
  .option("multiline", "true")
  .json(input_path)
  .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp"))
  .withColumn("eventId", uuidUdf()))

display(input_stream)

# COMMAND ----------

kafka_bootstrap_servers_tls = dbutils.secrets.get(scope="oetrta", key="rp_kafka_brokers")

# COMMAND ----------

# DBTITLE 1,WriteStream to Kafka
# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"
(input_stream
   .select(col("eventId").alias("key"), to_json(struct(col('payload'), col('schema'), col('processingTime'))).alias("value"))
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
   .option("kafka.security.protocol", "SSL") 
   .option("checkpointLocation", checkpoint_location )
   .option("topic", topic)
   .start()
)

# COMMAND ----------

# MAGIC %sql drop table if exists customer_patterns_bronze

# COMMAND ----------

# MAGIC %sql drop table if exists checking_account

# COMMAND ----------

# DBTITLE 1,New Python Producer for Checking Transactions
from pyspark.sql.functions import col, lit, round, when, current_timestamp, rand
import time 

from datetime import datetime
curr_time = datetime.now()
import pandas as pd 
import numpy as np
pdf = pd.DataFrame({datetime.now()}, columns=['updt_ts'])

debit_or_credit = np.random.uniform(0, 1, 1)
df = spark.range(10).withColumn("customer_id", col("id")%10).withColumn("scheduled_payment", col("id")%2).withColumn("txn_amount", col("id")%100).withColumn("debit_or_credit", when(col("customer_id") <= 3, round(rand())).otherwise(0)).drop("id").join(spark.createDataFrame(pdf)).withColumn("initial_balance", when(col("customer_id")%4 == 0, lit(150000)).otherwise(lit(10000)))

i = 0
while i < 100:
  time.sleep(1)
  pdf = pd.DataFrame({datetime.now()+timedelta(days=i)}, columns=['updt_ts'])
  df = df.union(spark.range(10).withColumn("id", col("id")+10*i).withColumn("customer_id", col("id")%10).withColumn("scheduled_payment", col("id")%2).withColumn("txn_amount", col("id")%100).withColumn("debit_or_credit", when(col("customer_id") <= 3, 1).otherwise(round(round(rand())))).drop("id").join(spark.createDataFrame(pdf)).withColumn("initial_balance", when(col("customer_id")%4 == 0, lit(150000)).otherwise(lit(10000))))
  i =i+1

# COMMAND ----------

# DBTITLE 1,Stage Data in Delta Table
df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable("banking.checking_account")

# COMMAND ----------

# Clear checkpoint location
credit_payment_cp = "/home/fs/credit_payment_cp2/"
dbutils.fs.rm(credit_payment_cp, True)

# COMMAND ----------

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"
(spark.readStream.option("startingOffsets", "earliest" ).format("delta").table("banking.checking_account")
   .select(uuidUdf().alias("key"), to_json(struct(col('customer_id'), col('scheduled_payment'), col('txn_amount'), col('debit_or_credit'), col('updt_ts'), col('initial_balance'))).alias("value"))
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
   .option("kafka.security.protocol", "SSL") 
   .option("checkpointLocation", credit_payment_cp )
   .option("topic", "checking_acct")
   .start()
)