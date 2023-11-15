# Databricks notebook source
# MAGIC %md # TGZ File Arrival Simulation
# MAGIC
# MAGIC This notebook could be scheduled on some recurring basis to trigger new file arrival of tar.gz email archives to an S3 location

# COMMAND ----------

# MAGIC %fs mkdirs tmp/tar_emails/

# COMMAND ----------

import datetime

def get_partition_timestamp():
    # Get the current time in UTC
    now = datetime.datetime.utcnow()
    # Format the time as a string that is safe to use as an S3 object key
    timestamp_str = now.strftime('%Y_%m_%d_%H_%M')
    return timestamp_str

partition_key = get_partition_timestamp()

# COMMAND ----------

# Set the S3 bucket name and key prefix where the final TAR file will be saved
BUCKET_NAME = '<S3 bucket name>'
DIRECTORY_PATH = f'dlt-demo/tar_files/{partition_key}/'
print(DIRECTORY_PATH)

# Set the directory path where the .eml files will be saved
# Note: this is a local path on the cluster where the notebook is running. E.g. it is NOT DBFS or S3. The random files get cleaned up either at the end of the notebook, or when the cluster terminates.
DIR_PATH = 'tmp/tar_emails/'

# COMMAND ----------

import os
import random
import string
import tarfile
import gzip
import boto3

#Set number of emails to generate
num_emls = 20

# Set the minimum and maximum sizes for each email in bytes
MIN_SIZE = 214572000
MAX_SIZE = 314572000

# Set the size limit for each TAR file in bytes
TAR_SIZE_LIMIT = 314572000 # 30Mb

# Set the list of characters to use for generating random content
CHARACTERS = string.ascii_letters

# Generate a random content string of a given size in bytes
def generate_content(size):
    return ''.join(random.choice(CHARACTERS) for _ in range(size))

# Create a random .eml file with a given filename and size in bytes
def create_eml_file(filename, size):
    with open(filename, 'w') as f:
        f.write(f"From: sender@example.com\nTo: recipient@example.com\nSubject: Random Content\n\n{generate_content(size)}")

# Create a random .eml file with a unique filename and size between MIN_SIZE and MAX_SIZE
def create_random_eml_file():
    size = random.randint(MIN_SIZE, MAX_SIZE)
    filename = f"{DIR_PATH}/email_{size}.eml"
    create_eml_file(filename, size)

# Create a directory to store the .eml files if it doesn't already exist
if not os.path.exists(DIR_PATH):
    os.makedirs(DIR_PATH)

# Generate 100 random .eml files
for i in range(num_emls):
    create_random_eml_file()
    print("Created random email:",i)

s3 = boto3.client('s3')

# Create a TAR file for each set of files that meets the size limit
tar_num = 1
tar_size = 0
tar_files = []
email_counter = 0
for filename in os.listdir(DIR_PATH):
    # Get the size of the file
    size = os.path.getsize(f"{DIR_PATH}/{filename}")
    print("size of file ", filename, " is " ,size)
    
    # Check if the file is an .eml file and is less than 2Mb in size
    if filename.endswith('.eml') and size < TAR_SIZE_LIMIT:
        # Add the file to the list of files for the current TAR
        tar_files.append(filename)
        tar_size += size
        print("total tar_size", tar_size)
        
        # If the size limit has been reached, create a new TAR file
        if tar_size >= TAR_SIZE_LIMIT:
            # Create the TAR file
            tar_filename = f"emails_{tar_num}.tar"
            with tarfile.open(tar_filename, 'w') as tar:
                for tar_file in tar_files:
                    tar.add(f"{DIR_PATH}/{tar_file}", arcname=tar_file)
                    email_counter+=1
            
            # Compress the TAR file using gzip
            with open(tar_filename, 'rb') as f_in:
                with gzip.open(f"{tar_filename}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            
            # Upload the compressed TAR file to S3
            s3.upload_file(f"{tar_filename}.gz", BUCKET_NAME, DIRECTORY_PATH+f"{tar_filename}.gz")
            print("Uploaded file to:", BUCKET_NAME+DIRECTORY_PATH+f"{tar_filename}.gz")

            tar_size = 0
            tar_num += 1
            tar_files = []
print("Total emails added to Tars:", email_counter)

# Clean up local files

# COMMAND ----------

# MAGIC %md Clean up files for next run

# COMMAND ----------

# MAGIC %fs rm temp/emails/ -r

# COMMAND ----------

# %sh head  temp/emails/email_48271.eml
