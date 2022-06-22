# Databricks notebook source
# MAGIC %md
# MAGIC # TwitterStream to S3 / DBFS
# MAGIC 
# MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/267b91c1-52f0-49e3-8b77-9a50b34f5d39)
# MAGIC * [Huggingface Sentiment Analysis](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/2328309019401991)

# COMMAND ----------

# should use databricks secrets and the CLI to store and retrieve those keys in a safe way.
#
# for a first try, you can setup you twitter keys here
consumer_key = "XXXX"
consumer_secret = "XXXX"
access_token = "XXX"
access_token_secret = "XXXX"

# in my demo, I read in the keys from another notebook in the cell below (which can be savely removed or commented out)


# COMMAND ----------

# MAGIC %run "./TwitterSetup"

# COMMAND ----------

#!/databricks/python3/bin/python -m pip install --upgrade pip

# COMMAND ----------

!pip install tweepy jsonpickle

# COMMAND ----------

import tweepy
import calendar
import time
import jsonpickle
import sys


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, timeout=60)
print(f'Twitter screen name: {api.verify_credentials().screen_name}')


# Subclass Stream  
class TweetStream(tweepy.Stream):

    def __init__(self, filename):
        tweepy.Stream.__init__(self, consumer_key=consumer_key, consumer_secret=consumer_secret,
                             access_token=access_token, access_token_secret=access_token_secret)
        self.filename = filename
        self.text_count = 0
        self.tweet_stack = []


    def on_status(self, status):
        #print('*'+status.text)
        self.text_count = self.text_count + 1
        self.tweet_stack.append(status)
    
        # when to print
        if (self.text_count % 1 == 0):
            print(f'retrieving tweet {self.text_count}: {status.text}')

        # how many tweets to batch into one file
        if (self.text_count % 5 == 0):
            self.write_file()
            self.tweet_stack = []

        # hard exit after collecting n tweets
        if (self.text_count == 50000):
            raise Exception("Finished job")

    def write_file(self):
        file_timestamp = calendar.timegm(time.gmtime())
        fname = self.filename + '/tweets_' + str(file_timestamp) + '.json'


        f = open(fname, 'w')
        for tweet in self.tweet_stack:
            f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
        f.close()
        print("Wrote local file ", fname)

    def on_error(self, status_code):
        print("Error with code ", status_code)
        sys.exit()


# Initialize instance of the subclass
tweet_stream = TweetStream("/dbfs/data/twitter_dataeng2")

# Filter realtime Tweets by keyword
try:
    tweet_stream.filter(languages=["en","de","es"],track=["Databricks", "data science","AI/ML","data lake","machine learning","lakehouse","DLT","Delta Live Tables"])



except Exception as e:
    print("some error ", e)
    print("Writing out tweets file before I have to exit")
    tweet_stream.write_file()
finally:
    print("Downloaded tweets ", tweet_stream.text_count)
    tweet_stream.disconnect()


# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a DBFS directory, check for number of files, deletes a certain number of files ...

# COMMAND ----------

# create a directory to buffer the streamed data
!mkdir "/dbfs/data/twitter_dataeng2"

# COMMAND ----------

# create a directory to buffer the streamed data
!ls -l /dbfs/data/twitter_dataeng2 | wc

# COMMAND ----------

files = dbutils.fs.ls("/data/twitter_dataeng2")
del = 400
print(f'number of files: {len(files)}')
print(f'number of files to delete: {del}')


for x, file in enumerate(files):
  # delete n files from directory
  if x < del :
    # print(x, file)
    dbutils.fs.rm(file.path)

    
# use dbutils to copy over files... 
# dbutils.fs.cp("/data/twitter_dataeng/" +f, "/data/twitter_dataeng2/")
