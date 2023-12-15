# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

file_type = 'csv' 
first_row_is_header = 'true'
delimiter = ','

aws_keys_df = spark.read.format(file_type)\
.option('header', first_row_is_header)\
.option('sep', delimiter)\
.load('/FileStore/tables/authentication_credentials.csv')



# COMMAND ----------

ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

AWS_S3_BUCKET = 'user-1272e2b5acdf-bucket'
MOUNT_NAME = '/mnt/s3_bucket'

SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/s3_bucket'))

# COMMAND ----------

file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.pin/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_pin = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_pin)

# COMMAND ----------

file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.geo/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_geo = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_geo)

# COMMAND ----------

file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.user/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_user = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_user)

# COMMAND ----------


