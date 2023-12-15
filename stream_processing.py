# Databricks notebook source
from pyspark.sql.types import *
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

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1272e2b5acdf-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()


# COMMAND ----------

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1272e2b5acdf-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1272e2b5acdf-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_pin = df_pin.selectExpr('CAST(data as STRING)')
df_geo = df_geo.selectExpr('CAST(data as STRING)')
df_user = df_user.selectExpr('CAST(data as STRING)')




# COMMAND ----------

schema_pin = StructType([StructField("index", IntegerType(), True),
                    StructField("unique_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("poster_name", StringType(), True),
                    StructField("follower_count", StringType(), True),
                    StructField("tag_list", StringType(), True),
                    StructField("is_image_or_video", StringType(), True),
                    StructField("image_src", StringType(), True),
                    StructField("downloaded", IntegerType(), True),
                    StructField("save_location", StringType(), True),
                    StructField("category", StringType(), True) ])

schema_geo = StructType([StructField('ind', IntegerType(), True), 
                       StructField('country', StringType(), True), 
                       StructField('latitude', StringType(), True),  
                       StructField('longitude', StringType(), True), 
                       StructField('timestamp', StringType(), True) ])

schema_user = StructType([StructField('ind', IntegerType(), True), 
                       StructField('age', IntegerType(), True), 
                       StructField('date_joined', StringType(), True),  
                       StructField('first_name', StringType(), True), 
                       StructField('last_name', StringType(), True) ])

# COMMAND ----------

df_normalized_pin = df_pin.withColumn("json_data", from_json('data', schema_pin))

df_normalized_geo = df_geo.withColumn("json_data", from_json('data', schema_geo)) 

df_normalized_user = df_user.withColumn('json_data', from_json('data', schema_user))

# COMMAND ----------

df_normalized_pin = df_normalized_pin.select('json_data.*')

df_normalized_geo = df_normalized_geo.select('json_data.*')

df_normalized_user = df_normalized_user.select('json_data.*')

# COMMAND ----------

from pyspark.sql.functions import *

df_normalized_pin = df_normalized_pin.dropna(subset=['follower_count'])

df_normalized_pin = df_normalized_pin.withColumnRenamed('index', 'ind')

patterns = [('k', ',000'), ('M', ',000000'), (',', '')]

for pattern, replacement in patterns:
    df_normalized_pin = df_normalized_pin.withColumn('follower_count', regexp_replace('follower_count', pattern, replacement)) 

df_normalized_pin = df_normalized_pin.withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) \
    .withColumn('follower_count', df_normalized_pin['follower_count'].cast('int')) \
    .withColumn('ind', df_normalized_pin['ind'].cast('int')) \
    .drop('downloaded') \
    .select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')


# COMMAND ----------

from pyspark import *
from pyspark.sql import functions as F

df_normalized_geo = df_normalized_geo.dropna()

df_normalized_geo = df_normalized_geo.withColumn('ind', df_normalized_geo['ind'].cast('int')) \
    .withColumn('timestamp', df_normalized_geo['timestamp'].cast('timestamp')) \
    .select('ind', 'country', F.array('latitude', 'longitude').alias('coordinates'), 'timestamp')


# COMMAND ----------

from pyspark import *

df_normalized_user = df_normalized_user.withColumn('user_name', concat(('first_name'), lit(' '), col('last_name')))

df_normalized_user = df_normalized_user.drop('first_name', 'last_name') \
    .withColumn('date_joined', df_normalized_user['date_joined'].cast('timestamp')) \
    .withColumn('ind', df_normalized_user['ind'].cast('int')) \
    .withColumn('age', df_normalized_user['age'].cast('int')) \
    .select('ind', 'user_name', 'age', 'date_joined')


# COMMAND ----------

df_normalized_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("1272e2b5acdf_pin_table")

# COMMAND ----------

df_normalized_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("1272e2b5acdf_geo_table")

# COMMAND ----------

df_normalized_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("1272e2b5acdf_user_table")

# COMMAND ----------

df_clean_pin = spark.read.format('delta').table('1272e2b5acdf_pin_table')
display(df_clean_pin)

df_clean_geo = spark.read.format('delta').table('1272e2b5acdf_geo_table')
display(df_clean_geo)

df_clean_user = spark.read.format('delta').table('1272e2b5acdf_user_table')
display(df_clean_user)

# COMMAND ----------


