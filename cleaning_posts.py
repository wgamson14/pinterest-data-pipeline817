# Databricks notebook source
file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.pin/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_pin = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_pin)

# COMMAND ----------

from pyspark.sql.functions import *

df_pin = df_pin.dropna()

df_pin = df_pin.withColumnRenamed('index', 'ind')

df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', 'k', ',000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', 'M', ',000000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', ',', ''))
df_pin = df_pin.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))

df_pin = df_pin.withColumn('follower_count', df_pin['follower_count'].cast('int'))
df_pin = df_pin.withColumn('ind', df_pin['ind'].cast('int'))
df_pin = df_pin.drop('downloaded')

df_pin = df_pin.select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')




# COMMAND ----------

file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.geo/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_geo = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_geo)

# COMMAND ----------

from pyspark import *
from pyspark.sql import functions as F

df_geo = df_geo.dropna()
df_geo = df_geo.withColumn('ind', df_geo['ind'].cast('int'))
df_geo = df_geo.withColumn('timestamp', df_geo['timestamp'].cast('timestamp'))
df_geo = df_geo.select('ind', 'country', F.array('latitude', 'longitude').alias('coordinates'), 'timestamp')



# COMMAND ----------

file_location = '/mnt/s3_bucket/topics/1272e2b5acdf.user/partition=0/*.json'
file_type = 'json'

infer_schema = 'true'

df_user = spark.read.format(file_type) \
.option('inferSchema', infer_schema) \
.load(file_location)

display(df_user)

# COMMAND ----------

from pyspark import *

df_user = df_user.withColumn('user_name', concat(('first_name'), lit(' '), col('last_name')))

df_user = df_user.drop('first_name', 'last_name')

df_user = df_user.withColumn('date_joined', df_user['date_joined'].cast('timestamp'))
df_user = df_user.withColumn('ind', df_user['ind'].cast('int'))
df_user = df_user.withColumn('age', df_user['age'].cast('int'))

df_user = df_user.select('ind', 'user_name', 'age', 'date_joined')


# COMMAND ----------

df_pin.write.parquet('/mnt/data/df_pin.parquet')
df_geo.write.parquet('/mnt/data/df_geo.parquet')
df_user.write.parquet('/mnt/data/df_user.parquet')

# COMMAND ----------


