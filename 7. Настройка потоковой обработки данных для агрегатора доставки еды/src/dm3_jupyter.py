import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
import findspark
findspark.init()
findspark.find()

import math
from datetime import datetime
from datetime import timedelta
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Задание параметров
date = '2022-05-31'
d = 10
dir_src = '/user/master/data/geo/events'
dir_tgt = '/user/kubes00/project/dm3'

# Запуск spark-сессии
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('yarn')\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 2) \
    .appName("test_dm3").getOrCreate()

# Города Австралии доработаны вручную и содержат временные зоны в файле geo_with_tz.csv
df_geo = spark.read.csv(path = "/user/kubes00/project/geo_with_tz.csv", sep=",", inferSchema=True, header=True)\
    .select(F.col("id"), F.col("city"), (F.col("lat").cast('double')*math.pi/180).alias("city_lat"), (F.col("lon").cast('double')*math.pi/180).alias("city_lon"), F.col("tz"))

# Формирование перечня исходных данных
paths = [f"{dir_src}/date={(datetime.strptime(date, '%Y-%m-%d') - timedelta(days = x)).strftime('%Y-%m-%d')}" for x in range(int(d))]

# Выборка нужных атрибутов событий
df_events = spark.read.parquet(*paths) \
    .where(((F.col('event_type') == 'message') & (F.col('event.message_to').isNotNull())) | (F.col('event_type') == 'subscription')) \
    .withColumn('message_lat', F.col("lat").cast('double')*math.pi/180) \
    .withColumn('message_lon', F.col("lon").cast('double')*math.pi/180) \
    .withColumn('user_id', F.when(F.col('event_type') == 'subscription', F.col('event.user')).otherwise(F.col('event.message_from'))) \
    .withColumn('ts', F.when(F.col('event_type') == 'subscription', F.col('event.datetime')).otherwise(F.col('event.message_ts'))) \
    .select(F.col("event.message_id").alias("message_id"), F.col("user_id"), F.col("message_lat"), F.col("message_lon"), F.col("ts"), F.col('event_type'), F.col("event.subscription_channel").alias("subscription_channel"), F.col("event.message_from").alias("message_from"), F.col("event.message_to").alias("message_to"))

# Разделение событий на сообщения и подписки
df_messages = df_events.where(F.col('event_type') == 'message')
df_subscriptions = df_events.where(F.col('event_type') == 'subscription')

# Определение ближайшего города для каждого события
df_distances = df_messages.crossJoin(df_geo.hint("broadcast")) \
    .withColumn('dist', F.lit(2)*F.lit(6371)*F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('message_lat')-F.col('city_lat'))/F.lit(2)),2) \
           +F.cos('message_lat') \
           *F.cos('city_lat') \
           *F.pow(F.sin((F.col('message_lon')-F.col('city_lon'))/F.lit(2)),2))
        )
    ) \
    .withColumn('rank', F.row_number().over(Window().partitionBy('message_id').orderBy('dist'))) \
    .filter(F.col('rank')==1) \
    .select('message_id', 'event_type', 'user_id', 'message_lat', 'message_lon', 'ts', 'dist', 'message_from', 'message_to', 'id', 'tz')

# Определение пользователей, подписанных на один канал
users_in_chanel = df_subscriptions.select('subscription_channel', 'user_id')
user_left = users_in_chanel.withColumnRenamed('user_id', 'user_left')
user_right = users_in_chanel.withColumnRenamed('user_id', 'user_right')
users_pair = user_left.join(user_right,[user_left.subscription_channel == user_right.subscription_channel, user_left.user_left != user_right.user_right],'inner').select('user_left', 'user_right').distinct()

# Определение пользователей не переписывающихся друг с другом
contacts = df_distances.select('message_from', 'message_to').distinct()
users_pair = users_pair.join(contacts, [((users_pair.user_left == contacts.message_from) & (users_pair.user_right == contacts.message_to)) | ((users_pair.user_right == contacts.message_from) & (users_pair.user_left == contacts.message_to))], 'leftanti')

# Определение актуальных значений для пользователей
window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
user_coordinates = df_distances.select(
    'user_id', 
    'tz',
    F.first('message_lat', True).over(window).alias('act_lat'),
    F.first('message_lon', True).over(window).alias('act_lng'),
    F.first('id', True).over(window).alias('zone_id'),
    F.first('ts', True).over(window).alias('act_ts'),
).distinct()

# Формирование финального датафрейма для витрины 3
df_dm3 = users_pair \
    .join(user_coordinates, users_pair.user_left == user_coordinates.user_id, 'left') \
    .withColumnRenamed('user_id', 'lu').withColumnRenamed('act_lat', 'lat1').withColumnRenamed('act_lng', 'lng1').withColumnRenamed('zone_id', 'zone_id1').withColumnRenamed('act_ts', 'act_ts1').withColumnRenamed('tz', 'tz1') \
    .join(user_coordinates, users_pair.user_right == user_coordinates.user_id, 'left') \
    .withColumnRenamed('user_id', 'ru').withColumnRenamed('act_lat', 'lat2').withColumnRenamed('act_lng', 'lng2').withColumnRenamed('zone_id', 'zone_id2').withColumnRenamed('act_ts', 'act_ts2').withColumnRenamed('tz', 'tz2') \
    .withColumn('distance',
        F.lit(2)*F.lit(6371)*F.asin(
            F.sqrt(
                F.pow(F.sin((F.col('lat2') - F.col('lat1'))/F.lit(2)), 2)\
                + F.cos('lat1') * F.cos('lat2') * F.pow(F.sin((F.col('lng2')-F.col('lng1'))/F.lit(2)), 2)
            )
        )
    ) \
    .where(F.col('distance') <= 1) \
    .select(
        'user_left',
        'user_right',
        F.current_timestamp().alias('processed_dttm'),
        F.when(F.col('zone_id1') == F.col('zone_id2'), F.col('zone_id1')).alias('zone_id'),
        F.from_utc_timestamp(F.col('act_ts1'), F.col('tz1')).alias('local_time')
    )

# Запись на HDFS
df_dm3.write.option("header",True).mode("overwrite").parquet(dir_tgt)
