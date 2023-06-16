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
d = 30
dir_src = '/user/master/data/geo/events'
dir_tgt = '/user/kubes00/project/dm1'

# Запуск spark-сессии
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('yarn')\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 2) \
    .appName("test_dm1").getOrCreate()

# Города Австралии доработаны вручную и содержат временные зоны в файле geo_with_tz.csv
df_geo = spark.read.csv(path = "/user/kubes00/project/geo_with_tz.csv", sep=",", inferSchema=True, header=True)\
    .select(F.col("city"), (F.col("lat").cast('double')*math.pi/180).alias("city_lat"), (F.col("lon").cast('double')*math.pi/180).alias("city_lon"), F.col("tz"))

# Формирование перечня исходных данных
paths = [f"{dir_src}/date={(datetime.strptime(date, '%Y-%m-%d') - timedelta(days = x)).strftime('%Y-%m-%d')}" for x in range(int(d))]

# Выборка нужных атрибутов событий
df_events = spark.read.parquet(*paths) \
    .where("event_type = 'message' and event.message_id is not null") \
    .select(F.col("event.message_id").alias("message_id"), F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date"), F.col("event.message_from").alias("user_id"), (F.col("lat").cast('double')*math.pi/180).alias("message_lat"),(F.col("lon").cast('double')*math.pi/180).alias("message_lon"))

# Определение ближайшего города для каждого события
df_distances = df_events.crossJoin(df_geo.hint("broadcast"))\
    .withColumn('dist', F.lit(2)*F.lit(6371)*F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('message_lat')-F.col('city_lat'))/F.lit(2)),2) \
           +F.cos('message_lat') \
           *F.cos('city_lat') \
           *F.pow(F.sin((F.col('message_lon')-F.col('city_lon'))/F.lit(2)),2))
        )
    ) \
    .withColumn('rank', F.row_number().over(Window().partitionBy('message_id').orderBy('dist')))\
    .filter(F.col('rank')==1)\
    .select('message_id', 'date', 'user_id', 'city', 'tz', 'dist')

# Вычисление актуальных городов
df_active = df_distances \
    .withColumn("row",F.row_number().over(Window().partitionBy("user_id").orderBy(F.col("date").desc()))) \
    .filter(F.col("row") == 1) \
    .withColumnRenamed("city", "act_city") \
    .select('user_id', 'act_city')

# Промежуточный датафрейм
df_tmp = df_distances \
    .withColumn('max_date',F.max('date').over(Window().partitionBy('user_id'))) \
    .withColumn('city_prev',F.lag('city',-1,'start').over(Window().partitionBy('user_id').orderBy(F.col('date').desc()))) \
    .filter(F.col('city') != F.col('city_prev'))

# Вычисление домашних городов
df_home_city = df_tmp \
    .withColumn('date_lag', F.coalesce(
            F.lag('date').over(Window().partitionBy('user_id').orderBy(F.col('date').desc())),
            F.col('max_date')
    )) \
    .withColumn('date_diff',F.datediff(F.col('date_lag'),F.col('date'))) \
    .filter(F.col('date_diff') > 27) \
    .withColumn('row',F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").desc()))) \
    .filter(F.col('row') == 1) \
    .drop('dist','city_prev','date_lag','row','date_diff','max_date') \
    .withColumn("local_time", F.from_utc_timestamp(F.col("date"),F.col('tz'))) 

# Вычисление количества и названий посещенных городов
df_cities_count = df_tmp.groupBy("user_id").count().withColumnRenamed("count", "travel_count")
df_cities_names = df_tmp.groupBy("user_id").agg(F.collect_list('city').alias('travel_array'))

# Формирование финального датафрейма для витрины 1
df_dm1 = df_active \
    .join(df_home_city, 'user_id', how='left') \
    .join(df_cities_count, 'user_id', how='left') \
    .join(df_cities_names, 'user_id', how='left')

# Запись на HDFS
df_dm1.write.option("header",True).partitionBy("user_id").mode("overwrite").parquet(dir_tgt)