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
dir_tgt = '/user/kubes00/project/dm2'

# Запуск spark-сессии
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('yarn')\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 2) \
    .appName("test_dm2").getOrCreate()

# Города Австралии доработаны вручную и содержат временные зоны в файле geo_with_tz.csv
df_geo = spark.read.csv(path = "/user/kubes00/project/geo_with_tz.csv", sep=",", inferSchema=True, header=True)\
    .select(F.col("city"), (F.col("lat").cast('double')*math.pi/180).alias("city_lat"), (F.col("lon").cast('double')*math.pi/180).alias("city_lon"), F.col("tz"))

# Формирование перечня исходных данных
paths = [f"{dir_src}/date={(datetime.strptime(date, '%Y-%m-%d') - timedelta(days = x)).strftime('%Y-%m-%d')}" for x in range(int(d))]

# Выборка нужных атрибутов событий
df_events = spark.read.parquet(*paths) \
    .where("event_type = 'message' or event_type = 'reaction' or event_type = 'subscription'") \
    .select(F.col("event.message_id").alias("message_id"), F.col("event_type"), F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date"), F.col("event.message_from").alias("user_id"), (F.col("lat").cast('double')*math.pi/180).alias("message_lat"),(F.col("lon").cast('double')*math.pi/180).alias("message_lon"))

# Определение ближайшего города для каждого события
df_distances = df_events.crossJoin(df_geo.hint("broadcast")) \
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
    .select('message_id', 'event_type', 'date', 'user_id', 'city', 'dist')

# Вычисление показателей по сообщениям
df_message_benchmarks = df_distances.filter(F.col('event_type')=='message') \
    .withColumnRenamed("city", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_message",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_message",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_message','week','week_message') \
    .distinct()

# Вычисление показателей по реакциям
df_reaction_benchmarks = df_distances.filter(F.col('event_type')=='reaction') \
    .withColumnRenamed("city", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_reaction",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_reaction",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_reaction','week','week_reaction') \
    .distinct()

# Вычисление показателей по подпискам
df_subscription_benchmarks = df_distances.filter(F.col('event_type')=='subscription') \
    .withColumnRenamed("city", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("week_subscription",F.count("message_id").over(Window.partitionBy("zone_id", "week"))) \
    .withColumn("month_subscription",F.count("message_id").over(Window.partitionBy("zone_id", "month"))) \
    .select('zone_id','month','month_subscription','week','week_subscription') \
    .distinct()

# Вычисление показателей по пользователям
df_users_benchmarks = df_distances \
    .withColumnRenamed("city", "zone_id") \
    .withColumn("week", F.date_trunc("week",F.col('date'))) \
    .withColumn("month", F.date_trunc("month",F.col('date'))) \
    .withColumn("row", (F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").asc())))).filter(F.col("row") == 1) \
    .withColumn("week_user", (F.count("row").over(Window.partitionBy("zone_id", "week")))) \
    .withColumn("month_user", (F.count("row").over(Window.partitionBy("zone_id", "month")))) \
    .select("zone_id", "week", "month", "week_user", "month_user").distinct()

# Формирование финального датафрейма для витрины 2
df_dm2 = df_users_benchmarks \
    .join(df_message_benchmarks, ['zone_id','week','month'], how = 'full') \
    .join(df_reaction_benchmarks, ['zone_id','week','month'], how ='full') \
    .join(df_subscription_benchmarks, ['zone_id','week','month'], how ='full') \
    .fillna(0) \
    .select('month','week','zone_id','week_message','week_reaction','week_subscription','week_user','month_message','month_reaction','month_subscription','month_user') \
    .orderBy('month', 'week','zone_id')

# Запись на HDFS
df_dm2.write.option("header",True).mode("overwrite").parquet(dir_tgt)
