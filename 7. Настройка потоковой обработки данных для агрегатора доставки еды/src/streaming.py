import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType, LongType

postgresql_settings_in = {
    'user': 'student',
    'password': 'de-student',
    'driver': 'org.postgresql.Driver',
}

postgresql_settings_out = {
    'user': 'jovyan',
    'password': 'jovyan',
    'driver': 'org.postgresql.Driver',
}

TOPIC_IN = 'student.topic.cohort6.kubes00_in'
TOPIC_OUT = 'student.topic.cohort6.kubes00_out'

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ])
    spark = (SparkSession.builder
        .appName(test_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )
    return spark

# вычитываем всех пользователей с подпиской на рестораны
def read_table(spark: SparkSession) -> DataFrame:
    subscribers_restaurant_df = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
        .option("dbtable", "subscribers_restaurants")
        .options(**postgresql_settings_in)
        .option("maxOffsetsPerTrigger", 20)
        .load())
    return subscribers_restaurant_df

# читаем из топика Kafka сообщения с акциями от ресторанов 
def read_stream(spark: SparkSession) -> DataFrame:
    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType(), nullable=True),
        StructField("adv_campaign_id", StringType(), nullable=True),
        StructField("adv_campaign_content", StringType(), nullable=True),
        StructField("adv_campaign_owner", StringType(), nullable=True),
        StructField("adv_campaign_owner_contact", StringType(), nullable=True),
        StructField("adv_campaign_datetime_start", LongType(), nullable=True),
        StructField("adv_campaign_datetime_end", LongType(), nullable=True),
        StructField("datetime_created", LongType(), nullable=True),
    ])
    # получаем выходной поток в виде датафрейма
    restaurant_read_stream_df = (spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
        #.option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts')
        #.option('kafka.ssl.truststore.password', 'changeit')
        .option("subscribe", TOPIC_IN)
        .load()
    )
    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_read_stream_df = (restaurant_read_stream_df
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
        .selectExpr('event.*')
        .filter((col("adv_campaign_datetime_start") <= current_timestamp_utc) & (col("adv_campaign_datetime_end") >= current_timestamp_utc))
    )
    return filtered_read_stream_df

def join(restaurant_read_stream_df, subscribers_restaurant_df) -> DataFrame:
    return (restaurant_read_stream_df
        .join(subscribers_restaurant_df, ['restaurant_id'], how = 'inner') 
        .drop(subscribers_restaurant_df.id)
        .withColumn('trigger_datetime_created', lit(current_timestamp_utc))
        .dropDuplicates()
    )

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df_postgres = df.withColumn('feedback', lit(''))
    df_postgres.write.jdbc(url='jdbc:postgresql://localhost:5432/de', table="subscribers_feedback", mode="append", properties=postgresql_settings_out)
    # создаём df для отправки в Kafka. Сериализация в json.
    df_kafka = df \
        .withColumn('value', to_json(struct(
            col('restaurant_id'),
            col('adv_campaign_id'),
            col('adv_campaign_content'),
            col('adv_campaign_owner'),
            col('adv_campaign_owner_contact'),
            col('adv_campaign_datetime_start'),
            col('adv_campaign_datetime_end'),
            col('datetime_created'),
            col('client_id'),
            col('trigger_datetime_created'),
        ))) \
        .select('value')
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_kafka.write.format("kafka") \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
        .option("topic", TOPIC_OUT) \
        .option("truncate", False) \
        .save()
    # очищаем память от df
    df.unpersist()

if __name__ == "__main__":
    spark = spark_init('RestaurantSubscribeStreamingService')

    df_table = read_table(spark)
    df_table.printSchema()
    df_table.show()

    df_stream = read_stream(spark)
    df_stream.printSchema()

    result_df = join(df_stream, df_table)

    # запускаем стриминг
    result_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()