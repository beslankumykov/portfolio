import airflow
import os 
import math
import pyspark.sql.functions as F
from airflow import DAG
from datetime import date, datetime
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

# Параметры по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
}

# Параметры дага
dag = DAG(
    dag_id="dag_dm",
    schedule_interval=None,
    default_args=default_args,
)

# Объявление задачи через SparkSubmitOperator
t1 = SparkSubmitOperator(
    task_id = 'dm1_task',
    dag = dag,
    application ='/lessons/scripts/dm1.py' ,
    conn_id = 'yarn_spark',
    application_args = ['2022-05-31', '30',  '/user/master/data/geo/events', '/user/kubes00/project/dm1'],
    executor_cores = 2,
    executor_memory = '4g'
)

# Объявление задачи через SparkSubmitOperator
t2 = SparkSubmitOperator(
    task_id = 'dm2_task',
    dag = dag,
    application ='/lessons/scripts/dm2.py' ,
    conn_id = 'yarn_spark',
    application_args = ['2022-05-31', '30',  '/user/master/data/geo/events', '/user/kubes00/project/dm2'],
    executor_cores = 2,
    executor_memory = '4g'
)

# Объявление задачи через SparkSubmitOperator
t3 = SparkSubmitOperator(
    task_id = 'dm1_task',
    dag = dag,
    application ='/lessons/scripts/dm3.py' ,
    conn_id = 'yarn_spark',
    application_args = ['2022-05-31', '30',  '/user/master/data/geo/events', '/user/kubes00/project/dm3'],
    executor_cores = 2,
    executor_memory = '4g'
)

# Последовательность задач
t1 >> t2 >> t3