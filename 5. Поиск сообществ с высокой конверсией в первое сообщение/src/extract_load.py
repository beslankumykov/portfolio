import os
import boto3
import logging
import vertica_python
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Подключение логгера
task_logger = logging.getLogger('airflow.task')

# Параметры подключения к S3 для выгрузки файла
SERVICE_NAME = 's3'
ENDPOINT_URL = 'https://storage.yandexcloud.net'
AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
BUCKET = 'sprint6'
PATH = '/data'

# Параметры подключения к DWH на СУБД Vertica
conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': 'KUBES00YANDEXRU',       
             'password': 'aGfh7BEOO61qmPp',
             'database': 'dwh',
             'autocommit': True
}

# SQL для загрузки таблицы group_log в слой STAGING
sql_load_stg = """
    TRUNCATE TABLE KUBES00YANDEXRU__STAGING.group_log;
    COPY KUBES00YANDEXRU__STAGING.group_log FROM LOCAL '/data/group_log.csv' DELIMITER ',' REJECTED DATA AS TABLE KUBES00YANDEXRU__STAGING.group_log_rej;
"""

# SQL для загрузки таблицы l_user_group_activity в детальный слой
sql_load_lnk = """
    INSERT INTO KUBES00YANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
    select distinct
        hash(gl.user_id, gl.group_id),
        hu.hk_user_id, 
        hg.hk_group_id,
        now() as load_dt,
        's3' as load_src
    from 
        KUBES00YANDEXRU__STAGING.group_log as gl
    left join 
        KUBES00YANDEXRU__DWH.h_users hu on hu.user_id = gl.user_id
    left join 
        KUBES00YANDEXRU__DWH.h_groups hg on hg.group_id = gl.group_id
    where 
        hash(gl.user_id, gl.group_id) not in (select hk_l_user_group_activity from KUBES00YANDEXRU__DWH.l_user_group_activity); 
"""

# SQL для загрузки таблицы s_auth_history в детальный слой
sql_load_sat = """
    INSERT INTO KUBES00YANDEXRU__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
    select
        luga.hk_l_user_group_activity,
        gl.user_id_from,
        gl.event,
        gl.datetime,
        now() as load_dt,
        's3' as load_src
    from 
        KUBES00YANDEXRU__STAGING.group_log as gl
    left join 
        KUBES00YANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
    left join 
        KUBES00YANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
    left join 
        KUBES00YANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
    where 
        luga.hk_l_user_group_activity not in (select hk_l_user_group_activity from KUBES00YANDEXRU__DWH.s_auth_history); 
"""

# Процедура для выгрузки данных из S3 по заданным параметрам
def get_from_s3(p_service_name, p_endpoint_url, p_aws_access_key_id, p_aws_secret_access_key, p_bucket, p_path, p_file):
    try:
        os.mkdir(p_path)
    except OSError:
        task_logger.info("The directory %s was already created" %p_path)
    else:
        task_logger.info("The directory %s is created" %p_path)

    session = boto3.session.Session()
    s3_client = session.client(
        service_name = p_service_name,
        endpoint_url = p_endpoint_url,
        aws_access_key_id = p_aws_access_key_id,
        aws_secret_access_key = p_aws_secret_access_key,
    )
    s3_client.download_file(
        Bucket = p_bucket,
        Key = p_file,
        Filename = p_path + '/' + p_file
    ) 

# Процедура для выпонения SQL-выражения в СУБД Vertica
def execute_sql_stmt (sql_stmt, conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()  
        cur.execute(sql_stmt)
        cur.close()
        conn.commit()
        conn.close()

# Описание дага
with DAG(
        'extract_load',
        start_date = datetime.today(),
        schedule_interval = None
) as dag:

    task_get_group_log_from_s3 = PythonOperator(
        task_id = 'get_group_log_from_s3',
        python_callable = get_from_s3,
        op_kwargs = {
            'p_service_name': SERVICE_NAME, 
            'p_endpoint_url': ENDPOINT_URL, 
            'p_aws_access_key_id': AWS_ACCESS_KEY_ID, 
            'p_aws_secret_access_key': AWS_SECRET_ACCESS_KEY, 
            'p_bucket': BUCKET, 
            'p_path': PATH,
            'p_file': 'group_log.csv', 
        }
    )

    task_print_10_lines = BashOperator(
        task_id = 'print_10_lines',
        bash_command = "head -10 /data/group_log.csv"
    )

    task_load_stg_group_log = PythonOperator(
        task_id = 'load_stg_group_log',
        python_callable = execute_sql_stmt,
        op_kwargs = {'sql_stmt': sql_load_stg, 'conn_info': conn_info}
    )

    task_load_dwh_l_user_group_activity = PythonOperator(
        task_id = 'load_dwh_l_user_group_activity',
        python_callable = execute_sql_stmt,
        op_kwargs = {'sql_stmt': sql_load_lnk, 'conn_info': conn_info}
    )

    task_load_dwh_s_auth_history = PythonOperator(
        task_id = 'load_dwh_s_auth_history',
        python_callable = execute_sql_stmt,
        op_kwargs = {'sql_stmt': sql_load_sat, 'conn_info': conn_info}
    )

    task_get_group_log_from_s3 >> task_print_10_lines >> task_load_stg_group_log >> task_load_dwh_l_user_group_activity >> task_load_dwh_s_auth_history
