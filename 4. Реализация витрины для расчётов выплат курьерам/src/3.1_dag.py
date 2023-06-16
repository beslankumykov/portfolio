import requests
from bson.json_util import dumps
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

headers = {"X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f", "X-Nickname": "Beslan", "X-Cohort": "3"}   
pg_hook = PostgresHook(postgres_conn_id='dwh')
pg_conn = pg_hook.get_conn()

def get_full_data(pg_table):
    offset = 0
    response = requests.get(f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+pg_table+"?offset="+str(offset),headers=headers).json()
    cur = pg_conn.cursor()
    cur.execute(f"truncate table stg.{pg_table}")
    while len(response) > 0:
        records = [{"object_value": dumps(res)} for res in response]
        for record in records:
            cur.execute(f"insert into stg.{pg_table} (info) values (%(object_value)s);", record)
        offset += 50
        response = requests.get(f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+pg_table+"?offset="+str(offset),headers=headers).json()
    pg_conn.commit()

def get_increment(pg_table):
    cur = pg_conn.cursor()
    cur.execute("select coalesce(max(workflow_settings::int),0) from stg.srv_wf_settings where workflow_key = '"+pg_table+"'");
    offset = cur.fetchall()[0][0]
    response = requests.get("https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+pg_table+"?offset="+str(offset),headers=headers).json()
    while len(response) > 0:
        records = [{"object_value": dumps(res)} for res in response]
        for record in records:
            cur.execute(f"insert into stg.{pg_table} (info) values (%(object_value)s)", record)
        offset += 50
        response = requests.get("https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+pg_table+"?offset="+str(offset),headers=headers).json()
    cur.execute("insert into stg.srv_wf_settings (workflow_key, workflow_settings) select '" + pg_table + "', count(*)::text from stg." + pg_table)
    pg_conn.commit()

with DAG(
        'dag_sp5',
        start_date=datetime.today(),
        schedule_interval=None
) as dag:

    task_load_stg_restaurants = PythonOperator(
        task_id='load_stg_restaurants',
        python_callable=get_full_data,
        op_kwargs={'pg_table': 'restaurants'}
    )

    task_load_stg_couriers = PythonOperator(
        task_id='load_stg_couriers',
        python_callable=get_full_data,
        op_kwargs={'pg_table': 'couriers'}
    )

    task_load_stg_deliveries = PythonOperator(
        task_id='load_stg_deliveries',
        python_callable=get_increment,
        op_kwargs={'pg_table': 'deliveries'}
    )

    task_load_dds_dm_restaurant = PostgresOperator(
        task_id='load_dds_dm_restaurant',
        postgres_conn_id='dwh',
        sql="""
            begin;
                insert into dds.dm_restaurant (restaurant_id, restaurant_name)
                select 
                    info::JSON->>'_id', 
                    info::JSON->>'name'
                from 
                    stg.restaurants
                on conflict (restaurant_id) do update set
                    restaurant_name = excluded.restaurant_name;
                commit;
            end;
        """
    )

    task_load_dds_dm_courier = PostgresOperator(
        task_id='load_dds_dm_courier',
        postgres_conn_id='dwh',
        sql="""
            begin;
                insert into dds.dm_courier (courier_id, courier_name)
                select 
                    info::JSON->>'_id', 
                    info::JSON->>'name'
                from 
                    stg.couriers
                on conflict (courier_id) do update set
                    courier_name = excluded.courier_name;
                commit;
            end;
        """
    )

    task_load_dds_dm_timestamp = PostgresOperator(
        task_id='load_dds_dm_timestamp',
        postgres_conn_id='dwh',
        sql="""
            begin;
                insert into dds.dm_timestamp (ts, year, month, day, time, date)
                select
                    ts::timestamp(3),
                    extract(year from ts::timestamp(3)) as year,
                    extract(month from ts::timestamp(3)) as month,
                    extract(day from ts::timestamp(3)) as day,
                    ts::time as time,
                    ts::date as date
                from 
                    (
                    select info::JSON->>'order_ts' ts from stg.deliveries
                    UNION
                    select info::JSON->>'delivery_ts' ts from stg.deliveries
                    ) x
                on conflict (ts) do update set
                    year = excluded.year, 
                    month = excluded.month, 
                    day = excluded.day, 
                    time = excluded.time, 
                    date = excluded.date;
                commit;
            end;
        """
    )

    task_load_dds_dm_order = PostgresOperator(
        task_id='load_dds_dm_order',
        postgres_conn_id='dwh',
        sql="""
            begin;
                insert into dds.dm_order (order_id, timestamp_id, restaurant_id)
                select 
                    info::JSON->>'order_id',
                    t.id,
                    r.id 
                from 
                    stg.deliveries d
                join
                    dds.dm_timestamp t on (d.info::JSON->>'order_ts')::timestamp = t.ts
                join
                    dds.dm_restaurant r on 1=1
                on conflict (order_id) do update set
                    timestamp_id = excluded.timestamp_id, 
                    restaurant_id = excluded.restaurant_id;
                commit;
            end;
        """
    )

    task_load_dds_fct_delivery = PostgresOperator(
        task_id='load_dds_fct_delivery',
        postgres_conn_id='dwh',
        sql="""
            begin;
                insert into dds.fct_delivery (delivery_id, order_id, courier_id, timestamp_id, address, rate, summ, tip_sum)
                select 
                    info::JSON->>'delivery_id',
                    o.id,
                    c.id,
                    t.id,
                    (info::JSON->>'address')::varchar,
                    (info::JSON->>'rate')::int,
                    (info::JSON->>'sum')::numeric(14, 2),
                    (info::JSON->>'tip_sum')::numeric(14, 2)
                from 
                    stg.deliveries d
                join
                    dds.dm_order o on (d.info::JSON->>'order_id')::varchar = o.order_id 
                join
                    dds.dm_courier c on (d.info::JSON->>'courier_id')::varchar = c.courier_id
                join
                    dds.dm_timestamp t on (d.info::JSON->>'delivery_ts')::timestamp = t.ts
                on conflict (delivery_id) do update set
                    order_id = excluded.order_id,
                    courier_id = excluded.courier_id,
                    timestamp_id = excluded.timestamp_id, 
                    address = excluded.address,
                    rate = excluded.rate,
                    summ = excluded.summ,
                    tip_sum = excluded.tip_sum;
                commit;
            end;
        """
    )

    task_load_cdm_dm_courier_ledger = PostgresOperator(
        task_id='load_cdm_dm_courier_ledger',
        postgres_conn_id='dwh',
        sql="""
            begin;
                truncate table cdm.dm_courier_ledger;
                insert into cdm.dm_courier_ledger (
                    courier_id, -- ID курьера, которому перечисляем
                    courier_name, -- ФИО курьера
                    settlement_year, -- год отчёта
                    settlement_month, -- месяц отчёта
                    orders_count, -- количество заказов за месяц
                    orders_total_sum, -- общая стоимость заказов
                    rate_avg, -- средний рейтинг курьера по оценкам пользователей.
                    order_processing_fee, -- сумма, удержанная компанией за обработку заказов, вычисляется как orders_total_sum * 0.25
                    courier_order_sum, -- сумма, которую необходимо перечислить курьеру за доставленные заказы в зависимости от рейтинга
                    courier_tips_sum, -- сумма, которую пользователи оставили курьеру в качестве чаевых
                    courier_reward_sum -- сумма, которую необходимо перечислить курьеру, вычисляется как courier_order_sum + courier_tips_sum * 0.95
                )
                select 
                    y.courier_id,
                    y.courier_name,
                    y.settlement_year,
                    y.settlement_month,
                    y.orders_count,
                    y.orders_total_sum,
                    y.rate_avg,
                    y.order_processing_fee,
                    y.courier_order_sum,
                    y.courier_tips_sum,
                    y.courier_order_sum + y.courier_tips_sum * 0.95
                from
                    (
                    select 
                        x.courier_id,
                        x.courier_name,
                        x.settlement_year,
                        x.settlement_month,
                        x.orders_count,
                        x.orders_total_sum,
                        x.rate_avg,
                        x.order_processing_fee,
                        case 
                            when x.rate_avg < 4 then rate_avg_0_4
                            when x.rate_avg >= 4 and x.rate_avg < 4.5 then rate_avg_4_45
                            when x.rate_avg >= 4.5 and x.rate_avg < 4.9 then rate_avg_45_49
                            when x.rate_avg >= 4.9 then rate_avg_49_5
                        end courier_order_sum,
                        x.courier_tips_sum
                    from
                        (
                        select 
                            c.courier_id,
                            c.courier_name,
                            t."year" as settlement_year,
                            t."month" as settlement_month,
                            count(o.order_id) as orders_count,
                            sum(d.summ) as orders_total_sum,
                            avg(d.rate) as rate_avg,
                            sum(d.summ) * 0.25 as order_processing_fee,
                            sum(d.tip_sum) courier_tips_sum,
                            sum(greatest(d.summ*0.05,100)) as rate_avg_0_4,
                            sum(greatest(d.summ*0.07,150)) as rate_avg_4_45,
                            sum(greatest(d.summ*0.08,175)) as rate_avg_45_49,
                            sum(greatest(d.summ*0.1,200)) as rate_avg_49_5
                        from 
                            dds.fct_delivery d
                        join
                            dds.dm_order o on o.id = d.order_id 
                        join
                            dds.dm_courier c on c.id = d.courier_id
                        join 
                            dds.dm_timestamp t on t.id = d.timestamp_id
                        group by
                            c.courier_id, c.courier_name, t."year", t."month"
                        ) x
                    ) y;
                commit;
            end;
        """
    )

    task_load_stg_restaurants >> task_load_dds_dm_restaurant
    task_load_stg_couriers >> task_load_dds_dm_courier
    task_load_stg_deliveries >> task_load_dds_dm_timestamp
    [task_load_stg_deliveries, task_load_dds_dm_restaurant, task_load_dds_dm_timestamp] >> task_load_dds_dm_order
    [task_load_stg_deliveries, task_load_dds_dm_order, task_load_dds_dm_courier, task_load_dds_dm_timestamp] >> task_load_dds_fct_delivery
    [task_load_dds_fct_delivery, task_load_dds_dm_order, task_load_dds_dm_courier, task_load_dds_dm_timestamp] >> task_load_cdm_dm_courier_ledger
