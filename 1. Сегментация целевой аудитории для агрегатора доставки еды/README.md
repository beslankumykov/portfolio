# Сегментация целевой аудитории для агрегатора доставки еды

## Этапы выполнения:
1. Выяснить требования к целевой витрине.
2. Изучить структуру исходных данных.
3. Подготовить витрину.
4. Доработать представления.


## Что такое RFM
RFM (от англ. Recency, Frequency, Monetary Value) — способ сегментации клиентов, при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз тот или иной клиент покупал что-то. На основе этого выбирают клиентские категории, на которые стоит направить маркетинговые усилия. Каждого клиента оценивают по трём факторам:
- Recency (пер. «давность») — сколько времени прошло с момента последнего заказа. Необходимо найти последний orders.order_ts по каждому user_id.
- Frequency (пер. «частота») — количество заказов. Необходимо посчитать количество orders.order_id по каждому user_id.
- Monetary Value (пер. «денежная ценность») — сумма затрат клиента. Необходимо посчитать сумму значений orders.payment по каждому user_id.



## Описание проекта
В базе две схемы: production с оперативнымы таблицами и analysis с витриной данных dm_rfm_segments, отображающей RFM-классификацию, которая состоит из таких полей:
- user_id
- recency (число от 1 до 5)
- frequency (число от 1 до 5)
- monetary_value (число от 1 до 5)

## План построения витрины для RFM-анализа:
1. В data_quality.md указать информацию о качестве данных в источнике.
2. Создать представления в схеме analysis на основе оперативных данных в схеме production без преобразований. Скрипты поместить в файл views.sql.
3. Создать таблицу analysis.dm_rfm_segments, которая будет являться витриной данных. Скрипт поместить в файл datamart_ddl.sql.
4. Создать промежуточные таблицы под каждый показатель в схеме analysis с названиями tmp_rfm_recency, tmp_rfm_frequency, tmp_rfm_monetary_value.
5. Написать sql-запросы для заполнения каждой из промежуточных таблиц. Скрипты поместить в файлы tmp_rfm_recency.sql, tmp_rfm_frequency.sql, analysis.tmp_rfm_monetary_value. При написании скриптов учесть, что метрики должны быть построены на успешно выполненных заказах (статус Closed) и на данных с начала 2021 года.
6. Написать sql-запрос для заполнения витрины analysis.dm_rfm_segments на основе ранее подготовленных таблиц. Скрипт поместить в файл datamart_query.sql.
7. Скопировать в файл datamart_query.sql первые десять строк из полученной таблицы, отсортированные по user_id.
8. Перестроить представление analysis.Orders так, чтобы поле status соответствовало последнему по времени статусу из таблицы production.OrderStatusLog. Скрипт поместить в файл orders_view.sql.

## Проверка качества данных
1. Проверка таблицы с заказами на глубину:
```
select DATE(DATE_TRUNC('month', order_ts)) as month, count(*) total_records from production.orders group by month order by month;
```
Проверка показала, что в источнике есть данные только за два месяца 2022-го года. 
Соответственно метрики будут построены по этим данным.

2. Проверка используемых полей в таблице с заказами на полноту:
```
select count(case when production.order_ts is null then 1 end) as empty_val_cnt from orders;
select count(case when production.order_id is null then 1 end) as empty_val_cnt from orders;
select count(case when production.payment is null then 1 end) as empty_val_cnt from orders;
```
Проверка показала, что в источнике нет пустых значений по интересующим полям.

3. Проверка того, что при фильтре по статусу Closed по всем клиентам есть необходимая информация:
```
select * from
(
select u.id, max(o.order_ts) max_order_ts, count(o.order_id) count_order_id, sum(o.payment) sum_payment
from analysis.users u
left join orders o on u.id = o.user_id 
and o.status = (select id from analysis.orderstatuses os where key = 'Closed')
group by u.id
) t
where max_order_ts is null or count_order_id = 0 or sum_payment is null;
```
Проверка показала, что по 12-ти клиентам при фильтре по статусу Closed в таблице orders нет информации. Их нужно учесть при ранжировании.

4. Дополнительно в источнике используются следующие ограничения для контроля качества данных в таблице с заказами:
```
ALTER TABLE production.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);
ALTER TABLE production.orders ADD CONSTRAINT orders_check CHECK ((cost = (payment + bonus_payment)));
```

