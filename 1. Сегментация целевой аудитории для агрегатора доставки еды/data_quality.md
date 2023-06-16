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
Полный перечень ограничений приведен в отдельном документе: 
https://docs.google.com/spreadsheets/d/1MIc4xCww7sTV_IpScqHlEf8IJhdot5yhkXIyw5X1QUE/edit?usp=sharing
