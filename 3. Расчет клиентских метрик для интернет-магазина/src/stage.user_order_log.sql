delete from staging.user_order_log where date_time::date = '{{ds}}';

insert into staging.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount)
select date_time::timestamp, city_id::int4, city_name, customer_id::int4, first_name, last_name, item_id::int4, item_name, quantity::int8, payment_amount::numeric(10,2) from
(
select uol.*, row_number() over (partition by id order by id) rn from staging.user_order_log_raw uol where uol.date_time::Date = '{{ds}}'
) x
where rn = 1;