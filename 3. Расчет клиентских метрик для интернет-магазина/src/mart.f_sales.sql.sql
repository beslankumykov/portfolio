delete from mart.f_sales f
where f.date_id = 
(select date_id from mart.d_calendar where date_actual::Date = '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select 
    dc.date_id, 
    item_id, 
    customer_id, 
    city_id, 
    quantity, 
    case when uol.status = 'shipped' then payment_amount when uol.status = 'refunded' then -payment_amount else 0 end,
    status
from 
    staging.user_order_log uol
left join 
    mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where 
    uol.date_time::Date = '{{ds}}';