insert into analysis.tmp_rfm_monetary_value (user_id, monetary_value)
select u.id user_id, ntile(5) OVER(ORDER BY sum(o.payment) nulls first) monetary_value
from analysis.users u
left join analysis.orders o on u.id = o.user_id 
and o.status = (select id from analysis.orderstatuses os where key = 'Closed') 
and o.order_ts >= to_date('01.01.2021','dd.mm.yyyy')
group by u.id;
