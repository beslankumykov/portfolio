insert into analysis.tmp_rfm_frequency (user_id, frequency)
select u.id user_id, ntile(5) OVER(ORDER BY count(o.order_id) nulls first) frequency
from analysis.users u
left join analysis.orders o on u.id = o.user_id 
and o.status = (select id from analysis.orderstatuses os where key = 'Closed') 
and o.order_ts >= to_date('01.01.2021','dd.mm.yyyy')
group by u.id;
