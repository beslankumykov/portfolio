insert into analysis.tmp_rfm_recency (user_id, recency)
select u.id user_id, ntile(5) OVER(ORDER BY max(o.order_ts) nulls first) recency
from analysis.users u
left join analysis.orders o on u.id = o.user_id 
and o.status = (select id from analysis.orderstatuses os where key = 'Closed') 
and o.order_ts >= to_date('01.01.2021','dd.mm.yyyy')
group by u.id;
