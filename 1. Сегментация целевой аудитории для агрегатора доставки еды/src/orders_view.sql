create or replace view analysis.orders as
select 
	o.order_id, o.order_ts, o.user_id, o.bonus_payment, o.payment, o.cost, o.bonus_grant, s.status
from 
	production.orders o
left join 
	(
	select order_id, status_id as status from production.orderstatuslog where (order_id, dttm) in 
	(select order_id, max(dttm) from production.orderstatuslog group by order_id)
	) s on o.order_id = s.order_id;
