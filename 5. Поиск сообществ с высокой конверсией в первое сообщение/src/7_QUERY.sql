with
	cte0 as (
		select hk_group_id from KUBES00YANDEXRU__DWH.h_groups order by registration_dt limit 10
	),
	cte1 as (
		select
			luga.hk_group_id, count(luga.hk_user_id) as cnt_added_users
		from 
			KUBES00YANDEXRU__DWH.s_auth_history sah
		join
			KUBES00YANDEXRU__DWH.l_user_group_activity luga on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
		where 
			sah.event = 'add' and 
			luga.hk_group_id in (select hk_group_id from cte0)
		group by 
			luga.hk_group_id
	),
	cte2 as (
		select
			lgd.hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
		from 
			KUBES00YANDEXRU__DWH.l_user_message as lum
		left join 
			KUBES00YANDEXRU__DWH.l_groups_dialogs as lgd on lum.hk_message_id = lgd.hk_message_id
		where 
			lgd.hk_group_id in (select hk_group_id from cte0)
		group by 
			lgd.hk_group_id
	),
	cte3 as (
		select 
			luga.hk_group_id, cte2.cnt_users_in_group_with_messages/count(distinct luga.hk_user_id) as group_conversion
		from 
			KUBES00YANDEXRU__DWH.l_user_group_activity luga 
		join 
			cte2 on cte2.hk_group_id = luga.hk_group_id
		group by 
			luga.hk_group_id, cte2.cnt_users_in_group_with_messages
	)
select
	cte1.hk_group_id, cte1.cnt_added_users, cte2.cnt_users_in_group_with_messages, cte3.group_conversion
from 
	cte1
join 
	cte2 on cte1.hk_group_id = cte2.hk_group_id
join 
	cte3 on cte1.hk_group_id = cte3.hk_group_id
order by 
	4 desc;