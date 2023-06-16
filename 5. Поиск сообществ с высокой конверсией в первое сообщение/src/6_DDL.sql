drop table if exists KUBES00YANDEXRU__DWH.s_auth_history;
create table KUBES00YANDEXRU__DWH.s_auth_history (
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history REFERENCES KUBES00YANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(12),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);