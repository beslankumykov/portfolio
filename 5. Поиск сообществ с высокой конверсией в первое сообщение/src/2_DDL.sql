drop table if exists KUBES00YANDEXRU__STAGING.group_log;
create table KUBES00YANDEXRU__STAGING.group_log
(
	group_id int PRIMARY KEY, -- уникальный идентификатор группы
	user_id int, -- уникальный идентификатор пользователя
	user_id_from int, -- уникальный идентификатор пригласившего пользователя
	event varchar(6), -- действие, которое совершено пользователем user_id (create, add, leave)
	datetime datetime -- время совершения event
)
order by datetime
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);