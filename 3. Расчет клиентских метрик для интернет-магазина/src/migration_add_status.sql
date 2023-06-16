-- Добавление статуса в основную таблицу слоя staging
alter table staging.user_order_log add status varchar(8);

-- Добавление статуса в таблицу фактов в витрине
alter table mart.f_sales add status varchar(8) default 'shipped';