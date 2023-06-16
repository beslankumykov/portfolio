-- Добавление таблицы в слой staging для данных без преобразований
CREATE TABLE staging.user_order_log_raw (
	id varchar(100),
	date_time varchar(100),
	city_id varchar(100),
	city_name varchar(100),
	customer_id varchar(100),
	first_name varchar(100),
	last_name varchar(100),
	item_id varchar(100),
	item_name varchar(100),
	quantity varchar(100),
	payment_amount varchar(100),
	status varchar(100)
);
