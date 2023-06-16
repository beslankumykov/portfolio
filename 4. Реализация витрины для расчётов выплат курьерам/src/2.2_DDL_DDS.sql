drop table if exists dds.dm_restaurant;
CREATE TABLE dds.dm_restaurant (
	id int GENERATED ALWAYS AS identity CONSTRAINT dm_restaurant_pkey PRIMARY KEY,
	restaurant_id varchar not null,
	restaurant_name varchar not null
);
ALTER TABLE dds.dm_restaurant ADD CONSTRAINT dm_restaurant_restaurant_id_unique UNIQUE (restaurant_id);

drop table if exists dds.dm_courier;
CREATE TABLE dds.dm_courier (
	id int GENERATED ALWAYS AS identity CONSTRAINT dm_courier_pkey PRIMARY KEY,
	courier_id varchar not null,
	courier_name varchar not null
);
ALTER TABLE dds.dm_courier ADD CONSTRAINT dm_courier_courier_id_unique UNIQUE (courier_id);

drop table if exists dds.dm_timestamp;
CREATE TABLE dds.dm_timestamp (
	id int GENERATED ALWAYS AS identity CONSTRAINT dm_timestamp_pkey PRIMARY KEY,
	ts timestamp not null,
	year smallint not null constraint dm_timestamps_year_check check (year >= 2022 and year < 2500),
	month smallint not null constraint dm_timestamps_month_check check (month >= 1 and month <= 12),
	day smallint not null constraint dm_timestamps_day_check check (day >= 1 and day <= 31),
	time time not null,
	date date not null
);
ALTER TABLE dds.dm_timestamp ADD CONSTRAINT dm_timestamp_ts_unique UNIQUE (ts);

drop table if exists dds.dm_order cascade;
CREATE TABLE dds.dm_order (
	id int GENERATED ALWAYS AS identity CONSTRAINT dm_order_pkey PRIMARY KEY,
	order_id varchar not null,
	timestamp_id int not null,
	restaurant_id integer not null
);
ALTER TABLE dds.dm_order ADD CONSTRAINT dm_order_order_id_unique UNIQUE (order_id);
alter table dds.dm_order add constraint dm_order_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurant (id) ON DELETE CASCADE;
alter table dds.dm_order add constraint dm_order_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamp (id) ON DELETE CASCADE;

drop table if exists dds.fct_delivery cascade;
CREATE TABLE dds.fct_delivery (
	id int GENERATED ALWAYS AS identity CONSTRAINT fct_delivery_pkey PRIMARY KEY,
	delivery_id int not null,
	order_id int not null,
	courier_id int not null,
	timestamp_id int not null,
	address varchar not null,
	rate int not null,
	summ numeric(14, 2) not null,
	tip_sum numeric(14, 2) not null
);
ALTER TABLE dds.fct_delivery ADD CONSTRAINT fct_delivery_delivery_id_unique UNIQUE (delivery_id);
alter table dds.fct_delivery add constraint fct_delivery_order_id_fkey foreign key (order_id) references dds.dm_order (id) ON DELETE CASCADE;
alter table dds.fct_delivery add constraint fct_delivery_courier_id_fkey foreign key (courier_id) references dds.dm_courier (id) ON DELETE CASCADE;
alter table dds.fct_delivery add constraint fct_delivery_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamp (id) ON DELETE CASCADE;