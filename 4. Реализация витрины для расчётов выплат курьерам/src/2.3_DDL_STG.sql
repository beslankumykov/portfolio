drop table if exists stg.restaurants;
CREATE TABLE stg.restaurants (
	id serial CONSTRAINT restaurants_pkey PRIMARY KEY,
	info text
);

drop table if exists stg.couriers;
CREATE TABLE stg.couriers (
	id serial CONSTRAINT couriers_pkey PRIMARY KEY,
	info text
);

drop table if exists stg.deliveries;
CREATE TABLE stg.deliveries (
	id serial CONSTRAINT deliveries_pkey PRIMARY KEY,
	info text
);

drop table stg.srv_wf_settings;
create table if not exists stg.srv_wf_settings (
	id int GENERATED ALWAYS AS identity,
	workflow_key varchar,
	workflow_settings text
);