drop table if exists cdm.dm_courier_ledger;
create table cdm.dm_courier_ledger (
	id int GENERATED ALWAYS AS identity, -- идентификатор записи
	courier_id int, -- ID курьера, которому перечисляем
	courier_name varchar, -- ФИО курьера
	settlement_year int, -- год отчёта
	settlement_month int, -- месяц отчёта
	orders_count int, -- количество заказов за месяц
	orders_total_sum numeric(14, 2), -- общая стоимость заказов
	rate_avg numeric(14, 2), -- средний рейтинг курьера по оценкам пользователей.
	order_processing_fee numeric(14, 2), -- сумма, удержанная компанией за обработку заказов, вычисляется как orders_total_sum * 0.25
	courier_order_sum numeric(14, 2), -- сумма, которую необходимо перечислить курьеру за доставленные заказы в зависимости от рейтинга
	courier_tips_sum numeric(14, 2), -- сумма, которую пользователи оставили курьеру в качестве чаевых
	courier_reward_sum numeric(14, 2) -- сумма, которую необходимо перечислить курьеру, вычисляется как courier_order_sum + courier_tips_sum * 0.95
);

ALTER TABLE cdm.dm_courier_ledger ADD PRIMARY KEY (id);

ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN orders_count SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN orders_total_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN rate_avg SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN order_processing_fee SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_order_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_tips_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_courier_ledger ALTER COLUMN courier_reward_sum SET DEFAULT 0;

ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK (courier_order_sum >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK (courier_tips_sum >= 0);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK (courier_reward_sum >= 0);

ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year >= 2000 and settlement_year < 3000);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month >= 1 and settlement_month < 12);

ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month);