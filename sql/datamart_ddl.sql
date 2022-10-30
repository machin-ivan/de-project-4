CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar(30) NULL,
	courier_name varchar(50) NULL,
	settlement_year int2 NULL,
	settlement_month int2 NULL,
	orders_count int4 NULL,
	orders_total_sum numeric(14, 2) NULL,
	rate_avg numeric(3, 2) NULL,
	order_processing_fee numeric(14, 2) NULL,
	courier_order_sum numeric(14, 2) NULL,
	courier_tips_sum numeric(14, 2) NULL,
	courier_reward_sum numeric(14, 2) NULL,
	CONSTRAINT dm_courier_ledger_pk PRIMARY KEY (id)
);