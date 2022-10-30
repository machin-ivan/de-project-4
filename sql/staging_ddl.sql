CREATE TABLE stg.restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar(30) NULL,
	restaurant_name varchar(50) NULL,
	CONSTRAINT restaurants_pk PRIMARY KEY (id)
);

CREATE TABLE stg.couriers (
	id serial4 NOT NULL,
	courier_id varchar(30) NULL,
	courier_name varchar(50) NULL,
	CONSTRAINT couriers_pk PRIMARY KEY (id)
);

CREATE TABLE stg.deliveries (
	id serial4 NOT NULL,
	order_id varchar(30) NULL,
	order_ts timestamp NULL,
	delivery_id varchar(30) NULL,
	courier_id varchar(30) NULL,
	address text NULL,
	delivery_ts timestamp NULL,
	rate smallint NULL,
	order_sum numeric(14,2) NULL,
	order_tip_sum numeric(14,2) NULL,
	CONSTRAINT deliveries_pk PRIMARY KEY (id)
);
