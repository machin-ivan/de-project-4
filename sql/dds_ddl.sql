CREATE TABLE dds.restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar(30) NULL,
	restaurant_name varchar(50) NULL,
	CONSTRAINT restaurants_pk PRIMARY KEY (id)
);

CREATE TABLE dds.couriers (
	id serial4 NOT NULL,
	courier_id varchar(30) NULL,
	courier_name varchar(50) NULL,
	CONSTRAINT couriers_pk PRIMARY KEY (id)
);

CREATE TABLE dds.deliveries (
	id serial4 NOT NULL,
	delivery_id varchar(30) NULL,
	courier_id int4 NULL,
	address text NULL,
	delivery_ts timestamp NULL,
	CONSTRAINT deliveries_pk PRIMARY KEY (id)
);

CREATE TABLE dds.orders (
	id serial4 NOT NULL,
	order_id varchar(30) NULL,
	order_ts timestamp NULL,
	delivery_id int4 NULL,
	rate int2 NULL,
	order_sum numeric(14, 2) NULL,
	order_tip_sum numeric(14, 2) NULL,
	CONSTRAINT orders_pk PRIMARY KEY (id)
);

ALTER TABLE dds.orders ADD CONSTRAINT orders_fk FOREIGN KEY (delivery_id) REFERENCES dds.deliveries(id);
ALTER TABLE dds.deliveries ADD CONSTRAINT deliveries_fk FOREIGN KEY (courier_id) REFERENCES dds.couriers(id);
