from datetime import datetime
from airflow import DAG
from helper_operators import MigrateOperator


DAG_ID = 'dds_load_dag'

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2022, 1, 1),
    schedule='0 0 * * *'
) as dag:
    query = '''
        INSERT INTO dds.restaurants (restaurant_id, restaurant_name)
        SELECT restaurant_id, restaurant_name
        FROM stg.restaurants;
    '''
    load_dds_restaurants = MigrateOperator(task_id='load_dds_restaurants',
                                            sql=query)
    load_dds_restaurants.execute()

    query = '''
        INSERT INTO dds.couriers (courier_id, courier_name)
        SELECT courier_id, courier_name
        FROM stg.couriers;
    '''
    load_dds_couriers = MigrateOperator(task_id='load_dds_couriers',
                                            sql=query)
    load_dds_couriers.execute()
    
    query = '''
        INSERT INTO dds.deliveries (delivery_id, courier_id, address, delivery_ts)
        SELECT delivery_id, c.id, address, delivery_ts
        FROM stg.deliveries d
        JOIN dds.couriers c using(courier_id);
    '''
    load_dds_deliveries = MigrateOperator(task_id='load_dds_deliveries',
                                            sql=query)
    load_dds_deliveries.execute()

    query = '''
        INSERT INTO dds.orders (order_id, order_ts, delivery_id, rate, order_sum, order_tip_sum)
        SELECT order_id, order_ts, dd.id, rate, order_sum, order_tip_sum
        FROM stg.deliveries d
        JOIN dds.deliveries dd using(delivery_id)
    '''
    load_dds_orders = MigrateOperator(task_id='load_dds_orders',
                                            sql=query)
    load_dds_orders.execute()