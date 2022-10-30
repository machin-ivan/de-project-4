import airflow
from datetime import datetime
from airflow import DAG
from helper_operators import ResponseOperator, LoadOperator


DAG_ID = 'stg_load_dag'

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2022, 1, 1),
    schedule='0 0 * * *'
) as dag:
    get_restaurants = ResponseOperator(task_id = 'get_restaurants',
                                    table='restaurants',
                                    sort_field = 'id',
                                    sort_direction = 'asc',
                                    limit = 50,
                                    offset = 0)
    
    get_restaurants.execute()

    load_restaurants = LoadOperator(task_id = 'load_stg_restaurants',
                                    lines = get_restaurants.response,
                                    columns = '(restaurant_id, restaurant_name)',
                                    table_name = 'stg.restaurants')

    load_restaurants.execute()


    get_couriers = ResponseOperator(task_id = 'get_couriers',
                                    table='couriers',
                                    sort_field = 'id',
                                    sort_direction = 'asc',
                                    limit = 50,
                                    offset = 0)
    
    get_couriers.execute()

    load_couriers = LoadOperator(task_id = 'load_stg_couriers',
                                lines = get_couriers.response,
                                columns = '(courier_id, courier_name)',
                                table_name = 'stg.couriers')

    load_couriers.execute()


    get_deliveries = ResponseOperator(task_id = 'get_deliveries',
                                    table='deliveries',
                                    sort_field = 'id',
                                    sort_direction = 'asc',
                                    limit = 50,
                                    offset = 0)
    
    get_deliveries.execute()

    load_deliveries = LoadOperator(task_id = 'load_stg_deliveries',
                                lines = get_deliveries.response,
                                columns = '(order_id, order_ts, delivery_id, courier_id, ' \
                                    'address, delivery_ts, rate, order_sum, order_tip_sum)',
                                table_name = 'stg.deliveries')

    load_deliveries.execute()
    
    # [get_restaurants, get_couriers, get_deliveries] >> [load_restaurants, load_couriers, load_deliveries]