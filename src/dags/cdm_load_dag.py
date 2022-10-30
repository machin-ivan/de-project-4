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
        insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month,
        orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,
        courier_tips_sum, courier_reward_sum)
        select courier_id
            ,courier_name
            ,settlement_year
            ,settlement_month
            ,orders_count
            ,orders_total_sum
            ,rate_avg
            ,order_processing_fee
            ,sum(courier_order_payment) courier_order_sum
            ,max(courier_tips_sum) courier_tips_sum
            ,(sum(courier_order_payment) + max(courier_tips_sum)) * 0.95 courier_reward_sum
        from (with cte as
            (select d.courier_id courier_id
                ,c.courier_name courier_name
                ,extract(year from order_ts) settlement_year
                ,extract(month from order_ts) settlement_month
                ,count(o.*) orders_count
                ,sum(o.order_sum) orders_total_sum
                ,avg(rate::float) rate_avg
                ,sum(o.order_sum)*0.25 order_processing_fee
                ,sum(o.order_tip_sum) courier_tips_sum
            from dds.orders o 
            join dds.deliveries d on o.delivery_id = d.id 
            join dds.couriers c on d.courier_id = c.id
            group by d.courier_id, c.courier_name ,extract(year from order_ts), extract(month from order_ts))
        select cte.*
            ,case 
                when cte.rate_avg < 4 then case
                                            when o2.order_sum * 0.05 <= 100 then 100
                                            else o2.order_sum * 0.05
                                        end
                when cte.rate_avg >= 4 and cte.rate_avg < 4.5 then case
                                                                    when o2.order_sum * 0.07 <= 150 then 150
                                                                    else o2.order_sum * 0.07
                                                                end
                when cte.rate_avg >= 4.5 and cte.rate_avg < 4.9 then case
                                                                    when o2.order_sum * 0.08 <= 175 then 175
                                                                    else o2.order_sum * 0.08
                                                                end
                else case
                        when o2.order_sum * 0.1 <= 200 then 200
                        else o2.order_sum * 0.1
                    end
            end courier_order_payment
        from cte
        left join dds.deliveries d on cte.courier_id = d.courier_id
        join dds.orders o2 on d.id = o2.delivery_id
        order by cte.courier_id) t
        group by courier_id, courier_name, settlement_year, settlement_month, orders_count, 
            orders_total_sum, rate_avg, order_processing_fee;
    '''
    load_cdm_restaurants = MigrateOperator(task_id='load_dds_restaurants',
                                            sql=query)
    load_cdm_restaurants.execute()
