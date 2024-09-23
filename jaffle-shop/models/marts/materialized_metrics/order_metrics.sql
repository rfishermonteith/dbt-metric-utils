SELECT * FROM {{ 
    dbt_metric_utils_materialize(
        metrics=['orders', 'new_customer_orders', 'order_total', 'food_orders', 'drink_orders'],
        group_by=['metric_time__day']
    ) 
}}
