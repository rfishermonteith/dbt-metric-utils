SELECT * FROM {{ 
    dbt_metric_utils_materialize(
        metrics=['count_lifetime_orders', 'lifetime_spend_pretax', 'average_order_value'],
        group_by=['customer']
    ) 
}}