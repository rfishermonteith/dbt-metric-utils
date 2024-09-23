SELECT * FROM {{ 
    dbt_metric_utils_materialize(
        metrics=['revenue', 'food_revenue', 'drink_revenue'],
        group_by=['metric_time__day']
    ) 
}}

