{%- macro dbt_metric_utils_materialize(metrics, dimensions, group_by, limit, time_start, time_end, where, order_by) -%}
    {% if execute %}
        {%- set lookup_key -%}
            metrics={{metrics}},dimensions={{dimensions}},group_by={{group_by}},limit={{limit}},time_start={{time_start}},time_end={{time_end}},where={{where_constraint}},order_by={{order_by}}
        {%- endset -%}

        {{ log("Lookup key: " ~ lookup_key) }}

        {%- set output -%}
            {{ var(lookup_key, None) }}
        {%- endset -%}

        {{ log("Output: " ~ output) }}
        
        {%- if output is none -%}
            {%- set error_message -%}
                Could not find a metric query for '{{ lookup_key }}'
            {%- endset -%}
            {{ exceptions.raise_compiler_error(error_message) }}
        {%- endif -%}

        ( {{- output -}} )
    {% else %}
        1
    {% endif %}
{%- endmacro -%}