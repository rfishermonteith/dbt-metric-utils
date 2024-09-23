{%- macro dbt_metric_utils_encode_materialize_lookup_key(metrics, dimensions, group_by, limit, time_start, time_end, where, order_by) -%}

metrics={{ metrics }},dimensions={{ dimensions }},group_by={{ group_by }},limit={{ limit }},time_start={{ time_start }},time_end={{ time_end }},where={{ where }},order_by={{ order_by }}

{%- endmacro -%}


{%- macro dbt_metric_utils_materialize(metrics, dimensions, group_by, limit, time_start, time_end, where, order_by) -%}

{%- set lookup_key -%}
    {{ dbt_metric_utils_encode_materialize_lookup_key(metrics, dimensions, group_by, limit, time_start, time_end, where, order_by) }}
{%- endset -%}

{# `execute` will be truthy during dbt compile and run. At parse time, we can return a default hardcoded value. #}
{%- if execute -%}
    {%- set output -%}
        {{ var(lookup_key, 'no_query') }}
    {%- endset -%}

    {{- log("Lookup key: " ~ lookup_key) -}}
    {{- log("Output: " ~ output) -}}
    
    {%- if output == 'no_query' -%}
        {%- set error_message -%}
            Could not find a metric query for '{{ lookup_key }}'
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    ({{- output -}})
{%- else -%}
    {{- lookup_key -}}
{%- endif -%}

{%- endmacro -%}