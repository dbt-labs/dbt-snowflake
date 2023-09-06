{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}
    {{- log('Applying CREATE to: ' ~ relation) -}}

    create or replace dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        as (
            {{ sql }}
        )
    ;
    {{ snowflake__refresh_dynamic_table(relation) }}

{%- endmacro %}
