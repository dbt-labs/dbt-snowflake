{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    create dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        as (
            {{ sql }}
        )
    ;

{%- endmacro %}
