{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    create dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        refresh_mode '{{ config.get("refresh_mode") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        as (
            {{ sql }}
        )

{%- endmacro %}
