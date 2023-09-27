{% macro snowflake__get_replace_dynamic_table_sql(relation, sql) %}

    create or replace dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        as (
            {{ sql }}
        )
    ;
    {{ snowflake__refresh_dynamic_table(relation) }}

{% endmacro %}
