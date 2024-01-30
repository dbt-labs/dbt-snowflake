{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    create dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        refresh_mode = '{{ config.get("refresh_mode") }}'
        initialize = '{{ config.get("initialize") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        {% if config.get("comment") %}
            comment = '{{ config.get("comment") }}'
        {% endif %}
        as (
            {{ sql }}
        )
    ;

{%- endmacro %}
