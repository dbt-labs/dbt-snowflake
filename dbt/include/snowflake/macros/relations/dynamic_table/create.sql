{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    create dynamic table {{ relation }}
        target_lag = '{{ config.get("target_lag") }}'
        warehouse = {{ config.get("snowflake_warehouse") }}
        {% if config.get("refresh_mode") %}
            refresh_mode = '{{ config.get("refresh_mode") }}'
        {% endif %}
        {% if config.get("initialize") %}
            initialize = '{{ config.get("initialize") }}'
        {% endif %}
        {% if config.get("comment") %}
            comment = '{{ config.get("comment") }}'
        {% endif %}
        as (
            {{ sql }}
        )
    ;

{%- endmacro %}
