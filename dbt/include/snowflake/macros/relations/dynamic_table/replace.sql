{% macro snowflake__get_replace_dynamic_table_sql(relation, sql) -%}

    {%- set dynamic_table = relation.from_config(config.model) -%}

    create or replace dynamic table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {% if dynamic_table.refresh_mode %}
        refresh_mode = {{ dynamic_table.refresh_mode }}
        {% endif %}
        {% if dynamic_table.initialize %}
        initialize = {{ dynamic_table.initialize }}
        {% endif %}
        as (
            {{ sql }}
        )

{%- endmacro %}
