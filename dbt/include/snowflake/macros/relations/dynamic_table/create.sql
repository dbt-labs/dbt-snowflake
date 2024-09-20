{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

{%- set dynamic_table = relation.from_config(config.model) -%}

create dynamic table {{ relation }}
    target_lag = '{{ dynamic_table.target_lag }}'
    warehouse = {{ dynamic_table.snowflake_warehouse }}
    {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
    {{ optional('initialize', dynamic_table.initialize) }}
    as (
        {{ sql }}
    )

{%- endmacro %}
