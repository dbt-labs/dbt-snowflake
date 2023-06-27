{#- /*
    This file contains DDL that gets consumed in `./materialization.sql`. These macros could be used elsewhere as they
    do not care that they are being called by `./materialization.sql`; but the original intention was to support
    the materialization of dynamic tables. These macros represent the basic interactions
    dbt-snowflake requires of dynamic tables in Snowflake:
        - ALTER
        - CREATE
        - DESCRIBE
        - DROP
        - REFRESH
        - REPLACE
    These macros all take a SnowflakeDynamicTableConfig instance as an input. This class can be found in:
        `dbt/adapters/snowflake/relation_configs/dynamic_table.py`

    Used in:
        `dbt/include/snowflake/macros/materializations/dynamic_table/materialization.sql`
    Uses:
        `dbt/adapters/snowflake/relation.py`
        `dbt/adapters/snowflake/relation_configs/`
*/ -#}


{% macro alter_dynamic_table_sql(new_dynamic_table, existing_dynamic_table) -%}
    {{- log('Applying ALTER to: ' ~ new_dynamic_table.fully_qualified_path) -}}

    {#- /*
        We need to get the config changeset to determine if we require a full refresh (happens if any change
        in the changeset requires a full refresh or if an unmonitored change was detected)
        or if we can get away with altering the dynamic table in place.
    */ -#}
    {% set config_changeset = adapter.Relation.dynamic_table_config_changeset(new_dynamic_table, existing_dynamic_table) %}

    {% if config_changeset.requires_full_refresh %}

        {{ replace_dynamic_table_sql(new_dynamic_table) }}

    {% else %}

        {%- set target_lag = config_changeset.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ new_dynamic_table.fully_qualified_path) -}}{%- endif -%}
        {%- set warehouse = config_changeset.warehouse -%}
        {%- if warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ new_dynamic_table.fully_qualified_path) -}}{%- endif -%}

        alter dynamic table {{ new_dynamic_table.fully_qualified_path }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if warehouse %}warehouse = {{ warehouse.context }}{% endif %}

    {%- endif -%}

{%- endmacro %}


{% macro create_dynamic_table_sql(dynamic_table) -%}
    {{- log('Applying CREATE to: ' ~ dynamic_table.fully_qualified_path) -}}

    create or replace dynamic table {{ dynamic_table.fully_qualified_path }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.warehouse }}
        as (
            {{ dynamic_table.query }}
        )
    ;
    {{ refresh_dynamic_table_sql(dynamic_table) }}

{%- endmacro %}


{% macro describe_dynamic_table(dynamic_table) %}
    {{- log('Getting DESCRIBE on: ' ~ dynamic_table.fully_qualified_path) -}}

    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ dynamic_table.name }}'
            in schema {{ dynamic_table.schema.fully_qualified_path }}
        ;
        select
            "name",
            "schema_name",
            "database_name",
            "text",
            "target_lag",
            "warehouse"
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set _dynamic_table = run_query(_dynamic_table_sql) %}

    {% do return({'dynamic_table': _dynamic_table}) %}

{% endmacro %}


{% macro drop_dynamic_table_sql(dynamic_table) %}
    {{- log('Applying DROP to: ' ~ dynamic_table.fully_qualified_path) -}}

    drop dynamic table if exists {{ dynamic_table.fully_qualified_path }}
{% endmacro %}


{% macro refresh_dynamic_table_sql(dynamic_table) -%}
    {{- log('Applying REFRESH to: ' ~ dynamic_table.fully_qualified_path) -}}

    alter dynamic table {{ dynamic_table.fully_qualified_path }} refresh
{%- endmacro %}


{% macro replace_dynamic_table_sql(new_dynamic_table, existing_relation) -%}
    {{- log('Applying REPLACE to: ' ~ new_dynamic_table.fully_qualified_path) -}}

    {{ snowflake__drop_relation_sql(existing_relation) }};
    {{ create_dynamic_table_sql(new_dynamic_table) }}
{%- endmacro %}
