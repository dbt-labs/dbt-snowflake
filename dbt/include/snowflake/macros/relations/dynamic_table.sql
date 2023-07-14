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
        `dbt/adapters/snowflake/relation/models/dynamic_table.py`

    Used in:
        `dbt/include/snowflake/macros/materializations/dynamic_table.sql`
    Uses:
        `dbt/adapters/snowflake/relation/`
        `dbt/adapters/snowflake/impl.py`
*/ -#}


{% macro snowflake__alter_dynamic_table_template(existing_dynamic_table, target_dynamic_table) -%}
    {{- log('Applying ALTER to: ' ~ existing_dynamic_table.fully_qualified_path) -}}

    {#- /*
        We need to get the config changeset to determine if we require a full refresh (happens if any change
        in the changeset requires a full refresh or if an unmonitored change was detected)
        or if we can get away with altering the dynamic table in place.
    */ -#}

    {%- if target_dynamic_table == existing_dynamic_table -%}
        {{- exceptions.warn("No changes were identified for: " ~ existing_dynamic_table) -}}

    {%- else -%}
        {% set _changeset = adapter.make_changeset(existing_dynamic_table, target_dynamic_table) %}

        {% if _changeset.requires_full_refresh %}
            {{ replace_template(existing_dynamic_table, target_dynamic_table) }}

        {% else %}

            {%- set target_lag = _changeset.target_lag -%}
            {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_dynamic_table.fully_qualified_path) -}}{%- endif -%}
            {%- set warehouse = _changeset.warehouse -%}
            {%- if warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_dynamic_table.fully_qualified_path) -}}{%- endif -%}

            alter dynamic table {{ existing_dynamic_table.fully_qualified_path }} set
                {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
                {% if warehouse %}warehouse = {{ warehouse.context }}{% endif %}

        {%- endif -%}
    {%- endif -%}

{%- endmacro %}


{% macro snowflake__create_dynamic_table_template(dynamic_table) -%}

    create or replace dynamic table {{ dynamic_table.fully_qualified_path }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.warehouse }}
        as (
            {{ dynamic_table.query }}
        )
    ;
    {{ refresh_template(dynamic_table) }}

{%- endmacro %}


{% macro snowflake__describe_dynamic_table_template(dynamic_table) %}

    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ dynamic_table.name }}'
            in schema {{ dynamic_table.schema.fully_qualified_path }}
        ;
        select
            "name",
            "schema_name",
            "database_name",
            "text" as "query",
            "target_lag",
            "warehouse"
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set _dynamic_table = run_query(_dynamic_table_sql) %}

    {% do return({'relation': _dynamic_table}) %}

{% endmacro %}


{% macro snowflake__drop_dynamic_table_template(dynamic_table) %}
    {{- log('Applying DROP to: ' ~ dynamic_table.fully_qualified_path) -}}
    drop dynamic table if exists {{ dynamic_table.fully_qualified_path }} cascade
{% endmacro %}


{% macro snowflake__refresh_dynamic_table_template(dynamic_table) -%}
    {{- log('Applying REFRESH to: ' ~ dynamic_table.fully_qualified_path) -}}
    alter dynamic table {{ dynamic_table.fully_qualified_path }} refresh
{%- endmacro %}


{% macro snowflake__rename_dynamic_table_template(dynamic_table, new_name) -%}
    {{- exceptions.raise_compiler_error(
        "Snowflake does not support the renaming of dynamic tables. This macro was called by: " ~ dynamic_table
    ) -}}
{%- endmacro %}
