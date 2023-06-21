{% macro snowflake__alter_dynamic_table_sql(
    dynamic_table,
    configuration_changes,
    existing_relation,
    backup_relation,
    intermediate_relation
) -%}
    {{- log('Applying ALTER to: ' ~ dynamic_table.path) -}}

    {% if configuration_changes.requires_full_refresh %}

        {{ snowflake__replace_dynamic_table_sql(dynamic_table, existing_relation, backup_relation, intermediate_relation) }}

    -- otherwise apply individual changes as needed
    {% else %}

        {%- set target_lag = configuration_changes.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ dynamic_table.path) -}}{%- endif -%}
        {%- set warehouse = configuration_changes.warehouse -%}
        {%- if warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ dynamic_table.path) -}}{%- endif -%}

        alter dynamic table {{ dynamic_table.path }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if warehouse %}warehouse = {{ warehouse.context }}{% endif %}

    {%- endif -%}

{%- endmacro %}


{% macro snowflake__create_dynamic_table_sql(dynamic_table) -%}
    {{- log('Applying CREATE to: ' ~ dynamic_table.path) -}}

    create or replace dynamic table {{ dynamic_table.path }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.warehouse }}
        as ({{ dynamic_table.query }})
    ;
    alter dynamic table {{ dynamic_table.path }} refresh

{%- endmacro %}


{% macro snowflake__replace_dynamic_table_sql(dynamic_table, existing_relation, backup_relation, intermediate_relation) -%}
    {{- log('Applying REPLACE to: ' ~ dynamic_table.path) -}}

    {{ snowflake__drop_relation_sql(existing_relation) }};
    {{ snowflake__create_dynamic_table_sql(dynamic_table) }}
{%- endmacro %}


{% macro snowflake__refresh_dynamic_table_sql(dynamic_table) -%}
    {{- log('Applying REFRESH to: ' ~ dynamic_table.path) -}}

    alter dynamic table {{ dynamic_table.path }} refresh
{%- endmacro %}


{% macro snowflake__dynamic_table_configuration_changes(dynamic_table) -%}
    {{- log('Getting CHANGES on: ' ~ dynamic_table.path) -}}

    {% set existing_dynamic_table = snowflake__describe_dynamic_table(dynamic_table) %}
    {% set _configuration_changes = this.dynamic_table_config_changeset(dynamic_table, existing_dynamic_table) %}
    {% do return(_configuration_changes) %}
{%- endmacro %}


{% macro snowflake__drop_dynamic_table_sql(dynamic_table) %}
    {{- log('Applying DROP to: ' ~ dynamic_table.path) -}}

    drop dynamic table if exists {{ dynamic_table.path }}
{% endmacro %}


{% macro snowflake__describe_dynamic_table(dynamic_table) %}
    {{- log('Getting DESCRIBE on: ' ~ dynamic_table.path) -}}

    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ dynamic_table.name }}'
            in schema {{ dynamic_table.schema_path }}
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
