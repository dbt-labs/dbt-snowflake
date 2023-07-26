{% macro snowflake__get_alter_dynamic_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) -%}
    {{- log('Applying ALTER to: ' ~ relation) -}}

    {% if _changeset.requires_full_refresh %}
        {{- snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -}}

    {% else %}

        {%- set target_lag = configuration_changes.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set warehouse = configuration_changes.warehouse -%}
        {%- if warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}

        alter dynamic table {{ existing_relation }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if warehouse %}warehouse = {{ warehouse.context }}{% endif %}

    {%- endif -%}

{%- endmacro %}


{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}
    {{- log('Applying CREATE to: ' ~ relation) -}}

    create or replace dynamic table {{ relation }}
        lag = '{{ config.get("target_lag") }}'
        warehouse = {{ config.get("warehouse") }}
        as (
            {{ sql }}
        )
    ;
    {{ snowflake__refresh_dynamic_table(relation) }}

{%- endmacro %}


{% macro snowflake__describe_dynamic_table(relation) %}
    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ relation.identifier }}'
            in schema {{ relation.database }}.{{ relation.schema }}
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

    {% do return({'dynamic_table': _dynamic_table}) %}
{% endmacro %}


{% macro snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -%}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{ snowflake__get_drop_dynamic_table_sql(existing_relation) }};
    {{ snowflake__get_create_dynamic_table_as_sql(relation, sql) }}
{%- endmacro %}


{% macro snowflake__refresh_dynamic_table(relation) -%}
    {{- log('Applying REFRESH to: ' ~ relation) -}}

    alter dynamic table {{ relation }} refresh
{%- endmacro %}


{% macro snowflake__get_dynamic_table_configuration_changes(existing_relation, new_config) -%}
    {% set _existing_dynamic_table = snowflake__describe_dynamic_table(existing_relation) %}
    {% set _configuration_changes = existing_relation.dynamic_table_config_changeset(_existing_dynamic_table, new_config) %}
    {% do return(_configuration_changes) %}
{%- endmacro %}


{% macro snowflake__get_drop_dynamic_table_sql(relation) %}
    drop dynamic table if exists {{ relation }}
{% endmacro %}
