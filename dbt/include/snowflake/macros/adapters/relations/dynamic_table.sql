{% macro snowflake__get_alter_dynamic_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) -%}
    {{- log('Applying ALTER to: ' ~ relation) -}}
    {{- snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -}}
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


{% macro snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -%}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{ snowflake__get_drop_dynamic_table_sql(existing_relation) }};
    {{ snowflake__get_create_dynamic_table_as_sql(relation, sql) }}
{%- endmacro %}


{% macro snowflake__refresh_dynamic_table(relation) -%}
    {{- log('Applying REFRESH to: ' ~ relation) -}}

    alter dynamic table {{ relation }} refresh
{%- endmacro %}


{% macro snowflake__get_dynamic_table_configuration_changes(relation, new_config) -%}
    {{- log('Determining configuration changes on: ' ~ relation) -}}
    {%- do return(None) -%}
{%- endmacro %}


{% macro snowflake__get_drop_dynamic_table_sql(relation) %}
    drop dynamic table if exists {{ relation }}
{% endmacro %}
