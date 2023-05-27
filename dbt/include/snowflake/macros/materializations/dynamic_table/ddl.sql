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
    {{- get_create_view_as_sql(relation, sql) -}}
{%- endmacro %}


{% macro snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -%}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{ drop_relation(existing_relation) }}
    {{ snowflake__get_create_dynamic_table_as_sql(relation, sql) }}
{%- endmacro %}


{% macro snowflake__refresh_dynamic_table(relation) -%}
    {{- log('Applying REFRESH to: ' ~ relation) -}}
    {{ '' }}
{%- endmacro %}


{% macro snowflake__get_dynamic_table_configuration_changes(relation, new_config) -%}
    {{- log('Determining configuration changes on: ' ~ relation) -}}
    {%- set last_refresh = run_query(snowflake__get_dynamic_table_latest_refresh(relation)) -%}
    {%- set _refresh_strategy_updates = relation.get_refresh_strategy_updates(last_refresh, new_config) -%}

    {% set _configuration_changes = {} %}

    {%- if _refresh_strategy_updates -%}
        {%- set _dummy = _configuration_changes.update({"refresh_strategy": _refresh_strategy_updates}) -%}
    {%- endif -%}

    {%- do return(_configuration_changes) -%}

{%- endmacro %}


{% macro snowflake__get_dynamic_table_latest_refresh(relation) %}
    select top 1 *
    from table(information_schema.dynamic_table_refresh_history(name => {{ relation }}))
    order by refresh_version desc;
{% endmacro %}
