{% macro snowflake__get_replace_dynamic_table_as_sql(target_relation, sql, existing_relation, backup_relation, intermediate_relation) -%}
    {{- log('Applying REPLACE to: ' ~ target_relation) -}}
    {{ snowflake__get_drop_dynamic_table_sql(existing_relation) }};
    {{ snowflake__get_create_dynamic_table_as_sql(target_relation, sql) }}
{%- endmacro %}
