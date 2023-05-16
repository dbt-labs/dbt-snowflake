{%- macro snowflake__get_alter_dynamic_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) -%}
    {{- snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -}}
{%- endmacro -%}


{%- macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}
    {{- get_create_table_as_sql(False, relation, sql) -}}
{%- endmacro -%}


{%- macro snowflake__get_replace_dynamic_table_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) -%}
    {{- drop_relation_if_exists(existing_relation) -}}
    {{- get_create_table_as_sql(False, relation, sql) -}}
{%- endmacro -%}


{%- macro snowflake__refresh_dynamic_table(relation) -%}
    {{- '' -}}
{%- endmacro -%}


{%- macro snowflake__get_dynamic_table_configuration_changes(relation, new_config) -%}
    {%- do return({}) -%}
{%- endmacro -%}
