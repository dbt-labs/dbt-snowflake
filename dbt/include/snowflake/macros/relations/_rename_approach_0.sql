/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

Docstrings have been added to communicate _intended_ usage. They do not cover all supported usage however, as is the issue we're facing here.

This represents the current logic flow for views (tables would be the same):
- supports fully qualified names as strings
- supports relations
- supports moving relations to another database/schema
- unintentionally moves `relation` for `dbt-snowflake` if a `use schema` or `use database` statement runs first
    and `new_name` is an identifier, e.g. when calling `get_create_backup_sql()`
*/

------------------------------------------ dbt-adapters ------------------------------------------
{%- macro get_rename_sql(relation, new_name) -%}
    /*
    Args:
        relation: BaseRelation - relation to be renamed
        new_name: str - new identifier for `relation`
    Returns: templated string
    */
    {{- log('Applying RENAME to: ' ~ relation) -}}
    {{- adapter.dispatch('get_rename_sql', 'dbt')(relation, new_name) -}}
{%- endmacro -%}


{%- macro default__get_rename_sql(relation, new_name) -%}
    {%- if relation.is_view -%}
        {{ get_rename_view_sql(relation, new_name) }}

    {%- elif relation.is_table -%}
        {{ get_rename_table_sql(relation, new_name) }}

    {%- elif relation.is_materialized_view -%}
        {{ get_rename_materialized_view_sql(relation, new_name) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`get_rename_sql` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}


{% macro get_rename_view_sql(relation, new_name) %}
    {{- adapter.dispatch('get_rename_view_sql', 'dbt')(relation, new_name) -}}
{% endmacro %}


------------------------------------------ dbt-snowflake ------------------------------------------
{%- macro snowflake__get_rename_view_sql(relation, new_name) -%}
    /*
    Args:
        relation: BaseRelation - relation to be renamed
        new_name: str - new identifier for `relation`
    Returns: templated string
    */
    alter view {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
