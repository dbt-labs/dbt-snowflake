/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

Implement at the `dbt-snowflake` level in `snowflake__get_rename_x_sql()`:
- only affects `dbt-snowflake`
- removes support for fully qualified names
- removes support for relations
- removes support for moving relations to another database and/or schema
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
    alter view {{ relation }} rename to {{ relation.incorporate(path={"identifier": new_name}) }}
{%- endmacro -%}
