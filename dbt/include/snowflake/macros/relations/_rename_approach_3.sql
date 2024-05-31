/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

Implement at the `dbt-snowflake` level by overriding `default__get_rename_sql()`:
- only affects `dbt-snowflake`
- removes support for fully qualified names
- still supports relations
- still supports moving relations to another database and/or schema
    however this requires using a relation instead of a fully qualified name
*/

------------------------------------------ dbt-adapters ------------------------------------------
{%- macro get_rename_sql(relation, new_name) -%}
    /*
    Args:
        relation: BaseRelation - relation to be renamed
        new_name: str - new identifier for `relation` (intended usage)
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
------------ CHANGE: added the entire macro below ------------
{%- macro snowflake__get_rename_sql(relation, new_name) -%}
    {% set new_name = relation.incorporate(path={"identifier": new_name}).render() if new_name is string else new_name.render() %}
    {{ default__get_rename_sql(relation, new_name) }}
{%- endmacro -%}


{%- macro snowflake__get_rename_view_sql(relation, new_name) -%}
    /*
    Args:
        relation: BaseRelation - relation to be renamed
        new_name: str - new identifier for `relation` (intended usage)
    Returns: templated string
    */
    alter view {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
