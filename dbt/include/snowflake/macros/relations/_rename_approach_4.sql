/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

Implement at the `dbt-snowflake` level by overriding `snowflake__get_create_backup_sql()` and `snowflake__get_rename_intermediate_sql()`:
- only affects `dbt-snowflake`
- only affects backup/rename, which is where we're seeing the issue
- still supports fully qualified names
- still supports relations
- still supports moving relations to another database and/or schema
- rename macro behavior matches Snowflake rename behavior
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
{%- macro snowflake__get_create_backup_sql(relation) -%}

    -- get the standard backup name
    {% set backup_relation = make_backup_relation(relation, relation.type) %}

    -- drop any pre-existing backup
    {{ get_drop_sql(backup_relation) }};

    {{ get_rename_sql(relation, backup_relation.render()) }}

{%- endmacro -%}


------------ CHANGE: added the entire macro below ------------
{%- macro snowflake__get_rename_intermediate_sql(relation) -%}

    -- get the standard intermediate name
    {% set intermediate_relation = make_intermediate_relation(relation) %}

    {{ get_rename_sql(intermediate_relation, relation.render()) }}

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
