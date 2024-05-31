/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

Implement at the `dbt-adapters` level in `get_rename_sql()`:
- affects all adapters, may require fixes for platforms that don't support renaming with a fully qualified name (unclear if this is a concern)
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
        new_name: Union[str, BaseRelation] - new identifier or BaseRelation instance for `relation`
    Returns: templated string
    */
    {{- log('Applying RENAME to: ' ~ relation) -}}
    ------------ CHANGE: added the line below ------------
    {% set new_name = relation.incorporate(path={"identifier": new_name}).render() if new_name is string else new_name.render() %}
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
        new_name: str - new fully qualified path, for `relation`
    Returns: templated string
    */
    alter view {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
