# wrap `adapter.get_relation()` in a macro
MACRO_GET_RELATION = """
{% macro get_relation(database, schema, identifier) %}
    {% set relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=identifier
    ) %}

    {{ return(relation) }}
{% endmacro %}
"""


# mirrors dbt_utils._is_relation: https://github.com/dbt-labs/dbt-utils/blob/main/macros/jinja_helpers/_is_relation.sql
# instead of throwing a compiler error, return the value of the check in the if statement
MACRO_IS_RELATION = """
{% macro is_relation(obj) %}
    {% set if_condition = (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) %}
    {{ return(if_condition) }}
{% endmacro %}
"""


# combines the above two macros, but keeps the exception
MACRO_CHECK_GET_RELATION_IS_RELATION = """
{% macro check_get_relation_is_relation(database, schema, identifier) %}

    {% set relation = get_relation(
          database=database,
          schema=schema,
          identifier=identifier
    ) %}

    {{ return(is_relation(relation)) }}

{% endmacro %}
"""
