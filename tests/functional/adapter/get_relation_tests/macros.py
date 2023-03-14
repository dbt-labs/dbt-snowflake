# wrap `adapter.get_relation()` in a macro
GET_RELATION = """
{% macro get_relation() %}
    {% set relation = adapter.get_relation(
        database=target.database,
        schema="DBT_CORE_ISSUE_7024",
        identifier="FACT"
    ) %}

    {{ return(relation) }}
{% endmacro %}
"""

# mirrors dbt_utils._is_relation: https://github.com/dbt-labs/dbt-utils/blob/main/macros/jinja_helpers/_is_relation.sql
# instead of throwing a compiler error, return the value of the check in the if statement
IS_RELATION = """
{% macro is_relation(obj) %}
    {% set if_condition = (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) %}
    {{ return(if_condition) }}
{% endmacro %}
"""
# same as above, but throws exception
ASSERT_RELATION = """
{% macro is_relation(obj) %}
    {% set a_relation = (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) %}
    {% if not a_relation %}
        {% do exceptions.raise_compiler_error("Macro expected a Relation but received the value: " ~ obj) %}
    {% endif %}
    {{ return(a_relation) }}
{% endmacro %}
"""


# combines the above two macros, but keeps the exception
CHECK_GET_RELATION_IS_RELATION = """
{% macro check_get_relation_is_relation(database, schema, identifier) %}
    {% set relation = get_relation() %}
    {{ return(is_relation(relation)) }}
{% endmacro %}
"""
