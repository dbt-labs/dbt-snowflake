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
{% macro is_relation(obj, macro) %}
    {% set if_condition = (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) %}
    {{ return(if_condition) }}
{% endmacro %}
"""


# combines the above two macros, but keeps the exception
MACRO_CHECK_GET_RELATION_IS_RELATION = """
{% macro check_get_relation_is_relation() %}

    {% set my_relation = adapter.get_relation(
          database=target.database,
          schema=target.schema,
          identifier="FACT"
    ) %}

    {% if not (my_relation is mapping and my_relation.get('metadata', {}).get('type', '').endswith('Relation')) %}
        {% do exceptions.raise_compiler_error("Log: " ~ my_relation) %}
    {% endif %}

    {{ return(my_relation) }}

{% endmacro %}
"""


# test whether the relation is found using `get_relation`
# either way return all the pieces to the log by selecting them in the if block
TEST_GET_RELATION_FACT = """
{% set relation = get_relation(
    database=target.database,
    schema=target.schema,
    identifier='FACT'
) -%}

{% if relation is none %}
    select '{{ relation.path.database }}' as actual_database,
           '{{ relation.path.schema }}' as actual_schema,
           '{{ relation.path.identifier }}' as actual_table,
           '{{ target.database }}' as expected_database,
           '{{ target.schema }}' as expected_schema,
           'fact' as expected_table,
           {{ is_relation(relation) }} as is_relation_results
{% else %}
    select 1 where 1 = 0
{% endif %}
"""
TEST_GET_RELATION_AD_HOC = """
{% set relation = get_relation(
    database=target.database,
    schema=target.schema,
    identifier='AD_HOC'
) %}

{%- if relation is none -%}
    select '{{ relation.path.database }}' as actual_database,
           '{{ relation.path.schema }}' as actual_schema,
           '{{ relation.path.identifier }}' as actual_table,
           '{{ target.database }}' as expected_database,
           '{{ target.schema }}' as expected_schema,
           'fact' as expected_table,
           {{ is_relation(relation) }} as is_relation_results
{% else %}
    select 1 where 1 = 0
{% endif %}
"""


# test whether the relation is found using `is_relation`
# either way return all the pieces to the log by selecting them in the if block
TEST_IS_RELATION_FACT = """
{% set relation = adapter.Relation.create(
    database=target.database,
    schema=target.schema,
    identifier='FACT'
) %}

{%- if not is_relation(relation) -%}
    select '{{ relation.path.database }}' as actual_database,
           '{{ relation.path.schema }}' as actual_schema,
           '{{ relation.path.identifier }}' as actual_table,
           '{{ target.database }}' as expected_database,
           '{{ target.schema }}' as expected_schema,
           'fact' as expected_table,
           {{ is_relation(relation) }} as is_relation_results
{% else %}
    select 1 where 1 = 0
{% endif %}
"""
TEST_IS_RELATION_AD_HOC = """
{% set relation = adapter.Relation.create(
    database=target.database,
    schema=target.schema,
    identifier='AD_HOC'
) %}

{%- if not is_relation(relation) -%}
    select '{{ relation.path.database }}' as actual_database,
           '{{ relation.path.schema }}' as actual_schema,
           '{{ relation.path.identifier }}' as actual_table,
           '{{ target.database }}' as expected_database,
           '{{ target.schema }}' as expected_schema,
           'fact' as expected_table,
           {{ is_relation(relation) }} as is_relation_results
{% else %}
    select 1 where 1 = 0
{% endif %}
"""
