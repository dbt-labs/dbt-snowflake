MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


MY_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='120        seconds',
) }}
select * from {{ ref('my_seed') }}
"""


MACRO__LAST_REFRESH = """
{% macro snowflake__test__last_refresh(schema, identifier) %}
    {% set _sql %}
    select max(refresh_start_time) as last_refresh
    from table(information_schema.dynamic_table_refresh_history())
    where schema_name = '{{ schema }}'
    and name = '{{ identifier }}'
    {% endset %}
    {{ return(run_query(_sql)) }}
{% endmacro %}
"""
