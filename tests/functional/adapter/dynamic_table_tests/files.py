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
    target_lag='2        minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""

MY_DYNAMIC_TABLE_WITH_DYNAMIC_WAREHOUSE = """
{{ config(
    materialized = 'dynamic_table',
    snowflake_warehouse = get_warehouse('xsmall'),
    target_lag = 'downstream',
    on_configuration_change = 'apply',
) }}

select * from {{ ref('my_seed') }}

"""

GET_WAREHOUSE_MACRO = """
{% macro get_warehouse(size) %}
    {% set warehouses = {
        'xsmall': 'DBT_TESTING',
        'small': 'DBT_TESTING',
        'medium': 'DBT_TESTING',
        'large': 'DBT_TESTING',
    } %}
    {{ return(warehouses[size]) }}
{% endmacro %}
"""
