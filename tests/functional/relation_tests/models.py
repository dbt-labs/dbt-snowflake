SEED = """
id,value
1,100
2,200
3,300
""".strip()


TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""
