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
    warehouse='DBT_TESTING',
    target_lag='60 seconds',
) }}
select * from {{ ref('my_seed') }}
"""
