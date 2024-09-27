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


DYNAMIC_ICEBERG_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""

ICEBERG_TABLE = """
{{ config(
    materialized='table',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
) }}
select * from {{ ref('my_seed') }}
"""

INCREMENTAL_ICEBERG_TABLE = """
{{ config(
    materialized='incremental',
    table_format='iceberg',
    incremental_strategy='append',
    unique_key="id",
    external_volume = "s3_iceberg_snow",
) }}
select * from {{ ref('my_seed') }}
"""


INCREMENTAL_TABLE = """
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key="id",
) }}
select * from {{ ref('my_seed') }}
"""
