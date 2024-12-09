SEED = """
id,value
1,100
2,200
3,300
""".strip()


DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


EXPLICIT_AUTO_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='AUTO',
) }}
select * from {{ ref('my_seed') }}
"""

IMPLICIT_AUTO_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_DOWNSTREAM = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='DOWNSTREAM',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='5 minutes',
    refresh_mode='INCREMENTAL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE_REPLACE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='FULL',
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE_ALTER = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='5 minutes',
    refresh_mode='INCREMENTAL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_ICEBERG_TABLE_REPLACE = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='2 minutes',
    refresh_mode='FULL',
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
) }}
select * from {{ ref('my_seed') }}
"""
