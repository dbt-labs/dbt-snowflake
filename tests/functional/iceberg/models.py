_MODEL_BASIC_TABLE_MODEL = """
{{
  config(
    materialized = "table",
    cluster_by=['id'],
  )
}}
select 1 as id
"""

_MODEL_BASIC_ICEBERG_MODEL = """
{{
  config(
    transient = "true",
    materialized = "table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
  )
}}

select * from {{ ref('first_table') }}
"""

_MODEL_BASIC_ICEBERG_MODEL_WITH_PATH = """
{{
  config(
    transient = "true",
    materialized = "table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_root="root_path",
  )
}}

select * from {{ ref('first_table') }}
"""

_MODEL_BASIC_ICEBERG_MODEL_WITH_PATH_SUBPATH = """
{{
  config(
    transient = "true",
    materialized = "table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_root="root_path",
    base_location_subpath="subpath",
  )
}}

select * from {{ ref('first_table') }}
"""

_MODEL_BASIC_DYNAMIC_TABLE_MODEL = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
    table_format='iceberg',
    external_volume='s3_iceberg_snow',
) }}

select * from {{ ref('first_table') }}
"""

_MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH = """
{{
  config(
    transient = "transient",
    materialized = "dynamic_table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_root="root_path",
  )
}}

select * from {{ ref('first_table') }}
"""

_MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH_SUBPATH = """
{{
  config(
    transient = "true",
    materialized = "dynamic_table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_root="root_path",
    base_location_subpath='subpath',
  )
}}

select * from {{ ref('first_table') }}
"""


_MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_SUBPATH = """
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='DBT_TESTING',
    target_lag='1 minute',
    refresh_mode='INCREMENTAL',
    table_format='iceberg',
    external_volume='s3_iceberg_snow',
    base_location_subpath='subpath',
) }}

select * from {{ ref('first_table') }}
"""

_MODEL_BUILT_ON_ICEBERG_TABLE = """
{{
  config(
    materialized = "table",
  )
}}
select * from {{ ref('iceberg_table') }}
"""

_MODEL_TABLE_BEFORE_SWAP = """
{{
  config(
    materialized = "table",
  )
}}
select 1 as id
"""

_MODEL_VIEW_BEFORE_SWAP = """
select 1 as id
"""

_MODEL_TABLE_FOR_SWAP_ICEBERG = """
{{
  config(
    materialized = "table",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
  )
}}
select 1 as id
"""
