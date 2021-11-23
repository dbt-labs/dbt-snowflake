{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    query_tag = var("query_tag") + '_incremental'
) }}

select 1 as id
