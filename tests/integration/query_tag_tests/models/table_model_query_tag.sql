{{ config(
    materialized = 'table',
    query_tag = var("query_tag") + '_table'
) }}

select 1 as id
