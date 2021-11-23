{{ config(
    materialized = 'view',
    query_tag = var("query_tag") + '_view'
) }}

select 1 as id
