{{
    config(
        materialized='incremental',
        unique_key=['state', 'asdf']
    )
}}

select * from {{ ref('seed') }}
