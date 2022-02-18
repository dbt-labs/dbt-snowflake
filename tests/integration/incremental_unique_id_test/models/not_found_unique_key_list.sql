-- a unique key list with any element not in the model itself should error out

{{
    config(
        materialized='incremental',
        unique_key=['state', 'asdf']
    )
}}

select * from {{ ref('seed') }}
