{{ config(materialized='view') }}
select 2 as id, 'Bob' as name, 1 as "order", 1 as "ORDER", 1 as "OrDer", cast(1 as text)
