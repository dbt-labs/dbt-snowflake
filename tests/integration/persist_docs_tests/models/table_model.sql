{{ config(materialized='table') }}
select 1 as id, 'Joe' as name, 1 as "order", 1 as "ORDER", 1 as "OrDer"
