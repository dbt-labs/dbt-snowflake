drop table if exists {database}.{schema}.change_relation_type_tbl cascade;

create or replace table {database}.{schema}.change_relation_type_tbl as
(select '{{ var ("materialized") }}' as materialization );
