{% macro snowflake__describe_dynamic_table(relation) %}
{%- set _dynamic_table_sql -%}
show dynamic tables
    like '{{ relation.identifier }}'
    in schema {{ relation.database }}.{{ relation.schema }}
;
select
    "name",
    "schema_name",
    "database_name",
    "text",
    "target_lag",
    "warehouse",
    "refresh_mode"
from table(result_scan(last_query_id()))
{%- endset %}
{% set _dynamic_table = run_query(_dynamic_table_sql) %}

{% if adapter.behavior.enable_iceberg_materializations.no_warn %}
{%- set _catalog_sql -%}
show iceberg tables
    like '{{ relation.identifier }}'
    in schema {{ relation.database }}.{{ relation.schema }}
;
select
    "catalog_name",
    "external_volume_name",
    "base_location"
from table(result_scan(last_query_id()))
{%- endset %}
{% set _catalog = run_query(_catalog_sql) %}
{% else %}
{% set _catalog = none %}
{% endif %}

{% do return({'dynamic_table': _dynamic_table, 'catalog': _catalog}) %}
{% endmacro %}
