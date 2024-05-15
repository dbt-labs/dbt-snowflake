{% macro snowflake__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

    {# {{ log('XXX: columns: ' ~ columns, info=True) }}
    {{ log('XXX: partitions: ' ~ columns, info=True) }}
    {% set partition_map = partitions|map(attribute='name')|join(', ') %}
    {{ log('XXX: partition_map: ' ~ partition_map, info=True) }} #}




    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}

  {%- set relation = api.Relation.create(
      database=source_node.database, schema=source_node.schema, identifier=source_node.name,
      type='external_table') -%}

{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}

    create or replace external table {{ relation }}
    (

    {%- for column in columns %}
        {{ log('column: ' ~ column.name, info=True) }}
        {%- set column_alias = column.name %}
        {%- set col_expression -%}
                {%- set col_id = 'value:c' ~ loop.index if is_csv else 'value:' ~ column_alias -%}
                (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
        {%- endset %}
        {{column_alias}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
        {{- ',' if not loop.last -}}
    {% endfor %}

    )
    location = {{external.location}} {# stage #}

    file_format = {{external.file_format}}

{% endmacro %}
