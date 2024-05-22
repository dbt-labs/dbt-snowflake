{% macro snowflake__create_external_table(relation, columns) %}

    {% set file_format = config.get('file_format') %}
    {% set location = config.get('location') %}
    {% set partitions = config.get('partitions') %}
    {% set partition_map = partitions|map(attribute='name')|join(', ') %}

    {%- set is_csv = is_csv(file_format) -%}


{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}

    create or replace external table {{ relation }}
    (

    {%- for column in columns %}
        {%- set column_alias = column.name %}
        {%- set col_expression -%}
                {%- set col_id = 'value:c' ~ loop.index if is_csv else 'value:' ~ column_alias -%}
                (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
        {%- endset %}
        {{column_alias}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
        {{- ',' if not loop.last -}}
    {% endfor %}

    )
    location = {{location}} {# stage #}

    file_format = {{file_format}}

{% endmacro %}
