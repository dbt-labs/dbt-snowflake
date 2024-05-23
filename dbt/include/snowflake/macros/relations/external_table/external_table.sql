{% macro snowflake__create_external_table(relation, columns) %}

    {% set file_format = config.get('file_format') %}
    {% set location = config.get('location') %}
    {% set partitions = config.get('partitions') %}
    {% set partition_map = partitions|map(attribute='name')|join(', ') %}

    {%- set is_csv = is_csv(file_format) -%}


{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}

    create or replace external table {{ relation }}
    {%- if columns or partitions or infer_schema -%}
    (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if not loop.last or columns|length > 0 or infer_schema -}}
        {%- endfor -%}{%- endif -%}
        {%- if not infer_schema -%}
            {%- for column in columns %}
                {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
                {%- set column_alias -%}
                    {%- if 'alias' in column and column.quote -%}
                        {{adapter.quote(column.alias)}}
                    {%- elif 'alias' in column -%}
                        {{column.alias}}
                    {%- else -%}
                        {{column_quoted}}
                    {%- endif -%}
                {%- endset %}
                {%- set col_expression -%}
                    {%- if column.expression -%}
                        {{column.expression}}
                    {%- else -%}
                        {%- set col_id = 'value:c' ~ loop.index if is_csv else 'value:' ~ column_alias -%}
                        (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
                    {%- endif -%}
                {%- endset %}
                {{column_alias}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
                {{- ',' if not loop.last -}}
            {% endfor %}
        {% else %}
        {%- for column in columns_infer %}
                {%- set col_expression -%}
                    {%- set col_id = 'value:' ~ column[0] -%}
                    (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
                {%- endset %}
                {{column[0]}} {{column[1]}} as ({{col_expression}}::{{column[1]}})
                {{- ',' if not loop.last -}}
            {% endfor %}
        {%- endif -%}
    )
    {%- endif -%}
    {% if partitions %} partition by ({{partitions|map(attribute='name')|join(', ')}}) {% endif %}
    location = {{location}} {# stage #}
    {% if auto_refresh in (true, false) -%}
      auto_refresh = {{auto_refresh}}
    {%- endif %}
    {% if aws_sns_topic -%}
      aws_sns_topic = '{{aws_sns_topic}}'
    {%- endif %}
    {% if table_format | lower == "delta" %}
      refresh_on_create = false
    {% endif %}
    {% if pattern -%} pattern = '{{pattern}}' {%- endif %}
    {% if integration -%} integration = '{{integration}}' {%- endif %}
    file_format = {{file_format}}
    {% if table_format -%} table_format = '{{table_format}}' {%- endif %}

{% endmacro %}
