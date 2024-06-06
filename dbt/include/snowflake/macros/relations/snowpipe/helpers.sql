{% macro snowflake_create_empty_table(relation, columns) %}

    create or replace table {{ relation }} (
        {% if columns|length == 0 %}
            value variant,
        {% else -%}
        {%- for column in columns -%}
            {{column.name}} {{column.data_type}},
        {% endfor -%}
        {% endif %}
            metadata_filename varchar,
            metadata_file_row_number bigint,
            metadata_file_last_modified timestamp,
            _dbt_copied_at timestamp
    );

{% endmacro %}

{% macro snowflake_get_copy_sql(relation, columns, explicit_transaction=false) %}
{# This assumes you have already created an external stage #}

    {% set location = config.get('location') %}
    {% set file_format = config.get('file_format') %}

    {% set pattern = config.get('pattern') %}
    {%- set is_csv = is_csv(file_format) %}

    {% set snowpipe = config.get('snowpipe', none) %}
    {%- set copy_options = snowpipe.get('copy_options', none) -%}

    {%- if explicit_transaction -%} begin; {%- endif %}

    copy into {{ relation }}
    from (
        select
        {% if columns|length == 0 %}
            $1::variant as value,
        {% else -%}
        {%- for column in columns -%}
            {%- set col_expression -%}
                {%- if is_csv -%}nullif(${{loop.index}},''){# special case: get columns by ordinal position #}
                {%- else -%}nullif($1:{{column.name}},''){# standard behavior: get columns by name #}
                {%- endif -%}
            {%- endset -%}
            {{col_expression}}::{{column.data_type}} as {{column.name}},
        {% endfor -%}
        {% endif %}
            metadata$filename::varchar as metadata_filename,
            metadata$file_row_number::bigint as metadata_file_row_number,
            metadata$file_last_modified::timestamp as metadata_file_last_modified,
            metadata$start_scan_time::timestamp as _dbt_copied_at
        from {{location}} {# stage #}
    )
    file_format = {{file_format}}
    {% if pattern -%} pattern = '{{pattern}}' {%- endif %}
    {% if copy_options %} {{copy_options}} {% endif %};

    {% if explicit_transaction -%} commit; {%- endif -%}

{% endmacro %}


{% macro snowflake_create_snowpipe(relation, columns) %}

    {% set snowpipe = config.get('snowpipe', none) %}

{# https://docs.snowflake.com/en/sql-reference/sql/create-pipe.html #}
    create or replace pipe {{ relation }}
        {% if snowpipe.auto_ingest -%} auto_ingest = {{snowpipe.auto_ingest}} {%- endif %}
        {% if snowpipe.aws_sns_topic -%} aws_sns_topic = '{{snowpipe.aws_sns_topic}}' {%- endif %}
        {% if snowpipe.integration -%} integration = '{{snowpipe.integration}}' {%- endif %}
        {% if snowpipe.error_integration -%} error_integration = '{{snowpipe.error_integration}}' {%- endif %}
        as {{ snowflake_get_copy_sql(relation, columns) }}

{% endmacro %}

{% macro snowflake_refresh_snowpipe(relation) %}

    {% set snowpipe = config.get('snowpipe', none) %}
    {% set auto_ingest = snowpipe.get('auto_ingest', false) if snowpipe is mapping %}

    {% if auto_ingest is true %}

        {% do return([]) %}

    {% else %}

        {% set ddl %}
        alter pipe {{ relation }} refresh
        {% endset %}

        {{ return([ddl]) }}

    {% endif %}

{% endmacro %}
