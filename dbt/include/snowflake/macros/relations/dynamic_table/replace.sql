{% macro snowflake__get_replace_dynamic_table_sql(relation, sql) -%}

    {%- set dynamic_table = relation.from_config(config.model) -%}
    {%- set iceberg = config.get('object_format', default='') == 'iceberg' -%}

    {# Configure for extended Object Format #}
    {% if iceberg %}
      {%- set object_format = 'iceberg' -%}
    {%- else -%}
      {%- set object_format = '' -%}
    {%- endif -%}

    create or replace dynamic {{ object_format }} table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {%- if iceberg %}
          external_volume = {{ config.get('external_volume') }}
          catalog = 'snowflake'
          base_location = {{ config.get('base_location') }}
        {%- else -%}
          {% if dynamic_table.refresh_mode %}
          refresh_mode = {{ dynamic_table.refresh_mode }}
          {% endif %}
          {% if dynamic_table.initialize %}
          initialize = {{ dynamic_table.initialize }}
          {% endif %}
        {%- endif %}
        as (
            {{ sql }}
        )

{%- endmacro %}
