{% macro snowflake__current_timestamp() -%}
  convert_timezone('UTC', current_timestamp())
{%- endmacro %}

{% macro snowflake__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp_ntz('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro snowflake__snapshot_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}

{% macro snowflake__current_timestamp_backcompat() %}
    convert_timezone('UTC', {{current_timestamp()}})
{% endmacro %}