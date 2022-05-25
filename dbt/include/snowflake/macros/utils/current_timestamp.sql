{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{dbt_utils.current_timestamp()}})::{{dbt_utils.type_timestamp()}}
{% endmacro %}
