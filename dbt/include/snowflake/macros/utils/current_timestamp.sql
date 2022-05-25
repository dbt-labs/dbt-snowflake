{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{dbt.current_timestamp()}})::{{dbt.type_timestamp()}}
{% endmacro %}
