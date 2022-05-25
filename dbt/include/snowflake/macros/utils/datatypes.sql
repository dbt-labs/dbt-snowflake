{% macro snowflake__type_string() %}
    varchar
{% endmacro %}

{% macro snowflake__type_timestamp() %}
    timestamp_ntz
{% endmacro %}
