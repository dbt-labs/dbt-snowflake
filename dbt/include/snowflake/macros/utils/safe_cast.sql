{% macro snowflake__safe_cast(field, type) %}
    {% set field_as_sting =  dbt.string_literal(field) if field is number else field %}
    try_cast({{field_as_sting}} as {{type}})
{% endmacro %}
