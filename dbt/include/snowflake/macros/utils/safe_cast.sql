{% macro snowflake__safe_cast(field, type) %}
    {% if (type|upper != "VARIANT") -%}
        {% set field_as_sting =  dbt.string_literal(field) if field is number else field %}
        try_cast({{field_as_sting}} as {{type}})
    {% else -%}
        cast({{field}} as {{type}})
    {% endif -%}
{% endmacro %}
