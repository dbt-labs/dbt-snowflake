{% macro snowflake__safe_cast(field, type) %}
    {% if type|upper == "VARIANT" -%}
        {% set field_as_sting =  dbt.string_literal(field) if field is number else field %}
        try_cast({{field_as_sting}} as {{type}})
    {% elif type|upper == "GEOMETRY" -%}
        try_to_geometry({{field}})
    {% elif type|upper == "GEOGRAPHY" -%}
        try_to_geography({{field}})
    {% else -%}
        {{ adapter.dispatch('cast', 'dbt')(field, type) }}
    {% endif -%}
{% endmacro %}
