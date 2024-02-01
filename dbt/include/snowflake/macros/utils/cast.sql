{% macro snowflake__cast(field, type) %}
    {% if (type|upper == "GEOGRAPHY") -%}
        to_geography({{field}} as {{type}})
    {% elif (type|upper == "GEOMETRY") -%}
        to_geometry({{field}} as {{type}})
    {% else -%}
        cast({{field}} as {{type}})
    {% endif -%}
{% endmacro %}
