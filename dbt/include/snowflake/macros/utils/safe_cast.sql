{% macro snowflake__safe_cast(field, type) %}
  {#-- Dumb for now --#}
  {% if type|lower in ['array', 'variant'] and field is string %}
    cast(try_parse_json({{field}}) as {{type}})
  {% elif field is string %}
    try_cast({{field}} as {{type}})
  {% else %}
    cast({{field}} as {{type}})
  {% endif %}
{% endmacro %}
