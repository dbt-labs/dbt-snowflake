{% macro optional(name, value, quote_char = '') %}
{% if value is not none %}{{ name }} = {{ quote_char }}{{ value }}{{ quote_char }}{% endif %}
{% endmacro %}
