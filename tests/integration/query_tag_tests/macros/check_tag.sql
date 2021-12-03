{% macro check_query_tag() %}

  {% if execute %}
    {% set query_tag = get_current_query_tag() %}
    {% if query_tag != var("query_tag") %}
      {{ exceptions.raise_compiler_error("Query tag not used!") }}
    {% endif %}
  {% endif %}

{% endmacro %}
