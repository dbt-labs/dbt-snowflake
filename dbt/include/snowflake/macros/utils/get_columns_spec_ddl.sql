{% macro snowflake__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {% if config.get('constraints_enabled', False) %}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns -%}
      {%- set col = user_provided_columns[i] -%}
      {% set constraints = col['constraint'] -%}
      {%- set check = col['check'] -%}
      {# do we want to raise a warning if there are "checks" as they are not supported in Snowflake #}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ x or "" }} {% endfor %} {% if check -%} check {{ check or "" }} {%- endif %} {{ "," if not loop.last }}
    {%- endfor %}
  )
  {% endif %}
{% endmacro %}
