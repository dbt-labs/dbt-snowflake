{% macro snowflake__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {% if config.get('constraints_enabled', False) %}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns -%}
      {%- set col = user_provided_columns[i] -%}
      {% set constraints = col['constraints'] -%}
      {%- set check = col['check'] -%}
      {%- if check -%}
        {{exceptions.warn("We noticed you have `check` configs, these are NOT compatible with Snowflake and will be ignored")}}
      {%- endif %}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ x or "" }} {% endfor %} {% if check -%} {%- endif %} {{ "," if not loop.last }}
    {%- endfor %}
  )
  {% endif %}
{% endmacro %}
