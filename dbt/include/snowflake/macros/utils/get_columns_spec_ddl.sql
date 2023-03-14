{% macro snowflake__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {%- set ns = namespace(at_least_one_check=False) -%}
  {%- set user_provided_columns = model['columns'] -%}
  (
  {% for i in user_provided_columns %}
    {%- set col = user_provided_columns[i] -%}
    {%- set constraints = col['constraints'] -%}
    {{ col['name'] }} {{ col['data_type'] }}
      {%- for x in constraints -%}
        {%- if x.type == "check" -%}
          {%- set ns.at_least_one_check = True -%}
        {%- else -%}
          {{ " not null" if x.type == "not_null" else " unique" if x.type == "unique" else " primary key" if x.type == "primary_key" else " foreign key" if x.type == "foreign key" else ""
 }}{{ x.expression or "" }}
        {%- endif -%}
      {%- endfor -%}
      {{ "," if not loop.last }}
  {% endfor -%}
  )
  {%- if ns.at_least_one_check -%}
    {{exceptions.warn("We noticed you have check constraints in your configs, these are NOT compatible with Snowflake and will be ignored")}}
  {%- endif %}
{% endmacro %}
