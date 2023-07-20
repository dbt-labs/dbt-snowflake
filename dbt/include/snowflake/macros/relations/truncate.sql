{% macro snowflake__truncate_relation(relation) -%}
  {% set truncate_dml %}
    truncate table {{ relation }}
  {% endset %}
  {% call statement('truncate_relation') -%}
    {{ snowflake_dml_explicit_transaction(truncate_dml) }}
  {%- endcall %}
{% endmacro %}
