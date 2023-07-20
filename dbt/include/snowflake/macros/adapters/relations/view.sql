{% macro snowflake__create_view_as(relation, sql) -%}
  {{ snowflake__create_view_as_with_temp_flag(relation, sql) }}
{% endmacro %}


{% macro snowflake__create_view_as_with_temp_flag(relation, sql, is_temporary=False) -%}
  {%- set secure = config.get('secure', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create or replace {% if secure -%}
    secure
  {%- endif %} {% if is_temporary -%}
    temporary
  {%- endif %} view {{ relation }}
  {% if config.persist_column_docs() -%}
    {% set model_columns = model.columns %}
    {% set query_columns = get_columns_in_query(sql) %}
    {{ get_persist_docs_column_list(model_columns, query_columns) }}

  {%- endif %}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {% if copy_grants -%} copy grants {%- endif %} as (
    {{ sql }}
  );
{% endmacro %}


{% macro get_persist_docs_column_list(model_columns, query_columns) %}
(
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
)
{% endmacro %}
