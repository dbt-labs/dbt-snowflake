{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if (column_name|upper in column_dict) -%}
    {% set matched_column = column_name|upper -%}
  {% elif (column_name|lower in column_dict) -%}
    {% set matched_column = column_name|lower -%}
  {% elif (column_name in column_dict) -%}
    {% set matched_column = column_name -%}
  {% else -%}
    {% set matched_column = None -%}
  {% endif -%}
  {% if matched_column -%}
    {{ adapter.quote(column_name) }} COMMENT $${{ column_dict[matched_column]['description'] | replace('$', '[$]') }}$$
  {%- else -%}
    {{ adapter.quote(column_name) }} COMMENT $$$$
  {%- endif -%}
{% endmacro %}


{% macro snowflake__alter_relation_comment(relation, relation_comment) -%}
  comment on {{ relation.type }} {{ relation }} IS $${{ relation_comment | replace('$', '[$]') }}$$;
{% endmacro %}


{% macro snowflake__alter_column_comment(relation, column_dict) -%}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    alter {{ relation.type }} {{ relation }} alter
    {% for column_name in existing_columns if (column_name in existing_columns) or (column_name|lower in existing_columns) %}
        {{ get_column_comment_sql(column_name, column_dict) }} {{- ',' if not loop.last else ';' }}
    {% endfor %}
{% endmacro %}
