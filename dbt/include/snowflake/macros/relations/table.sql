{% macro snowflake__drop_table_template(table) %}
    drop table if exists {{ table.fully_qualified_path }} cascade
{% endmacro %}


{%- macro snowflake__rename_table_template(table, new_name) -%}
    alter table {{ table.fully_qualified_path }} rename to {{ new_name }}
{%- endmacro -%}
