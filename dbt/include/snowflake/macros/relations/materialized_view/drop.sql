{% macro snowflake__get_drop_materialized__view_sql(relation) %}
    drop materialized view if exists {{ relation }}
{% endmacro %}
