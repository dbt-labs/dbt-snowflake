{% macro snowflake__get_replace_materialized_view_sql(relation, sql) %}
    {{ snowflake__create_materialized_view_as(relation, sql) }}
{% endmacro %}
