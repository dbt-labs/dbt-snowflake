{% macro snowflake_get_build_snowpipe_sql(relation, columns) %}

    {{ snowflake_create_empty_table(relation, columns) }}

    {{ snowflake_get_copy_sql(relation, columns, explicit_transaction) }}

    {{ snowflake_create_snowpipe(relation, columns) }}

{% endmacro %}
