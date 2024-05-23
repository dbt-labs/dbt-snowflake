{% macro snowflake_get_build_snowpipe_sql(relation) %}

    {{ snowflake_create_empty_table(relation) }}

    {{ snowflake_get_copy_sql(relation, explicit_transaction) }}

    {{ snowflake_create_snowpipe(relation) }}

{% endmacro %}
