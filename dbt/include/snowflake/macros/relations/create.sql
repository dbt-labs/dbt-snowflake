{% macro snowflake__get_create_sql(relation, sql) %}
    {#-
    -- Remove materialized view
    -- Add dynamic table
    -#}

    {% if relation.is_view %}
        {{ get_create_view_as_sql(relation, sql) }}

    {% elif relation.is_table %}
        {{ get_create_table_as_sql(False, relation, sql) }}

    {% elif relation.is_dynamic_table %}
        {{ snowflake__get_create_dynamic_table_as_sql(relation, sql) }}

    {% else %}
        {{- exceptions.raise_compiler_error("`get_create_sql` has not been implemented for: " ~ relation.type ) -}}

    {% endif %}

{% endmacro %}
