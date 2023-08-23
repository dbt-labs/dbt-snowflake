{% macro snowflake__get_drop_sql(relation) -%}
    {#-
    -- Remove materialized view
    -- Add dynamic table
    -#}

    {%- if relation.is_view -%}
        {{ drop_view(relation) }}

    {%- elif relation.is_table -%}
        {{ drop_table(relation) }}

    {%- elif relation.is_dynamic_table -%}
        {{ snowflake__get_drop_dynamic_table_sql(relation) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`get_drop_sql` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{% endmacro %}


{% macro snowflake__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        {{ get_drop_sql(relation) }}
    {%- endcall %}
{% endmacro %}
