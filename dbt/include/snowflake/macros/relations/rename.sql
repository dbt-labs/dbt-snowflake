{%- macro snowflake__get_rename_sql(relation, new_name) -%}
    {#-
    -- Remove materialized view
    -- Dynamic tables cannot be renamed
    -#}

    {%- if relation.is_view -%}
        {{ get_rename_view_sql(relation, new_name) }}

    {%- elif relation.is_table -%}
        {{ get_rename_table_sql(relation, new_name) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`get_rename_sql` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
    {% call statement('rename_relation') -%}
        {{ get_rename_sql(from_relation, to_relation.render()) }}
    {%- endcall %}
{% endmacro %}
