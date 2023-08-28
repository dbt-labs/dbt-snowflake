{% macro snowflake__describe_dynamic_table(relation) %}
    {%- set _dynamic_table_sql -%}
        show dynamic tables
            like '{{ relation.identifier }}'
            in schema {{ relation.database }}.{{ relation.schema }}
        ;
        select
            "name",
            "schema_name",
            "database_name",
            "text",
            "target_lag",
            "warehouse"
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set _dynamic_table = run_query(_dynamic_table_sql) %}

    {% do return({'dynamic_table': _dynamic_table}) %}
{% endmacro %}
