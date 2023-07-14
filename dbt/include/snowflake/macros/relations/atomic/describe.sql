{%- macro snowflake__describe_template(relation) -%}

    {%- if relation.type == 'dynamic_table' -%}
        {% do return(snowflake__describe_dynamic_table_template(relation)) %}

    {%- else -%}
        {% do return(default__describe_template(relation)) %}

    {%- endif -%}

{%- endmacro -%}
