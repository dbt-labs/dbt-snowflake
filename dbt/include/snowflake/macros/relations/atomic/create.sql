{%- macro snowflake__create_template(relation) -%}

    {%- if relation.type == 'dynamic_table' -%}
        {{ snowflake__create_dynamic_table_template(relation) }}

    {%- else -%}
        {{- default__create_template(relation) -}}

    {%- endif -%}

{%- endmacro -%}
