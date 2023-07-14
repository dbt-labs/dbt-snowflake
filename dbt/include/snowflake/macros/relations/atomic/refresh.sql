{%- macro snowflake__refresh_template(relation) -%}

    {%- if relation.type == 'dynamic_table' -%}
        {{ snowflake__refresh_dynamic_table_template(relation) }}

    {%- else -%}
        {{- default__refresh_template(relation) -}}

    {%- endif -%}

{%- endmacro -%}
