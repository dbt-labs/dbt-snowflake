{%- macro snowflake__rename_template(relation, new_name) -%}

    {%- if relation.type == 'dynamic_table' -%}
        {{ snowflake__rename_dynamic_table_template(relation, new_name) }}

    {%- else -%}
        {{- default__rename_template(relation, new_name) -}}

    {%- endif -%}

{%- endmacro -%}
