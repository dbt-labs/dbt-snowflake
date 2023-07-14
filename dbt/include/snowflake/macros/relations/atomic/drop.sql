{%- macro snowflake__drop_template(relation) -%}

    {%- if relation.type == 'dynamic_table' -%}
        {{ snowflake__drop_dynamic_table_template(relation) }}

    {%- else -%}
        {{- default__drop_template(relation) -}}

    {%- endif -%}

{%- endmacro -%}
