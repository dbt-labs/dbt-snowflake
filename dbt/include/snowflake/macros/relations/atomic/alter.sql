{%- macro snowflake__alter_template(existing_relation, target_relation) -%}

    {%- if existing_relation.type == 'dynamic_table' -%}
        {{ snowflake__alter_dynamic_table_template(existing_relation, target_relation) }}

    {%- else -%}
        {{- default__alter_template(existing_relation, target_relation) -}}

    {%- endif -%}

{%- endmacro -%}
